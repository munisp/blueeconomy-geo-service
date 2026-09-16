package connectors

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPCSCoordinateConversion(t *testing.T) {
	// Double degrees → fixed-point micro-degrees at the import boundary.
	require.Equal(t, int32(-4_062_000), microsFromDegrees(-4.062))
	require.Equal(t, int32(39_672_000), microsFromDegrees(39.672))
	require.Equal(t, int32(0), microsFromDegrees(0))
	require.Equal(t, uint32(10_500), milliknotsFromKnots(10.5))
	require.Equal(t, uint32(359_900), millidegreesFromDegrees(359.9))
}

func TestPCSImporterValidate(t *testing.T) {
	importer := &PCSImporter{}
	require.Error(t, importer.Validate())
	importer.DSN = "postgres://pcs"
	require.Error(t, importer.Validate()) // no pipeline
}

// TestPCSWatermarkTieGroupNoLoss is the H1 regression: when more rows than
// BatchSize share one message_ts (the source PK is (mmsi, message_ts)), the
// composite (message_ts, mmsi) cursor must page through the whole tie group
// without dropping or double-consuming a single row. It simulates the poll
// SQL semantics using the same predicate the query declares.
func TestPCSWatermarkTieGroupNoLoss(t *testing.T) {
	require.Contains(t, pcsPollQuery, "message_ts = $1 AND mmsi > $2")
	require.Contains(t, pcsPollQuery, "ORDER BY message_ts ASC, mmsi ASC")

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	// 7 rows at one timestamp + 3 at the next, unordered mmsi values.
	var source []pcsAISRow
	for i, mmsi := range []string{"636019825", "205123000", "477156300", "311000942", "228001234", "538004321", "412999888"} {
		source = append(source, pcsAISRow{MMSI: mmsi, MessageTS: base, Latitude: float64(i)})
	}
	for i, mmsi := range []string{"205123000", "477156300", "636019825"} {
		source = append(source, pcsAISRow{MMSI: mmsi, MessageTS: base.Add(time.Second), Latitude: float64(100 + i)})
	}

	// page mirrors the SQL: WHERE strictlyAfter(cursor) ORDER BY (ts,mmsi) LIMIT n.
	page := func(cursor pcsCursor, limit int) []pcsAISRow {
		var out []pcsAISRow
		for _, row := range source {
			if cursor.strictlyAfter(row.MessageTS, row.MMSI) {
				out = append(out, row)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].MessageTS.Equal(out[j].MessageTS) {
				return out[i].MessageTS.Before(out[j].MessageTS)
			}
			return out[i].MMSI < out[j].MMSI
		})
		if len(out) > limit {
			out = out[:limit]
		}
		return out
	}

	cursor := pcsCursor{ts: base.Add(-time.Minute)}
	consumed := map[string]int{} // payload identity mmsi@ts -> count
	batch := 4                   // < tie-group size: boundary cuts inside ties
	for pages := 0; pages < 10; pages++ {
		rows := page(cursor, batch)
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			key := fmt.Sprintf("%s@%d", row.MMSI, row.MessageTS.Unix())
			consumed[key]++
		}
		last := rows[len(rows)-1]
		cursor = pcsCursor{ts: last.MessageTS.UTC(), mmsi: last.MMSI}
	}
	require.Len(t, consumed, len(source), "every source row consumed exactly once across tie groups")
	for key, count := range consumed {
		require.Equal(t, 1, count, "row %s consumed %d times", key, count)
	}
}

func TestPCSCursorStrictlyAfter(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	cursor := pcsCursor{ts: ts, mmsi: "205123000"}
	require.False(t, cursor.strictlyAfter(ts, "205123000"))      // the cursor row itself
	require.False(t, cursor.strictlyAfter(ts, "111111111"))      // same ts, earlier mmsi
	require.True(t, cursor.strictlyAfter(ts, "636019825"))       // same ts, later mmsi
	require.True(t, cursor.strictlyAfter(ts.Add(time.Second), "111111111"))
	require.False(t, cursor.strictlyAfter(ts.Add(-time.Second), "999999999"))
}
