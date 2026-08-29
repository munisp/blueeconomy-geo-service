// Phase-6 remediation regression tests. Same infrastructure gates as
// pipeline_integration_test.go.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// TestStaticOutOfOrder proves a delayed AIS type-5/24 report whose
// observed_at predates the current SCD-2 row is dropped without touching
// the open row, while in-order reports still rotate versions.
func TestStaticOutOfOrder(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	report := store.StaticReport{
		StaticReportID: "itest-sta-1",
		MMSI:           "000001003",
		IMO:            "1234567",
		ShipName:       "ITEST CURRENT",
		ShipTypeCode:   70,
		SourceClass:    "AIS",
		Classification: "PUBLIC",
		ObservedAt:     base,
	}
	applied, err := h.store.UpsertVesselStatic(ctx, report)
	require.NoError(t, err)
	require.True(t, applied)

	// Out-of-order: one hour older, different payload. Must be dropped
	// (applied=false, no error) and leave the open row untouched.
	stale := report
	stale.StaticReportID = "itest-sta-0"
	stale.ShipName = "ITEST STALE"
	stale.ObservedAt = base.Add(-time.Hour)
	applied, err = h.store.UpsertVesselStatic(ctx, stale)
	require.NoError(t, err)
	require.False(t, applied, "stale static report must be dropped")

	var name string
	var validTo *time.Time
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT ship_name, valid_to FROM vessels_static WHERE mmsi = '000001003' AND valid_to IS NULL`).
		Scan(&name, &validTo))
	require.Equal(t, "ITEST CURRENT", name, "current row must be untouched")
	require.Nil(t, validTo)
	var rows int
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM vessels_static WHERE mmsi = '000001003'`).Scan(&rows))
	require.Equal(t, 1, rows, "no historical version may be opened for a stale report")

	// In-order: closes the current row (valid_to > valid_from holds) and
	// opens the new version.
	newer := report
	newer.StaticReportID = "itest-sta-2"
	newer.ShipName = "ITEST RENAMED"
	newer.ObservedAt = base.Add(time.Hour)
	applied, err = h.store.UpsertVesselStatic(ctx, newer)
	require.NoError(t, err)
	require.True(t, applied)
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT ship_name FROM vessels_static WHERE mmsi = '000001003' AND valid_to IS NULL`).Scan(&name))
	require.Equal(t, "ITEST RENAMED", name)
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM vessels_static WHERE mmsi = '000001003'`).Scan(&rows))
	require.Equal(t, 2, rows, "in-order rotation keeps one closed + one open row")
}
