package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DensityCellRow is one occupied grid cell of the vessel-density aggregate.
// Cells are integer micro-degree squares over latest_positions; only cells
// with at least one recorded vessel are returned — empty cells are omitted
// (never zero-filled by the server; the client renders absence honestly).
type DensityCellRow struct {
	// CellLatMicros/CellLonMicros are the south-west corner of the cell.
	CellLatMicros int32 `json:"cellLatMicros"`
	CellLonMicros int32 `json:"cellLonMicros"`
	VesselCount   int   `json:"vesselCount"`
	// LatestObservedAt is the freshest position inside the cell.
	LatestObservedAt time.Time `json:"latestObservedAt"`
}

// MaxDensityCells bounds one density query (fail-closed guard against
// full-world scans at fine cell sizes).
const MaxDensityCells = 250_000

// DensityGrid aggregates latest_positions into an integer micro-degree grid
// clipped to the bbox, clearance-filtered. cellSizeMicros must be positive.
func (store *Store) DensityGrid(ctx context.Context, minLonMicros, minLatMicros, maxLonMicros, maxLatMicros int32, cellSizeMicros int64, clearedLabels []string) ([]DensityCellRow, time.Time, error) {
	if cellSizeMicros <= 0 {
		return nil, time.Time{}, errors.New("cell size must be positive micro-degrees")
	}
	// Fail closed on degenerate grids before touching the database.
	cellsLat := (int64(maxLatMicros)-int64(minLatMicros))/cellSizeMicros + 1
	cellsLon := (int64(maxLonMicros)-int64(minLonMicros))/cellSizeMicros + 1
	if cellsLat*cellsLon > MaxDensityCells {
		return nil, time.Time{}, fmt.Errorf("bbox at this cell size yields %d cells (max %d): enlarge the cell or shrink the bbox", cellsLat*cellsLon, MaxDensityCells)
	}
	rows, err := store.pool.Query(ctx, `SELECT
		(floor(l.latitude_micros::float8 / $6) * $6)::bigint,
		(floor(l.longitude_micros::float8 / $6) * $6)::bigint,
		count(*), max(l.observed_at)
		FROM latest_positions l
		WHERE l.classification = ANY($1)
		  AND ST_Intersects(l.geom, ST_MakeEnvelope($2, $3, $4, $5, 4326)::geography)
		GROUP BY 1, 2`,
		clearedLabels,
		float64(minLonMicros)/1e6, float64(minLatMicros)/1e6,
		float64(maxLonMicros)/1e6, float64(maxLatMicros)/1e6, cellSizeMicros)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("density grid: %w", err)
	}
	defer rows.Close()
	cells := make([]DensityCellRow, 0)
	var asOf time.Time
	for rows.Next() {
		var latIdx, lonIdx int64
		var cell DensityCellRow
		if err := rows.Scan(&latIdx, &lonIdx, &cell.VesselCount, &cell.LatestObservedAt); err != nil {
			return nil, time.Time{}, fmt.Errorf("scan density cell: %w", err)
		}
		cell.CellLatMicros = int32(latIdx)
		cell.CellLonMicros = int32(lonIdx)
		if cell.LatestObservedAt.After(asOf) {
			asOf = cell.LatestObservedAt
		}
		cells = append(cells, cell)
	}
	return cells, asOf, rows.Err()
}
