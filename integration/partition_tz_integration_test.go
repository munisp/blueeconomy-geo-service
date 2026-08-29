// PRA-128 regression: geo_ensure_position_partition must bind daily
// partition bounds to the UTC calendar day regardless of the session
// TimeZone. Before 0011_partition_tz_utc.sql the function cast
// day::timestamptz in the session TZ, misrouting rows on non-UTC servers.
// This test sets the session TZ to Asia/Shanghai (UTC+8) and proves the
// created bounds and the row routing stay UTC.
package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

func TestPartitionBoundsAreUTCRegardlessOfSessionTZ(t *testing.T) {
	dsn := os.Getenv("GEO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("GEO_TEST_PG_DSN is required for integration tests")
	}
	ctx := context.Background()

	// Migrate (0011 recreates the function UTC-explicitly).
	migrator, err := store.New(ctx, dsn, dsn)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(ctx, migrator, db.MigrationsFS))
	migrator.Close()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// A day with no pre-existing partition, far from the migration date.
	day := "2037-03-04"
	partition := "ais_positions_20370304"
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+partition)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+partition)
	})

	// Provision the partition from a session pinned to a non-UTC timezone.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SET LOCAL TIME ZONE 'Asia/Shanghai'`)
	require.NoError(t, err)
	var created string
	require.NoError(t, tx.QueryRow(ctx, `SELECT geo_ensure_position_partition($1::date)`, day).Scan(&created))
	require.Equal(t, partition, created)
	require.NoError(t, tx.Commit(ctx))

	// The partition bounds must be the UTC calendar day — under the old
	// session-TZ cast they would have been 2037-03-03/04 16:00:00+00.
	var bounds string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT pg_get_expr(relpartbound, relfilenode) FROM pg_class WHERE relname = $1`,
		partition).Scan(&bounds))
	require.Contains(t, bounds, "2037-03-04 00:00:00+00", "lower bound must be UTC midnight, got: %s", bounds)
	require.Contains(t, bounds, "2037-03-05 00:00:00+00", "upper bound must be UTC midnight, got: %s", bounds)

	// Rows in the first post-midnight UTC hour route to the UTC-day
	// partition even when inserted from a non-UTC session.
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SET LOCAL TIME ZONE 'Asia/Shanghai'`)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO ais_positions (
		position_report_id, mmsi, source_class, geom, latitude_micros, longitude_micros,
		position_accuracy, receiver_id, classification, observed_at)
		VALUES ('itest-tz-1', '000001009', 'AIS', ST_GeogFromText('POINT(3.3725 6.418)'),
		6418000, 3372500, 'HIGH', 'itest-rx', 'PUBLIC', $1)`,
		time.Date(2037, 3, 4, 0, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var routed string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT tableoid::regclass::text FROM ais_positions WHERE position_report_id = 'itest-tz-1'`).Scan(&routed))
	require.Equal(t, partition, routed, "00:30 UTC must route to the UTC-day partition")
}
