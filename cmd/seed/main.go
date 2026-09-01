// Command seed loads deterministic, idempotent demo data into the
// blueeconomy-geo-service database.
//
// Safety doctrine:
//   - REFUSES to run when ENV=production or PROFILE=prod.
//   - Requires explicit SEED_DEMO=true.
//   - All statements are INSERT ... ON CONFLICT DO NOTHING with deterministic
//     identifiers, so repeated runs converge to the same state.
package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed seed.sql
var seedSQL string

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run() error {
	env := strings.ToLower(os.Getenv("ENV"))
	profile := strings.ToLower(os.Getenv("PROFILE"))
	if env == "production" || profile == "prod" {
		return fmt.Errorf("refusing to seed: ENV=%q PROFILE=%q (production gate)", os.Getenv("ENV"), os.Getenv("PROFILE"))
	}
	if strings.ToLower(os.Getenv("SEED_DEMO")) != "true" {
		return fmt.Errorf("refusing to seed: set SEED_DEMO=true to acknowledge loading synthetic demo data")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, seedSQL); err != nil {
		return fmt.Errorf("apply seed: %w", err)
	}
	var tables, rows int
	if err := conn.QueryRow(ctx, `
		SELECT count(*), 0 FROM pg_tables WHERE schemaname='public'`).Scan(&tables, &rows); err != nil {
		return err
	}
	fmt.Printf("seed applied idempotently: %d tables present; all demo rows are synthetic Nigerian maritime data\n", tables)
	return nil
}
