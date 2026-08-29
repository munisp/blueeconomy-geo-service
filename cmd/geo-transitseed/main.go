// geo-transitseed loads the operator-maintained transit registry seed
// document (YAML or JSON) into the tenant-scoped registry tables
// (migration 0009). The registry is the source of truth the GTFS static
// feed factory, the GTFS-RT producer and the ETA engine consume.
//
// Usage:
//
//	GEO_PG_DSN=postgres://geo:...@host:5432/geo \
//	GEO_INGEST_PG_DSN=postgres://geo_ingest:...@host:5432/geo \
//	geo-transitseed -tenant niwa -file registry.yaml
//
// The load is idempotent (upserts) and fails closed on the first defect.
// See fixtures/transit_seed.example.yaml for the documented format.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

func main() {
	tenant := flag.String("tenant", "", "tenant the registry belongs to (required)")
	file := flag.String("file", "", "seed document path, YAML or JSON (required)")
	flag.Parse()
	if strings.TrimSpace(*tenant) == "" || strings.TrimSpace(*file) == "" {
		fmt.Fprintln(os.Stderr, "usage: geo-transitseed -tenant TENANT -file seed.yaml")
		os.Exit(2)
	}
	dsn := strings.TrimSpace(os.Getenv("GEO_PG_DSN"))
	ingestDSN := strings.TrimSpace(os.Getenv("GEO_INGEST_PG_DSN"))
	if dsn == "" || ingestDSN == "" {
		fmt.Fprintln(os.Stderr, "GEO_PG_DSN and GEO_INGEST_PG_DSN are required")
		os.Exit(2)
	}
	document, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read seed: %v\n", err)
		os.Exit(1)
	}
	seed, err := store.ParseTransitSeed(document)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	ctx := context.Background()
	storage, err := store.New(ctx, dsn, ingestDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer storage.Close()
	if err := storage.SeedTransitRegistry(ctx, *tenant, seed); err != nil {
		fmt.Fprintf(os.Stderr, "seed failed (no partial success guarantee beyond upsert idempotency): %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("seeded transit registry for tenant %q: %d agencies, %d routes, %d stops, %d calendars, %d trips, %d stop-time groups, %d assignments\n",
		*tenant, len(seed.Agencies), len(seed.Routes), len(seed.Stops), len(seed.Calendars),
		len(seed.Trips), len(seed.StopTimes), len(seed.Assignments))
}
