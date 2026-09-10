package mrv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllocateOverlap(t *testing.T) {
	day := 24 * time.Hour
	pFrom := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	pTo := pFrom.Add(day) // report period: Jan 10
	// Voyage covers exactly half the report period → half the fuel.
	wFrom := pFrom.Add(12 * time.Hour)
	wTo := pTo
	require.Equal(t, uint64(500_000), allocateOverlap(pFrom, pTo, wFrom, wTo, 1_000_000))
	// Full containment → everything.
	require.Equal(t, uint64(1_000_000), allocateOverlap(pFrom, pTo, pFrom, pTo, 1_000_000))
	// No overlap → zero.
	require.Equal(t, uint64(0), allocateOverlap(pFrom, pTo, pTo, pTo.Add(day), 1_000_000))
	require.Equal(t, uint64(0), allocateOverlap(pFrom, pTo, pFrom.Add(-day), pFrom, 1_000_000))
	// Degenerate period → zero (fail closed, never divide by zero).
	require.Equal(t, uint64(0), allocateOverlap(pFrom, pFrom, pFrom, pTo, 1_000_000))
	// Partial 25% overlap.
	require.Equal(t, uint64(250_000), allocateOverlap(pFrom, pTo, pFrom.Add(-day), pFrom.Add(6*time.Hour), 1_000_000))
}

func TestVoyageEmissionsContractRegistered(t *testing.T) {
	// The outbox drain fails closed on unregistered event types; the
	// per-voyage event must be wired to its topic and contract resource.
	contract, ok := eventContracts[EventVoyageEmissions]
	require.True(t, ok, "mrv.voyage-emissions.v1 must be registered")
	require.Equal(t, "mrv.voyage-emissions", contract.topic)
	require.Equal(t, "MrvVoyageEmissionsComputed", contract.resourceType)
	require.Equal(t, "CONFIDENTIAL", contract.classification)
}
