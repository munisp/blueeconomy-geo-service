package gtfsrt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func micros(degrees float64) int32 {
	return int32(degrees * 1_000_000)
}

func TestHaversineKnownDistance(t *testing.T) {
	// One degree of latitude ≈ 111.195 km.
	distance := haversineMeters(cord(0, 0), cord(0, 0.01))
	require.InDelta(t, 1111.95, distance, 2.0)
}

func cord(lat, lon float64) Coord {
	return Coord{LatitudeMicros: micros(lat), LongitudeMicros: micros(lon)}
}

func TestSnapStraightRouteMidpoint(t *testing.T) {
	// Straight route: (0,0) → (0, 0.01) → (0, 0.02), vessel on the path
	// halfway between stop 1 and stop 2.
	path := NewRoutePath([]Coord{cord(0, 0), cord(0, 0.01), cord(0, 0.02)})
	require.InDelta(t, 2223.9, path.Total, 4.0)
	progress, offRoute := path.Snap(cord(0, 0.005))
	require.InDelta(t, 555.97, progress, 3.0) // halfway along leg 1
	require.InDelta(t, 0, offRoute, 0.5)
	progress, _ = path.Snap(cord(0, 0.01))
	require.InDelta(t, 1111.95, progress, 3.0) // exactly at stop 2
}

func TestSnapOffRouteDistance(t *testing.T) {
	path := NewRoutePath([]Coord{cord(0, 0), cord(0, 0.01)})
	// Vessel 0.001° east of the midpoint (~111 m off route at equator).
	_, offRoute := path.Snap(cord(0.001, 0.005))
	require.InDelta(t, 111.19, offRoute, 1.0)
}

func TestMedianMilliknots(t *testing.T) {
	median, ok := MedianMilliknots([]uint32{9000, 11000, 10000, 10500, 9500})
	require.True(t, ok)
	require.InDelta(t, 10000, median, 0.01)
	median, ok = MedianMilliknots([]uint32{8000, 12000})
	require.True(t, ok)
	require.InDelta(t, 10000, median, 0.01)
	_, ok = MedianMilliknots(nil)
	require.False(t, ok, "no observations must be reported honestly")
}

func TestComputeETAsRemainingStops(t *testing.T) {
	path := NewRoutePath([]Coord{cord(0, 0), cord(0, 0.01), cord(0, 0.02)})
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	// Vessel 1 km along at 5.144 m/s (10 kn): stop 2 is ~112 m ahead,
	// stop 3 ~1224 m; the first stop is passed and must not be predicted.
	etas := path.ComputeETAs(1000, 5.144, now)
	require.Len(t, etas, 2, "the passed stop must not be predicted")
	require.Equal(t, 1, etas[0].StopIndex)
	require.InDelta(t, 111.95, etas[0].RemainingMeters, 3.0)
	require.InDelta(t, (111.95 / 5.144), etas[0].ETA.Sub(now).Seconds(), 2.0)
	require.InDelta(t, (1223.9 / 5.144), etas[1].ETA.Sub(now).Seconds(), 3.0)
}

func TestComputeETAsRejectsZeroSpeed(t *testing.T) {
	path := NewRoutePath([]Coord{cord(0, 0), cord(0, 0.01)})
	require.Empty(t, path.ComputeETAs(0, 0, time.Now()),
		"zero speed must never produce a fabricated ETA")
}

func TestMatchTrip(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	windows := []TripWindow{
		{TripID: "early", FirstDeparture: now.Add(-3 * time.Hour), LastArrival: now.Add(-2 * time.Hour)},
		{TripID: "current", FirstDeparture: now.Add(-30 * time.Minute), LastArrival: now.Add(30 * time.Minute)},
		{TripID: "next", FirstDeparture: now.Add(10 * time.Minute), LastArrival: now.Add(time.Hour)},
		{TripID: "later", FirstDeparture: now.Add(2 * time.Hour), LastArrival: now.Add(3 * time.Hour)},
	}
	matched, ok := MatchTrip(windows, now, 15*time.Minute, 30*time.Minute)
	require.True(t, ok)
	require.Equal(t, "next", matched.TripID,
		"within overlapping slack windows the most recently (about-to-be) started trip wins")
	// Far outside every window: no match, no guess.
	_, ok = MatchTrip(windows, now.Add(6*time.Hour), 15*time.Minute, 30*time.Minute)
	require.False(t, ok)
}
