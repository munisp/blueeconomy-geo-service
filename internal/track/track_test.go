package track

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectGaps(t *testing.T) {
	pts := []Point{
		{AtUnix: 1000}, {AtUnix: 1030}, {AtUnix: 2000}, {AtUnix: 2030},
	}
	gaps := DetectGaps(pts, 60)
	require.Len(t, gaps, 1)
	require.Equal(t, int64(1030), gaps[0].StartUnix)
	require.Equal(t, int64(2000), gaps[0].EndUnix)
	require.Equal(t, int64(970), gaps[0].DurationS)
	require.Empty(t, DetectGaps(pts, 5000))
	require.Empty(t, DetectGaps(pts[:1], 60))
	require.Empty(t, DetectGaps(nil, 60))
}

func TestDistanceMeters(t *testing.T) {
	// 1 degree of latitude ≈ 111.195 km.
	d := DistanceMeters(0, 0, 1_000_000, 0)
	require.InDelta(t, 111_195, d, 1500)
	// Same point.
	require.InDelta(t, 0, DistanceMeters(-4_050_000, 39_670_000, -4_050_000, 39_670_000), 1e-9)
	// Antimeridian crossing: 179.5E to -179.5E is ~1 degree, not 359.
	d = DistanceMeters(0, 179_500_000, 0, -179_500_000)
	require.InDelta(t, 111_195, d, 2000)
}

func TestEstimateApproach(t *testing.T) {
	// Vessel 10 nautical miles from port at 10 kn → ETA 3600 s.
	portLat, portLon := int32(0), int32(0)
	vesselLat := int32(166_667) // ~10 NM north (1 NM = 1 arc-minute)
	eta := EstimateApproach(Point{LatMicros: vesselLat, LonMicros: 0, AtUnix: 1000, SogMillikn: 10_000}, portLat, portLon, 1100)
	require.Equal(t, ConfidenceHigh, eta.Confidence)
	require.InDelta(t, 3600, eta.ETASeconds, 60)
	require.InDelta(t, 18_522, eta.DistanceMeters, 500)

	// Stale position (>2h): LOW confidence but still computed.
	eta = EstimateApproach(Point{LatMicros: vesselLat, LonMicros: 0, AtUnix: 1000, SogMillikn: 10_000}, portLat, portLon, 1000+3*3600)
	require.Equal(t, ConfidenceLow, eta.Confidence)
	require.Positive(t, eta.ETASeconds)

	// Moored vessel: no ETA, distance still reported.
	eta = EstimateApproach(Point{LatMicros: vesselLat, LonMicros: 0, AtUnix: 1000, SogMillikn: 200}, portLat, portLon, 1100)
	require.Equal(t, int64(-1), eta.ETASeconds)
	require.Positive(t, eta.DistanceMeters)

	// Unknown speed: no ETA (never invented).
	eta = EstimateApproach(Point{LatMicros: vesselLat, LonMicros: 0, AtUnix: 1000, SogMillikn: -1}, portLat, portLon, 1100)
	require.Equal(t, int64(-1), eta.ETASeconds)
}
