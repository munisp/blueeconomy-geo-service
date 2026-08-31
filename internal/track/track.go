// Package track holds the pure trajectory analytics behind the WP-10
// vessel-track APIs: time-window gap detection, fixed-point surface
// distance, and the port-approach ETA heuristic with honest confidence
// labelling. No positions are ever interpolated or synthesized here — gaps
// are REPORTED, not filled.
package track

import "math"

// Point is one recorded position on a vessel track.
type Point struct {
	LatMicros  int32
	LonMicros  int32
	AtUnix     int64
	SogMillikn int32 // <0 when unknown
}

// Gap is a stretch of the track with no recorded positions longer than the
// caller's threshold. Gaps are reported, never filled.
type Gap struct {
	StartUnix  int64
	EndUnix    int64
	DurationS  int64
	FromLatMicros int32
	FromLonMicros int32
	ToLatMicros   int32
	ToLonMicros   int32
}

// DetectGaps returns every interval between consecutive recorded points
// whose spacing exceeds maxGapSeconds. Points must be time-ascending (they
// are sorted by the query layer; unsorted input is tolerated defensively).
func DetectGaps(points []Point, maxGapSeconds int64) []Gap {
	if maxGapSeconds <= 0 || len(points) < 2 {
		return nil
	}
	var gaps []Gap
	for i := 1; i < len(points); i++ {
		prev, cur := points[i-1], points[i]
		d := cur.AtUnix - prev.AtUnix
		if d > maxGapSeconds {
			gaps = append(gaps, Gap{
				StartUnix: prev.AtUnix, EndUnix: cur.AtUnix, DurationS: d,
				FromLatMicros: prev.LatMicros, FromLonMicros: prev.LonMicros,
				ToLatMicros: cur.LatMicros, ToLonMicros: cur.LonMicros,
			})
		}
	}
	return gaps
}

// earthRadiusM is the mean Earth radius in metres.
const earthRadiusM = 6_371_008.8

// DistanceMeters is the equirectangular surface distance between two
// micro-degree points (accurate to <0.5% for the platform's operating
// ranges; documented in the API). Crosses the antimeridian correctly.
func DistanceMeters(aLat, aLon, bLat, bLon int32) float64 {
	lat1 := float64(aLat) / 1e6 * math.Pi / 180
	lat2 := float64(bLat) / 1e6 * math.Pi / 180
	dLat := lat2 - lat1
	dLonDeg := float64(bLon-aLon) / 1e6
	if dLonDeg > 180 {
		dLonDeg -= 360
	} else if dLonDeg < -180 {
		dLonDeg += 360
	}
	dLon := dLonDeg * math.Pi / 180
	x := dLon * math.Cos((lat1+lat2)/2)
	return math.Sqrt(x*x+dLat*dLat) * earthRadiusM
}

// ETAConfidence labels how much trust the heuristic ETA deserves.
type ETAConfidence string

const (
	// ConfidenceHigh: fresh position (<15 min) and a credible speed.
	ConfidenceHigh ETAConfidence = "HIGH"
	// ConfidenceMedium: position up to 2h old, or very low speed.
	ConfidenceMedium ETAConfidence = "MEDIUM"
	// ConfidenceLow: stale position (>2h) — ETA is indicative only.
	ConfidenceLow ETAConfidence = "LOW"
)

// ApproachETA is the port-approach estimate.
type ApproachETA struct {
	DistanceMeters float64
	// ETASeconds is travel time at the vessel's recorded speed; -1 when no
	// honest estimate exists (unknown/zero speed → never invented).
	ETASeconds  int64
	SpeedKnots  float64
	Confidence  ETAConfidence
	Explanation string
}

// EstimateApproach computes a distance/speed ETA heuristic. nowUnix is
// explicit so callers stamp provenance. Speed below 0.5 kn is treated as
// "not making way": no ETA (a moored vessel's approach ETA is meaningless),
// but the distance is still reported.
func EstimateApproach(p Point, portLat, portLon int32, nowUnix int64) ApproachETA {
	dist := DistanceMeters(p.LatMicros, p.LonMicros, portLat, portLon)
	ageS := nowUnix - p.AtUnix
	confidence := ConfidenceHigh
	switch {
	case ageS > 2*3600:
		confidence = ConfidenceLow
	case ageS > 15*60:
		confidence = ConfidenceMedium
	}
	out := ApproachETA{
		DistanceMeters: dist,
		ETASeconds:     -1,
		Confidence:     confidence,
	}
	if p.SogMillikn < 500 {
		out.Explanation = "vessel not making way (SOG < 0.5 kn) or speed unknown: no honest ETA"
		return out
	}
	speedMS := float64(p.SogMillikn) / 1000.0 * 0.514444
	out.SpeedKnots = float64(p.SogMillikn) / 1000.0
	out.ETASeconds = int64(dist / speedMS)
	if confidence == ConfidenceHigh {
		out.Explanation = "distance/recorded-speed heuristic; assumes constant course and speed"
	} else {
		out.Explanation = "distance/recorded-speed heuristic on a STALE position; treat as indicative only"
	}
	return out
}
