// ETA engine v1 — pure functions. A vessel's position is snapped onto the
// route polyline (straight legs between consecutive stops; shape refinement
// is a later iteration), the remaining along-route distance per stop is
// divided by a smoothed speed (rolling median of the last N reported SOG
// values). ETAs are COMPUTED from positions/speeds only — never read from
// crew-entered AIS fields, never interpolated from nothing.
package gtfsrt

import (
	"math"
	"sort"
	"time"
)

// earthRadiusMeters is the WGS84 mean radius for haversine distances.
const earthRadiusMeters = 6371008.8

// knotsPerMilliknot converts the fixed-point milliknot doctrine to knots.
const knotsPerMilliknot = 0.001

// metersPerSecondPerKnot is the exact nautical-mile conversion.
const metersPerSecondPerKnot = 1852.0 / 3600.0

// milliknotsToMetersPerSecond converts fixed-point milliknots to m/s.
func milliknotsToMetersPerSecond(milliknots float64) float64 {
	return milliknots * knotsPerMilliknot * metersPerSecondPerKnot
}

// Coord is a WGS84 coordinate in fixed-point micro-degrees.
type Coord struct {
	LatitudeMicros  int32
	LongitudeMicros int32
}

func (coord Coord) latRadians() float64 { return float64(coord.LatitudeMicros) / 1e6 * math.Pi / 180 }
func (coord Coord) lonRadians() float64 { return float64(coord.LongitudeMicros) / 1e6 * math.Pi / 180 }

// haversineMeters is the great-circle distance between two coordinates.
func haversineMeters(a, b Coord) float64 {
	lat1, lat2 := a.latRadians(), b.latRadians()
	deltaLat := lat2 - lat1
	deltaLon := b.lonRadians() - a.lonRadians()
	h := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	return 2 * earthRadiusMeters * math.Asin(math.Sqrt(h))
}

// RoutePath is the ordered stop polyline of one trip with precomputed
// cumulative distances (meters from the first stop along the legs).
type RoutePath struct {
	// Cumulative[i] is the along-route distance of stop i from the route
	// start (Cumulative[0] == 0).
	Cumulative []float64
	// Total is the full route length in meters.
	Total float64

	// planar coordinates (meters, equirectangular around the route mean
	// latitude) used for projection; parallel to the stop list.
	xs []float64
	ys []float64

	xsOrigin float64 // origin longitude, radians
	ysOrigin float64 // origin latitude, radians
	meanLat  float64 // mean route latitude, radians
}

// NewRoutePath builds the path for an ordered stop sequence.
func NewRoutePath(stops []Coord) RoutePath {
	path := RoutePath{Cumulative: make([]float64, len(stops))}
	if len(stops) == 0 {
		return path
	}
	meanLat := 0.0
	for _, stop := range stops {
		meanLat += stop.latRadians()
	}
	meanLat /= float64(len(stops))
	cosLat := math.Cos(meanLat)
	origin := stops[0]
	path.meanLat = meanLat
	path.xsOrigin = origin.lonRadians()
	path.ysOrigin = origin.latRadians()
	path.xs = make([]float64, len(stops))
	path.ys = make([]float64, len(stops))
	for i, stop := range stops {
		path.xs[i] = (stop.lonRadians() - origin.lonRadians()) * cosLat * earthRadiusMeters
		path.ys[i] = (stop.latRadians() - origin.latRadians()) * earthRadiusMeters
		if i > 0 {
			path.Cumulative[i] = path.Cumulative[i-1] + haversineMeters(stops[i-1], stop)
		}
	}
	path.Total = path.Cumulative[len(stops)-1]
	return path
}

// Snap projects a vessel position onto the route path and returns the
// along-route progress (meters from the route start) and the off-route
// distance (meters to the nearest point on the polyline). With a
// single-stop path progress is 0 and the off-route distance is the
// distance to that stop.
func (path RoutePath) Snap(vessel Coord) (progressMeters, offRouteMeters float64) {
	if len(path.xs) == 0 {
		return 0, math.Inf(1)
	}
	vx := (vessel.lonRadians() - path.lonOriginRadians()) * math.Cos(path.meanLatitudeRadians()) * earthRadiusMeters
	vy := (vessel.latRadians() - path.latOriginRadians()) * earthRadiusMeters
	if len(path.xs) == 1 {
		dx, dy := vx-path.xs[0], vy-path.ys[0]
		return 0, math.Hypot(dx, dy)
	}
	bestDistance := math.Inf(1)
	bestProgress := 0.0
	for i := 0; i+1 < len(path.xs); i++ {
		ax, ay := path.xs[i], path.ys[i]
		bx, by := path.xs[i+1], path.ys[i+1]
		segX, segY := bx-ax, by-ay
		segLen2 := segX*segX + segY*segY
		t := 0.0
		if segLen2 > 0 {
			t = ((vx-ax)*segX + (vy-ay)*segY) / segLen2
			t = math.Max(0, math.Min(1, t))
		}
		projX, projY := ax+t*segX, ay+t*segY
		distance := math.Hypot(vx-projX, vy-projY)
		if distance < bestDistance {
			bestDistance = distance
			legLength := math.Sqrt(segLen2)
			bestProgress = path.Cumulative[i] + t*legLength
		}
	}
	return bestProgress, bestDistance
}

func (path RoutePath) latOriginRadians() float64    { return path.ysOrigin }
func (path RoutePath) lonOriginRadians() float64    { return path.xsOrigin }
func (path RoutePath) meanLatitudeRadians() float64 { return path.meanLat }

// MedianMilliknots returns the rolling median of reported SOG samples
// (fixed-point milliknots). An empty input yields 0, false — the honest
// "no observations" signal that triggers the documented fallback.
func MedianMilliknots(samples []uint32) (float64, bool) {
	if len(samples) == 0 {
		return 0, false
	}
	ordered := make([]uint32, len(samples))
	copy(ordered, samples)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return float64(ordered[middle]), true
	}
	return float64(ordered[middle-1]+ordered[middle]) / 2, true
}

// StopETA is one computed arrival prediction.
type StopETA struct {
	StopIndex       int
	RemainingMeters float64
	ETA             time.Time
}

// ComputeETAs returns arrival predictions for every stop strictly ahead of
// the vessel's along-route progress. speedMetersPerSecond must be > 0;
// callers never pass a fabricated speed (the fallback path does not call
// this function).
func (path RoutePath) ComputeETAs(progressMeters, speedMetersPerSecond float64, now time.Time) []StopETA {
	if speedMetersPerSecond <= 0 {
		return nil
	}
	etas := make([]StopETA, 0, len(path.Cumulative))
	for i, cumulative := range path.Cumulative {
		remaining := cumulative - progressMeters
		if remaining < 0 {
			// Already passed this stop — do not predict it.
			continue
		}
		etas = append(etas, StopETA{
			StopIndex:       i,
			RemainingMeters: remaining,
			ETA:             now.Add(time.Duration(remaining / speedMetersPerSecond * float64(time.Second))),
		})
	}
	return etas
}

// TripWindow is one candidate trip's absolute service window on a date.
type TripWindow struct {
	TripID         string
	FirstDeparture time.Time
	LastArrival    time.Time
}

// MatchTrip selects the trip a vessel is most plausibly serving at now:
// among trips whose window (extended by pre/post slack) covers now, the
// one that started most recently. Returns false when no trip plausibly
// matches — the caller then emits no trip reference rather than guessing.
func MatchTrip(windows []TripWindow, now time.Time, preSlack, postSlack time.Duration) (TripWindow, bool) {
	var best TripWindow
	found := false
	for _, window := range windows {
		starts := window.FirstDeparture.Add(-preSlack)
		ends := window.LastArrival.Add(postSlack)
		if now.Before(starts) || now.After(ends) {
			continue
		}
		if !found || window.FirstDeparture.After(best.FirstDeparture) {
			best = window
			found = true
		}
	}
	return best, found
}
