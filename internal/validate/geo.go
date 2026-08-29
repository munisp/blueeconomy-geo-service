package validate

import "math"

// haversineMetres is the great-circle distance between two fixed-point
// (micro-degree) positions in metres. Used only for plausibility checks, not
// for contract output, so floating-point math is acceptable here.
func haversineMetres(lat1Micros, lon1Micros, lat2Micros, lon2Micros int32) float64 {
	const earthRadiusMetres = 6371008.8
	lat1 := float64(lat1Micros) / 1e6 * math.Pi / 180
	lat2 := float64(lat2Micros) / 1e6 * math.Pi / 180
	dLat := float64(lat2Micros-lat1Micros) / 1e6 * math.Pi / 180
	dLon := float64(lon2Micros-lon1Micros) / 1e6 * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMetres * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
