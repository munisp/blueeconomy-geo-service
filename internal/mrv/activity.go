// AIS-derived activity estimation: distance-under-way and hours-under-way
// per vessel per period computed from the geo-service position plane
// (ais_positions), as the verification cross-check. AIS-derived values are
// a cross-check, never a substitute: reported fuel remains the DCS record
// of truth and discrepancies feed the verifier decision.
//
// Methodology (fixed-point in/out; the only float use is the internal
// great-circle leg length, immediately rounded to integer milli-nautical
// miles):
//  1. Positions for the MMSI inside [from, to) are ordered by time and
//     split into segments wherever the gap exceeds the configured
//     segmentation gap (default 2 h, matching the platform trajectory
//     segmentation doctrine).
//  2. Inside a segment, each consecutive pair whose later fix reports
//     SOG >= the configured under-way threshold (default 1.0 kn) counts as
//     under way: its great-circle leg length adds to distance and its
//     elapsed time to hours under way.
//  3. Coverage is the observed span (last fix - first fix) over the
//     requested period; below the configured minimum coverage the estimate
//     is honestly flagged insufficient and the quantities are not
//     authoritative.
//  4. The input digest is sha256 over the canonical fix list, so every
//     estimate is reproducible and auditable.
package mrv

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"
)

// ActivityParams are the operator-tunable methodology parameters (env
// configuration; these are estimation methodology, not regulatory
// parameters).
type ActivityParams struct {
	// SogThresholdMilliknots is the under-way SOG floor (default 1000 = 1.0 kn).
	SogThresholdMilliknots uint32
	// SegmentGap bounds consecutive fixes inside one segment (default 2 h).
	SegmentGap time.Duration
	// MinCoveragePermille is the minimum observed-span coverage (default 500 = 50%).
	MinCoveragePermille uint32
}

// DefaultActivityParams returns the documented methodology defaults.
func DefaultActivityParams() ActivityParams {
	return ActivityParams{
		SogThresholdMilliknots: 1000,
		SegmentGap:             2 * time.Hour,
		MinCoveragePermille:    500,
	}
}

// Validate fails closed on degenerate methodology parameters.
func (params ActivityParams) Validate() error {
	if params.SogThresholdMilliknots == 0 {
		return errors.New("AIS under-way SOG threshold must be positive")
	}
	if params.SegmentGap <= 0 || params.SegmentGap > 24*time.Hour {
		return errors.New("AIS segmentation gap must be in (0, 24h]")
	}
	if params.MinCoveragePermille == 0 || params.MinCoveragePermille > 1000 {
		return errors.New("AIS minimum coverage must be in (0, 1000] permille")
	}
	return nil
}

// ActivityFix is one fixed-point position input to the estimator.
type ActivityFix struct {
	ObservedAt    time.Time
	LatMicros     int32
	LonMicros     int32
	SogMilliknots uint32
}

// ActivityEstimate is the estimator output.
type ActivityEstimate struct {
	DistanceNmMilli      uint64
	HoursUnderwayMinutes uint64
	InsufficientCoverage bool
	FixCount             int
	InputDigestSha256    string
}

// EstimateActivity computes the AIS-derived estimate for an ordered fix
// list (callers sort by ObservedAt ascending). An empty/degenerate fix set
// yields an insufficient-coverage estimate, never an error.
func EstimateActivity(fixes []ActivityFix, from, to time.Time, params ActivityParams) (ActivityEstimate, error) {
	if !from.Before(to) {
		return ActivityEstimate{}, errors.New("estimate period from must be before to")
	}
	if err := params.Validate(); err != nil {
		return ActivityEstimate{}, err
	}
	estimate := ActivityEstimate{InputDigestSha256: digestFixes(fixes)}
	estimate.FixCount = len(fixes)
	if len(fixes) < 2 {
		estimate.InsufficientCoverage = true
		return estimate, nil
	}
	// Coverage: observed span over the requested period.
	observedSpan := fixes[len(fixes)-1].ObservedAt.Sub(fixes[0].ObservedAt)
	periodSpan := to.Sub(from)
	coveragePermille := int64(0)
	if periodSpan > 0 {
		coveragePermille = observedSpan.Milliseconds() * 1000 / periodSpan.Milliseconds()
	}
	if coveragePermille < int64(params.MinCoveragePermille) {
		estimate.InsufficientCoverage = true
		return estimate, nil
	}

	var distanceNmMilli uint64
	var underwaySeconds int64
	segmentStart := 0
	for i := 1; i < len(fixes); i++ {
		previous, current := fixes[i-1], fixes[i]
		gap := current.ObservedAt.Sub(previous.ObservedAt)
		if gap > params.SegmentGap {
			segmentStart = i
			continue
		}
		if gap <= 0 {
			continue
		}
		if i == segmentStart {
			continue // first fix of a segment has no inbound leg
		}
		if current.SogMilliknots < params.SogThresholdMilliknots {
			continue // not under way on this leg
		}
		legNmMilli := greatCircleMilliNM(previous.LatMicros, previous.LonMicros, current.LatMicros, current.LonMicros)
		distanceNmMilli += legNmMilli
		underwaySeconds += int64(gap.Seconds())
	}
	estimate.DistanceNmMilli = distanceNmMilli
	estimate.HoursUnderwayMinutes = uint64(underwaySeconds / 60)
	return estimate, nil
}

// greatCircleMilliNM is the haversine great-circle distance between two
// fixed-point micro-degree positions, in milli-nautical miles. Floats are
// used only inside this function; the result is rounded to integer
// milli-nm at the boundary.
func greatCircleMilliNM(latMicrosA, lonMicrosA, latMicrosB, lonMicrosB int32) uint64 {
	const earthRadiusMeters = 6371008.8 // IUGG mean radius, for the spherical leg model
	toRadians := func(micros int32) float64 {
		return float64(micros) / 1e6 * math.Pi / 180
	}
	latA, latB := toRadians(latMicrosA), toRadians(latMicrosB)
	dLat := latB - latA
	dLon := toRadians(lonMicrosB) - toRadians(lonMicrosA)
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(latA)*math.Cos(latB)*math.Sin(dLon/2)*math.Sin(dLon/2)
	meters := 2 * earthRadiusMeters * math.Asin(math.Min(1, math.Sqrt(h)))
	return uint64(math.Round(meters / 1.852 * 1000))
}

// digestFixes renders the canonical fix list and digests it:
// one line per fix "RFC3339|latMicros|lonMicros|sogMilliknots".
func digestFixes(fixes []ActivityFix) string {
	sum := sha256.New()
	for _, fix := range fixes {
		fmt.Fprintf(sum, "%s|%d|%d|%d\n",
			fix.ObservedAt.UTC().Format(time.RFC3339), fix.LatMicros, fix.LonMicros, fix.SogMilliknots)
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil))
}

// CrosscheckResult classifies the AIS estimate against reported values.
type CrosscheckResult string

const (
	CrosscheckMatch                CrosscheckResult = "match"
	CrosscheckDiscrepant           CrosscheckResult = "discrepant"
	CrosscheckInsufficientCoverage CrosscheckResult = "insufficient_coverage"
	CrosscheckNoReportedValues     CrosscheckResult = "no_reported_values"
)

// Crosscheck compares the AIS-derived estimate to reported distance/hours
// within the configured tolerance (permille of the reported value). AIS is
// a cross-check only — the outcome informs the verifier, it never mutates
// reported values.
func Crosscheck(estimate ActivityEstimate, reportedDistanceNmMilli, reportedHoursMinutes uint64, tolerancePermille uint32) CrosscheckResult {
	if estimate.InsufficientCoverage {
		return CrosscheckInsufficientCoverage
	}
	if reportedDistanceNmMilli == 0 && reportedHoursMinutes == 0 {
		return CrosscheckNoReportedValues
	}
	within := func(reported, estimated uint64) bool {
		if reported == 0 {
			return estimated == 0
		}
		var delta uint64
		if estimated > reported {
			delta = estimated - reported
		} else {
			delta = reported - estimated
		}
		return delta*1000 <= reported*uint64(tolerancePermille)
	}
	if within(reportedDistanceNmMilli, estimate.DistanceNmMilli) &&
		within(reportedHoursMinutes, estimate.HoursUnderwayMinutes) {
		return CrosscheckMatch
	}
	return CrosscheckDiscrepant
}
