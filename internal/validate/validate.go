// Package validate is the sanity filter between decode and the event bus.
// Every reject publishes the suspect report to the vessels.quarantine topic —
// suspect traffic is intelligence and is never silently dropped. All checks
// operate on fixed-point contract values (micro-degrees, milli-knots).
package validate

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Verdict classifies the outcome of a check.
type Verdict string

const (
	// VerdictOK admits the report to the hot path.
	VerdictOK Verdict = "OK"
	// VerdictQuarantine routes the report to vessels.quarantine with a reason.
	VerdictQuarantine Verdict = "QUARANTINE"
)

// Reason codes (stable strings for metrics and the quarantine record).
const (
	ReasonLatitudeBounds    = "latitude-out-of-bounds"
	ReasonLongitudeBounds   = "longitude-out-of-bounds"
	ReasonSentinelLatitude  = "sentinel-latitude-91"
	ReasonSentinelLongitude = "sentinel-longitude-181"
	ReasonNullIsland        = "null-island"
	ReasonImpossibleSpeed   = "impossible-speed"
	ReasonMMSIFormat        = "mmsi-format"
	ReasonCourseBounds      = "course-out-of-bounds"
	ReasonSpeedSentinel     = "speed-not-available-sentinel"
	ReasonBifurcation       = "same-mmsi-bifurcation"
)

// PositionReport is the fixed-point input to the validator. MMSI may be
// empty only for APP_REPORT-sourced reports.
type PositionReport struct {
	MMSI                         string
	SourceClass                  string
	LatitudeMicros               int32
	LongitudeMicros              int32
	SpeedOverGroundMilliknots    uint32
	CourseOverGroundMillidegrees uint32
	ObservedAt                   time.Time
	ReceiverID                   string
}

// Finding is one validation outcome.
type Finding struct {
	Verdict Verdict
	Reason  string
	Detail  string
}

// Error renders the finding for logs.
func (finding Finding) Error() string {
	return fmt.Sprintf("%s: %s", finding.Reason, finding.Detail)
}

var mmsiPattern = regexp.MustCompile(`^[0-9]{9}$`)

const (
	// maxLatitudeMicros / maxLongitudeMicros are the physical bounds.
	maxLatitudeMicros  = 90_000_000
	maxLongitudeMicros = 180_000_000
	// sentinelLatitudeMicros / sentinelLongitudeMicros are the AIS "not
	// available" sentinels (91° / 181°).
	sentinelLatitudeMicros  = 91_000_000
	sentinelLongitudeMicros = 181_000_000
	// speedNotAvailableMilliknots is the AIS SOG 102.3 kn sentinel.
	speedNotAvailableMilliknots = 102_300
	// maxPlausibleSpeedMilliknots is the impossible-speed threshold (60 kn).
	maxPlausibleSpeedMilliknots = 60_000
	// maxCourseMillidegrees bounds COG; 360.0° exactly is valid.
	maxCourseMillidegrees = 360_000
	// bifurcationSpeedMilliknots: implied displacement speed above this
	// between consecutive fixes of one MMSI is a spoof indicator.
	bifurcationSpeedMilliknots = 60_000
)

// CheckMMSI enforces the 9-digit MMSI format. Nigerian-flagged vessels carry
// MID 657; the flag is recorded (metric label) but never grounds rejection —
// foreign traffic in Nigerian waters is legitimate.
func CheckMMSI(mmsi string) error {
	if !mmsiPattern.MatchString(mmsi) {
		return errors.New("MMSI must be exactly 9 decimal digits")
	}
	return nil
}

// IsNigerianFlagged reports whether the MMSI carries the Nigeria MID (657).
func IsNigerianFlagged(mmsi string) bool {
	return len(mmsi) == 9 && mmsi[:3] == "657"
}

// StaticChecks runs the stateless sanity checks on one report, returning all
// findings. An empty slice admits the report.
func StaticChecks(report PositionReport) []Finding {
	findings := make([]Finding, 0, 2)
	if report.SourceClass != "APP_REPORT" {
		if err := CheckMMSI(report.MMSI); err != nil {
			findings = append(findings, Finding{VerdictQuarantine, ReasonMMSIFormat, err.Error()})
		}
	} else if report.MMSI != "" {
		if err := CheckMMSI(report.MMSI); err != nil {
			findings = append(findings, Finding{VerdictQuarantine, ReasonMMSIFormat, err.Error()})
		}
	}
	lat := report.LatitudeMicros
	lon := report.LongitudeMicros
	if lat == sentinelLatitudeMicros || lat == -sentinelLatitudeMicros {
		findings = append(findings, Finding{VerdictQuarantine, ReasonSentinelLatitude, "AIS 91-degree latitude sentinel"})
	} else if lat > maxLatitudeMicros || lat < -maxLatitudeMicros {
		findings = append(findings, Finding{VerdictQuarantine, ReasonLatitudeBounds, fmt.Sprintf("latitude %d micro-degrees outside ±90 degrees", lat)})
	}
	if lon == sentinelLongitudeMicros || lon == -sentinelLongitudeMicros {
		findings = append(findings, Finding{VerdictQuarantine, ReasonSentinelLongitude, "AIS 181-degree longitude sentinel"})
	} else if lon > maxLongitudeMicros || lon < -maxLongitudeMicros {
		findings = append(findings, Finding{VerdictQuarantine, ReasonLongitudeBounds, fmt.Sprintf("longitude %d micro-degrees outside ±180 degrees", lon)})
	}
	if lat == 0 && lon == 0 {
		findings = append(findings, Finding{VerdictQuarantine, ReasonNullIsland, "(0,0) null-island position"})
	}
	if report.SpeedOverGroundMilliknots == speedNotAvailableMilliknots {
		findings = append(findings, Finding{VerdictQuarantine, ReasonSpeedSentinel, "AIS 102.3 knot speed-not-available sentinel"})
	} else if report.SpeedOverGroundMilliknots > maxPlausibleSpeedMilliknots {
		findings = append(findings, Finding{VerdictQuarantine, ReasonImpossibleSpeed, fmt.Sprintf("reported speed %d milliknots exceeds 60 knots", report.SpeedOverGroundMilliknots)})
	}
	if report.CourseOverGroundMillidegrees > maxCourseMillidegrees {
		findings = append(findings, Finding{VerdictQuarantine, ReasonCourseBounds, fmt.Sprintf("course %d millidegrees outside 0..360000", report.CourseOverGroundMillidegrees)})
	}
	return findings
}

// lastFix is the previous admitted position of one vessel.
type lastFix struct {
	latitudeMicros  int32
	longitudeMicros int32
	observedAt      time.Time
	receiverID      string
}

// Tracker holds per-vessel consecutive-report state for impossible-speed and
// same-MMSI-bifurcation (spoof indicator) checks. Safe for concurrent use.
type Tracker struct {
	mu    sync.Mutex
	fixes map[string]lastFix
	now   func() time.Time
}

// NewTracker builds an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{fixes: make(map[string]lastFix), now: time.Now}
}

// vesselKey scopes fixes by MMSI (or vessel reference for app reports).
func vesselKey(report PositionReport) string {
	if report.MMSI != "" {
		return "mmsi:" + report.MMSI
	}
	return "ref:" + report.SourceClass + ":" + report.ReceiverID
}

// CheckConsecutive compares a statically-clean report against the vessel's
// previous admitted fix and records it. It returns a bifurcation finding
// when the implied displacement speed exceeds the plausible bound — two
// concurrent tracks under one MMSI are a spoof indicator and must be
// quarantined, never dropped.
func (tracker *Tracker) CheckConsecutive(report PositionReport) *Finding {
	key := vesselKey(report)
	tracker.mu.Lock()
	previous, seen := tracker.fixes[key]
	tracker.fixes[key] = lastFix{
		latitudeMicros:  report.LatitudeMicros,
		longitudeMicros: report.LongitudeMicros,
		observedAt:      report.ObservedAt,
		receiverID:      report.ReceiverID,
	}
	tracker.mu.Unlock()
	if !seen {
		return nil
	}
	elapsed := report.ObservedAt.Sub(previous.observedAt)
	if elapsed <= 0 {
		// Out-of-order or duplicate timestamps cannot prove bifurcation.
		return nil
	}
	distanceMetres := haversineMetres(previous.latitudeMicros, previous.longitudeMicros, report.LatitudeMicros, report.LongitudeMicros)
	// metres / second → milliknots: kn = m/s * 1.9438445
	impliedMilliknots := distanceMetres / elapsed.Seconds() * 1943.8445
	if impliedMilliknots > bifurcationSpeedMilliknots {
		detail := fmt.Sprintf("implied displacement %.0f kn over %.0f s between consecutive fixes (receivers %q then %q)",
			impliedMilliknots/1000, elapsed.Seconds(), previous.receiverID, report.ReceiverID)
		return &Finding{VerdictQuarantine, ReasonBifurcation, detail}
	}
	return nil
}

// Reset forgets all per-vessel state (tests and operator flush).
func (tracker *Tracker) Reset() {
	tracker.mu.Lock()
	tracker.fixes = make(map[string]lastFix)
	tracker.mu.Unlock()
}
