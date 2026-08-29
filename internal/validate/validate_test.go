package validate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func baseReport() PositionReport {
	return PositionReport{
		MMSI:                         "657210300",
		SourceClass:                  "AIS",
		LatitudeMicros:               6_418_000,
		LongitudeMicros:              3_372_500,
		SpeedOverGroundMilliknots:    8_400,
		CourseOverGroundMillidegrees: 127_500,
		ObservedAt:                   time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC),
		ReceiverID:                   "ais-rx-apapa-02",
	}
}

func TestStaticChecksAdmitsValidReport(t *testing.T) {
	require.Empty(t, StaticChecks(baseReport()))
}

func TestStaticChecksRejectsSentinels(t *testing.T) {
	report := baseReport()
	report.LatitudeMicros = 91_000_000
	report.LongitudeMicros = 181_000_000
	findings := StaticChecks(report)
	reasons := map[string]bool{}
	for _, finding := range findings {
		reasons[finding.Reason] = true
		require.Equal(t, VerdictQuarantine, finding.Verdict)
	}
	require.True(t, reasons[ReasonSentinelLatitude])
	require.True(t, reasons[ReasonSentinelLongitude])
}

func TestStaticChecksRejectsOutOfBounds(t *testing.T) {
	report := baseReport()
	report.LatitudeMicros = 90_000_001
	require.NotEmpty(t, StaticChecks(report))
	report = baseReport()
	report.LongitudeMicros = -180_000_001
	require.NotEmpty(t, StaticChecks(report))
}

func TestStaticChecksRejectsNullIsland(t *testing.T) {
	report := baseReport()
	report.LatitudeMicros = 0
	report.LongitudeMicros = 0
	findings := StaticChecks(report)
	require.Len(t, findings, 1)
	require.Equal(t, ReasonNullIsland, findings[0].Reason)
}

func TestStaticChecksRejectsImpossibleReportedSpeed(t *testing.T) {
	report := baseReport()
	report.SpeedOverGroundMilliknots = 61_000
	findings := StaticChecks(report)
	require.Len(t, findings, 1)
	require.Equal(t, ReasonImpossibleSpeed, findings[0].Reason)
}

func TestStaticChecksRejectsSpeedSentinel(t *testing.T) {
	report := baseReport()
	report.SpeedOverGroundMilliknots = 102_300
	findings := StaticChecks(report)
	require.Len(t, findings, 1)
	require.Equal(t, ReasonSpeedSentinel, findings[0].Reason)
}

func TestCheckMMSI(t *testing.T) {
	require.NoError(t, CheckMMSI("657210300"))
	require.Error(t, CheckMMSI("65721030"))
	require.Error(t, CheckMMSI("6572103000"))
	require.Error(t, CheckMMSI("65721030A"))
	require.Error(t, CheckMMSI(""))
}

func TestIsNigerianFlagged(t *testing.T) {
	require.True(t, IsNigerianFlagged("657210300"))
	require.False(t, IsNigerianFlagged("235081000"))
	require.False(t, IsNigerianFlagged("657"))
}

func TestStaticChecksRequireMMSIExceptAppReport(t *testing.T) {
	report := baseReport()
	report.MMSI = ""
	require.NotEmpty(t, StaticChecks(report), "AIS without MMSI must be quarantined")
	report.SourceClass = "APP_REPORT"
	require.Empty(t, StaticChecks(report), "APP_REPORT may omit MMSI")
}

func TestTrackerBifurcationSpoofIndicator(t *testing.T) {
	tracker := NewTracker()
	first := baseReport()
	require.Nil(t, tracker.CheckConsecutive(first))

	// Same MMSI reappears 300 km away 5 minutes later — physically
	// impossible, a same-MMSI bifurcation spoof indicator.
	second := baseReport()
	second.LatitudeMicros = 4_000_000 // ~4.0°N
	second.LongitudeMicros = 7_000_000
	second.ObservedAt = first.ObservedAt.Add(5 * time.Minute)
	second.ReceiverID = "ais-rx-bonny-01"
	finding := tracker.CheckConsecutive(second)
	require.NotNil(t, finding)
	require.Equal(t, VerdictQuarantine, finding.Verdict)
	require.Equal(t, ReasonBifurcation, finding.Reason)
}

func TestTrackerAdmitsPlausibleMovement(t *testing.T) {
	tracker := NewTracker()
	first := baseReport()
	require.Nil(t, tracker.CheckConsecutive(first))

	// ~0.02° (≈2.2 km) in 10 minutes ≈ 12 kn — plausible.
	second := baseReport()
	second.LatitudeMicros = first.LatitudeMicros + 20_000
	second.ObservedAt = first.ObservedAt.Add(10 * time.Minute)
	require.Nil(t, tracker.CheckConsecutive(second))
}

func TestTrackerIgnoresOutOfOrder(t *testing.T) {
	tracker := NewTracker()
	first := baseReport()
	require.Nil(t, tracker.CheckConsecutive(first))
	stale := baseReport()
	stale.LatitudeMicros = 4_000_000
	stale.ObservedAt = first.ObservedAt.Add(-time.Hour)
	require.Nil(t, tracker.CheckConsecutive(stale))
}
