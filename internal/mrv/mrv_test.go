// Unit tests for the MRV boundary: fixed-point arithmetic golden vectors
// (transcribed from the phase-8 spec §1.2 factor table — MEPC.245(66) as
// amended by MEPC.308(73)/MEPC.364(79), EU MRV Annex I; source URLs:
// https://cedelft.eu/wp-content/uploads/sites/2/2022/12/CE_Delft_EMSA_210113_Update-of-biofuels-in-Shipping_FINAL.pdf
// Table 43 reproducing the MEPC.308(73) CF table), envelope build/sign
// round-trips, CII fail-closed posture and the AIS activity estimator.
package mrv

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

func testSigner(t *testing.T) (*sign.Signer, ed25519.PublicKey) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := sign.NewSignerForProducer(privateKey, "0", ProducerName)
	require.NoError(t, err)
	require.Equal(t, "blueeconomy-geo-service-mrv-0", signer.KeyID())
	return signer, privateKey.Public().(ed25519.PublicKey)
}

// TestCO2GoldenVectors transcribes the spec §1.2 Cf table (MEPC.308(73)
// CF table reproduced in the CE Delft/EMSA source): CO2 = fuel x Cf in
// fixed point (milli-tonnes x nano / 1e9), one vector per seeded grade.
func TestCO2GoldenVectors(t *testing.T) {
	vectors := []struct {
		grade         string
		factorNano    uint64
		fuelMilli     uint64
		expectedMilli uint64
	}{
		// 1000.000 t HFO x 3.114 = 3114.000 t CO2.
		{"HFO_RME-RMK", 3_114_000_000, 1_000_000, 3_114_000},
		// 12.500 t MDO/MGO x 3.206 = 40.075 t CO2.
		{"MDO_MGO_DMX-DMB", 3_206_000_000, 12_500, 40_075},
		// 100.000 t LNG x 2.750 = 275.000 t CO2.
		{"LNG", 2_750_000_000, 100_000, 275_000},
		// 10.000 t LFO x 3.151 = 31.510 t CO2.
		{"LFO_RMA-RMD", 3_151_000_000, 10_000, 31_510},
		// 1000.000 t methanol x 1.375 = 1375.000 t CO2.
		{"METHANOL", 1_375_000_000, 1_000_000, 1_375_000},
		// 1000.000 t LPG propane x 3.000 = 3000.000 t CO2.
		{"LPG_PROPANE", 3_000_000_000, 1_000_000, 3_000_000},
	}
	for _, vector := range vectors {
		got, err := CO2MilliTonnes(vector.fuelMilli, vector.factorNano)
		require.NoError(t, err, vector.grade)
		require.Equal(t, vector.expectedMilli, got, vector.grade)
	}
}

// TestCO2OverflowFailsClosed proves the fixed-point guard.
func TestCO2OverflowFailsClosed(t *testing.T) {
	_, err := CO2MilliTonnes(^uint64(0), 3_114_000_000)
	require.Error(t, err)
	_, err = CO2MilliTonnes(1_000, 0)
	require.Error(t, err)
}

// TestU64WireForm proves the proto-JSON string rendering of fixed-point
// quantities (mrv.proto uint64 convention; fixtures/mrv/*.json).
func TestU64WireForm(t *testing.T) {
	raw, err := json.Marshal(struct {
		V U64 `json:"v"`
	}{V: 12500})
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"12500"}`, string(raw))
	var decoded struct {
		V U64 `json:"v"`
	}
	require.NoError(t, json.Unmarshal([]byte(`{"v":"39075000"}`), &decoded))
	require.Equal(t, U64(39_075_000), decoded.V)
	require.Error(t, json.Unmarshal([]byte(`{"v":"12.5"}`), &decoded.V))
	require.Error(t, json.Unmarshal([]byte(`{"v":-1}`), &decoded.V))
}

// TestEnvelopeRoundTrip builds and verifies one envelope per contract
// event family: exact topics/eventTypes, proto-JSON field rendering,
// classification floors and the JWS-EdDSA/JCS signature round-trip.
func TestEnvelopeRoundTrip(t *testing.T) {
	signer, publicKey := testSigner(t)
	principal := sign.Provenance{PrincipalID: "svc-mrv", PrincipalRole: "mrv-producer"}
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	distance := U64(3_120_000)
	hours := U64(11_160)

	cases := []struct {
		eventType      string
		topic          string
		resource       any
		classification string
		recordClass    bool
		ledgerHash     string
		resourceType   string
	}{
		{EventFuelReport, TopicFuelReports, FuelReportResource{
			ReportID: "mrvf-1", ImoNumber: "9074729", ExternalReference: "op-77-01",
			PeriodFrom: now.Add(-720 * time.Hour), PeriodTo: now, Consumer: ConsumerMainEngine,
			FuelGrade: "MDO_MGO_DMX-DMB", Method: MethodA, FuelTonnesMilli: 12_500,
			DistanceNmMilli: &distance, HoursUnderwayMinutes: &hours, BdnReference: "bdn-1",
			EvidenceDigestSha256: "sha256:" + strings.Repeat("a", 64), ReportedAt: now,
		}, "CONFIDENTIAL", true, "", "MrvFuelReportRecorded"},
		{EventVoyage, TopicVoyages, VoyageResource{
			VoyageID: "mrvv-1", ImoNumber: "9074729", Source: VoyageSourceOperator,
			GeofenceEvidenceDigestSha256: []string{}, RecordedAt: now,
		}, "CONFIDENTIAL", true, "", "MrvVoyageRecorded"},
		{EventVerification, TopicVerifications, VerificationResource{
			VerificationID: "mrvx-1", AnnualReportID: "mrva-1", Decision: DecisionVerify,
			DecidedAt: now,
		}, "CONFIDENTIAL", true, "", "MrvVerificationRecorded"},
		{EventEmissionsAnnual, TopicAnnualReports, EmissionsAnnualResource{
			AnnualReportID: "mrva-1", ImoNumber: "9074729", CalendarYear: 2026,
			State: ReportStateSubmitted, CO2TonnesMilli: 39_075_000, DistanceNmMilli: 38_000_000,
			HoursUnderwayMinutes: 138_240, FactorSetDigestSha256: "sha256:" + strings.Repeat("b", 64),
			SubmittedAt: now,
		}, "CONFIDENTIAL", true, "", "MrvEmissionsAnnualReportSubmitted"},
		{EventSoC, TopicSoC, StatementOfComplianceResource{
			SocID: "mrvs-1", AnnualReportID: "mrva-1", ImoNumber: "9074729",
			CalendarYear: 2026, ArtifactDigestSha256: "sha256:" + strings.Repeat("c", 64), IssuedAt: now,
		}, "INTERNAL", false, "sha256:" + strings.Repeat("c", 64), "MrvStatementOfComplianceIssued"},
		{EventActivityEstimate, TopicActivityEstimates, ActivityEstimateResource{
			EstimateID: "mrve-1", ImoNumber: "9074729", Mmsi: "657123400",
			PeriodFrom: now.Add(-720 * time.Hour), PeriodTo: now,
			DistanceNmMilli: 3_095_000, HoursUnderwayMinutes: 11_040,
			InputDigestSha256: "sha256:" + strings.Repeat("d", 64), ComputedAt: now,
		}, "INTERNAL", false, "", "MrvActivityEstimateComputed"},
	}
	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			require.Equal(t, tc.topic, eventContracts[tc.eventType].topic)
			raw, err := BuildSignedEnvelope(tc.eventType,
				"11111111-2222-3333-4444-555555555555", "corr-1", tc.resource, now, principal,
				tc.ledgerHash, signer)
			require.NoError(t, err)
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &envelope))
			require.JSONEq(t, `"1.0"`, string(envelope["envelopeVersion"]))
			require.JSONEq(t, `"`+tc.eventType+`"`, string(envelope["eventType"]))
			require.JSONEq(t, `"`+ProducerName+`"`, string(envelope["producer"]))
			require.JSONEq(t, `"`+tc.classification+`"`, string(envelope["classification"]))
			if tc.recordClass {
				require.JSONEq(t, `"CONFIDENTIAL"`, string(envelope["recordClassification"]))
			} else {
				_, present := envelope["recordClassification"]
				require.False(t, present, "INTERNAL events omit recordClassification (fixtures/mrv)")
			}
			// The resource carries the proto message Any projection.
			var bundle struct {
				Entry []struct {
					Resource map[string]json.RawMessage `json:"resource"`
				} `json:"entry"`
			}
			require.NoError(t, json.Unmarshal(envelope["fhir"], &bundle))
			require.Len(t, bundle.Entry, 1)
			require.JSONEq(t, `"type.googleapis.com/blueeconomy.contracts.v1.`+tc.resourceType+`"`,
				string(bundle.Entry[0].Resource["@type"]))
			// Signature round-trip against the producer public key.
			require.NoError(t, VerifySignedEnvelope(raw, publicKey))
			// Tampering fails closed.
			tampered := strings.Replace(string(raw), `"9074729"`, `"9074730"`, 1)
			if tampered != string(raw) {
				require.Error(t, VerifySignedEnvelope(json.RawMessage(tampered), publicKey))
			}
		})
	}
}

// TestEnvelopeFailsClosed proves the builder rejects unknown types, a nil
// signer and malformed identifiers.
func TestEnvelopeFailsClosed(t *testing.T) {
	signer, _ := testSigner(t)
	principal := sign.Provenance{PrincipalID: "svc-mrv", PrincipalRole: "mrv-producer"}
	now := time.Now().UTC()
	_, err := BuildSignedEnvelope("mrv.bogus.v1", "11111111-2222-3333-4444-555555555555",
		"corr", map[string]string{"a": "b"}, now, principal, "", signer)
	require.Error(t, err)
	_, err = BuildSignedEnvelope(EventFuelReport, "11111111-2222-3333-4444-555555555555",
		"corr", map[string]string{"a": "b"}, now, principal, "", nil)
	require.Error(t, err)
	_, err = BuildSignedEnvelope(EventFuelReport, "not-a-uuid",
		"corr", map[string]string{"a": "b"}, now, principal, "", signer)
	require.Error(t, err)
	_, err = BuildSignedEnvelope(EventFuelReport, "11111111-2222-3333-4444-555555555555",
		"", map[string]string{"a": "b"}, now, principal, "", signer)
	require.Error(t, err)
}

// TestParseNano exercises the fixed-point config decimal parser.
func TestParseNano(t *testing.T) {
	value, err := parseNano("0.474")
	require.NoError(t, err)
	require.Equal(t, uint64(474_000_000), value)
	value, err = parseNano("474000000")
	require.NoError(t, err)
	require.Equal(t, uint64(474_000_000_000_000_000), value)
	_, err = parseNano("4.74e-1")
	require.Error(t, err)
	_, err = parseNano("0.1234567891")
	require.Error(t, err)
	_, err = parseNano("-1")
	require.Error(t, err)
}

// TestCIIAbsentConfigIsNotComputable proves the honest UNAVAILABLE posture:
// no operator-approved config => NOT_COMPUTABLE, never an estimate.
func TestCIIAbsentConfigIsNotComputable(t *testing.T) {
	var config *CIIConfig
	outcome := config.ComputeCII("BULK_CARRIER", 40_000, 70_000, 2026, 39_075_000, 38_000_000)
	require.True(t, outcome.NotComputable)
	require.Nil(t, outcome.AttainedNano)
}

// TestCIIFixtureConfig proves the computation mechanics against a test
// fixture config document (test-only parameters; production parameters ship
// only as an operator-approved, source-cited document).
func TestCIIFixtureConfig(t *testing.T) {
	// Fixture parameters (NOT regulatory values): a=5.0, c=0.5, z=0.05,
	// boundaries 0.9/1.0/1.1/1.2 of required.
	document := `{
	  "version": "fixture-0", "approvedBy": "itest-flag-admin", "approvedAt": "2026-01-01",
	  "citation": "TEST FIXTURE ONLY — no regulatory values",
	  "shipTypes": {"BULK_CARRIER": {
	    "capacityField": "DWT",
	    "referenceLineANano": "5.0", "referenceLineCNano": "0.5",
	    "reductionFactorsZNano": {"2026": "0.05"},
	    "ratingBoundariesNano": ["0.9", "1.0", "1.1", "1.2"],
	    "citation": "TEST FIXTURE ONLY"
	  }}}`
	path := t.TempDir() + "/cii.json"
	require.NoError(t, os.WriteFile(path, []byte(document), 0o600))
	config, err := LoadCIIConfig(path)
	require.NoError(t, err)

	// capacity 70_000 dwt, distance 38_000 nm, CO2 42.560 t:
	// attained = 42_560_000 g / (70_000 x 38_000) = 0.016 g/(t nm) exactly.
	outcome := config.ComputeCII("BULK_CARRIER", 40_000, 70_000, 2026, 42_560, 38_000_000)
	require.False(t, outcome.NotComputable)
	require.NotNil(t, outcome.AttainedNano)
	require.Equal(t, uint64(16_000_000), uint64(*outcome.AttainedNano))
	// required = 0.95 x 5.0 x 70000^-0.5 = 0.017953312... (nano).
	require.Equal(t, uint64(17_953_312), uint64(*outcome.RequiredNano))
	// attained 0.016 <= required x 0.9 (0.0161579...) => A.
	require.Equal(t, "A", outcome.Rating)

	// 100x the CO2 blows past every boundary => E.
	outcome = config.ComputeCII("BULK_CARRIER", 40_000, 70_000, 2026, 4_256_000, 38_000_000)
	require.Equal(t, "E", outcome.Rating)

	// Unconfigured ship type / year fail honest.
	outcome = config.ComputeCII("TANKER", 40_000, 70_000, 2026, 1, 1)
	require.True(t, outcome.NotComputable)
	outcome = config.ComputeCII("BULK_CARRIER", 40_000, 70_000, 2031, 1, 1)
	require.True(t, outcome.NotComputable)
}

// TestCIIConfigFailsClosed proves malformed operator documents are rejected.
func TestCIIConfigFailsClosed(t *testing.T) {
	_, err := LoadCIIConfig(t.TempDir() + "/absent.json")
	require.Error(t, err)
	path := t.TempDir() + "/bad.json"
	require.NoError(t, os.WriteFile(path, []byte(`{"version":"x"}`), 0o600))
	_, err = LoadCIIConfig(path)
	require.Error(t, err)
}

// TestActivityEstimator covers the AIS methodology: segmentation, SOG
// threshold, fixed-point distance and honest insufficient coverage.
func TestActivityEstimator(t *testing.T) {
	params := DefaultActivityParams()
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)

	// 13 hourly fixes along the equator (span 12 h of the 24 h period,
	// inside the 2 h segmentation gap): lon 3.0 -> 3.06 degrees in hourly
	// 0.005-degree steps at 10 kn. 0.005 deg equatorial = 0.3 nm per leg.
	hourly := func(sog uint32) []ActivityFix {
		out := make([]ActivityFix, 0, 13)
		for i := 0; i <= 12; i++ {
			out = append(out, ActivityFix{
				ObservedAt:    from.Add(time.Duration(i+1) * time.Hour),
				LatMicros:     0,
				LonMicros:     int32(3_000_000 + i*5_000),
				SogMilliknots: sog,
			})
		}
		return out
	}
	estimate, err := EstimateActivity(hourly(10_000), from, to, params)
	require.NoError(t, err)
	require.False(t, estimate.InsufficientCoverage)
	// 12 legs x 0.3 nm = 3.6 nm; allow +-0.5% for the spherical model.
	require.InDelta(t, 3_600_000, estimate.DistanceNmMilli, 18_000)
	require.Equal(t, uint64(720), estimate.HoursUnderwayMinutes)
	require.True(t, strings.HasPrefix(estimate.InputDigestSha256, "sha256:"))

	// SOG below the underway threshold counts nothing.
	estimate, err = EstimateActivity(hourly(200), from, to, params)
	require.NoError(t, err)
	require.False(t, estimate.InsufficientCoverage)
	require.Zero(t, estimate.DistanceNmMilli)
	require.Zero(t, estimate.HoursUnderwayMinutes)

	// Sparse coverage is honest.
	estimate, err = EstimateActivity([]ActivityFix{
		{ObservedAt: from.Add(time.Hour), LatMicros: 0, LonMicros: 3_000_000, SogMilliknots: 10_000},
	}, from, to, params)
	require.NoError(t, err)
	require.True(t, estimate.InsufficientCoverage)

	// A gap beyond the segmentation gap splits segments: no leg spans it.
	// Segment A: hourly fixes +1h..+3h (2 legs); 7-hour AIS gap; segment B:
	// hourly fixes +10h..+13h (3 legs). 5 legs x 0.3 nm = 1.5 nm, 300 min.
	gapped := make([]ActivityFix, 0, 7)
	for _, hour := range []int{1, 2, 3, 10, 11, 12, 13} {
		gapped = append(gapped, ActivityFix{
			ObservedAt:    from.Add(time.Duration(hour) * time.Hour),
			LatMicros:     0,
			LonMicros:     int32(3_000_000 + hour*5_000),
			SogMilliknots: 10_000,
		})
	}
	estimate, err = EstimateActivity(gapped, from, to, params)
	require.NoError(t, err)
	require.False(t, estimate.InsufficientCoverage)
	require.InDelta(t, 1_500_000, estimate.DistanceNmMilli, 7_500)
	require.Equal(t, uint64(300), estimate.HoursUnderwayMinutes)
}

// TestCrosscheck covers the match/discrepant/insufficient classification.
func TestCrosscheck(t *testing.T) {
	estimate := ActivityEstimate{DistanceNmMilli: 1_000_000, HoursUnderwayMinutes: 600}
	require.Equal(t, CrosscheckMatch, Crosscheck(estimate, 1_050_000, 620, 100))
	require.Equal(t, CrosscheckDiscrepant, Crosscheck(estimate, 2_000_000, 600, 100))
	require.Equal(t, CrosscheckInsufficientCoverage,
		Crosscheck(ActivityEstimate{InsufficientCoverage: true}, 1_000_000, 600, 100))
	require.Equal(t, CrosscheckNoReportedValues, Crosscheck(estimate, 0, 0, 100))
}

// TestFactorSetHashDeterministic pins the factor-set digest rendering.
func TestFactorSetHashDeterministic(t *testing.T) {
	row := FactorRow{FactorKey: "HFO_RME-RMK", Gas: "CO2", FactorNano: 3_114_000_000,
		Unit: "tCO2/t_fuel", SourceCitation: "MEPC.245(66)", ValidFrom: time.Date(2018, 3, 1, 0, 0, 0, 0, time.UTC)}
	first := FactorSetHash([]FactorRow{row})
	require.True(t, strings.HasPrefix(first, "sha256:"))
	require.Len(t, first, len("sha256:")+64)
	require.Equal(t, first, FactorSetHash([]FactorRow{row}))
}

// TestSignerProducerSeparation proves the mrv signer kid carries the mrv
// producer name and the base64url public key encoding for key directories.
func TestSignerProducerSeparation(t *testing.T) {
	signer, publicKey := testSigner(t)
	require.True(t, strings.HasPrefix(signer.KeyID(), "blueeconomy-geo-service-mrv-"))
	encoded := base64.RawURLEncoding.EncodeToString(publicKey)
	require.Len(t, encoded, 43)
}
