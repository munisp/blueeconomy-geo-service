// geo-envelopefixturegen emits SIGNED envelope fixtures for the WP-10 geo
// contract shapes (geo.track-window.v1, geo.port-approach-eta.v1 and a DWELL
// geo.geofence-event.v1) into fixtures/envelopes/. The fixtures carry real
// Ed25519 signatures over the canonical envelope from a documented FIXTURE-
// ONLY key (the private half lives in this file, clearly labelled, and is
// rejected by every production wiring — fixture keys must never be deployed).
//
// Track content is recorded-history-shaped SAMPLE data (SYNTH vessels), never
// live traffic, matching the fixtures/ provenance doctrine.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

func main() {
	// Deterministic FIXTURE-ONLY key (documented, never deployed): fixtures
	// are byte-for-byte reproducible and verifiable by consumers.
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	_ = rand.Reader
	fmt.Printf("# fixture-only public key (do not deploy): %x\n", pub)
	signer, err := sign.NewSignerForProducer(priv, "0", "blueeconomy-geo-service-fixturegen")
	if err != nil {
		fatal(err)
	}
	principal := sign.Provenance{PrincipalID: "f47ac10b-58cc-4372-a567-0e02b2c3d479", PrincipalRole: "geo-producer"}
	base := time.Unix(1_782_918_800, 0).UTC()

	write := func(name, eventType, correlationID string, payload any, occurredAt time.Time) {
		env, err := sign.NewEnvelope(eventType, correlationID, payload, occurredAt, principal, signer)
		if err != nil {
			fatal(err)
		}
		encoded, err := json.MarshalIndent(env, "", "  ")
		if err != nil {
			fatal(err)
		}
		if err := os.MkdirAll("fixtures/envelopes", 0o755); err != nil {
			fatal(err)
		}
		if err := os.WriteFile("fixtures/envelopes/"+name, encoded, 0o644); err != nil {
			fatal(err)
		}
		fmt.Println("wrote fixtures/envelopes/" + name)
	}

	write("geo.track-window.v1.json", sign.EventTrackWindow, "corr-wp10-track-0001", sign.VesselTrackWindow{
		MMSI:        "657210300",
		WindowStart: base,
		WindowEnd:   base.Add(2 * time.Hour),
		Points: []sign.TrackPointPayload{
			{LatitudeMicros: 6_395_500, LongitudeMicros: 3_398_800, SogMilliknots: 12_400, ObservedAt: base},
			{LatitudeMicros: 6_401_200, LongitudeMicros: 3_410_500, SogMilliknots: 12_100, ObservedAt: base.Add(10 * time.Minute)},
			{LatitudeMicros: 6_412_800, LongitudeMicros: 3_429_900, SogMilliknots: 11_800, ObservedAt: base.Add(95 * time.Minute)},
		},
		Gaps: []sign.TrackGapPayload{{
			Start: base.Add(10 * time.Minute), End: base.Add(95 * time.Minute), DurationSeconds: 5100,
			FromLatitudeMicros: 6_401_200, FromLongitudeMicros: 3_410_500,
			ToLatitudeMicros: 6_412_800, ToLongitudeMicros: 3_429_900,
		}},
		MaxGapSeconds:  1800,
		Classification: "INTERNAL",
	}, base.Add(2*time.Hour))

	write("geo.port-approach-eta.v1.json", sign.EventPortApproachEta, "corr-wp10-eta-0001", sign.PortApproachEtaPayload{
		MMSI: "657210300", PortCode: "KEMBA",
		DistanceMeters: 18_522, EtaSeconds: 3600, SpeedKnots: 10.0,
		Confidence: "HIGH", Explanation: "distance/recorded-speed heuristic; assumes constant course and speed",
		PositionObservedAt: base.Add(95 * time.Minute),
		Classification:     "INTERNAL",
	}, base.Add(96*time.Minute))

	write("geo.geofence-event.dwell.v1.json", sign.EventGeofenceEvent, "corr-wp10-dwell-0001", sign.GeofenceEventRecorded{
		GeofenceEventID: "gfe-wp10-dwell-0001",
		ZoneID:          "anchorage.kilindini",
		ZoneName:        "Kilindini Anchorage (sample)",
		Event:           "DWELL",
		MMSI:            "657210300",
		LatitudeMicros:  -4_500_000, LongitudeMicros: 39_500_000,
		OccurredAt:     base.Add(70 * time.Minute),
		Classification: "INTERNAL",
	}, base.Add(70*time.Minute))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
