// Package decode turns raw NMEA 0183 AIVDM/AIVDO sentences into normalized
// fixed-point position and static reports using github.com/BertoldVdb/go-ais (the
// fleet-sanctioned fork of nilsmagnus/go-ais per GEO_ARCHITECTURE D4; the nilsmagnus upstream aisnmea subpackage does not compile at its final commit).
// Multi-fragment messages are reassembled by the
// library's VDM assembler; tag blocks (receiver provenance, timestamps) are
// preserved on the decoded frame. All output coordinates and speeds are
// fixed-point integers (micro-degrees, milli-knots, milli-degrees) per the
// geo.*.v1 contracts — the floating-point values produced by the decoder are
// converted exactly once, at this boundary, and raw AIS latitude/longitude
// sentinel values are preserved for the validator to reject.
package decode

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
)

// DecodedFrame is one fully reassembled AIS message with its tag block
// provenance. Exactly one of Position / Static is populated.
type DecodedFrame struct {
	// TagBlock is the raw NMEA tag block (e.g. "\s:ais-rx-apapa-02,c:1709315661*5C\")
	// preserved verbatim for provenance; empty when the sentence carried none.
	TagBlock string
	// ReceiverID is the tag-block source field (s:), when present.
	ReceiverID string
	// TagUnixSeconds is the tag-block UNIX timestamp field (c:), when present.
	TagUnixSeconds int64
	Position       *PositionFrame
	Static         *StaticFrame
}

// PositionFrame is a normalized fixed-point position report decoded from AIS
// message types 1, 2, 3, 18, 19 or 27.
type PositionFrame struct {
	MMSI                         string
	LatitudeMicros               int32
	LongitudeMicros              int32
	SpeedOverGroundMilliknots    uint32
	CourseOverGroundMillidegrees uint32
	HeadingMillidegrees          *uint32
	NavStatus                    *int32
	PositionAccuracy             string
	AISMessageType               int32
	ShipName                     string
}

// StaticFrame is normalized static/voyage data decoded from AIS message
// types 5, 19 and 24.
type StaticFrame struct {
	MMSI                string
	IMO                 string
	Callsign            string
	ShipName            string
	ShipTypeCode        int32
	DimensionBowM       uint32
	DimensionSternM     uint32
	DimensionPortM      uint32
	DimensionStarboardM uint32
	DraughtMillimetres  uint32
	Destination         string
	ETA                 *time.Time
	EpfsType            string
	AISMessageType      int32
}

// Decoder wraps the go-ais NMEA codec. It is safe for concurrent use; the
// fragment reassembly buffer is shared across callers (fragments of one
// message may arrive interleaved on a busy listener, keyed internally by
// channel and sequence id).
type Decoder struct {
	codec *aisnmea.NMEACodec
	mu    sync.Mutex
}

// NewDecoder builds a decoder with strict (non-short) message acceptance.
func NewDecoder() *Decoder {
	return &Decoder{codec: aisnmea.NMEACodecNew(ais.CodecNew(false, false))}
}

// BufferedFragments reports how many incomplete multi-fragment messages are
// held by the reassembler (observability for truncated feeds).
func (decoder *Decoder) BufferedFragments() int {
	decoder.mu.Lock()
	defer decoder.mu.Unlock()
	return decoder.codec.BufferedMessages()
}

// ErrIncompleteFragment is returned when a sentence is a non-final fragment
// of a multi-fragment message; the completed frame is delivered when the
// final fragment arrives.
var ErrIncompleteFragment = errors.New("sentence buffered awaiting remaining fragments")

// Sentence decodes one NMEA sentence. AIVDM/AIVDO multi-fragment sequences
// are reassembled transparently; non-VDM/VDO sentences are rejected.
func (decoder *Decoder) Sentence(sentence string) (*DecodedFrame, error) {
	sentence = strings.TrimSpace(sentence)
	if sentence == "" {
		return nil, errors.New("empty NMEA sentence")
	}
	tagBlock := ""
	if strings.HasPrefix(sentence, "\\") {
		end := strings.Index(sentence[1:], "\\")
		if end < 0 {
			return nil, errors.New("unterminated NMEA tag block")
		}
		tagBlock = sentence[:end+2]
	}
	decoder.mu.Lock()
	packet, err := decoder.codec.ParseSentence(sentence)
	decoder.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("decode NMEA sentence: %w", err)
	}
	if packet == nil {
		return nil, ErrIncompleteFragment
	}
	frame := &DecodedFrame{TagBlock: tagBlock, ReceiverID: packet.TagBlock.Source, TagUnixSeconds: packet.TagBlock.Time}
	switch message := packet.Packet.(type) {
	case ais.PositionReport:
		if !message.Valid {
			return nil, errors.New("invalid position report payload")
		}
		navStatus := int32(message.NavigationalStatus)
		frame.Position = &PositionFrame{
			MMSI:                         mmsiString(message.UserID),
			LatitudeMicros:               latFineToMicros(message.Latitude),
			LongitudeMicros:              lonFineToMicros(message.Longitude),
			SpeedOverGroundMilliknots:    knotsToMilliknots(float64(message.Sog)),
			CourseOverGroundMillidegrees: degreesToMillidegrees(float64(message.Cog)),
			HeadingMillidegrees:          headingToMillidegrees(message.TrueHeading),
			NavStatus:                    &navStatus,
			PositionAccuracy:             accuracy(message.PositionAccuracy),
			AISMessageType:               int32(message.MessageID),
		}
	case ais.StandardClassBPositionReport:
		if !message.Valid {
			return nil, errors.New("invalid class B position report payload")
		}
		frame.Position = &PositionFrame{
			MMSI:                         mmsiString(message.UserID),
			LatitudeMicros:               latFineToMicros(message.Latitude),
			LongitudeMicros:              lonFineToMicros(message.Longitude),
			SpeedOverGroundMilliknots:    knotsToMilliknots(float64(message.Sog)),
			CourseOverGroundMillidegrees: degreesToMillidegrees(float64(message.Cog)),
			HeadingMillidegrees:          headingToMillidegrees(message.TrueHeading),
			PositionAccuracy:             accuracy(message.PositionAccuracy),
			AISMessageType:               int32(message.MessageID),
		}
	case ais.ExtendedClassBPositionReport:
		if !message.Valid {
			return nil, errors.New("invalid extended class B position report payload")
		}
		frame.Position = &PositionFrame{
			MMSI:                         mmsiString(message.UserID),
			LatitudeMicros:               latFineToMicros(message.Latitude),
			LongitudeMicros:              lonFineToMicros(message.Longitude),
			SpeedOverGroundMilliknots:    knotsToMilliknots(float64(message.Sog)),
			CourseOverGroundMillidegrees: degreesToMillidegrees(float64(message.Cog)),
			HeadingMillidegrees:          headingToMillidegrees(message.TrueHeading),
			PositionAccuracy:             accuracy(message.PositionAccuracy),
			AISMessageType:               int32(message.MessageID),
			ShipName:                     cleanAisText(message.Name),
		}
		// Type 19 also carries static data.
		frame.Static = &StaticFrame{
			MMSI:                mmsiString(message.UserID),
			ShipName:            cleanAisText(message.Name),
			ShipTypeCode:        int32(message.Type),
			DimensionBowM:       uint32(message.Dimension.A),
			DimensionSternM:     uint32(message.Dimension.B),
			DimensionPortM:      uint32(message.Dimension.C),
			DimensionStarboardM: uint32(message.Dimension.D),
			EpfsType:            epfsType(message.FixType),
			AISMessageType:      int32(message.MessageID),
		}
	case ais.LongRangeAisBroadcastMessage:
		if !message.Valid {
			return nil, errors.New("invalid long-range broadcast payload")
		}
		navStatus := int32(message.NavigationalStatus)
		frame.Position = &PositionFrame{
			MMSI:                         mmsiString(message.UserID),
			LatitudeMicros:               latCoarseToMicros(message.Latitude),
			LongitudeMicros:              lonCoarseToMicros(message.Longitude),
			SpeedOverGroundMilliknots:    uint32(message.Sog) * 1000,
			CourseOverGroundMillidegrees: uint32(message.Cog) * 1000,
			NavStatus:                    &navStatus,
			PositionAccuracy:             accuracy(message.PositionAccuracy),
			AISMessageType:               int32(message.MessageID),
		}
	case ais.ShipStaticData:
		if !message.Valid {
			return nil, errors.New("invalid static and voyage data payload")
		}
		frame.Static = &StaticFrame{
			MMSI:                mmsiString(message.UserID),
			IMO:                 imoString(message.ImoNumber),
			Callsign:            cleanAisText(message.CallSign),
			ShipName:            cleanAisText(message.Name),
			ShipTypeCode:        int32(message.Type),
			DimensionBowM:       uint32(message.Dimension.A),
			DimensionSternM:     uint32(message.Dimension.B),
			DimensionPortM:      uint32(message.Dimension.C),
			DimensionStarboardM: uint32(message.Dimension.D),
			DraughtMillimetres:  draughtToMillimetres(float64(message.MaximumStaticDraught)),
			Destination:         cleanAisText(message.Destination),
			ETA:                 etaToTime(message.Eta),
			EpfsType:            epfsType(message.FixType),
			AISMessageType:      int32(message.MessageID),
		}
	case ais.StaticDataReport:
		if !message.Valid {
			return nil, errors.New("invalid static data report payload")
		}
		static := &StaticFrame{MMSI: mmsiString(message.UserID), AISMessageType: int32(message.MessageID)}
		if message.PartNumber {
			static.Callsign = cleanAisText(message.ReportB.CallSign)
			static.ShipTypeCode = int32(message.ReportB.ShipType)
			static.DimensionBowM = uint32(message.ReportB.Dimension.A)
			static.DimensionSternM = uint32(message.ReportB.Dimension.B)
			static.DimensionPortM = uint32(message.ReportB.Dimension.C)
			static.DimensionStarboardM = uint32(message.ReportB.Dimension.D)
			static.EpfsType = epfsType(message.ReportB.FixType)
		} else {
			static.ShipName = cleanAisText(message.ReportA.Name)
		}
		frame.Static = static
	default:
		header := packet.Packet.GetHeader()
		if header == nil {
			return nil, errors.New("decoded AIS message carries no header")
		}
		return nil, fmt.Errorf("AIS message type %d is not a position or static report", header.MessageID)
	}
	return frame, nil
}

// mmsiString renders the 30-bit AIS user id as a 9-digit MMSI string.
func mmsiString(userID uint32) string {
	return fmt.Sprintf("%09d", userID)
}

// imoString renders the IMO number, empty when not reported.
func imoString(imo uint32) string {
	if imo == 0 {
		return ""
	}
	return fmt.Sprintf("%07d", imo)
}

// latFineToMicros converts a decoder latitude (already in degrees) to
// micro-degrees, preserving the 91° sentinel (91.0° = 91000000 micros) for
// the validator to reject.
func latFineToMicros(value ais.FieldLatLonFine) int32 {
	return int32(math.Round(float64(value) * 1e6))
}

func lonFineToMicros(value ais.FieldLatLonFine) int32 {
	return int32(math.Round(float64(value) * 1e6))
}

// latCoarseToMicros converts a type-27 coarse latitude (degrees) to
// micro-degrees.
func latCoarseToMicros(value ais.FieldLatLonCoarse) int32 {
	return int32(math.Round(float64(value) * 1e6))
}

func lonCoarseToMicros(value ais.FieldLatLonCoarse) int32 {
	return int32(math.Round(float64(value) * 1e6))
}

// knotsToMilliknots converts decoder knots (tenths) to fixed-point
// milli-knots, preserving the 102.3 kn "not available" sentinel for the
// validator.
func knotsToMilliknots(knots float64) uint32 {
	return uint32(math.Round(knots * 1000))
}

// degreesToMillidegrees converts decoder degrees (tenths) to fixed-point
// milli-degrees, preserving sentinels above 360° for the validator.
func degreesToMillidegrees(degrees float64) uint32 {
	return uint32(math.Round(degrees * 1000))
}

// headingToMillidegrees converts the 9-bit true heading; 511 is the AIS "not
// available" sentinel and maps to an absent heading.
func headingToMillidegrees(heading uint16) *uint32 {
	if heading > 360 {
		return nil
	}
	value := uint32(heading) * 1000
	return &value
}

func draughtToMillimetres(metres float64) uint32 {
	return uint32(math.Round(metres * 1000))
}

func accuracy(high bool) string {
	if high {
		return "HIGH"
	}
	return "LOW"
}

// epfsType maps the AIS EPFS code table to the fail-closed EpfsType wire
// values; unknown codes fail closed to UNSPECIFIED-adjacent rejection by the
// validator (empty string is never emitted).
func epfsType(fixType uint8) string {
	switch fixType {
	case 1:
		return "GPS"
	case 2:
		return "GLONASS"
	case 3:
		return "COMBINED_GPS_GLONASS"
	case 4:
		return "LORAN_C"
	case 5:
		return "CHAYKA"
	case 6:
		return "INTEGRATED_NAVIGATION"
	case 7:
		return "OBSERVED"
	case 8:
		return "GALILEO"
	case 15:
		return "INTERNAL_GNSS"
	default:
		return "UNSPECIFIED"
	}
}

// etaToTime renders the AIS ETA fields against the current year; nil when no
// ETA was reported.
func etaToTime(eta ais.FieldETA) *time.Time {
	if eta.Month == 0 || eta.Day == 0 {
		return nil
	}
	year := time.Now().UTC().Year()
	value := time.Date(year, time.Month(eta.Month), int(eta.Day), int(eta.Hour), int(eta.Minute), 0, 0, time.UTC)
	return &value
}

// cleanAisText trims AIS six-bit text padding ('@' and trailing spaces).
func cleanAisText(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "@")
}
