package decode

import (
	"strings"
	"testing"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	nmea "github.com/adrianmo/go-nmea"
	"github.com/stretchr/testify/require"
)

// encodeSentences renders REAL-format AIVDM sentences from message structs
// using the go-ais encoder — fixtures are synthetic but wire-valid (correct
// checksums, bit layout and fragmentation).
func encodeSentences(t *testing.T, packet ais.Packet) []string {
	t.Helper()
	codec := aisnmea.NMEACodecNew(ais.CodecNew(false, false))
	sentences := codec.EncodeSentence(aisnmea.VdmPacket{
		Packet:      packet,
		TalkerID:    "AI",
		MessageType: "VDM",
	})
	require.NotEmpty(t, sentences, "encoder produced no sentences")
	return sentences
}

// decodeAll feeds sentences through the decoder until a complete frame
// arrives (multi-fragment reassembly).
func decodeAll(t *testing.T, decoder *Decoder, sentences []string) *DecodedFrame {
	t.Helper()
	for i, sentence := range sentences {
		frame, err := decoder.Sentence(sentence)
		if i < len(sentences)-1 {
			require.ErrorIs(t, err, ErrIncompleteFragment)
			continue
		}
		require.NoError(t, err)
		require.NotNil(t, frame)
		return frame
	}
	t.Fatal("no complete frame decoded")
	return nil
}

func TestDecodeClassAPositionReport(t *testing.T) {
	sentences := encodeSentences(t, ais.PositionReport{
		Header:             ais.Header{MessageID: 1, UserID: 657210300},
		Valid:              true,
		NavigationalStatus: 0,
		Sog:                8.4,
		PositionAccuracy:   true,
		Longitude:          3.3725,
		Latitude:           6.418,
		Cog:                127.5,
		TrueHeading:        126,
	})
	decoder := NewDecoder()
	frame := decodeAll(t, decoder, sentences)
	require.NotNil(t, frame.Position)
	require.Equal(t, "657210300", frame.Position.MMSI)
	require.Equal(t, int32(6418000), frame.Position.LatitudeMicros)
	require.Equal(t, int32(3372500), frame.Position.LongitudeMicros)
	require.Equal(t, uint32(8400), frame.Position.SpeedOverGroundMilliknots)
	require.Equal(t, uint32(127500), frame.Position.CourseOverGroundMillidegrees)
	require.NotNil(t, frame.Position.HeadingMillidegrees)
	require.Equal(t, uint32(126000), *frame.Position.HeadingMillidegrees)
	require.NotNil(t, frame.Position.NavStatus)
	require.Equal(t, int32(0), *frame.Position.NavStatus)
	require.Equal(t, "HIGH", frame.Position.PositionAccuracy)
	require.Equal(t, int32(1), frame.Position.AISMessageType)
}

func TestDecodeClassBPositionReport(t *testing.T) {
	sentences := encodeSentences(t, ais.StandardClassBPositionReport{
		Header:    ais.Header{MessageID: 18, UserID: 657221000},
		Valid:     true,
		Sog:       5.2,
		Longitude: 3.21,
		Latitude:  6.11,
		Cog:       90.0,
	})
	decoder := NewDecoder()
	frame := decodeAll(t, decoder, sentences)
	require.NotNil(t, frame.Position)
	require.Equal(t, "657221000", frame.Position.MMSI)
	require.Equal(t, int32(18), frame.Position.AISMessageType)
	require.Equal(t, uint32(5200), frame.Position.SpeedOverGroundMilliknots)
}

func TestDecodeStaticAndVoyageDataMultiFragment(t *testing.T) {
	sentences := encodeSentences(t, ais.ShipStaticData{
		Header:               ais.Header{MessageID: 5, UserID: 657210300},
		Valid:                true,
		ImoNumber:            9074729,
		CallSign:             "5NAB2",
		Name:                 "SAMPLE TRADER ONE",
		Type:                 70,
		Dimension:            ais.FieldDimension{A: 120, B: 20, C: 8, D: 12},
		FixType:              1,
		MaximumStaticDraught: 8.5,
		Destination:          "APAPA",
	})
	require.GreaterOrEqual(t, len(sentences), 2, "type 5 must fragment across multiple sentences")
	decoder := NewDecoder()
	frame := decodeAll(t, decoder, sentences)
	require.NotNil(t, frame.Static)
	require.Equal(t, "657210300", frame.Static.MMSI)
	require.Equal(t, "9074729", frame.Static.IMO)
	require.Equal(t, "5NAB2", frame.Static.Callsign)
	require.Equal(t, "SAMPLE TRADER ONE", frame.Static.ShipName)
	require.Equal(t, int32(70), frame.Static.ShipTypeCode)
	require.Equal(t, uint32(120), frame.Static.DimensionBowM)
	require.Equal(t, uint32(20), frame.Static.DimensionSternM)
	require.Equal(t, uint32(8), frame.Static.DimensionPortM)
	require.Equal(t, uint32(12), frame.Static.DimensionStarboardM)
	require.Equal(t, uint32(8500), frame.Static.DraughtMillimetres)
	require.Equal(t, "APAPA", frame.Static.Destination)
	require.Equal(t, "GPS", frame.Static.EpfsType)
}

func TestDecodeStaticDataReportPartA(t *testing.T) {
	sentences := encodeSentences(t, ais.StaticDataReport{
		Header:  ais.Header{MessageID: 24, UserID: 657221000},
		Valid:   true,
		ReportA: ais.StaticDataReportA{Valid: true, Name: "LAGOS FERRY SAMPLE"},
	})
	decoder := NewDecoder()
	frame := decodeAll(t, decoder, sentences)
	require.NotNil(t, frame.Static)
	require.Equal(t, "LAGOS FERRY SAMPLE", frame.Static.ShipName)
}

func TestDecodePreservesSentinelForValidator(t *testing.T) {
	// 91° latitude sentinel (AIS "not available") must survive decoding as
	// 91000000 micros so the validator can reject it.
	sentences := encodeSentences(t, ais.PositionReport{
		Header:    ais.Header{MessageID: 1, UserID: 657210300},
		Valid:     true,
		Latitude:  91.0,
		Longitude: 181.0,
	})
	decoder := NewDecoder()
	frame := decodeAll(t, decoder, sentences)
	require.NotNil(t, frame.Position)
	require.Equal(t, int32(91000000), frame.Position.LatitudeMicros)
	require.Equal(t, int32(181000000), frame.Position.LongitudeMicros)
}

func TestDecodeTagBlockPreserved(t *testing.T) {
	codec := aisnmea.NMEACodecNew(ais.CodecNew(false, false))
	sentences := codec.EncodeSentence(aisnmea.VdmPacket{
		Packet: ais.PositionReport{
			Header:   ais.Header{MessageID: 1, UserID: 657210300},
			Valid:    true,
			Latitude: 6.4, Longitude: 3.4,
		},
		TalkerID:    "AI",
		MessageType: "VDM",
		TagBlock:    nmea.TagBlock{Source: "ais-rx-apapa-02", Time: 1709315661},
	})
	require.NotEmpty(t, sentences)
	decoder := NewDecoder()
	frame, err := decoder.Sentence(sentences[0])
	require.NoError(t, err)
	require.NotNil(t, frame)
	require.True(t, strings.HasPrefix(frame.TagBlock, "\\"), "tag block must be preserved")
	require.Contains(t, frame.TagBlock, "s:ais-rx-apapa-02")
	require.Equal(t, "ais-rx-apapa-02", frame.ReceiverID)
	require.Equal(t, int64(1709315661), frame.TagUnixSeconds)
}

func TestDecodeRejectsNonVDM(t *testing.T) {
	decoder := NewDecoder()
	_, err := decoder.Sentence("$GPGLL,6250.5,N,00322.5,E,091421,A*2E")
	require.Error(t, err)
}

func TestDecodeRejectsGarbage(t *testing.T) {
	decoder := NewDecoder()
	_, err := decoder.Sentence("!AIVDM,1,1,,A,@@@@@@@,0*57")
	require.Error(t, err)
}
