package connectors

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildGT06Frame renders a wire-valid GT06 frame (start, length, protocol,
// payload, serial, X.25 CRC, stop) for tests.
func buildGT06Frame(protocol byte, payload []byte, serial uint16) []byte {
	length := byte(1 + len(payload) + 2 + 2)
	body := []byte{protocol}
	body = append(body, payload...)
	body = append(body, byte(serial>>8), byte(serial))
	crcInput := append([]byte{length}, body...)
	crc := crc16X25(crcInput)
	frame := []byte{gt06StartByte, gt06StartByte, length}
	frame = append(frame, body...)
	frame = append(frame, byte(crc>>8), byte(crc), 0x0D, 0x0A)
	return frame
}

// gpsPayload renders a 0x12 GPS payload for Lagos (6.418°N, 3.3725°E).
func gpsPayload() []byte {
	payload := make([]byte, 18)
	// 2026-08-29 09:14:21 UTC
	payload[0], payload[1], payload[2] = 26, 8, 29
	payload[3], payload[4], payload[5] = 9, 14, 21
	payload[6] = 9 // satellites
	// raw = degrees * 60 * 30000
	binary.BigEndian.PutUint32(payload[7:11], uint32(6.418*60*30000))
	binary.BigEndian.PutUint32(payload[11:15], uint32(3.3725*60*30000))
	payload[15] = 8 // knots
	// course 127°, East(bit12)=1, North(bit11)=0
	binary.BigEndian.PutUint16(payload[16:18], (1<<12)|127)
	return payload
}

func TestReadGT06FrameValid(t *testing.T) {
	raw := buildGT06Frame(gt06ProtoGPS, gpsPayload(), 7)
	frame, err := readGT06Frame(bufio.NewReader(bytes.NewReader(raw)))
	require.NoError(t, err)
	require.Equal(t, byte(gt06ProtoGPS), frame.protocol)
	require.Equal(t, uint16(7), frame.serial)
	require.Len(t, frame.payload, 18)
}

func TestReadGT06FrameRejectsBadCRC(t *testing.T) {
	raw := buildGT06Frame(gt06ProtoGPS, gpsPayload(), 7)
	raw[len(raw)-3] ^= 0xFF // corrupt CRC
	_, err := readGT06Frame(bufio.NewReader(bytes.NewReader(raw)))
	require.Error(t, err)
}

func TestParseGT06GPSFixedPoint(t *testing.T) {
	frame, err := readGT06Frame(bufio.NewReader(bytes.NewReader(buildGT06Frame(gt06ProtoGPS, gpsPayload(), 1))))
	require.NoError(t, err)
	fix, err := parseGT06GPS(frame)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 8, 29, 9, 14, 21, 0, time.UTC), fix.observedAt)
	require.InDelta(t, 6418000, fix.latitudeMicros, 1)
	require.InDelta(t, 3372500, fix.longitudeMicros, 1)
	require.Equal(t, uint32(8000), fix.speedMilliknots)
	require.Equal(t, uint32(127000), fix.courseMillidegrees)
}

func TestParseGT06GPSSouthWest(t *testing.T) {
	payload := gpsPayload()
	// South (bit11=1), West (bit12=0)
	binary.BigEndian.PutUint16(payload[16:18], (1<<11)|90)
	frame, err := readGT06Frame(bufio.NewReader(bytes.NewReader(buildGT06Frame(gt06ProtoGPS, payload, 1))))
	require.NoError(t, err)
	fix, err := parseGT06GPS(frame)
	require.NoError(t, err)
	require.Negative(t, fix.latitudeMicros)
	require.Negative(t, fix.longitudeMicros)
}

func TestParseGT06GPSRejectsBadDate(t *testing.T) {
	payload := gpsPayload()
	payload[1] = 13 // month 13
	frame, err := readGT06Frame(bufio.NewReader(bytes.NewReader(buildGT06Frame(gt06ProtoGPS, payload, 1))))
	require.NoError(t, err)
	_, err = parseGT06GPS(frame)
	require.Error(t, err)
}

func TestTokenizeIMEIStable(t *testing.T) {
	imei := []byte{0x86, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34}
	require.Equal(t, tokenizeIMEI(imei), tokenizeIMEI(imei))
	require.NotEqual(t, tokenizeIMEI(imei), tokenizeIMEI([]byte{0x86, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x35}))
	require.Len(t, tokenizeIMEI(imei), 8)
}
