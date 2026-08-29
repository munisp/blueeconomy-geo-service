package connectors

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// GT06/Concox binary tracker protocol (Tier-1 GSM/GPS store-forward
// trackers). Frame layout:
//
//	0x78 0x78 <length:1> <protocol:1> <payload...> <serial:2> <crc16:2> 0x0D 0x0A
//
// where length counts protocol..crc bytes. Implemented protocol numbers:
// 0x01 login (IMEI registration), 0x12 GPS location, 0x13 heartbeat
// (acknowledged, no position), 0x16 GPS+LBS location. CRC is ITU X.25 over
// length..serial. Frames failing framing or CRC checks are rejected and
// counted; the tracker is answered only after successful verification.
const (
	gt06StartByte      = 0x78
	gt06ProtoLogin     = 0x01
	gt06ProtoGPS       = 0x12
	gt06ProtoHeartbeat = 0x13
	gt06ProtoGPSLBS    = 0x16
)

// GT06Server is the TCP listener for GT06/Concox trackers.
type GT06Server struct {
	Addr     string
	Pipeline *Pipeline
	Logger   *log.Logger
}

// trackerSession holds per-connection tracker state: the vessel reference
// registered at login (never the raw IMEI on the event boundary — the
// registry tokenizes IMEI→vessel_ref).
type trackerSession struct {
	vesselRef string
}

// Run serves until ctx is cancelled; a bind failure aborts startup.
func (server *GT06Server) Run(ctx context.Context) error {
	if server.Pipeline == nil {
		return errors.New("gt06 server requires a pipeline")
	}
	if server.Addr == "" {
		return errors.New("gt06 server requires a listen address")
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("gt06 listen %s: %w", server.Addr, err)
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	logger := server.Logger
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("gt06: listening tcp %s", server.Addr)
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("gt06 accept: %w", err)
		}
		go server.serve(ctx, connection)
	}
}

// serve handles one tracker connection.
func (server *GT06Server) serve(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	session := &trackerSession{}
	reader := bufio.NewReader(connection)
	for ctx.Err() == nil {
		frame, err := readGT06Frame(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				server.Pipeline.Metrics.Inc("geo_gt06_frame_errors_total", nil)
			}
			return
		}
		server.Pipeline.Metrics.Inc("geo_gt06_frames_total", map[string]string{"protocol": fmt.Sprintf("0x%02x", frame.protocol)})
		switch frame.protocol {
		case gt06ProtoLogin:
			if len(frame.payload) < 8 {
				server.Pipeline.Metrics.Inc("geo_gt06_frame_errors_total", nil)
				return
			}
			// IMEI stays inside this boundary; the registry-tokenized vessel
			// reference is derived deterministically and pseudonymously.
			session.vesselRef = "trk-" + tokenizeIMEI(frame.payload[:8])
			_ = writeGT06Reply(connection, gt06ProtoLogin, frame.serial)
		case gt06ProtoHeartbeat:
			_ = writeGT06Reply(connection, gt06ProtoHeartbeat, frame.serial)
		case gt06ProtoGPS, gt06ProtoGPSLBS:
			position, err := parseGT06GPS(frame)
			if err != nil {
				server.Pipeline.Metrics.Inc("geo_gt06_frame_errors_total", nil)
				continue
			}
			if session.vesselRef == "" {
				// A tracker that never logged in carries no registry
				// reference; refuse the report (fail closed).
				server.Pipeline.Metrics.Inc("geo_gt06_unregistered_total", nil)
				continue
			}
			speed := position.speedMilliknots
			course := position.courseMillidegrees
			ingest := IngestPosition{
				Position: store.Position{
					VesselRef:                    session.vesselRef,
					SourceClass:                  sign.SourceGSMTracker,
					LatitudeMicros:               position.latitudeMicros,
					LongitudeMicros:              position.longitudeMicros,
					SpeedOverGroundMilliknots:    &speed,
					CourseOverGroundMillidegrees: &course,
					PositionAccuracy:             sign.AccuracyUnspecified,
					ReceiverID:                   "gt06-" + sanitizeReceiver(connection.RemoteAddr().String()),
					Classification:               string(sign.ClassificationPublic),
					ObservedAt:                   position.observedAt,
				},
				PayloadKey: dedupKey("vref:"+session.vesselRef, 0, position.latitudeMicros, position.longitudeMicros,
					speed, course, position.observedAt),
			}
			if err := server.Pipeline.HandlePosition(ctx, ingest); err != nil {
				server.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "gt06"})
			} else {
				_ = writeGT06Reply(connection, frame.protocol, frame.serial)
			}
		default:
			// Unknown protocol numbers are acknowledged and counted, never
			// interpreted.
			server.Pipeline.Metrics.Inc("geo_gt06_unsupported_total", nil)
		}
	}
}

// gt06Frame is one verified protocol frame.
type gt06Frame struct {
	protocol byte
	payload  []byte
	serial   uint16
}

// readGT06Frame reads and verifies one frame (start bytes, length, stop
// bytes, X.25 CRC).
func readGT06Frame(reader *bufio.Reader) (gt06Frame, error) {
	var frame gt06Frame
	// Hunt for the 0x78 0x78 start marker.
	for {
		first, err := reader.ReadByte()
		if err != nil {
			return frame, err
		}
		if first != gt06StartByte {
			continue
		}
		second, err := reader.Peek(1)
		if err != nil {
			return frame, err
		}
		if second[0] == gt06StartByte {
			if _, err := reader.ReadByte(); err != nil {
				return frame, err
			}
			break
		}
	}
	length, err := reader.ReadByte()
	if err != nil {
		return frame, err
	}
	if length < 5 || length > 128 {
		return frame, errors.New("gt06 frame length out of range")
	}
	body := make([]byte, int(length)) // protocol | payload | serial(2) | crc(2)
	if _, err := io.ReadFull(reader, body); err != nil {
		return frame, fmt.Errorf("gt06 truncated frame: %w", err)
	}
	stop := make([]byte, 2)
	if _, err := io.ReadFull(reader, stop); err != nil {
		return frame, err
	}
	if stop[0] != 0x0D || stop[1] != 0x0A {
		return frame, errors.New("gt06 stop bytes invalid")
	}
	// CRC covers the length byte plus body minus the CRC itself.
	crcInput := append([]byte{length}, body[:len(body)-2]...)
	expected := crc16X25(crcInput)
	actual := binary.BigEndian.Uint16(body[len(body)-2:])
	if expected != actual {
		return frame, fmt.Errorf("gt06 crc mismatch (want %04x got %04x)", expected, actual)
	}
	frame.protocol = body[0]
	frame.serial = binary.BigEndian.Uint16(body[len(body)-4 : len(body)-2])
	frame.payload = body[1 : len(body)-4]
	return frame, nil
}

// gt06GPSPayload is a normalized fixed-point GPS fix.
type gt06GPSPayload struct {
	observedAt         time.Time
	latitudeMicros     int32
	longitudeMicros    int32
	speedMilliknots    uint32
	courseMillidegrees uint32
}

// parseGT06GPS decodes the 0x12 GPS payload: datetime(6) satellites(1)
// latitude(4) longitude(4) speed(1) course+status(2). Coordinates are raw
// 1/30000-minute values; degrees = raw / 30000 / 60. South/West flags come
// from the course-status word.
func parseGT06GPS(frame gt06Frame) (gt06GPSPayload, error) {
	var fix gt06GPSPayload
	payload := frame.payload
	if frame.protocol == gt06ProtoGPSLBS {
		// 0x16 prefixes an MCC/MNC/LAC/CI LBS block before the GPS section;
		// locate the GPS section by its length byte (information length).
		if len(payload) < 1 {
			return fix, errors.New("gt06 0x16 payload too short")
		}
		// The GPS sub-record follows the LBS info; convention: payload[0] is
		// the LBS length. Skip it when plausible.
		lbsLength := int(payload[0])
		if lbsLength > 0 && lbsLength+1 < len(payload) {
			payload = payload[1+lbsLength:]
		}
	}
	if len(payload) < 14 {
		return fix, fmt.Errorf("gt06 gps payload %d bytes too short", len(payload))
	}
	year := 2000 + int(payload[0])
	month := time.Month(payload[1])
	day := int(payload[2])
	hour := int(payload[3])
	minute := int(payload[4])
	second := int(payload[5])
	if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 60 {
		return fix, errors.New("gt06 gps datetime out of range")
	}
	fix.observedAt = time.Date(year, month, day, hour, minute, second, 0, time.UTC)
	latitudeRaw := binary.BigEndian.Uint32(payload[7:11])
	longitudeRaw := binary.BigEndian.Uint32(payload[11:15])
	fix.speedMilliknots = uint32(payload[15]) * 1000
	courseStatus := binary.BigEndian.Uint16(payload[16:18])
	fix.courseMillidegrees = uint32(courseStatus&0x03FF) * 1000
	// Latitude/longitude raw units are 1/30000 minute → micro-degrees:
	// raw * 1e6 / (30000 * 60) = raw / 1.8; use exact rational scaling.
	latitudeMicros := int64(latitudeRaw) * 1_000_000 / 1_800_000
	longitudeMicros := int64(longitudeRaw) * 1_000_000 / 1_800_000
	if courseStatus&(1<<11) != 0 { // bit 11: 0=North 1=South
		latitudeMicros = -latitudeMicros
	}
	if courseStatus&(1<<12) == 0 { // bit 12: 1=East 0=West
		longitudeMicros = -longitudeMicros
	}
	fix.latitudeMicros = int32(latitudeMicros)
	fix.longitudeMicros = int32(longitudeMicros)
	return fix, nil
}

// writeGT06Reply acknowledges a protocol frame with its serial number.
func writeGT06Reply(writer io.Writer, protocol byte, serial uint16) error {
	length := byte(5) // protocol + serial(2) + crc(2)
	body := []byte{protocol, byte(serial >> 8), byte(serial)}
	crcInput := append([]byte{length}, body...)
	crc := crc16X25(crcInput)
	frame := []byte{gt06StartByte, gt06StartByte, length}
	frame = append(frame, body...)
	frame = append(frame, byte(crc>>8), byte(crc), 0x0D, 0x0A)
	_, err := writer.Write(frame)
	return err
}

// crc16X25 computes the ITU X.25 CRC used by the GT06 family.
func crc16X25(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, value := range data {
		crc ^= uint16(value)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0x8408
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

// tokenizeIMEI renders a pseudonymous registry-style vessel reference from
// the 8-byte BCD IMEI. The raw IMEI never crosses the event boundary.
func tokenizeIMEI(bcd []byte) string {
	var digits [16]byte
	for i, b := range bcd {
		digits[i*2] = '0' + (b >> 4)
		digits[i*2+1] = '0' + (b & 0x0F)
	}
	// FNV-1a over the IMEI digits produces a stable, pseudonymous token.
	hash := uint32(2166136261)
	for _, digit := range digits {
		hash ^= uint32(digit)
		hash *= 16777619
	}
	return fmt.Sprintf("%08x", hash)
}
