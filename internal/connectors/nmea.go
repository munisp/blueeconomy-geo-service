package connectors

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/decode"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// NMEAListener ingests AIVDM/AIVDO sentences from shore AIS receivers over
// TCP and UDP (port 10110 convention). Each connection/source gets its own
// decoder so multi-fragment sequences can never cross-mix between receivers.
type NMEAListener struct {
	TCPAddr  string
	UDPAddr  string
	Pipeline *Pipeline
	Logger   *log.Logger
}

// Run serves TCP and UDP until ctx is cancelled. Both listeners are
// fail-closed: a bind error aborts startup.
func (listener *NMEAListener) Run(ctx context.Context) error {
	if listener.Pipeline == nil {
		return errors.New("nmea listener requires a pipeline")
	}
	if listener.TCPAddr == "" && listener.UDPAddr == "" {
		return errors.New("nmea listener requires a TCP or UDP address")
	}
	logger := listener.Logger
	if logger == nil {
		logger = log.Default()
	}
	errorChannel := make(chan error, 2)
	running := 0
	if listener.TCPAddr != "" {
		tcp, err := net.Listen("tcp", listener.TCPAddr)
		if err != nil {
			return fmt.Errorf("nmea tcp listen %s: %w", listener.TCPAddr, err)
		}
		running++
		go func() {
			<-ctx.Done()
			_ = tcp.Close()
		}()
		go func() {
			for {
				connection, err := tcp.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errorChannel <- fmt.Errorf("nmea tcp accept: %w", err)
					return
				}
				go listener.serveConnection(ctx, connection)
			}
		}()
		logger.Printf("nmea: listening tcp %s", listener.TCPAddr)
	}
	if listener.UDPAddr != "" {
		udp, err := net.ListenPacket("udp", listener.UDPAddr)
		if err != nil {
			return fmt.Errorf("nmea udp listen %s: %w", listener.UDPAddr, err)
		}
		running++
		go func() {
			<-ctx.Done()
			_ = udp.Close()
		}()
		go listener.serveUDP(ctx, udp)
		logger.Printf("nmea: listening udp %s", listener.UDPAddr)
	}
	if running == 0 {
		return errors.New("nmea listener started no sockets")
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errorChannel:
		return err
	}
}

// serveConnection decodes one TCP receiver connection.
func (listener *NMEAListener) serveConnection(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	decoder := decode.NewDecoder()
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		listener.handleSentence(ctx, decoder, scanner.Text(), connection.RemoteAddr().String())
	}
}

// serveUDP decodes datagrams, keeping one decoder per remote receiver so
// fragments from different senders never share the reassembly buffer.
func (listener *NMEAListener) serveUDP(ctx context.Context, socket net.PacketConn) {
	decoders := make(map[string]*decode.Decoder)
	var mu sync.Mutex
	buffer := make([]byte, 65535)
	for {
		n, remote, err := socket.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			listener.Pipeline.Metrics.Inc("geo_nmea_read_errors_total", nil)
			continue
		}
		mu.Lock()
		decoder, ok := decoders[remote.String()]
		if !ok {
			decoder = decode.NewDecoder()
			decoders[remote.String()] = decoder
		}
		mu.Unlock()
		for _, line := range strings.Split(string(buffer[:n]), "\n") {
			listener.handleSentence(ctx, decoder, line, remote.String())
		}
	}
}

// handleSentence decodes one sentence and feeds the pipeline. Decode errors
// are counted and skipped (a malformed sentence carries no usable payload to
// quarantine); successfully decoded frames are always delivered.
func (listener *NMEAListener) handleSentence(ctx context.Context, decoder *decode.Decoder, sentence, source string) {
	sentence = strings.TrimSpace(sentence)
	if sentence == "" {
		return
	}
	frame, err := decoder.Sentence(sentence)
	if err != nil {
		if errors.Is(err, decode.ErrIncompleteFragment) {
			return
		}
		listener.Pipeline.Metrics.Inc("geo_nmea_decode_errors_total", nil)
		return
	}
	receiverID := frame.ReceiverID
	if receiverID == "" {
		receiverID = "nmea-" + sanitizeReceiver(source)
	}
	observedAt := time.Now().UTC()
	if frame.TagUnixSeconds > 0 {
		observedAt = time.Unix(frame.TagUnixSeconds, 0).UTC()
	}
	if frame.Position != nil {
		listener.Pipeline.Metrics.Inc("geo_nmea_frames_total", map[string]string{"kind": "position"})
		if err := listener.Pipeline.HandlePosition(ctx, positionFromFrame(frame.Position, receiverID, observedAt)); err != nil && listener.Logger != nil {
			listener.Logger.Printf("nmea: position pipeline error: %v", err)
			listener.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "nmea"})
		}
	}
	if frame.Static != nil {
		listener.Pipeline.Metrics.Inc("geo_nmea_frames_total", map[string]string{"kind": "static"})
		if err := listener.Pipeline.HandleStatic(ctx, staticFromFrame(frame.Static, observedAt)); err != nil && listener.Logger != nil {
			listener.Logger.Printf("nmea: static pipeline error: %v", err)
			listener.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "nmea"})
		}
	}
}

// positionFromFrame normalizes a decoded AIS position frame for the pipeline.
func positionFromFrame(frame *decode.PositionFrame, receiverID string, observedAt time.Time) IngestPosition {
	msgType := frame.AISMessageType
	return IngestPosition{
		Position: store.Position{
			MMSI:                         frame.MMSI,
			SourceClass:                  sign.SourceAIS,
			LatitudeMicros:               frame.LatitudeMicros,
			LongitudeMicros:              frame.LongitudeMicros,
			SpeedOverGroundMilliknots:    uint32Ptr(frame.SpeedOverGroundMilliknots),
			CourseOverGroundMillidegrees: uint32Ptr(frame.CourseOverGroundMillidegrees),
			HeadingMillidegrees:          frame.HeadingMillidegrees,
			NavStatus:                    frame.NavStatus,
			PositionAccuracy:             frame.PositionAccuracy,
			ReceiverID:                   receiverID,
			AISMessageType:               &msgType,
			ShipName:                     frame.ShipName,
			Classification:               string(sign.ClassificationPublic),
			ObservedAt:                   observedAt,
		},
		PayloadKey: dedupKey(frame.MMSI, frame.AISMessageType, frame.LatitudeMicros, frame.LongitudeMicros,
			frame.SpeedOverGroundMilliknots, frame.CourseOverGroundMillidegrees, observedAt),
	}
}

// staticFromFrame normalizes decoded static/voyage data for the pipeline.
func staticFromFrame(frame *decode.StaticFrame, observedAt time.Time) IngestStatic {
	return IngestStatic{Report: store.StaticReport{
		MMSI:                frame.MMSI,
		IMO:                 frame.IMO,
		Callsign:            frame.Callsign,
		ShipName:            frame.ShipName,
		ShipTypeCode:        frame.ShipTypeCode,
		DimensionBowM:       frame.DimensionBowM,
		DimensionSternM:     frame.DimensionSternM,
		DimensionPortM:      frame.DimensionPortM,
		DimensionStarboardM: frame.DimensionStarboardM,
		DraughtMillimetres:  frame.DraughtMillimetres,
		Destination:         frame.Destination,
		ETA:                 frame.ETA,
		EpfsType:            frame.EpfsType,
		SourceClass:         sign.SourceAIS,
		Classification:      string(sign.ClassificationPublic),
		ObservedAt:          observedAt,
	}}
}

// sanitizeReceiver renders a host:port source as a receiver id fragment.
func sanitizeReceiver(source string) string {
	host, _, err := net.SplitHostPort(source)
	if err != nil {
		host = source
	}
	return strings.NewReplacer(".", "-", ":", "-", "[", "", "]", "").Replace(host)
}

func uint32Ptr(value uint32) *uint32 { return &value }
