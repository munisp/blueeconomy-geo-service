package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/gorilla/websocket"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// AISStreamConfig configures the aisstream.io WebSocket connector. The
// service is a dev/gap-fill source of pre-decoded AIS JSON; the API key
// comes from the environment and the connector refuses to start without it.
type AISStreamConfig struct {
	APIKey string
	// URL overrides the upstream endpoint (tests only); empty uses the
	// canonical aisstream.io socket.
	URL string
	// BoundingBoxes limits the subscription (aisstream wire format:
	// [[latMin, lonMin], [latMax, lonMax]]). Defaults to the Nigerian AoI.
	BoundingBoxes [][][2]float64
}

const aisstreamURL = "wss://stream.aisstream.io/v0/stream"

// nigerianAoI is the default subscription box covering Nigeria's EEZ and
// approaches (Gulf of Guinea).
var nigerianAoI = [][][2]float64{{{2.5, -2.0}, {14.5, 15.5}}}

// AISStreamClient consumes pre-decoded aisstream.io JSON into the pipeline.
type AISStreamClient struct {
	Config   AISStreamConfig
	Pipeline *Pipeline
	Logger   *log.Logger
}

// streamMessage is the aisstream.io envelope: a discriminator plus the
// decoded message body and station metadata.
type streamMessage struct {
	MessageType string          `json:"MessageType"`
	Message     json.RawMessage `json:"Message"`
	MetaData    struct {
		MMSI      uint32  `json:"MMSI"`
		ShipName  string  `json:"ShipName"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		TimeUTC   string  `json:"time_utc"`
	} `json:"MetaData"`
}

type streamPositionReport struct {
	UserID             uint32  `json:"UserID"`
	Latitude           float64 `json:"Latitude"`
	Longitude          float64 `json:"Longitude"`
	Sog                float64 `json:"Sog"`
	Cog                float64 `json:"Cog"`
	TrueHeading        int64   `json:"TrueHeading"`
	NavigationalStatus int64   `json:"NavigationalStatus"`
	PositionAccuracy   bool    `json:"PositionAccuracy"`
	MessageID          int64   `json:"MessageID"`
}

type streamStaticData struct {
	UserID    uint32 `json:"UserID"`
	ImoNumber uint32 `json:"ImoNumber"`
	CallSign  string `json:"CallSign"`
	Name      string `json:"Name"`
	Type      int64  `json:"Type"`
	Dimension struct {
		A uint32 `json:"A"`
		B uint32 `json:"B"`
		C uint32 `json:"C"`
		D uint32 `json:"D"`
	} `json:"Dimension"`
	MaximumStaticDraught float64 `json:"MaximumStaticDraught"`
	Destination          string  `json:"Destination"`
	FixType              int64   `json:"FixType"`
	MessageID            int64   `json:"MessageID"`
}

// Run connects and consumes until ctx is cancelled, reconnecting with
// backoff on transient upstream errors. A missing API key fails closed.
func (client *AISStreamClient) Run(ctx context.Context) error {
	if client.Pipeline == nil {
		return errors.New("aisstream client requires a pipeline")
	}
	if client.Config.APIKey == "" {
		return errors.New("GEO_AISSTREAM_API_KEY is required for the aisstream connector")
	}
	logger := client.Logger
	if logger == nil {
		logger = log.Default()
	}
	url := client.Config.URL
	if url == "" {
		url = aisstreamURL
	}
	boxes := client.Config.BoundingBoxes
	if len(boxes) == 0 {
		boxes = nigerianAoI
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := client.consume(ctx, url, boxes)
		if ctx.Err() != nil {
			return nil
		}
		logger.Printf("aisstream: connection ended: %v (retry in %s)", err, backoff)
		client.Pipeline.Metrics.Inc("geo_aisstream_reconnects_total", nil)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// consume runs one WebSocket session.
func (client *AISStreamClient) consume(ctx context.Context, url string, boxes [][][2]float64) error {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	connection, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("aisstream dial: %w", err)
	}
	defer connection.Close()
	subscription := map[string]any{
		"APIKey":        client.Config.APIKey,
		"BoundingBoxes": boxes,
		"FilterMessageTypes": []string{
			"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport",
			"ShipStaticData", "StaticDataReport", "LongRangeAisBroadcastMessage",
		},
	}
	if err := connection.WriteJSON(subscription); err != nil {
		return fmt.Errorf("aisstream subscribe: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = connection.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"), time.Now().Add(time.Second))
		_ = connection.Close()
	}()
	for {
		_, raw, err := connection.ReadMessage()
		if err != nil {
			return fmt.Errorf("aisstream read: %w", err)
		}
		client.Pipeline.Metrics.Inc("geo_aisstream_messages_total", nil)
		if handleErr := client.handle(ctx, raw); handleErr != nil {
			client.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "aisstream"})
			if client.Logger != nil {
				client.Logger.Printf("aisstream: message handling error: %v", handleErr)
			}
		}
	}
}

// handle normalizes one upstream JSON message into the pipeline. Unknown
// message types are skipped; decode errors are counted.
func (client *AISStreamClient) handle(ctx context.Context, raw []byte) error {
	var envelope streamMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		client.Pipeline.Metrics.Inc("geo_aisstream_decode_errors_total", nil)
		return nil
	}
	observedAt := time.Now().UTC()
	if envelope.MetaData.TimeUTC != "" {
		if parsed, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", envelope.MetaData.TimeUTC); err == nil {
			observedAt = parsed.UTC()
		}
	}
	switch envelope.MessageType {
	case "PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport", "LongRangeAisBroadcastMessage":
		var report streamPositionReport
		if err := json.Unmarshal(envelope.Message, &report); err != nil {
			client.Pipeline.Metrics.Inc("geo_aisstream_decode_errors_total", nil)
			return nil
		}
		msgType := int32(report.MessageID)
		if msgType == 0 {
			msgType = 1
		}
		speed := uint32(math.Round(report.Sog * 1000))
		course := uint32(math.Round(report.Cog * 1000))
		accuracy := sign.AccuracyLow
		if report.PositionAccuracy {
			accuracy = sign.AccuracyHigh
		}
		var heading *uint32
		if report.TrueHeading >= 0 && report.TrueHeading <= 360 {
			value := uint32(report.TrueHeading) * 1000
			heading = &value
		}
		var navStatus *int32
		if envelope.MessageType == "PositionReport" {
			value := int32(report.NavigationalStatus)
			navStatus = &value
		}
		return client.Pipeline.HandlePosition(ctx, IngestPosition{
			Position: store.Position{
				MMSI:                         fmt.Sprintf("%09d", report.UserID),
				SourceClass:                  sign.SourceAIS,
				LatitudeMicros:               int32(math.Round(report.Latitude * 1e6)),
				LongitudeMicros:              int32(math.Round(report.Longitude * 1e6)),
				SpeedOverGroundMilliknots:    &speed,
				CourseOverGroundMillidegrees: &course,
				HeadingMillidegrees:          heading,
				NavStatus:                    navStatus,
				PositionAccuracy:             accuracy,
				ReceiverID:                   "aisstream.io",
				AISMessageType:               &msgType,
				ShipName:                     envelope.MetaData.ShipName,
				Classification:               string(sign.ClassificationPublic),
				ObservedAt:                   observedAt,
			},
			PayloadKey: dedupKey(fmt.Sprintf("%09d", report.UserID), msgType,
				int32(math.Round(report.Latitude*1e6)), int32(math.Round(report.Longitude*1e6)),
				speed, course, observedAt),
		})
	case "ShipStaticData", "StaticDataReport":
		var report streamStaticData
		if err := json.Unmarshal(envelope.Message, &report); err != nil {
			client.Pipeline.Metrics.Inc("geo_aisstream_decode_errors_total", nil)
			return nil
		}
		imo := ""
		if report.ImoNumber > 0 {
			imo = fmt.Sprintf("%07d", report.ImoNumber)
		}
		return client.Pipeline.HandleStatic(ctx, IngestStatic{Report: store.StaticReport{
			MMSI:                fmt.Sprintf("%09d", report.UserID),
			IMO:                 imo,
			Callsign:            report.CallSign,
			ShipName:            report.Name,
			ShipTypeCode:        int32(report.Type),
			DimensionBowM:       report.Dimension.A,
			DimensionSternM:     report.Dimension.B,
			DimensionPortM:      report.Dimension.C,
			DimensionStarboardM: report.Dimension.D,
			DraughtMillimetres:  uint32(math.Round(report.MaximumStaticDraught * 1000)),
			Destination:         report.Destination,
			EpfsType:            epfsFromCode(report.FixType),
			SourceClass:         sign.SourceAIS,
			Classification:      string(sign.ClassificationPublic),
			ObservedAt:          observedAt,
		}})
	default:
		return nil
	}
}

// epfsFromCode maps the AIS EPFS code to the contract wire value.
func epfsFromCode(code int64) string {
	switch code {
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
