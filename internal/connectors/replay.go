package connectors

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/decode"
)

// ReplayConnector replays a recorded NMEA file (GEO_REPLAY_FILE) through
// the pipeline for development and integration testing. Per the standing
// fail-closed doctrine it REFUSES to run in the prod profile: fixtures are
// synthetic, format-valid recordings of documented synthetic vessels and
// must never be presented as live traffic.
type ReplayConnector struct {
	File     string
	AppEnv   string
	Pipeline *Pipeline
	Logger   *log.Logger
	// Interval paces replayed sentences; 0 replays as fast as possible.
	Interval time.Duration
}

// Run streams the replay file once (or until ctx is cancelled).
func (connector *ReplayConnector) Run(ctx context.Context) error {
	if connector.Pipeline == nil {
		return errors.New("replay connector requires a pipeline")
	}
	if strings.EqualFold(strings.TrimSpace(connector.AppEnv), "prod") || strings.EqualFold(strings.TrimSpace(connector.AppEnv), "production") {
		return errors.New("GEO_REPLAY_FILE is forbidden when APP_ENV=prod (synthetic fixtures must never enter the live path)")
	}
	if strings.TrimSpace(connector.File) == "" {
		return errors.New("replay connector requires a file")
	}
	logger := connector.Logger
	if logger == nil {
		logger = log.Default()
	}
	file, err := os.Open(connector.File)
	if err != nil {
		return fmt.Errorf("open replay file: %w", err)
	}
	defer file.Close()
	decoder := decode.NewDecoder()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	replayed := 0
	for scanner.Scan() {
		if ctx.Err() != nil {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		frame, err := decoder.Sentence(line)
		if err != nil {
			if errors.Is(err, decode.ErrIncompleteFragment) {
				continue
			}
			connector.Pipeline.Metrics.Inc("geo_replay_decode_errors_total", nil)
			continue
		}
		observedAt := time.Now().UTC()
		if frame.TagUnixSeconds > 0 {
			observedAt = time.Unix(frame.TagUnixSeconds, 0).UTC()
		}
		if frame.Position != nil {
			if err := connector.Pipeline.HandlePosition(ctx, positionFromFrame(frame.Position, "replay-file", observedAt)); err != nil {
				connector.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "replay"})
			} else {
				replayed++
			}
		}
		if frame.Static != nil {
			if err := connector.Pipeline.HandleStatic(ctx, staticFromFrame(frame.Static, observedAt)); err != nil {
				connector.Pipeline.Metrics.Inc("geo_pipeline_errors_total", map[string]string{"connector": "replay"})
			}
		}
		if connector.Interval > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(connector.Interval):
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read replay file: %w", err)
	}
	logger.Printf("replay: %d positions replayed from %s (synthetic fixture data, not live traffic)", replayed, connector.File)
	connector.Pipeline.Metrics.Add("geo_replay_positions_total", nil, int64(replayed))
	return nil
}
