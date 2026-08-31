// Package congestion implements the WP-10 port-congestion BASELINE
// forecaster. It is deliberately honest about what it is: a seasonal-naive +
// damped Holt exponential-smoothing baseline over the recorded
// port_queue_observations series, with residual-based prediction intervals.
// It is NOT a machine-learned model and every forecast is labelled as a
// baseline. Fail-closed: with insufficient recorded history the forecaster
// returns ErrInsufficientHistory instead of inventing numbers.
package congestion

import (
	"errors"
	"math"
	"sort"
)

// Observation is one recorded queue-length measurement.
type Observation struct {
	ObservedAtUnix int64
	QueueLength    float64
}

// PointForecast is one horizon step with an 80%/95% prediction interval.
type PointForecast struct {
	Step        int     `json:"step"`
	AtUnix      int64   `json:"atUnix"`
	QueueLength float64 `json:"queueLength"`
	Lower80     float64 `json:"lower80"`
	Upper80     float64 `json:"upper80"`
	Lower95     float64 `json:"lower95"`
	Upper95     float64 `json:"upper95"`
}

// Forecast is the labelled output.
type Forecast struct {
	PortCode string `json:"portCode"`
	// Model is the honest model identity, e.g.
	// "seasonal-naive+holt-damped baseline v1".
	Model string `json:"model"`
	// SeasonalPeriod is the season length in observation steps that was
	// actually used (0 = non-seasonal fallback).
	SeasonalPeriod int `json:"seasonalPeriod"`
	// TrainedOn is the number of recorded observations used.
	TrainedOn    int             `json:"trainedOn"`
	Points       []PointForecast `json:"points"`
	BacktestMAE  float64         `json:"backtestMAE"`
	BacktestMAPE float64         `json:"backtestMAPE"`
}

// ErrInsufficientHistory is returned when the recorded series is too short
// to fit even the non-seasonal baseline (fewer than minHistory points).
var ErrInsufficientHistory = errors.New("INSUFFICIENT_HISTORY: recorded queue series too short to forecast honestly")

const minHistory = 8

// ModelID is the promoted model identity for the registry/model card.
const ModelID = "port-congestion-baseline"
const ModelVersion = "1.0.0"
const ModelLabel = "seasonal-naive+holt-damped baseline v" + ModelVersion

// ForecastSeries fits the baseline on observations (sorted ascending by
// time; will be sorted defensively) and projects horizon steps spaced
// stepSeconds apart. seasonalPeriod is in observation steps (e.g. 24 for a
// daily cycle of hourly observations); pass 0 to disable the seasonal
// component. z80/z95 are the Gaussian critical values for the intervals.
func ForecastSeries(portCode string, observations []Observation, stepSeconds int64, horizon int, seasonalPeriod int) (Forecast, error) {
	if len(observations) < minHistory {
		return Forecast{}, ErrInsufficientHistory
	}
	if horizon <= 0 || stepSeconds <= 0 {
		return Forecast{}, errors.New("horizon and stepSeconds must be positive")
	}
	obs := make([]Observation, len(observations))
	copy(obs, observations)
	sort.Slice(obs, func(i, j int) bool { return obs[i].ObservedAtUnix < obs[j].ObservedAtUnix })
	y := make([]float64, len(obs))
	for i, o := range obs {
		y[i] = o.QueueLength
	}
	if seasonalPeriod < 2 || len(y) < 2*seasonalPeriod {
		seasonalPeriod = 0 // honest non-seasonal fallback
	}

	// 1) Seasonal indices (multiplicative, normalized) from detrended ratio.
	seasonal := make([]float64, 0)
	deseasonalized := y
	if seasonalPeriod > 0 {
		seasonal = seasonalIndices(y, seasonalPeriod)
		deseasonalized = make([]float64, len(y))
		for i, v := range y {
			s := seasonal[i%seasonalPeriod]
			if s <= 0 {
				s = 1
			}
			deseasonalized[i] = v / s
		}
	}

	// 2) Damped Holt linear smoothing on the (de)seasonalized series.
	level, trend, fitted := holtDamped(deseasonalized, 0.4, 0.1, 0.9)

	// 3) Residual spread for prediction intervals (1-step in-sample).
	residualStd := 0.0
	count := 0
	for i := 1; i < len(y); i++ {
		scale := 1.0
		if seasonalPeriod > 0 {
			scale = seasonal[i%seasonalPeriod]
		}
		resid := y[i] - fitted[i]*scale
		residualStd += resid * resid
		count++
	}
	if count > 0 {
		residualStd = math.Sqrt(residualStd / float64(count))
	}

	// 4) Backtest over the last min(25%, 48) points (expanding origin).
	mae, mape := backtest(y, seasonalPeriod)

	lastAt := obs[len(obs)-1].ObservedAtUnix
	const phi = 0.9 // damping, must match holtDamped call
	points := make([]PointForecast, horizon)
	dampedSum := 0.0
	for h := 1; h <= horizon; h++ {
		dampedSum += math.Pow(phi, float64(h))
		base := level + dampedSum*trend
		scale := 1.0
		if seasonalPeriod > 0 {
			scale = seasonal[(len(y)+h-1)%seasonalPeriod]
		}
		mean := base * scale
		// Interval widens with sqrt(h) (random-walk error growth).
		w := residualStd * math.Sqrt(float64(h))
		points[h-1] = PointForecast{
			Step:        h,
			AtUnix:      lastAt + int64(h)*stepSeconds,
			QueueLength: clampNonNeg(mean),
			Lower80:     clampNonNeg(mean - 1.2816*w),
			Upper80:     clampNonNeg(mean + 1.2816*w),
			Lower95:     clampNonNeg(mean - 1.9599*w),
			Upper95:     clampNonNeg(mean + 1.9599*w),
		}
	}
	return Forecast{
		PortCode:       portCode,
		Model:          ModelLabel,
		SeasonalPeriod: seasonalPeriod,
		TrainedOn:      len(y),
		Points:         points,
		BacktestMAE:    mae,
		BacktestMAPE:   mape,
	}, nil
}

// seasonalIndices computes normalized multiplicative seasonal indices by
// averaging the ratio of each season position to its centred moving average.
func seasonalIndices(y []float64, period int) []float64 {
	sums := make([]float64, period)
	counts := make([]int, period)
	for i := 0; i+period < len(y); i++ {
		windowMean := 0.0
		for k := 0; k < period; k++ {
			windowMean += y[i+k]
		}
		windowMean /= float64(period)
		if windowMean <= 0 {
			continue
		}
		for k := 0; k < period; k++ {
			sums[(i+k)%period] += y[i+k] / windowMean
			counts[(i+k)%period]++
		}
	}
	indices := make([]float64, period)
	mean := 0.0
	for p := 0; p < period; p++ {
		if counts[p] > 0 {
			indices[p] = sums[p] / float64(counts[p])
		} else {
			indices[p] = 1
		}
		mean += indices[p]
	}
	mean /= float64(period)
	if mean <= 0 {
		mean = 1
	}
	for p := range indices {
		indices[p] /= mean
	}
	return indices
}

// holtDamped fits Holt's linear trend with damping phi. Returns final level,
// final trend and the 1-step in-sample fitted values (fitted[i] is the
// forecast for y[i] made at i-1; fitted[0] = y[0]).
func holtDamped(y []float64, alpha, beta, phi float64) (level, trend float64, fitted []float64) {
	level = y[0]
	if len(y) > 1 {
		trend = y[1] - y[0]
	}
	fitted = make([]float64, len(y))
	fitted[0] = y[0]
	for i := 1; i < len(y); i++ {
		fitted[i] = level + phi*trend
		prevLevel := level
		level = alpha*y[i] + (1-alpha)*(level+phi*trend)
		trend = beta*(level-prevLevel) + (1-beta)*phi*trend
	}
	return level, trend, fitted
}

// backtest does an expanding-origin 1-step backtest over the tail of the
// series using the seasonal-naive (or naive) reference forecast — the honest
// accuracy label for a baseline model. Returns (MAE, MAPE in percent).
func backtest(y []float64, seasonalPeriod int) (mae, mape float64) {
	n := len(y)
	window := n / 4
	if window > 48 {
		window = 48
	}
	if window < 4 {
		window = 4
	}
	start := n - window
	errSum, apeSum := 0.0, 0.0
	used := 0
	for i := start; i < n; i++ {
		ref := i - 1
		if seasonalPeriod > 0 && i-seasonalPeriod >= 0 {
			ref = i - seasonalPeriod
		}
		predicted := y[ref]
		actual := y[i]
		errSum += math.Abs(actual - predicted)
		if actual > 0 {
			apeSum += math.Abs(actual-predicted) / actual * 100
		}
		used++
	}
	if used == 0 {
		return 0, 0
	}
	return errSum / float64(used), apeSum / float64(used)
}

func clampNonNeg(v float64) float64 {
	if v < 0 || math.IsNaN(v) {
		return 0
	}
	return v
}
