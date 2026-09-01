package congestion

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordedFixture synthesizes the SHAPE of a recorded eCallUp queue history:
// a daily season (period 24, hourly steps) + level + trend. Used as the
// backtest fixture; it is test data, never served to clients.
func recordedFixture(n int, period int) []Observation {
	obs := make([]Observation, n)
	for i := 0; i < n; i++ {
		season := 6.0 + 4.0*math.Sin(2*math.Pi*float64(i%period)/float64(period))
		level := 8.0 + 0.02*float64(i)
		v := season + level
		if v < 0 {
			v = 0
		}
		obs[i] = Observation{ObservedAtUnix: 1_700_000_000 + int64(i)*3600, QueueLength: v}
	}
	return obs
}

func TestForecastSeasonalSeries(t *testing.T) {
	obs := recordedFixture(24*14, 24) // two weeks hourly
	fc, err := ForecastSeries("KEMBA", obs, 3600, 24, 24)
	require.NoError(t, err)
	require.Equal(t, "KEMBA", fc.PortCode)
	require.Equal(t, ModelLabel, fc.Model)
	require.Equal(t, 24, fc.SeasonalPeriod)
	require.Equal(t, len(obs), fc.TrainedOn)
	require.Len(t, fc.Points, 24)
	for i, p := range fc.Points {
		require.Equal(t, i+1, p.Step)
		require.GreaterOrEqual(t, p.QueueLength, 0.0)
		require.LessOrEqual(t, p.Lower80, p.QueueLength)
		require.GreaterOrEqual(t, p.Upper80, p.QueueLength)
		require.LessOrEqual(t, p.Lower95, p.Lower80)
		require.GreaterOrEqual(t, p.Upper95, p.Upper80)
		require.Equal(t, obs[len(obs)-1].ObservedAtUnix+int64(i+1)*3600, p.AtUnix)
	}
	// Intervals widen with the horizon.
	require.Less(t, fc.Points[0].Upper95-fc.Points[0].Lower95,
		fc.Points[23].Upper95-fc.Points[23].Lower95)
	// Backtest on a clean seasonal series must be decent for a baseline.
	t.Logf("backtest MAE=%.3f MAPE=%.2f%%", fc.BacktestMAE, fc.BacktestMAPE)
	require.Less(t, fc.BacktestMAPE, 15.0, "baseline MAPE on recorded seasonal history must be < 15%")
}

func TestForecastNonSeasonalFallback(t *testing.T) {
	obs := recordedFixture(20, 24)
	fc, err := ForecastSeries("KEMBA", obs, 3600, 6, 24)
	require.NoError(t, err)
	require.Equal(t, 0, fc.SeasonalPeriod, "series shorter than 2 seasons must fall back honestly")
}

func TestForecastInsufficientHistoryFailsClosed(t *testing.T) {
	_, err := ForecastSeries("KEMBA", recordedFixture(7, 24), 3600, 6, 24)
	require.ErrorIs(t, err, ErrInsufficientHistory)
	_, err = ForecastSeries("KEMBA", nil, 3600, 6, 24)
	require.ErrorIs(t, err, ErrInsufficientHistory)
}

func TestForecastRejectsBadArgs(t *testing.T) {
	obs := recordedFixture(30, 24)
	_, err := ForecastSeries("KEMBA", obs, 0, 6, 24)
	require.Error(t, err)
	_, err = ForecastSeries("KEMBA", obs, 3600, 0, 24)
	require.Error(t, err)
}

func TestForecastHandlesUnsortedInput(t *testing.T) {
	obs := recordedFixture(40, 24)
	obs[10], obs[20] = obs[20], obs[10]
	fc, err := ForecastSeries("KEMBA", obs, 3600, 3, 0)
	require.NoError(t, err)
	require.Len(t, fc.Points, 3)
}

func TestSeasonalIndicesNormalized(t *testing.T) {
	y := make([]float64, 96)
	for i := range y {
		y[i] = 10 + 5*math.Sin(2*math.Pi*float64(i%24)/24)
	}
	idx := seasonalIndices(y, 24)
	mean := 0.0
	for _, v := range idx {
		mean += v
	}
	require.InDelta(t, 1.0, mean/float64(len(idx)), 1e-9)
}

func TestBacktestPerfectSeasonal(t *testing.T) {
	// Perfectly periodic series: both the fitted model and the
	// seasonal-naive reference must score ~0 error.
	series := make([]float64, 96)
	for i := range series {
		series[i] = 5 + 3*math.Sin(2*math.Pi*float64(i%24)/24)
	}
	scores := backtest(series, 24)
	require.InDelta(t, 0.0, scores.naiveMAE, 1e-9)
	require.InDelta(t, 0.0, scores.naiveMAPE, 1e-9)
	require.InDelta(t, 0.0, scores.modelMAE, 1e-6)
	require.InDelta(t, 0.0, scores.modelMAPE, 1e-6)
}

func TestBacktestScoresFittedModelNotNaive(t *testing.T) {
	// Seasonal cycle (period 24) PLUS a trend: the seasonal-naive
	// reference misses the trend accumulated over a full season at every
	// origin, while the fitted seasonal-indices + damped-Holt model
	// tracks both. The reported model backtest MUST reflect the fitted
	// model (clearly better than naive here), not the naive reference
	// it used to measure.
	series := make([]float64, 24*14)
	for i := range series {
		series[i] = 6 + 4*math.Sin(2*math.Pi*float64(i%24)/24) + 8 + 0.1*float64(i)
	}
	scores := backtest(series, 24)
	require.InDelta(t, 2.4, scores.naiveMAE, 0.05,
		"seasonal-naive error on this fixture is exactly one season of trend (24 x 0.1)")
	require.Less(t, scores.modelMAE, 0.5*scores.naiveMAE,
		"fitted-model backtest must score the fitted model, which clearly beats naive on a seasonal+trend series")
	require.Less(t, scores.modelMAPE, scores.naiveMAPE)
}

func TestForecastBacktestReportsModelAndBaseline(t *testing.T) {
	// Seasonal series with a trend: the API must report BOTH the fitted
	// model accuracy and the naive baseline, and the fitted model must be
	// at least as good as the baseline it replaces.
	obs := recordedFixture(24*14, 24)
	fc, err := ForecastSeries("KEMBA", obs, 3600, 24, 24)
	require.NoError(t, err)
	require.Greater(t, fc.BacktestNaiveMAE, 0.0, "naive baseline must be reported for comparison")
	require.LessOrEqual(t, fc.BacktestMAE, fc.BacktestNaiveMAE,
		"fitted Holt+seasonal model must not lose to the naive reference on a clean seasonal+trend series")
	require.LessOrEqual(t, fc.BacktestMAPE, fc.BacktestNaiveMAPE)
}

func TestPredictOneStepMatchesServedModel(t *testing.T) {
	// The backtest's per-origin refit must be the SAME model the API
	// serves: a 1-step predictOneStep on the full training history must
	// reproduce the first point of the served forecast.
	obs := recordedFixture(24*14, 24)
	y := make([]float64, len(obs))
	for i, o := range obs {
		y[i] = o.QueueLength
	}
	fc, err := ForecastSeries("KEMBA", obs, 3600, 24, 24)
	require.NoError(t, err)
	require.InDelta(t, fc.Points[0].QueueLength, predictOneStep(y, 24, len(y)), 1e-9,
		"backtest refit must be identical to the served model pipeline")
}
