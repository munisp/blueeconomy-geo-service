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
	// Perfectly periodic series: seasonal-naive backtest error must be ~0.
	y := make([]Observation, 96)
	for i := range y {
		v := 5 + 3*math.Sin(2*math.Pi*float64(i%24)/24)
		y[i] = Observation{ObservedAtUnix: int64(i) * 3600, QueueLength: v}
	}
	series := make([]float64, len(y))
	for i, o := range y {
		series[i] = o.QueueLength
	}
	mae, mape := backtest(series, 24)
	require.InDelta(t, 0.0, mae, 1e-9)
	require.InDelta(t, 0.0, mape, 1e-9)
}
