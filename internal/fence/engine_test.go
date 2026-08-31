package fence

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// mombasa-ish test fence: 1° square in micro-degrees, closed ring.
func testSquare() []Point {
	return []Point{
		{LatMicros: -4_000_000, LonMicros: 39_000_000},
		{LatMicros: -4_000_000, LonMicros: 40_000_000},
		{LatMicros: -5_000_000, LonMicros: 40_000_000},
		{LatMicros: -5_000_000, LonMicros: 39_000_000},
		{LatMicros: -4_000_000, LonMicros: 39_000_000},
	}
}

func inside() Point  { return Point{LatMicros: -4_500_000, LonMicros: 39_500_000} }
func outside() Point { return Point{LatMicros: -6_000_000, LonMicros: 39_500_000} }

func TestValidateGeometry(t *testing.T) {
	require.NoError(t, ValidateGeometry(testSquare()))
	require.Error(t, ValidateGeometry(nil))
	require.Error(t, ValidateGeometry(testSquare()[:3]))
	open := testSquare()[:4]
	require.Error(t, ValidateGeometry(open), "open ring must be rejected")
	bad := testSquare()
	bad[1].LatMicros = 91_000_000
	require.Error(t, ValidateGeometry(bad))
}

func TestContains(t *testing.T) {
	sq := testSquare()
	require.True(t, Contains(sq, inside()))
	require.False(t, Contains(sq, outside()))
	// Boundary counts as inside.
	require.True(t, Contains(sq, Point{LatMicros: -4_000_000, LonMicros: 39_500_000}))
	// Far side of the world.
	require.False(t, Contains(sq, Point{LatMicros: 4_500_000, LonMicros: -39_500_000}))
}

func TestEnterExitTransitions(t *testing.T) {
	engine := NewEngine()
	f := Fence{GeofenceID: "port.mombasa.approach", Version: 3, Vertices: testSquare()}

	ev := engine.Observe("205123000", outside(), 5000, 1000, []Fence{f})
	require.Empty(t, ev, "starting outside emits nothing")

	ev = engine.Observe("205123000", inside(), 5000, 1010, []Fence{f})
	require.Equal(t, []Event{{GeofenceID: "port.mombasa.approach", Version: 3, Type: EventEnter, OccurredAtUnix: 1010}}, ev)

	ev = engine.Observe("205123000", inside(), 5000, 1020, []Fence{f})
	require.Empty(t, ev, "staying inside emits nothing")

	ev = engine.Observe("205123000", outside(), 5000, 1030, []Fence{f})
	require.Equal(t, []Event{{GeofenceID: "port.mombasa.approach", Version: 3, Type: EventExit, OccurredAtUnix: 1030}}, ev)
}

func TestDwellDetection(t *testing.T) {
	engine := NewEngine()
	f := Fence{
		GeofenceID: "anchorage.kilindini", Version: 1, Vertices: testSquare(),
		DwellThresholdSeconds: 600, DwellSpeedGateMilliknots: 1000,
	}
	// Enter moving fast — no dwell clock.
	require.Len(t, engine.Observe("205123000", inside(), 8000, 1000, []Fence{f}), 1)
	// Slow below gate at t=1100; threshold 600s reached at t=1700.
	require.Empty(t, engine.Observe("205123000", inside(), 500, 1100, []Fence{f}))
	require.Empty(t, engine.Observe("205123000", inside(), 500, 1600, []Fence{f}))
	ev := engine.Observe("205123000", inside(), 500, 1700, []Fence{f})
	require.Equal(t, []Event{{GeofenceID: "anchorage.kilindini", Version: 1, Type: EventDwell, OccurredAtUnix: 1700}}, ev)
	// One dwell per stay.
	require.Empty(t, engine.Observe("205123000", inside(), 500, 2400, []Fence{f}))
	// Speed gate resets the clock.
	engine2 := NewEngine()
	engine2.Observe("205123000", inside(), 500, 1000, []Fence{f})
	engine2.Observe("205123000", inside(), 9000, 1500, []Fence{f}) // fast: reset
	require.Empty(t, engine2.Observe("205123000", inside(), 500, 1600, []Fence{f}))
	require.Empty(t, engine2.Observe("205123000", inside(), 500, 2000, []Fence{f}), "400s < 600s after reset")
	require.Len(t, engine2.Observe("205123000", inside(), 500, 2200, []Fence{f}), 1)
	// Unknown speed (negative) never dwells.
	engine3 := NewEngine()
	engine3.Observe("205123000", inside(), -1, 1000, []Fence{f})
	require.Empty(t, engine3.Observe("205123000", inside(), -1, 5000, []Fence{f}))
}

func TestOutOfOrderReportsIgnored(t *testing.T) {
	engine := NewEngine()
	f := Fence{GeofenceID: "z", Version: 1, Vertices: testSquare()}
	engine.Observe("205123000", inside(), 1000, 2000, []Fence{f})
	require.Empty(t, engine.Observe("205123000", outside(), 1000, 1999, []Fence{f}),
		"out-of-order report must not emit a phantom EXIT")
}

func TestDeterministicMultiFenceOrdering(t *testing.T) {
	engine := NewEngine()
	f1 := Fence{GeofenceID: "b.fence", Version: 1, Vertices: testSquare()}
	f2 := Fence{GeofenceID: "a.fence", Version: 2, Vertices: testSquare()}
	ev := engine.Observe("205123000", inside(), 1000, 100, []Fence{f1, f2})
	require.Equal(t, "a.fence", ev[0].GeofenceID)
	require.Equal(t, "b.fence", ev[1].GeofenceID)
}

func TestFailClosedInputs(t *testing.T) {
	engine := NewEngine()
	f := Fence{GeofenceID: "z", Version: 1, Vertices: testSquare()}
	require.Empty(t, engine.Observe("", inside(), 1000, 100, []Fence{f}))
	require.Empty(t, engine.Observe("205123000", inside(), 1000, 0, []Fence{f}))
}
