package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeTransitionsEnterAndExit(t *testing.T) {
	transitions := ComputeTransitions(
		[]string{"zone-apapa", "zone-bonny"},
		[]string{"zone-apapa", "zone-lagos"})
	require.Equal(t, []Transition{
		{ZoneID: "zone-bonny", Direction: "ENTER"},
		{ZoneID: "zone-lagos", Direction: "EXIT"},
	}, transitions)
}

func TestComputeTransitionsNoChange(t *testing.T) {
	require.Empty(t, ComputeTransitions([]string{"a", "b"}, []string{"b", "a"}))
	require.Empty(t, ComputeTransitions(nil, nil))
}

func TestComputeTransitionsFirstSightingEntersAll(t *testing.T) {
	transitions := ComputeTransitions([]string{"zone-a"}, nil)
	require.Equal(t, []Transition{{ZoneID: "zone-a", Direction: "ENTER"}}, transitions)
}

func TestComputeTransitionsLeavingAllZones(t *testing.T) {
	transitions := ComputeTransitions(nil, []string{"zone-a", "zone-b"})
	require.Equal(t, []Transition{
		{ZoneID: "zone-a", Direction: "EXIT"},
		{ZoneID: "zone-b", Direction: "EXIT"},
	}, transitions)
}

func TestRenderMicrosExact(t *testing.T) {
	require.Equal(t, "6.418000", renderMicros(6418000))
	require.Equal(t, "3.372500", renderMicros(3372500))
	require.Equal(t, "-0.000001", renderMicros(-1))
	require.Equal(t, "0.000000", renderMicros(0))
	require.Equal(t, "-180.000000", renderMicros(-180000000))
	require.Equal(t, "90.000000", renderMicros(90000000))
}

func TestPointWKT(t *testing.T) {
	require.Equal(t, "POINT(3.372500 6.418000)", pointWKT(6418000, 3372500))
}
