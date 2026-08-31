// Package fence is the WP-10 geofence transition engine: pure, deterministic
// enter/exit/dwell detection over fixed-point micro-degree geometry. The
// engine is deliberately storage-free — PostGIS persists fence geometry and
// the emitted events, but the transition decision is computed here so it is
// exhaustively unit-testable without a database and can never silently
// disagree with the persisted geometry (vertices_micros is the same ring
// mirrored into geofences.geom at the write boundary).
package fence

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Point is a fixed-point micro-degree position.
type Point struct {
	LatMicros int32
	LonMicros int32
}

// Fence is one ACTIVE geofence version.
type Fence struct {
	GeofenceID string
	Version    int
	// Vertices is the closed polygon ring in micro-degrees. The ring may be
	// given open (first != last); Contains closes it internally.
	Vertices []Point
	// DwellThresholdSeconds > 0 enables DWELL detection: a vessel that stays
	// inside the fence at or below DwellSpeedGateMilliknots for at least the
	// threshold emits exactly one DWELL event per continuous stay.
	DwellThresholdSeconds    int
	DwellSpeedGateMilliknots int
}

// ValidateGeometry enforces the ring contract fail-closed.
func ValidateGeometry(vertices []Point) error {
	if len(vertices) < 4 {
		return fmt.Errorf("fence ring must have at least 4 vertices (closed triangle), got %d", len(vertices))
	}
	for i, v := range vertices {
		if v.LatMicros < -90_000_000 || v.LatMicros > 90_000_000 {
			return fmt.Errorf("vertex %d latitude %d out of range", i, v.LatMicros)
		}
		if v.LonMicros < -180_000_000 || v.LonMicros > 180_000_000 {
			return fmt.Errorf("vertex %d longitude %d out of range", i, v.LonMicros)
		}
	}
	if vertices[0] != vertices[len(vertices)-1] {
		return errors.New("fence ring must be closed (first vertex equals last)")
	}
	return nil
}

// Contains answers point-in-polygon with an even-odd ray cast in integer
// micro-degree space. Boundary points count as inside (a vessel exactly on
// the fence line is inside its governed area).
func Contains(vertices []Point, p Point) bool {
	n := len(vertices)
	if n < 4 {
		return false
	}
	inside := false
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		vi, vj := vertices[i], vertices[j]
		// Boundary check: collinear and within the segment's bounding box.
		if onSegment(vj, vi, p) {
			return true
		}
		if (vi.LatMicros > p.LatMicros) != (vj.LatMicros > p.LatMicros) {
			// x-coordinate of the edge/ray intersection, computed in int64 to
			// avoid overflow: lon = vi.lon + (p.lat-vi.lat)*(vj.lon-vi.lon)/(vj.lat-vi.lat)
			num := int64(p.LatMicros-vi.LatMicros) * int64(vj.LonMicros-vi.LonMicros)
			den := int64(vj.LatMicros - vi.LatMicros)
			var x int64
			if den > 0 {
				x = int64(vi.LonMicros) + (num+den/2)/den
			} else {
				x = int64(vi.LonMicros) + (num-den/2)/den
			}
			if int64(p.LonMicros) < x {
				inside = !inside
			}
		}
	}
	return inside
}

func onSegment(a, b, p Point) bool {
	cross := int64(b.LonMicros-a.LonMicros)*int64(p.LatMicros-a.LatMicros) -
		int64(b.LatMicros-a.LatMicros)*int64(p.LonMicros-a.LonMicros)
	if cross != 0 {
		return false
	}
	return p.LonMicros >= min32(a.LonMicros, b.LonMicros) && p.LonMicros <= max32(a.LonMicros, b.LonMicros) &&
		p.LatMicros >= min32(a.LatMicros, b.LatMicros) && p.LatMicros <= max32(a.LatMicros, b.LatMicros)
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// EventType is the fence transition kind.
type EventType string

const (
	EventEnter EventType = "ENTER"
	EventExit  EventType = "EXIT"
	EventDwell EventType = "DWELL"
)

// Event is one emitted transition, attributed to the exact fence version.
type Event struct {
	GeofenceID string
	Version    int
	Type       EventType
	// OccurredAtUnix is the observation timestamp (seconds); DWELL events
	// carry the moment the threshold was reached.
	OccurredAtUnix int64
}

// vesselFenceState is the per-(vessel, fence) memory.
type vesselFenceState struct {
	inside       bool
	enteredAt    int64
	dwellEmitted bool
	// slowSince is the first timestamp of the current continuous below-gate
	// speed stretch inside the fence; 0 means "not currently slow".
	slowSince int64
}

// Engine keeps per-vessel fence membership so a stream of position reports
// becomes a stream of ENTER/EXIT/DWELL transitions. It is fail-closed by
// construction: no positions are ever fabricated, and a report that
// predates the vessel's latest seen report is ignored (out-of-order
// protection) rather than emitting phantom transitions.
type Engine struct {
	// state[vesselKey][geofenceID]
	state map[string]map[string]*vesselFenceState
	last  map[string]int64
}

// NewEngine builds an empty engine.
func NewEngine() *Engine {
	return &Engine{state: map[string]map[string]*vesselFenceState{}, last: map[string]int64{}}
}

// Observe folds one position report into the engine and returns the
// transitions it caused, deterministically ordered by (geofenceID, type).
// fences must be the currently ACTIVE versions. speedMilliknots < 0 means
// "speed unknown" — unknown speed never counts toward dwell (fail-closed).
func (engine *Engine) Observe(vesselKey string, p Point, speedMilliknots int, atUnix int64, fences []Fence) []Event {
	vesselKey = strings.TrimSpace(vesselKey)
	if vesselKey == "" || atUnix <= 0 {
		return nil
	}
	if lastSeen, ok := engine.last[vesselKey]; ok && atUnix < lastSeen {
		return nil // out-of-order report: ignored, never rewinds state
	}
	engine.last[vesselKey] = atUnix
	perVessel, ok := engine.state[vesselKey]
	if !ok {
		perVessel = map[string]*vesselFenceState{}
		engine.state[vesselKey] = perVessel
	}

	sorted := make([]Fence, len(fences))
	copy(sorted, fences)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GeofenceID < sorted[j].GeofenceID })

	var events []Event
	for _, f := range sorted {
		st, ok := perVessel[f.GeofenceID]
		if !ok {
			st = &vesselFenceState{}
			perVessel[f.GeofenceID] = st
		}
		inside := Contains(f.Vertices, p)
		switch {
		case inside && !st.inside:
			events = append(events, Event{GeofenceID: f.GeofenceID, Version: f.Version, Type: EventEnter, OccurredAtUnix: atUnix})
			st.inside = true
			st.enteredAt = atUnix
			st.dwellEmitted = false
			st.slowSince = 0
		case !inside && st.inside:
			events = append(events, Event{GeofenceID: f.GeofenceID, Version: f.Version, Type: EventExit, OccurredAtUnix: atUnix})
			st.inside = false
			st.slowSince = 0
		}
		if inside && f.DwellThresholdSeconds > 0 && !st.dwellEmitted && speedMilliknots >= 0 && speedMilliknots <= f.DwellSpeedGateMilliknots {
			if st.slowSince == 0 {
				st.slowSince = atUnix
			}
			if atUnix-st.slowSince >= int64(f.DwellThresholdSeconds) {
				events = append(events, Event{GeofenceID: f.GeofenceID, Version: f.Version, Type: EventDwell, OccurredAtUnix: atUnix})
				st.dwellEmitted = true
			}
		} else if inside && speedMilliknots > f.DwellSpeedGateMilliknots && speedMilliknots >= 0 {
			// Moving above the gate resets the dwell clock (but a later
			// below-gate stretch may still trigger within the same stay).
			st.slowSince = 0
		}
	}
	return events
}
