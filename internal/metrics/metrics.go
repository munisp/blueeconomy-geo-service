// Package metrics provides the service's fail-closed observability: named
// atomic counters rendered in Prometheus text exposition format on /metrics.
// Dependencies on external telemetry exporters are deliberately avoided on
// the hot path; counters are the contract (quarantine volume, dedup hits,
// connector liveness).
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds named counters with a fixed label set.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Int64
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{counters: make(map[string]*atomic.Int64)}
}

// key renders name+labels as a stable exposition identifier.
func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for label := range labels {
		keys = append(keys, label)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString(name)
	builder.WriteByte('{')
	for i, label := range keys {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(label)
		builder.WriteString("=\"")
		builder.WriteString(strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n").Replace(labels[label]))
		builder.WriteByte('"')
	}
	builder.WriteByte('}')
	return builder.String()
}

// Inc increments the named counter (creating it on first use).
func (registry *Registry) Inc(name string, labels map[string]string) {
	registry.Add(name, labels, 1)
}

// Add adds delta to the named counter.
func (registry *Registry) Add(name string, labels map[string]string, delta int64) {
	identifier := key(name, labels)
	registry.mu.RLock()
	counter, ok := registry.counters[identifier]
	registry.mu.RUnlock()
	if !ok {
		registry.mu.Lock()
		counter, ok = registry.counters[identifier]
		if !ok {
			counter = &atomic.Int64{}
			registry.counters[identifier] = counter
		}
		registry.mu.Unlock()
	}
	counter.Add(delta)
}

// Snapshot returns a copy of all counter values.
func (registry *Registry) Snapshot() map[string]int64 {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	out := make(map[string]int64, len(registry.counters))
	for identifier, counter := range registry.counters {
		out[identifier] = counter.Load()
	}
	return out
}

// WritePrometheus renders the registry in Prometheus text exposition format.
func (registry *Registry) WritePrometheus(writer io.Writer) {
	snapshot := registry.Snapshot()
	identifiers := make([]string, 0, len(snapshot))
	for identifier := range snapshot {
		identifiers = append(identifiers, identifier)
	}
	sort.Strings(identifiers)
	for _, identifier := range identifiers {
		fmt.Fprintf(writer, "%s %d\n", identifier, snapshot[identifier])
	}
}
