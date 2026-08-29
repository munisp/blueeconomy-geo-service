// Package gtfsrt is the AIS→GTFS-Realtime adapter: it builds the three
// feed types (VehiclePositions, TripUpdates, Alerts) from the tenant's
// transit registry joined against the shared AIS position plane.
//
// FAIL-CLOSED DOCTRINE (Citizen Services Advisory §5): nothing is ever
// fabricated. A route-assigned vessel with no position, or with a position
// older than the staleness threshold, is OMITTED from the feed — honest
// absence, with the omission counted in metrics — never interpolated,
// never extrapolated. ETAs are computed from reported positions and
// speeds only; a vessel with zero speed observations falls back to the
// route default speed and its trip update is marked low-confidence
// (SCHEDULED + 300s uncertainty) instead of faking a live prediction. A
// vessel whose smoothed speed is below the movement threshold produces NO
// trip update (NO_SHOW-style omission) — a docked or drifting vessel's
// ETA is unknowable and must not be invented.
package gtfsrt

import (
	"context"
	"errors"
	"fmt"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// tracer returns the GTFS-RT feed builder tracer. With telemetry disabled
// the global provider is a no-op: feed-build spans are non-recording.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/munisp/blueeconomy-geo-service/internal/gtfsrt")
}

// GtfsRealtimeVersion is the spec version stamped into every FeedHeader.
const GtfsRealtimeVersion = "2.0"

// Config tunes the adapter. All thresholds have fail-safe defaults.
type Config struct {
	// StaleAfter is the maximum age of a usable position (default 120s).
	// Older positions → the entity is omitted from the feed.
	StaleAfter time.Duration
	// SnapMaxMeters is the maximum off-route distance for the
	// nearest-stop snap (default 200m). Beyond it the vessel is not
	// attributed to any stop and no ETA is computed.
	SnapMaxMeters float64
	// StopArriveMeters is the radius within which a slow vessel counts as
	// STOPPED_AT a stop (default 20m, per the published trip-matching
	// reference: stopped within 20m at ~zero speed).
	StopArriveMeters float64
	// StopSpeedMilliknots is the SOG below which a vessel within
	// StopArriveMeters of a stop is reported STOPPED_AT (default 500 =
	// 0.5 kn).
	StopSpeedMilliknots uint32
	// ETAMinSpeedMilliknots is the smoothed-speed floor for computing
	// ETAs (default 1000 = 1.0 kn). Below it the vessel is "not moving"
	// and its trip update is omitted (documented NO_SHOW-style behavior).
	ETAMinSpeedMilliknots uint32
	// SpeedSampleCount is N of the rolling median (default 5).
	SpeedSampleCount int
	// TripMatchPreSlack / TripMatchPostSlack extend a trip's service
	// window for matching (defaults 15m / 30m).
	TripMatchPreSlack  time.Duration
	TripMatchPostSlack time.Duration
	// Now is the clock (test hook; defaults to time.Now).
	Now func() time.Time
}

func (config Config) withDefaults() Config {
	if config.StaleAfter <= 0 {
		config.StaleAfter = 120 * time.Second
	}
	if config.SnapMaxMeters <= 0 {
		config.SnapMaxMeters = 200
	}
	if config.StopArriveMeters <= 0 {
		config.StopArriveMeters = 20
	}
	if config.StopSpeedMilliknots == 0 {
		config.StopSpeedMilliknots = 500
	}
	if config.ETAMinSpeedMilliknots == 0 {
		config.ETAMinSpeedMilliknots = 1000
	}
	if config.SpeedSampleCount <= 0 {
		config.SpeedSampleCount = 5
	}
	if config.TripMatchPreSlack <= 0 {
		config.TripMatchPreSlack = 15 * time.Minute
	}
	if config.TripMatchPostSlack <= 0 {
		config.TripMatchPostSlack = 30 * time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return config
}

// Builder constructs the three GTFS-RT feeds.
type Builder struct {
	store   *store.Store
	metrics *metrics.Registry
	config  Config
}

// NewBuilder wires the adapter fail-closed.
func NewBuilder(storage *store.Store, registry *metrics.Registry, config Config) (*Builder, error) {
	if storage == nil {
		return nil, errors.New("gtfsrt store is required")
	}
	if registry == nil {
		return nil, errors.New("gtfsrt metrics registry is required")
	}
	return &Builder{store: storage, metrics: registry, config: config.withDefaults()}, nil
}

func (builder *Builder) now() time.Time { return builder.config.Now().UTC() }

func feedHeader(now time.Time) *gtfs.FeedHeader {
	return &gtfs.FeedHeader{
		GtfsRealtimeVersion: proto.String(GtfsRealtimeVersion),
		Incrementality:      gtfs.FeedHeader_FULL_DATASET.Enum(),
		Timestamp:           proto.Uint64(uint64(now.Unix())),
	}
}

// stopVisit pairs a scheduled visit with the stop's coordinates.
type stopVisit struct {
	stopTime store.TransitStopTime
	stop     store.TransitStop
}

// tripPlan is one trip with its ordered visits and precomputed path.
type tripPlan struct {
	trip   store.TransitTrip
	visits []stopVisit
	path   RoutePath
}

// routePlan is one active route with its agency and trip plans.
type routePlan struct {
	route  store.TransitRoute
	agency *store.TransitAgency
	trips  []tripPlan
}

// plans indexes the registry snapshot for feed building. Trips without
// stop_times are dropped here (the static feed factory already fails
// closed on them; the realtime adapter simply cannot serve them).
func buildPlans(registry *store.TransitRegistry) (map[string]*routePlan, map[string]store.TransitCalendar) {
	agenciesByID := make(map[string]store.TransitAgency, len(registry.Agencies))
	for _, agency := range registry.Agencies {
		agenciesByID[agency.AgencyID] = agency
	}
	calendarsByID := make(map[string]store.TransitCalendar, len(registry.Calendars))
	for _, calendar := range registry.Calendars {
		calendarsByID[calendar.ServiceID] = calendar
	}
	stopsByID := make(map[string]store.TransitStop, len(registry.Stops))
	for _, stop := range registry.Stops {
		stopsByID[stop.StopID] = stop
	}
	stopTimesByTrip := make(map[string][]store.TransitStopTime)
	for _, stopTime := range registry.StopTimes { // loaded ORDER BY trip_id, stop_sequence
		stopTimesByTrip[stopTime.TripID] = append(stopTimesByTrip[stopTime.TripID], stopTime)
	}
	plans := make(map[string]*routePlan)
	for _, route := range registry.Routes {
		if !route.Active {
			continue
		}
		plan := &routePlan{route: route}
		if agency, ok := agenciesByID[route.AgencyID]; ok {
			agencyCopy := agency
			plan.agency = &agencyCopy
		}
		plans[route.RouteID] = plan
	}
	for _, trip := range registry.Trips {
		plan, ok := plans[trip.RouteID]
		if !ok {
			continue
		}
		times := stopTimesByTrip[trip.TripID]
		if len(times) == 0 {
			continue
		}
		tripPlan := tripPlan{trip: trip, visits: make([]stopVisit, 0, len(times))}
		coords := make([]Coord, 0, len(times))
		complete := true
		for _, stopTime := range times {
			stop, ok := stopsByID[stopTime.StopID]
			if !ok {
				complete = false
				break
			}
			tripPlan.visits = append(tripPlan.visits, stopVisit{stopTime: stopTime, stop: stop})
			coords = append(coords, Coord{LatitudeMicros: stop.LatitudeMicros, LongitudeMicros: stop.LongitudeMicros})
		}
		if !complete {
			continue
		}
		tripPlan.path = NewRoutePath(coords)
		plan.trips = append(plan.trips, tripPlan)
	}
	return plans, calendarsByID
}

// serviceDateBounds resolves the agency-local service day containing now
// and returns its midnight (agency timezone). Falls back to UTC when the
// agency timezone is unusable — documented degradation, not fabrication.
func serviceMidnight(agency *store.TransitAgency, now time.Time) time.Time {
	location := time.UTC
	if agency != nil {
		if loaded, err := time.LoadLocation(agency.Timezone); err == nil {
			location = loaded
		}
	}
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

// tripWindows computes the absolute service windows of a route's trips on
// the service day containing now.
func tripWindows(plan *routePlan, calendarsByID map[string]store.TransitCalendar, now time.Time) []TripWindow {
	midnight := serviceMidnight(plan.agency, now)
	windows := make([]TripWindow, 0, len(plan.trips))
	for _, tripPlan := range plan.trips {
		calendar, ok := calendarsByID[tripPlan.trip.ServiceID]
		if !ok || !calendarActiveOn(calendar, midnight) {
			continue
		}
		first := tripPlan.visits[0].stopTime
		last := tripPlan.visits[len(tripPlan.visits)-1].stopTime
		windows = append(windows, TripWindow{
			TripID:         tripPlan.trip.TripID,
			FirstDeparture: midnight.Add(time.Duration(first.DepartureSeconds) * time.Second),
			LastArrival:    midnight.Add(time.Duration(last.ArrivalSeconds) * time.Second),
		})
	}
	return windows
}

// calendarActiveOn reports whether the service runs on the service day
// starting at midnight (weekday + inclusive date bounds).
func calendarActiveOn(calendar store.TransitCalendar, midnight time.Time) bool {
	day := midnight.Format("2006-01-02")
	if day < calendar.StartDate.Format("2006-01-02") || day > calendar.EndDate.Format("2006-01-02") {
		return false
	}
	// Go: Sunday=0..Saturday=6; registry: index 0=Monday..6=Sunday.
	weekdayIndex := (int(midnight.Weekday()) + 6) % 7
	return calendar.Weekdays[weekdayIndex]
}

// activeAssignments filters route_vessels to assignments live at now on
// active routes, deterministically ordered (route_id, mmsi).
func activeAssignments(registry *store.TransitRegistry, plans map[string]*routePlan, now time.Time) []store.RouteVessel {
	out := make([]store.RouteVessel, 0, len(registry.Assignments))
	for _, assignment := range registry.Assignments { // loaded ORDER BY route_id, mmsi
		if _, ok := plans[assignment.RouteID]; !ok {
			continue
		}
		if assignment.ValidFrom != nil && now.Before(*assignment.ValidFrom) {
			continue
		}
		if assignment.ValidTo != nil && !now.Before(*assignment.ValidTo) {
			continue
		}
		out = append(out, assignment)
	}
	return out
}

// freshPositions joins the assignments against the shared position plane
// and applies the STALENESS RULE: a missing or stale position omits the
// vessel (counted per reason) — honest absence, never interpolation.
func (builder *Builder) freshPositions(ctx context.Context, assignments []store.RouteVessel,
	clearedLabels []string, now time.Time, feedName string) (map[string]store.LatestPosition, error) {
	mmsis := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		mmsis = append(mmsis, assignment.MMSI)
	}
	positions, err := builder.store.LatestPositionsByMMSI(ctx, mmsis, clearedLabels)
	if err != nil {
		return nil, err
	}
	fresh := make(map[string]store.LatestPosition, len(positions))
	for _, assignment := range assignments {
		position, ok := positions[assignment.MMSI]
		if !ok {
			builder.metrics.Inc("geo_gtfsrt_entities_omitted_total", map[string]string{"feed": feedName, "reason": "no_position"})
			continue
		}
		age := now.Sub(position.ObservedAt)
		if age > builder.config.StaleAfter {
			builder.metrics.Inc("geo_gtfsrt_entities_omitted_total", map[string]string{"feed": feedName, "reason": "stale"})
			continue
		}
		fresh[assignment.MMSI] = position
	}
	return fresh, nil
}

// stopAttribution snaps the vessel onto the trip path and resolves the
// current stop (sequence + status). off-route beyond SnapMaxMeters → no
// attribution (ok=false): the feed must not claim a stop the vessel is
// not demonstrably serving.
func (builder *Builder) stopAttribution(tripPlan tripPlan, vessel Coord,
	speedMilliknots *uint32) (sequence uint32, stopID string, status gtfs.VehiclePosition_VehicleStopStatus, ok bool) {
	progress, offRoute := tripPlan.path.Snap(vessel)
	if offRoute > builder.config.SnapMaxMeters {
		return 0, "", gtfs.VehiclePosition_IN_TRANSIT_TO, false
	}
	// Nearest stop by along-route distance.
	nearest := 0
	nearestDistance := tripPlan.path.Total
	for i, cumulative := range tripPlan.path.Cumulative {
		if d := abs64(cumulative - progress); d < nearestDistance {
			nearest, nearestDistance = i, d
		}
	}
	nearestVisit := tripPlan.visits[nearest]
	if speedMilliknots != nil && *speedMilliknots < builder.config.StopSpeedMilliknots &&
		haversineMeters(vessel, Coord{LatitudeMicros: nearestVisit.stop.LatitudeMicros, LongitudeMicros: nearestVisit.stop.LongitudeMicros}) <= builder.config.StopArriveMeters {
		return uint32(nearestVisit.stopTime.StopSequence), nearestVisit.stop.StopID,
			gtfs.VehiclePosition_STOPPED_AT, true
	}
	// Otherwise the vessel is IN_TRANSIT_TO the first stop still ahead.
	for i, cumulative := range tripPlan.path.Cumulative {
		if cumulative-progress > 0 {
			visit := tripPlan.visits[i]
			return uint32(visit.stopTime.StopSequence), visit.stop.StopID,
				gtfs.VehiclePosition_IN_TRANSIT_TO, true
		}
	}
	// Past the final stop — no honest attribution.
	return 0, "", gtfs.VehiclePosition_IN_TRANSIT_TO, false
}

func abs64(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

// matchAssignmentTrip resolves the trip a route-assigned vessel is
// serving at now (time-indexed schedule match).
func (builder *Builder) matchAssignmentTrip(plan *routePlan, calendarsByID map[string]store.TransitCalendar,
	now time.Time) (tripPlan, bool) {
	windows := tripWindows(plan, calendarsByID, now)
	window, matched := MatchTrip(windows, now, builder.config.TripMatchPreSlack, builder.config.TripMatchPostSlack)
	if !matched {
		return tripPlan{}, false
	}
	for _, candidate := range plan.trips {
		if candidate.trip.TripID == window.TripID {
			return candidate, true
		}
	}
	return tripPlan{}, false
}

// BuildVehiclePositions renders vehiclepositions.pb: one entity per
// route-assigned vessel with a FRESH position. MMSI = vehicle.id.
func (builder *Builder) BuildVehiclePositions(ctx context.Context, tenantID string, clearedLabels []string) ([]byte, error) {
	ctx, span := tracer().Start(ctx, "gtfsrt.build vehiclepositions")
	defer span.End()
	started := builder.now()
	registry, err := builder.store.LoadTransitRegistry(ctx, tenantID)
	if err != nil {
		builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "vehiclepositions"})
		return nil, fmt.Errorf("vehiclepositions registry: %w", err)
	}
	plans, calendarsByID := buildPlans(registry)
	assignments := activeAssignments(registry, plans, started)
	fresh, err := builder.freshPositions(ctx, assignments, clearedLabels, started, "vehiclepositions")
	if err != nil {
		builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "vehiclepositions"})
		return nil, err
	}
	feed := &gtfs.FeedMessage{Header: feedHeader(started), Entity: make([]*gtfs.FeedEntity, 0, len(fresh))}
	for _, assignment := range assignments {
		position, ok := fresh[assignment.MMSI]
		if !ok {
			continue
		}
		vehiclePosition := &gtfs.VehiclePosition{
			Vehicle: &gtfs.VehicleDescriptor{Id: proto.String(assignment.MMSI)},
			Position: &gtfs.Position{
				Latitude:  proto.Float32(float32(position.LatitudeMicros) / 1e6),
				Longitude: proto.Float32(float32(position.LongitudeMicros) / 1e6),
			},
			Timestamp: proto.Uint64(uint64(position.ObservedAt.Unix())),
		}
		if position.CourseOverGroundMillidegrees != nil {
			vehiclePosition.Position.Bearing = proto.Float32(float32(*position.CourseOverGroundMillidegrees) / 1000)
		}
		if position.SpeedOverGroundMilliknots != nil {
			vehiclePosition.Position.Speed = proto.Float32(float32(milliknotsToMetersPerSecond(float64(*position.SpeedOverGroundMilliknots))))
		}
		if plan, ok := plans[assignment.RouteID]; ok {
			if tripPlan, matched := builder.matchAssignmentTrip(plan, calendarsByID, started); matched {
				vehiclePosition.Trip = &gtfs.TripDescriptor{
					TripId:  proto.String(tripPlan.trip.TripID),
					RouteId: proto.String(assignment.RouteID),
				}
				if sequence, stopID, status, attributed := builder.stopAttribution(tripPlan,
					Coord{LatitudeMicros: position.LatitudeMicros, LongitudeMicros: position.LongitudeMicros},
					position.SpeedOverGroundMilliknots); attributed {
					vehiclePosition.CurrentStopSequence = proto.Uint32(sequence)
					vehiclePosition.StopId = proto.String(stopID)
					vehiclePosition.CurrentStatus = status.Enum()
				}
			} else {
				// No plausible trip: the route reference alone is honest.
				vehiclePosition.Trip = &gtfs.TripDescriptor{RouteId: proto.String(assignment.RouteID)}
			}
		}
		feed.Entity = append(feed.Entity, &gtfs.FeedEntity{
			Id:      proto.String("vp:" + assignment.MMSI),
			Vehicle: vehiclePosition,
		})
	}
	builder.recordBuild("vehiclepositions", started, len(feed.Entity))
	return proto.Marshal(feed)
}

// BuildTripUpdates renders tripupdates.pb: computed per-jetty ETAs.
// Fallback honesty: a vessel with NO speed observations is predicted at
// the route default speed with schedule_relationship=SCHEDULED and a 300s
// per-stop uncertainty (low confidence — never passed off as live). A
// vessel moving slower than the ETA speed floor yields NO trip update at
// all (NO_SHOW-style omission): a docked or drifting vessel's ETA is
// unknowable and must not be invented.
func (builder *Builder) BuildTripUpdates(ctx context.Context, tenantID string, clearedLabels []string) ([]byte, error) {
	ctx, span := tracer().Start(ctx, "gtfsrt.build tripupdates")
	defer span.End()
	started := builder.now()
	registry, err := builder.store.LoadTransitRegistry(ctx, tenantID)
	if err != nil {
		builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "tripupdates"})
		return nil, fmt.Errorf("tripupdates registry: %w", err)
	}
	plans, calendarsByID := buildPlans(registry)
	assignments := activeAssignments(registry, plans, started)
	fresh, err := builder.freshPositions(ctx, assignments, clearedLabels, started, "tripupdates")
	if err != nil {
		builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "tripupdates"})
		return nil, err
	}
	feed := &gtfs.FeedMessage{Header: feedHeader(started), Entity: make([]*gtfs.FeedEntity, 0, len(fresh))}
	for _, assignment := range assignments {
		position, ok := fresh[assignment.MMSI]
		if !ok {
			continue
		}
		plan := plans[assignment.RouteID]
		tripPlan, matched := builder.matchAssignmentTrip(plan, calendarsByID, started)
		if !matched {
			builder.metrics.Inc("geo_gtfsrt_eta_omitted_total", map[string]string{"reason": "no_trip"})
			continue
		}
		vessel := Coord{LatitudeMicros: position.LatitudeMicros, LongitudeMicros: position.LongitudeMicros}
		progress, offRoute := tripPlan.path.Snap(vessel)
		if offRoute > builder.config.SnapMaxMeters {
			// Vessel not demonstrably on the route: no honest ETA exists.
			builder.metrics.Inc("geo_gtfsrt_eta_omitted_total", map[string]string{"reason": "off_route"})
			continue
		}
		midnight := serviceMidnight(plan.agency, started)
		samples, err := builder.store.RecentSpeedSamples(ctx, assignment.MMSI, builder.config.SpeedSampleCount, clearedLabels)
		if err != nil {
			builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "tripupdates"})
			return nil, fmt.Errorf("tripupdates speed samples: %w", err)
		}
		builder.metrics.Add("geo_gtfsrt_speed_samples_total", nil, int64(len(samples)))
		medianMilliknots, haveObservations := MedianMilliknots(samples)
		tripUpdate := &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{
				TripId:               proto.String(tripPlan.trip.TripID),
				RouteId:              proto.String(assignment.RouteID),
				ScheduleRelationship: gtfs.TripDescriptor_SCHEDULED.Enum(),
			},
			Vehicle:   &gtfs.VehicleDescriptor{Id: proto.String(assignment.MMSI)},
			Timestamp: proto.Uint64(uint64(position.ObservedAt.Unix())),
		}
		// uncertaintySeconds marks prediction confidence: 60s for ETAs
		// computed from reported speeds, 300s for the documented fallback
		// (LOW CONFIDENCE — consumers must treat these as schedule-grade,
		// not live-grade, predictions).
		uncertainty := int32(60)
		var speedMilliknots float64
		mode := "live"
		switch {
		case !haveObservations:
			// Fallback (zero speed observations): the ONLY case the route
			// default speed is used. The trip descriptor stays
			// schedule_relationship=SCHEDULED and the stop events carry a
			// 300s uncertainty — low confidence, never passed off as live.
			speedMilliknots = float64(plan.route.DefaultSpeedMilliknots)
			uncertainty = 300
			mode = "scheduled_fallback"
		case medianMilliknots < float64(builder.config.ETAMinSpeedMilliknots):
			// Vessel not moving: its ETA is unknowable. Omit (documented
			// NO_SHOW-style behavior) rather than inventing one.
			builder.metrics.Inc("geo_gtfsrt_eta_omitted_total", map[string]string{"reason": "not_moving"})
			continue
		default:
			speedMilliknots = medianMilliknots
		}
		speed := milliknotsToMetersPerSecond(speedMilliknots)
		for _, eta := range tripPlan.path.ComputeETAs(progress, speed, started) {
			visit := tripPlan.visits[eta.StopIndex]
			scheduledArrival := midnight.Add(time.Duration(visit.stopTime.ArrivalSeconds) * time.Second)
			scheduledDeparture := midnight.Add(time.Duration(visit.stopTime.DepartureSeconds) * time.Second)
			tripUpdate.StopTimeUpdate = append(tripUpdate.StopTimeUpdate, &gtfs.TripUpdate_StopTimeUpdate{
				StopSequence: proto.Uint32(uint32(visit.stopTime.StopSequence)),
				StopId:       proto.String(visit.stop.StopID),
				Arrival: &gtfs.TripUpdate_StopTimeEvent{
					Delay:       proto.Int32(int32(eta.ETA.Unix() - scheduledArrival.Unix())),
					Time:        proto.Int64(eta.ETA.Unix()),
					Uncertainty: proto.Int32(uncertainty),
				},
				Departure: &gtfs.TripUpdate_StopTimeEvent{
					Delay:       proto.Int32(int32(eta.ETA.Unix() - scheduledDeparture.Unix())),
					Time:        proto.Int64(eta.ETA.Unix()),
					Uncertainty: proto.Int32(uncertainty),
				},
				ScheduleRelationship: gtfs.TripUpdate_StopTimeUpdate_SCHEDULED.Enum(),
			})
		}
		builder.metrics.Inc("geo_gtfsrt_eta_total", map[string]string{"mode": mode})
		feed.Entity = append(feed.Entity, &gtfs.FeedEntity{
			Id:         proto.String("tu:" + assignment.MMSI),
			TripUpdate: tripUpdate,
		})
	}
	builder.recordBuild("tripupdates", started, len(feed.Entity))
	return proto.Marshal(feed)
}

// alertCauses maps the registry's cause strings onto the spec enum.
var alertCauses = map[string]gtfs.Alert_Cause{
	"UNKNOWN_CAUSE":     gtfs.Alert_UNKNOWN_CAUSE,
	"OTHER_CAUSE":       gtfs.Alert_OTHER_CAUSE,
	"TECHNICAL_PROBLEM": gtfs.Alert_TECHNICAL_PROBLEM,
	"STRIKE":            gtfs.Alert_STRIKE,
	"DEMONSTRATION":     gtfs.Alert_DEMONSTRATION,
	"ACCIDENT":          gtfs.Alert_ACCIDENT,
	"HOLIDAY":           gtfs.Alert_HOLIDAY,
	"WEATHER":           gtfs.Alert_WEATHER,
	"MAINTENANCE":       gtfs.Alert_MAINTENANCE,
	"CONSTRUCTION":      gtfs.Alert_CONSTRUCTION,
	"POLICE_ACTIVITY":   gtfs.Alert_POLICE_ACTIVITY,
	"MEDICAL_EMERGENCY": gtfs.Alert_MEDICAL_EMERGENCY,
}

// alertEffects maps the registry's effect strings onto the spec enum.
var alertEffects = map[string]gtfs.Alert_Effect{
	"NO_SERVICE":          gtfs.Alert_NO_SERVICE,
	"REDUCED_SERVICE":     gtfs.Alert_REDUCED_SERVICE,
	"SIGNIFICANT_DELAYS":  gtfs.Alert_SIGNIFICANT_DELAYS,
	"DETOUR":              gtfs.Alert_DETOUR,
	"ADDITIONAL_SERVICE":  gtfs.Alert_ADDITIONAL_SERVICE,
	"MODIFIED_SERVICE":    gtfs.Alert_MODIFIED_SERVICE,
	"OTHER_EFFECT":        gtfs.Alert_OTHER_EFFECT,
	"UNKNOWN_EFFECT":      gtfs.Alert_UNKNOWN_EFFECT,
	"STOP_MOVED":          gtfs.Alert_STOP_MOVED,
	"NO_EFFECT":           gtfs.Alert_NO_EFFECT,
	"ACCESSIBILITY_ISSUE": gtfs.Alert_ACCESSIBILITY_ISSUE,
}

func translated(text string) *gtfs.TranslatedString {
	return &gtfs.TranslatedString{Translation: []*gtfs.TranslatedString_Translation{
		{Text: proto.String(text), Language: proto.String("en")},
	}}
}

// BuildAlerts renders alerts.pb from the tenant's active alerts.
func (builder *Builder) BuildAlerts(ctx context.Context, tenantID string) ([]byte, error) {
	ctx, span := tracer().Start(ctx, "gtfsrt.build alerts")
	defer span.End()
	started := builder.now()
	alerts, err := builder.store.ListActiveTransitAlerts(ctx, tenantID, started)
	if err != nil {
		builder.metrics.Inc("geo_feed_build_errors_total", map[string]string{"feed": "alerts"})
		return nil, fmt.Errorf("alerts query: %w", err)
	}
	feed := &gtfs.FeedMessage{Header: feedHeader(started), Entity: make([]*gtfs.FeedEntity, 0, len(alerts))}
	for _, row := range alerts {
		cause, ok := alertCauses[row.Cause]
		if !ok {
			cause = gtfs.Alert_UNKNOWN_CAUSE
		}
		effect, ok := alertEffects[row.Effect]
		if !ok {
			effect = gtfs.Alert_UNKNOWN_EFFECT
		}
		alert := &gtfs.Alert{
			Cause:  cause.Enum(),
			Effect: effect.Enum(),
		}
		selector := &gtfs.EntitySelector{}
		if row.RouteID != "" {
			selector.RouteId = proto.String(row.RouteID)
		}
		if row.StopID != "" {
			selector.StopId = proto.String(row.StopID)
		}
		alert.InformedEntity = []*gtfs.EntitySelector{selector}
		period := &gtfs.TimeRange{}
		if row.StartsAt != nil {
			period.Start = proto.Uint64(uint64(row.StartsAt.Unix()))
		}
		if row.EndsAt != nil {
			period.End = proto.Uint64(uint64(row.EndsAt.Unix()))
		}
		alert.ActivePeriod = []*gtfs.TimeRange{period}
		alert.HeaderText = translated(row.HeaderText)
		if row.DescriptionText != "" {
			alert.DescriptionText = translated(row.DescriptionText)
		}
		if row.URL != "" {
			alert.Url = translated(row.URL)
		}
		feed.Entity = append(feed.Entity, &gtfs.FeedEntity{
			Id:    proto.String("alert:" + row.AlertID),
			Alert: alert,
		})
	}
	builder.recordBuild("alerts", started, len(feed.Entity))
	return proto.Marshal(feed)
}

// recordBuild counts one feed build, its latency and the emitted entities.
func (builder *Builder) recordBuild(feedName string, started time.Time, emitted int) {
	builder.metrics.Inc("geo_feed_build_total", map[string]string{"feed": feedName})
	builder.metrics.Add("geo_feed_build_duration_ms_total", map[string]string{"feed": feedName},
		time.Since(started).Milliseconds())
	builder.metrics.Add("geo_gtfsrt_entities_emitted_total", map[string]string{"feed": feedName}, int64(emitted))
}
