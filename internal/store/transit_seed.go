// Transit registry seed loader. The registry is operator-maintained
// master data: NIWA/operators keep a reviewed YAML/JSON document under
// change control and load it idempotently (upserts) with
// cmd/geo-transitseed. YAML is a superset of JSON, so one loader covers
// both formats. Stop sequences are assigned in document order; times are
// seconds after midnight. See fixtures/transit_seed.example.yaml.
package store

import (
	"context"
	"fmt"
	"time"

	"go.yaml.in/yaml/v3"
)

// TransitSeed is the seed document (YAML or JSON).
type TransitSeed struct {
	Agencies []struct {
		AgencyID string `yaml:"agency_id" json:"agency_id"`
		Name     string `yaml:"name" json:"name"`
		URL      string `yaml:"url" json:"url"`
		Timezone string `yaml:"timezone" json:"timezone"`
		Lang     string `yaml:"lang" json:"lang"`
		Phone    string `yaml:"phone" json:"phone"`
	} `yaml:"agencies" json:"agencies"`
	Routes []struct {
		RouteID                string `yaml:"route_id" json:"route_id"`
		AgencyID               string `yaml:"agency_id" json:"agency_id"`
		ShortName              string `yaml:"short_name" json:"short_name"`
		LongName               string `yaml:"long_name" json:"long_name"`
		RouteType              int    `yaml:"route_type" json:"route_type"`
		DefaultSpeedMilliknots int    `yaml:"default_speed_milliknots" json:"default_speed_milliknots"`
		Active                 *bool  `yaml:"active" json:"active"`
	} `yaml:"routes" json:"routes"`
	Stops []struct {
		StopID          string `yaml:"stop_id" json:"stop_id"`
		Name            string `yaml:"name" json:"name"`
		LatitudeMicros  int32  `yaml:"latitude_micros" json:"latitude_micros"`
		LongitudeMicros int32  `yaml:"longitude_micros" json:"longitude_micros"`
		ZoneID          string `yaml:"zone_id" json:"zone_id"`
	} `yaml:"stops" json:"stops"`
	Calendars []struct {
		ServiceID string `yaml:"service_id" json:"service_id"`
		// Weekdays in GTFS order: Monday..Sunday (7 entries).
		Weekdays  []bool `yaml:"weekdays" json:"weekdays"`
		StartDate string `yaml:"start_date" json:"start_date"`
		EndDate   string `yaml:"end_date" json:"end_date"`
	} `yaml:"calendars" json:"calendars"`
	Trips []struct {
		TripID      string `yaml:"trip_id" json:"trip_id"`
		RouteID     string `yaml:"route_id" json:"route_id"`
		ServiceID   string `yaml:"service_id" json:"service_id"`
		Headsign    string `yaml:"headsign" json:"headsign"`
		DirectionID *int32 `yaml:"direction_id" json:"direction_id"`
	} `yaml:"trips" json:"trips"`
	// StopTimes groups visits per trip; StopSequence is assigned 1..N in
	// document order.
	StopTimes []struct {
		TripID string `yaml:"trip_id" json:"trip_id"`
		Stops  []struct {
			StopID           string `yaml:"stop_id" json:"stop_id"`
			ArrivalSeconds   int    `yaml:"arrival_seconds" json:"arrival_seconds"`
			DepartureSeconds int    `yaml:"departure_seconds" json:"departure_seconds"`
		} `yaml:"stops" json:"stops"`
	} `yaml:"stop_times" json:"stop_times"`
	Assignments []struct {
		RouteID   string `yaml:"route_id" json:"route_id"`
		MMSI      string `yaml:"mmsi" json:"mmsi"`
		IMO       string `yaml:"imo" json:"imo"`
		ValidFrom string `yaml:"valid_from" json:"valid_from"`
		ValidTo   string `yaml:"valid_to" json:"valid_to"`
	} `yaml:"assignments" json:"assignments"`
}

// ParseTransitSeed decodes a YAML or JSON seed document.
func ParseTransitSeed(document []byte) (*TransitSeed, error) {
	var seed TransitSeed
	if err := yaml.Unmarshal(document, &seed); err != nil {
		return nil, fmt.Errorf("parse transit seed: %w", err)
	}
	return &seed, nil
}

// SeedTransitRegistry loads a seed document idempotently (upserts) under
// the given tenant. Every section is validated as it is applied; the
// first defect aborts the load (fail closed — a half-loaded registry
// would produce a half-valid feed).
func (store *Store) SeedTransitRegistry(ctx context.Context, tenantID string, seed *TransitSeed) error {
	if seed == nil {
		return fmt.Errorf("transit seed document is required")
	}
	for i, agency := range seed.Agencies {
		if err := store.UpsertTransitAgency(ctx, tenantID, TransitAgency{
			AgencyID: agency.AgencyID, Name: agency.Name, URL: agency.URL,
			Timezone: agency.Timezone, Lang: agency.Lang, Phone: agency.Phone,
		}); err != nil {
			return fmt.Errorf("agencies[%d]: %w", i, err)
		}
	}
	for i, route := range seed.Routes {
		active := true
		if route.Active != nil {
			active = *route.Active
		}
		if err := store.UpsertTransitRoute(ctx, tenantID, TransitRoute{
			RouteID: route.RouteID, AgencyID: route.AgencyID, ShortName: route.ShortName,
			LongName: route.LongName, RouteType: route.RouteType,
			DefaultSpeedMilliknots: route.DefaultSpeedMilliknots, Active: active,
		}); err != nil {
			return fmt.Errorf("routes[%d]: %w", i, err)
		}
	}
	for i, stop := range seed.Stops {
		if err := store.UpsertTransitStop(ctx, tenantID, TransitStop{
			StopID: stop.StopID, Name: stop.Name,
			LatitudeMicros: stop.LatitudeMicros, LongitudeMicros: stop.LongitudeMicros,
			ZoneID: stop.ZoneID,
		}); err != nil {
			return fmt.Errorf("stops[%d]: %w", i, err)
		}
	}
	for i, calendar := range seed.Calendars {
		if len(calendar.Weekdays) != 7 {
			return fmt.Errorf("calendars[%d]: weekdays must have 7 entries (Monday..Sunday)", i)
		}
		startDate, err := time.Parse("2006-01-02", calendar.StartDate)
		if err != nil {
			return fmt.Errorf("calendars[%d]: start_date must be YYYY-MM-DD: %w", i, err)
		}
		endDate, err := time.Parse("2006-01-02", calendar.EndDate)
		if err != nil {
			return fmt.Errorf("calendars[%d]: end_date must be YYYY-MM-DD: %w", i, err)
		}
		var weekdays [7]bool
		copy(weekdays[:], calendar.Weekdays)
		if err := store.UpsertTransitCalendar(ctx, tenantID, TransitCalendar{
			ServiceID: calendar.ServiceID, Weekdays: weekdays,
			StartDate: startDate, EndDate: endDate,
		}); err != nil {
			return fmt.Errorf("calendars[%d]: %w", i, err)
		}
	}
	for i, trip := range seed.Trips {
		if err := store.UpsertTransitTrip(ctx, tenantID, TransitTrip{
			TripID: trip.TripID, RouteID: trip.RouteID, ServiceID: trip.ServiceID,
			Headsign: trip.Headsign, DirectionID: trip.DirectionID,
		}); err != nil {
			return fmt.Errorf("trips[%d]: %w", i, err)
		}
	}
	for i, group := range seed.StopTimes {
		times := make([]TransitStopTime, 0, len(group.Stops))
		for sequence, visit := range group.Stops {
			times = append(times, TransitStopTime{
				TripID: group.TripID, StopSequence: sequence + 1, StopID: visit.StopID,
				ArrivalSeconds: visit.ArrivalSeconds, DepartureSeconds: visit.DepartureSeconds,
			})
		}
		if err := store.ReplaceTransitStopTimes(ctx, tenantID, group.TripID, times); err != nil {
			return fmt.Errorf("stop_times[%d]: %w", i, err)
		}
	}
	for i, assignment := range seed.Assignments {
		validFrom, err := parseSeedTime(assignment.ValidFrom)
		if err != nil {
			return fmt.Errorf("assignments[%d]: valid_from must be RFC 3339: %w", i, err)
		}
		validTo, err := parseSeedTime(assignment.ValidTo)
		if err != nil {
			return fmt.Errorf("assignments[%d]: valid_to must be RFC 3339: %w", i, err)
		}
		if err := store.AssignRouteVessel(ctx, tenantID, RouteVessel{
			RouteID: assignment.RouteID, MMSI: assignment.MMSI, IMO: assignment.IMO,
			ValidFrom: validFrom, ValidTo: validTo,
		}); err != nil {
			return fmt.Errorf("assignments[%d]: %w", i, err)
		}
	}
	return nil
}

func parseSeedTime(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, err
	}
	utc := parsed.UTC()
	return &utc, nil
}
