// Transit registry access (migration 0009): the tenant-scoped route &
// jetty master data behind the GTFS static feed factory, the GTFS-RT
// producer and the ETA engine. Every method runs inside WithTenant (RLS
// default-deny); the registry is the operator-maintained source of truth
// and is never synthesized from position data.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// TransitAgency is one GTFS agency (operator/authority).
type TransitAgency struct {
	AgencyID string
	TenantID string
	Name     string
	URL      string
	Timezone string
	Lang     string
	Phone    string
}

// TransitRoute is one scheduled route (route_type 4 = ferry by default).
type TransitRoute struct {
	RouteID                string
	TenantID               string
	AgencyID               string
	ShortName              string
	LongName               string
	RouteType              int
	DefaultSpeedMilliknots int
	Active                 bool
}

// TransitStop is one jetty/terminal (fixed-point coordinates per doctrine).
type TransitStop struct {
	StopID          string
	TenantID        string
	Name            string
	LatitudeMicros  int32
	LongitudeMicros int32
	ZoneID          string
}

// TransitCalendar is a weekly service calendar with inclusive date bounds.
type TransitCalendar struct {
	ServiceID string
	TenantID  string
	Weekdays  [7]bool // index 0 = Monday .. 6 = Sunday (GTFS column order)
	StartDate time.Time
	EndDate   time.Time
}

// TransitTrip is one scheduled trip on a route under a service calendar.
type TransitTrip struct {
	TripID      string
	TenantID    string
	RouteID     string
	ServiceID   string
	Headsign    string
	DirectionID *int32
}

// TransitStopTime is one scheduled visit; times are seconds after midnight.
type TransitStopTime struct {
	TripID           string
	StopSequence     int
	StopID           string
	ArrivalSeconds   int
	DepartureSeconds int
}

// RouteVessel is a route ↔ MMSI assignment window.
type RouteVessel struct {
	RouteID   string
	MMSI      string
	IMO       string
	ValidFrom *time.Time
	ValidTo   *time.Time
}

// TransitAlert is a service alert for the GTFS-RT alerts feed.
type TransitAlert struct {
	AlertID         string
	TenantID        string
	Cause           string
	Effect          string
	RouteID         string // empty = unscoped
	StopID          string // empty = unscoped
	StartsAt        *time.Time
	EndsAt          *time.Time
	HeaderText      string
	DescriptionText string
	URL             string
	Active          bool
	CreatedBy       string
	CreatedAt       time.Time
}

// TransitRegistry is the full tenant snapshot the feed builders consume.
type TransitRegistry struct {
	Agencies    []TransitAgency
	Routes      []TransitRoute
	Stops       []TransitStop
	Calendars   []TransitCalendar
	Trips       []TransitTrip
	StopTimes   []TransitStopTime
	Assignments []RouteVessel
}

func requireID(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

// UpsertTransitAgency creates or replaces an agency row (seed + admin path).
func (store *Store) UpsertTransitAgency(ctx context.Context, tenantID string, agency TransitAgency) error {
	if err := requireID("agency_id", agency.AgencyID); err != nil {
		return err
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_agencies
			(agency_id, tenant_id, name, url, timezone, lang, phone)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (agency_id) DO UPDATE SET
				name = EXCLUDED.name, url = EXCLUDED.url, timezone = EXCLUDED.timezone,
				lang = EXCLUDED.lang, phone = EXCLUDED.phone`,
			agency.AgencyID, tenantID, agency.Name, agency.URL, agency.Timezone, agency.Lang, agency.Phone)
		return err
	})
}

// UpsertTransitRoute creates or replaces a route row.
func (store *Store) UpsertTransitRoute(ctx context.Context, tenantID string, route TransitRoute) error {
	if err := requireID("route_id", route.RouteID); err != nil {
		return err
	}
	if route.RouteType == 0 {
		route.RouteType = 4
	}
	if route.DefaultSpeedMilliknots == 0 {
		route.DefaultSpeedMilliknots = 6000
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_routes
			(route_id, tenant_id, agency_id, short_name, long_name, route_type, default_speed_milliknots, active)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (route_id) DO UPDATE SET
				agency_id = EXCLUDED.agency_id, short_name = EXCLUDED.short_name,
				long_name = EXCLUDED.long_name, route_type = EXCLUDED.route_type,
				default_speed_milliknots = EXCLUDED.default_speed_milliknots,
				active = EXCLUDED.active`,
			route.RouteID, tenantID, route.AgencyID, route.ShortName, route.LongName,
			route.RouteType, route.DefaultSpeedMilliknots, route.Active)
		return err
	})
}

// UpsertTransitStop creates or replaces a stop (jetty) row. The geography
// column is derived from the fixed-point micro-degree integers.
func (store *Store) UpsertTransitStop(ctx context.Context, tenantID string, stop TransitStop) error {
	if err := requireID("stop_id", stop.StopID); err != nil {
		return err
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_stops
			(stop_id, tenant_id, name, geom, latitude_micros, longitude_micros, zone_id)
			VALUES ($1,$2,$3,ST_GeogFromText($4),$5,$6,$7)
			ON CONFLICT (stop_id) DO UPDATE SET
				name = EXCLUDED.name, geom = EXCLUDED.geom,
				latitude_micros = EXCLUDED.latitude_micros,
				longitude_micros = EXCLUDED.longitude_micros,
				zone_id = EXCLUDED.zone_id`,
			stop.StopID, tenantID, stop.Name,
			pointWKT(stop.LatitudeMicros, stop.LongitudeMicros),
			stop.LatitudeMicros, stop.LongitudeMicros, stop.ZoneID)
		return err
	})
}

// UpsertTransitCalendar creates or replaces a service calendar.
func (store *Store) UpsertTransitCalendar(ctx context.Context, tenantID string, calendar TransitCalendar) error {
	if err := requireID("service_id", calendar.ServiceID); err != nil {
		return err
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_calendars
			(service_id, tenant_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (service_id) DO UPDATE SET
				monday = EXCLUDED.monday, tuesday = EXCLUDED.tuesday, wednesday = EXCLUDED.wednesday,
				thursday = EXCLUDED.thursday, friday = EXCLUDED.friday, saturday = EXCLUDED.saturday,
				sunday = EXCLUDED.sunday, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date`,
			calendar.ServiceID, tenantID,
			calendar.Weekdays[0], calendar.Weekdays[1], calendar.Weekdays[2], calendar.Weekdays[3],
			calendar.Weekdays[4], calendar.Weekdays[5], calendar.Weekdays[6],
			calendar.StartDate, calendar.EndDate)
		return err
	})
}

// UpsertTransitTrip creates or replaces a trip row.
func (store *Store) UpsertTransitTrip(ctx context.Context, tenantID string, trip TransitTrip) error {
	if err := requireID("trip_id", trip.TripID); err != nil {
		return err
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_trips
			(trip_id, tenant_id, route_id, service_id, headsign, direction_id)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (trip_id) DO UPDATE SET
				route_id = EXCLUDED.route_id, service_id = EXCLUDED.service_id,
				headsign = EXCLUDED.headsign, direction_id = EXCLUDED.direction_id`,
			trip.TripID, tenantID, trip.RouteID, trip.ServiceID, trip.Headsign, trip.DirectionID)
		return err
	})
}

// ReplaceTransitStopTimes atomically replaces a trip's stop sequence
// (delete + insert inside the tenant transaction). Times must be
// non-decreasing along the sequence — a schedule that is not monotonic is
// rejected here, at the storage boundary, before it can poison a feed.
func (store *Store) ReplaceTransitStopTimes(ctx context.Context, tenantID, tripID string, times []TransitStopTime) error {
	if err := requireID("trip_id", tripID); err != nil {
		return err
	}
	if len(times) == 0 {
		return errors.New("stop_times must not be empty")
	}
	previous := -1
	for i, stopTime := range times {
		if stopTime.StopID == "" {
			return fmt.Errorf("stop_times[%d]: stop_id is required", i)
		}
		if stopTime.ArrivalSeconds < previous {
			return fmt.Errorf("stop_times[%d]: arrival seconds must be monotonic per trip", i)
		}
		previous = stopTime.ArrivalSeconds
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM transit_stop_times WHERE trip_id = $1`, tripID); err != nil {
			return err
		}
		for _, stopTime := range times {
			sequence := stopTime.StopSequence
			if _, err := tx.Exec(ctx, `INSERT INTO transit_stop_times
				(tenant_id, trip_id, stop_sequence, stop_id, arrival_seconds, departure_seconds)
				VALUES ($1,$2,$3,$4,$5,$6)`,
				tenantID, tripID, sequence, stopTime.StopID,
				stopTime.ArrivalSeconds, stopTime.DepartureSeconds); err != nil {
				return err
			}
		}
		return nil
	})
}

// AssignRouteVessel upserts a route ↔ MMSI assignment window.
func (store *Store) AssignRouteVessel(ctx context.Context, tenantID string, assignment RouteVessel) error {
	if err := requireID("route_id", assignment.RouteID); err != nil {
		return err
	}
	if len(assignment.MMSI) != 9 {
		return fmt.Errorf("mmsi %q must be 9 digits", assignment.MMSI)
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_route_vessels
			(tenant_id, route_id, mmsi, imo, valid_from, valid_to)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (route_id, mmsi) DO UPDATE SET
				imo = EXCLUDED.imo, valid_from = EXCLUDED.valid_from, valid_to = EXCLUDED.valid_to`,
			tenantID, assignment.RouteID, assignment.MMSI, assignment.IMO,
			assignment.ValidFrom, assignment.ValidTo)
		return err
	})
}

// DeleteTransitRoute removes a route; referenced rows (trips, assignments)
// are removed with it. Alerts referencing the route block deletion (FK) —
// retire the alert first.
func (store *Store) DeleteTransitRoute(ctx context.Context, tenantID, routeID string) error {
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		for _, stmt := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM transit_stop_times WHERE trip_id IN (SELECT trip_id FROM transit_trips WHERE route_id = $1)`, []any{routeID}},
			{`DELETE FROM transit_trips WHERE route_id = $1`, []any{routeID}},
			{`DELETE FROM transit_route_vessels WHERE route_id = $1`, []any{routeID}},
			{`DELETE FROM transit_routes WHERE route_id = $1`, []any{routeID}},
		} {
			if _, err := tx.Exec(ctx, stmt.sql, stmt.args...); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteTransitStop removes a stop; stop_times referencing it are removed
// with it. Alerts referencing the stop block deletion (FK).
func (store *Store) DeleteTransitStop(ctx context.Context, tenantID, stopID string) error {
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM transit_stop_times WHERE stop_id = $1`, stopID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM transit_stops WHERE stop_id = $1`, stopID); err != nil {
			return err
		}
		return nil
	})
}

// CreateTransitAlert inserts an alert row (admin endpoint path). The row
// is attributed to the creating principal and is ALWAYS created active —
// retirement is an explicit operator act via DeactivateTransitAlert.
func (store *Store) CreateTransitAlert(ctx context.Context, tenantID string, alert TransitAlert) error {
	if err := requireID("alert_id", alert.AlertID); err != nil {
		return err
	}
	if alert.RouteID == "" && alert.StopID == "" {
		return errors.New("alert must be scoped to a route or a stop")
	}
	if strings.TrimSpace(alert.CreatedBy) == "" {
		return errors.New("alert created_by principal is required")
	}
	var routeID, stopID *string
	if alert.RouteID != "" {
		routeID = &alert.RouteID
	}
	if alert.StopID != "" {
		stopID = &alert.StopID
	}
	return store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO transit_alerts
			(alert_id, tenant_id, cause, effect, route_id, stop_id, starts_at, ends_at,
			 header_text, description_text, url, active, created_by)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			alert.AlertID, tenantID, alert.Cause, alert.Effect, routeID, stopID,
			alert.StartsAt, alert.EndsAt, alert.HeaderText, alert.DescriptionText,
			alert.URL, true, alert.CreatedBy)
		return err
	})
}

// DeactivateTransitAlert flips the operator kill-switch; the alert leaves
// the feed at the next build. Returns false when the alert is unknown to
// the tenant.
func (store *Store) DeactivateTransitAlert(ctx context.Context, tenantID, alertID string) (bool, error) {
	deactivated := false
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE transit_alerts SET active = false WHERE alert_id = $1`, alertID)
		if err != nil {
			return err
		}
		deactivated = tag.RowsAffected() == 1
		return nil
	})
	return deactivated, err
}

// ListActiveTransitAlerts returns the tenant's alerts whose window covers
// now and whose kill-switch is on — exactly what the alerts feed emits.
func (store *Store) ListActiveTransitAlerts(ctx context.Context, tenantID string, now time.Time) ([]TransitAlert, error) {
	alerts := make([]TransitAlert, 0)
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT alert_id, cause, effect, COALESCE(route_id, ''), COALESCE(stop_id, ''),
			starts_at, ends_at, header_text, description_text, url, active, created_by, created_at
			FROM transit_alerts
			WHERE active
			  AND (starts_at IS NULL OR starts_at <= $1)
			  AND (ends_at IS NULL OR ends_at > $1)
			ORDER BY alert_id`, now.UTC())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var alert TransitAlert
			if err := rows.Scan(&alert.AlertID, &alert.Cause, &alert.Effect, &alert.RouteID, &alert.StopID,
				&alert.StartsAt, &alert.EndsAt, &alert.HeaderText, &alert.DescriptionText, &alert.URL,
				&alert.Active, &alert.CreatedBy, &alert.CreatedAt); err != nil {
				return err
			}
			alert.TenantID = tenantID
			alerts = append(alerts, alert)
		}
		return rows.Err()
	})
	return alerts, err
}

// LoadTransitRegistry snapshots the tenant's full registry in one
// tenant-bound transaction (consistent view for one feed build).
func (store *Store) LoadTransitRegistry(ctx context.Context, tenantID string) (*TransitRegistry, error) {
	registry := &TransitRegistry{
		Agencies:    make([]TransitAgency, 0),
		Routes:      make([]TransitRoute, 0),
		Stops:       make([]TransitStop, 0),
		Calendars:   make([]TransitCalendar, 0),
		Trips:       make([]TransitTrip, 0),
		StopTimes:   make([]TransitStopTime, 0),
		Assignments: make([]RouteVessel, 0),
	}
	err := store.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		if err := queryRows(ctx, tx, `SELECT agency_id, name, url, timezone, lang, phone
			FROM transit_agencies ORDER BY agency_id`, func(rows pgx.Rows) error {
			var agency TransitAgency
			if err := rows.Scan(&agency.AgencyID, &agency.Name, &agency.URL, &agency.Timezone, &agency.Lang, &agency.Phone); err != nil {
				return err
			}
			agency.TenantID = tenantID
			registry.Agencies = append(registry.Agencies, agency)
			return nil
		}); err != nil {
			return fmt.Errorf("load agencies: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT route_id, agency_id, short_name, long_name, route_type,
			default_speed_milliknots, active FROM transit_routes ORDER BY route_id`, func(rows pgx.Rows) error {
			var route TransitRoute
			if err := rows.Scan(&route.RouteID, &route.AgencyID, &route.ShortName, &route.LongName,
				&route.RouteType, &route.DefaultSpeedMilliknots, &route.Active); err != nil {
				return err
			}
			route.TenantID = tenantID
			registry.Routes = append(registry.Routes, route)
			return nil
		}); err != nil {
			return fmt.Errorf("load routes: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT stop_id, name, latitude_micros, longitude_micros, zone_id
			FROM transit_stops ORDER BY stop_id`, func(rows pgx.Rows) error {
			var stop TransitStop
			if err := rows.Scan(&stop.StopID, &stop.Name, &stop.LatitudeMicros, &stop.LongitudeMicros, &stop.ZoneID); err != nil {
				return err
			}
			stop.TenantID = tenantID
			registry.Stops = append(registry.Stops, stop)
			return nil
		}); err != nil {
			return fmt.Errorf("load stops: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT service_id, monday, tuesday, wednesday, thursday, friday,
			saturday, sunday, start_date, end_date FROM transit_calendars ORDER BY service_id`, func(rows pgx.Rows) error {
			var calendar TransitCalendar
			if err := rows.Scan(&calendar.ServiceID, &calendar.Weekdays[0], &calendar.Weekdays[1],
				&calendar.Weekdays[2], &calendar.Weekdays[3], &calendar.Weekdays[4], &calendar.Weekdays[5],
				&calendar.Weekdays[6], &calendar.StartDate, &calendar.EndDate); err != nil {
				return err
			}
			calendar.TenantID = tenantID
			registry.Calendars = append(registry.Calendars, calendar)
			return nil
		}); err != nil {
			return fmt.Errorf("load calendars: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT trip_id, route_id, service_id, headsign, direction_id
			FROM transit_trips ORDER BY trip_id`, func(rows pgx.Rows) error {
			var trip TransitTrip
			if err := rows.Scan(&trip.TripID, &trip.RouteID, &trip.ServiceID, &trip.Headsign, &trip.DirectionID); err != nil {
				return err
			}
			trip.TenantID = tenantID
			registry.Trips = append(registry.Trips, trip)
			return nil
		}); err != nil {
			return fmt.Errorf("load trips: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT trip_id, stop_sequence, stop_id, arrival_seconds, departure_seconds
			FROM transit_stop_times ORDER BY trip_id, stop_sequence`, func(rows pgx.Rows) error {
			var stopTime TransitStopTime
			if err := rows.Scan(&stopTime.TripID, &stopTime.StopSequence, &stopTime.StopID,
				&stopTime.ArrivalSeconds, &stopTime.DepartureSeconds); err != nil {
				return err
			}
			registry.StopTimes = append(registry.StopTimes, stopTime)
			return nil
		}); err != nil {
			return fmt.Errorf("load stop_times: %w", err)
		}
		if err := queryRows(ctx, tx, `SELECT route_id, mmsi, imo, valid_from, valid_to
			FROM transit_route_vessels ORDER BY route_id, mmsi`, func(rows pgx.Rows) error {
			var assignment RouteVessel
			if err := rows.Scan(&assignment.RouteID, &assignment.MMSI, &assignment.IMO,
				&assignment.ValidFrom, &assignment.ValidTo); err != nil {
				return err
			}
			registry.Assignments = append(registry.Assignments, assignment)
			return nil
		}); err != nil {
			return fmt.Errorf("load route vessels: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return registry, nil
}

func queryRows(ctx context.Context, tx pgx.Tx, sql string, scan func(pgx.Rows) error) error {
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// LatestPosition is the shared-plane hot row for one vessel identity.
type LatestPosition struct {
	MMSI                         string
	LatitudeMicros               int32
	LongitudeMicros              int32
	SpeedOverGroundMilliknots    *uint32
	CourseOverGroundMillidegrees *uint32
	ObservedAt                   time.Time
}

// LatestPositionsByMMSI reads the SHARED position plane (no tenant column
// by design — see PositionPlane doctrine) at the caller's clearance for
// the given MMSIs. Unknown/cleared-out MMSIs are simply absent from the
// map: honest absence, never a synthesized position.
func (store *Store) LatestPositionsByMMSI(ctx context.Context, mmsis []string, clearedLabels []string) (map[string]LatestPosition, error) {
	out := make(map[string]LatestPosition, len(mmsis))
	if len(mmsis) == 0 {
		return out, nil
	}
	rows, err := store.pool.Query(ctx, `SELECT mmsi, latitude_micros, longitude_micros,
		speed_over_ground_milliknots, course_over_ground_millidegrees, observed_at
		FROM latest_positions
		WHERE mmsi = ANY($1) AND classification = ANY($2)`, mmsis, clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("latest positions by mmsi: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var position LatestPosition
		if err := rows.Scan(&position.MMSI, &position.LatitudeMicros, &position.LongitudeMicros,
			&position.SpeedOverGroundMilliknots, &position.CourseOverGroundMillidegrees, &position.ObservedAt); err != nil {
			return nil, fmt.Errorf("latest position scan: %w", err)
		}
		out[position.MMSI] = position
	}
	return out, rows.Err()
}

// RecentSpeedSamples returns the vessel's most recent reported SOG values
// (milliknots, newest first) from the shared position plane at the
// caller's clearance. The ETA engine medians over these; an empty result
// is an honest "no observations" and triggers the documented fallback.
func (store *Store) RecentSpeedSamples(ctx context.Context, mmsi string, limit int, clearedLabels []string) ([]uint32, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := store.pool.Query(ctx, `SELECT speed_over_ground_milliknots
		FROM ais_positions
		WHERE mmsi = $1 AND speed_over_ground_milliknots IS NOT NULL AND classification = ANY($3)
		ORDER BY observed_at DESC LIMIT $2`, mmsi, limit, clearedLabels)
	if err != nil {
		return nil, fmt.Errorf("recent speed samples: %w", err)
	}
	defer rows.Close()
	samples := make([]uint32, 0, limit)
	for rows.Next() {
		var sample uint32
		if err := rows.Scan(&sample); err != nil {
			return nil, fmt.Errorf("speed sample scan: %w", err)
		}
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}
