// Package gtfs is the GTFS static feed factory: it renders the tenant's
// transit registry (migration 0009) as a spec-valid, deterministic
// gtfs.zip — agency.txt, routes.txt, stops.txt, trips.txt, stop_times.txt,
// calendar.txt. Output is a pure function of the registry snapshot:
// identical registry → identical bytes → stable ETag. Nothing is inferred
// or synthesized; what is not in the registry is not in the feed.
package gtfs

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// BuildStaticZip renders the registry snapshot as a GTFS zip archive and
// returns the archive bytes plus a strong ETag (sha256 of the payload).
// The build fails closed when the registry is referentially incomplete
// (trips without stop_times, stop_times referencing unknown stops/trips,
// non-monotonic times): a broken feed must never be served.
func BuildStaticZip(registry *store.TransitRegistry) (payload []byte, etag string, err error) {
	if registry == nil {
		return nil, "", errors.New("transit registry snapshot is required")
	}
	files, err := renderFiles(registry)
	if err != nil {
		return nil, "", err
	}
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)
	// Deterministic entry order + zeroed timestamps: identical input must
	// produce identical bytes (stable ETag, cache-friendly 304s).
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.Modified = time.Time{}
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return nil, "", fmt.Errorf("zip entry %s: %w", name, err)
		}
		if _, err := writer.Write(files[name]); err != nil {
			return nil, "", fmt.Errorf("zip entry %s: %w", name, err)
		}
	}
	if err := zipWriter.Close(); err != nil {
		return nil, "", fmt.Errorf("zip close: %w", err)
	}
	sum := sha256.Sum256(buffer.Bytes())
	return buffer.Bytes(), `"` + hex.EncodeToString(sum[:]) + `"`, nil
}

// renderFiles produces every required GTFS text file, validating
// referential integrity before a single byte is zipped.
func renderFiles(registry *store.TransitRegistry) (map[string][]byte, error) {
	stopsByID := make(map[string]store.TransitStop, len(registry.Stops))
	for _, stop := range registry.Stops {
		stopsByID[stop.StopID] = stop
	}
	tripsByID := make(map[string]store.TransitTrip, len(registry.Trips))
	for _, trip := range registry.Trips {
		tripsByID[trip.TripID] = trip
	}
	routesByID := make(map[string]store.TransitRoute, len(registry.Routes))
	for _, route := range registry.Routes {
		routesByID[route.RouteID] = route
	}
	calendarsByID := make(map[string]store.TransitCalendar, len(registry.Calendars))
	for _, calendar := range registry.Calendars {
		calendarsByID[calendar.ServiceID] = calendar
	}
	agencyIDs := make(map[string]struct{}, len(registry.Agencies))
	for _, agency := range registry.Agencies {
		agencyIDs[agency.AgencyID] = struct{}{}
	}

	// Referential integrity — fail closed on a broken registry.
	stopTimesByTrip := make(map[string][]store.TransitStopTime)
	for _, stopTime := range registry.StopTimes {
		if _, ok := tripsByID[stopTime.TripID]; !ok {
			return nil, fmt.Errorf("stop_times reference unknown trip %q", stopTime.TripID)
		}
		if _, ok := stopsByID[stopTime.StopID]; !ok {
			return nil, fmt.Errorf("stop_times reference unknown stop %q (trip %q)", stopTime.StopID, stopTime.TripID)
		}
		stopTimesByTrip[stopTime.TripID] = append(stopTimesByTrip[stopTime.TripID], stopTime)
	}
	for tripID, times := range stopTimesByTrip {
		previous := -1
		for _, stopTime := range times { // loaded ORDER BY trip_id, stop_sequence
			if stopTime.ArrivalSeconds < previous {
				return nil, fmt.Errorf("trip %q has non-monotonic stop_times", tripID)
			}
			previous = stopTime.ArrivalSeconds
		}
	}
	for _, trip := range registry.Trips {
		if _, ok := routesByID[trip.RouteID]; !ok {
			return nil, fmt.Errorf("trip %q references unknown route %q", trip.TripID, trip.RouteID)
		}
		if _, ok := calendarsByID[trip.ServiceID]; !ok {
			return nil, fmt.Errorf("trip %q references unknown service %q", trip.TripID, trip.ServiceID)
		}
		if len(stopTimesByTrip[trip.TripID]) == 0 {
			return nil, fmt.Errorf("trip %q has no stop_times", trip.TripID)
		}
	}
	for _, route := range registry.Routes {
		if _, ok := agencyIDs[route.AgencyID]; !ok {
			return nil, fmt.Errorf("route %q references unknown agency %q", route.RouteID, route.AgencyID)
		}
	}

	files := make(map[string][]byte, 6)
	var err error
	if files["agency.txt"], err = renderCSV([]string{
		"agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone",
	}, func(add func([]string)) {
		for _, agency := range registry.Agencies {
			add([]string{agency.AgencyID, agency.Name, agency.URL, agency.Timezone, agency.Lang, agency.Phone})
		}
	}); err != nil {
		return nil, err
	}
	if files["routes.txt"], err = renderCSV([]string{
		"route_id", "agency_id", "route_short_name", "route_long_name", "route_type",
	}, func(add func([]string)) {
		for _, route := range registry.Routes {
			if !route.Active {
				continue
			}
			add([]string{route.RouteID, route.AgencyID, route.ShortName, route.LongName,
				strconv.Itoa(route.RouteType)})
		}
	}); err != nil {
		return nil, err
	}
	if files["stops.txt"], err = renderCSV([]string{
		"stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id",
	}, func(add func([]string)) {
		for _, stop := range registry.Stops {
			add([]string{stop.StopID, stop.Name, renderMicros(stop.LatitudeMicros),
				renderMicros(stop.LongitudeMicros), stop.ZoneID})
		}
	}); err != nil {
		return nil, err
	}
	if files["trips.txt"], err = renderCSV([]string{
		"route_id", "service_id", "trip_id", "trip_headsign", "direction_id",
	}, func(add func([]string)) {
		for _, trip := range registry.Trips {
			direction := ""
			if trip.DirectionID != nil {
				direction = strconv.Itoa(int(*trip.DirectionID))
			}
			add([]string{trip.RouteID, trip.ServiceID, trip.TripID, trip.Headsign, direction})
		}
	}); err != nil {
		return nil, err
	}
	if files["stop_times.txt"], err = renderCSV([]string{
		"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
	}, func(add func([]string)) {
		for _, stopTime := range registry.StopTimes {
			add([]string{stopTime.TripID, renderHMS(stopTime.ArrivalSeconds),
				renderHMS(stopTime.DepartureSeconds), stopTime.StopID,
				strconv.Itoa(stopTime.StopSequence)})
		}
	}); err != nil {
		return nil, err
	}
	if files["calendar.txt"], err = renderCSV([]string{
		"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
		"start_date", "end_date",
	}, func(add func([]string)) {
		for _, calendar := range registry.Calendars {
			row := []string{calendar.ServiceID}
			for _, runs := range calendar.Weekdays {
				row = append(row, renderBool(runs))
			}
			row = append(row, calendar.StartDate.Format("20060102"), calendar.EndDate.Format("20060102"))
			add(row)
		}
	}); err != nil {
		return nil, err
	}
	return files, nil
}

// renderCSV serializes one GTFS text file (header + rows, \n newlines per
// the CSV convention accepted by the canonical validators).
func renderCSV(header []string, rows func(add func([]string))) ([]byte, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(header); err != nil {
		return nil, err
	}
	var writeErr error
	rows(func(row []string) {
		if writeErr != nil {
			return
		}
		writeErr = writer.Write(row)
	})
	if writeErr != nil {
		return nil, writeErr
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// renderMicros renders fixed-point micro-degrees as an exact decimal
// string (no binary float round-trip), matching the store doctrine.
func renderMicros(micros int32) string {
	negative := micros < 0
	absolute := int64(micros)
	if negative {
		absolute = -absolute
	}
	text := fmt.Sprintf("%d.%06d", absolute/1_000_000, absolute%1_000_000)
	if negative {
		return "-" + text
	}
	return text
}

// renderHMS renders seconds-after-midnight as GTFS HH:MM:SS (hours may
// exceed 24 for after-midnight trips).
func renderHMS(seconds int) string {
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, (seconds%3600)/60, seconds%60)
}

func renderBool(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
