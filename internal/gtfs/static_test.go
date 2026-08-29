package gtfs

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// testRegistry is a small two-stop weekday ferry route.
func testRegistry() *store.TransitRegistry {
	return &store.TransitRegistry{
		Agencies: []store.TransitAgency{{
			AgencyID: "niwa", Name: "NIWA", URL: "https://niwa.gov.ng", Timezone: "Africa/Lagos", Lang: "en",
		}},
		Routes: []store.TransitRoute{{
			RouteID: "R1", AgencyID: "niwa", ShortName: "F1", LongName: "Marina — Ikorodu",
			RouteType: 4, DefaultSpeedMilliknots: 8000, Active: true,
		}},
		Stops: []store.TransitStop{
			{StopID: "S1", Name: "Marina Jetty", LatitudeMicros: 6451830, LongitudeMicros: 3400100, ZoneID: "Z1"},
			{StopID: "S2", Name: "Ikorodu Terminal", LatitudeMicros: 6619400, LongitudeMicros: 3503300, ZoneID: "Z2"},
		},
		Calendars: []store.TransitCalendar{{
			ServiceID: "WEEKDAY", Weekdays: [7]bool{true, true, true, true, true, false, false},
			StartDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EndDate:   time.Date(2027, 12, 31, 0, 0, 0, 0, time.UTC),
		}},
		Trips: []store.TransitTrip{{
			TripID: "T1", RouteID: "R1", ServiceID: "WEEKDAY", Headsign: "Ikorodu",
		}},
		StopTimes: []store.TransitStopTime{
			{TripID: "T1", StopSequence: 1, StopID: "S1", ArrivalSeconds: 28800, DepartureSeconds: 28800},
			{TripID: "T1", StopSequence: 2, StopID: "S2", ArrivalSeconds: 32400, DepartureSeconds: 32400},
		},
	}
}

func unzip(t *testing.T, payload []byte) map[string][]string {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	require.NoError(t, err)
	out := make(map[string][]string)
	for _, file := range reader.File {
		handle, err := file.Open()
		require.NoError(t, err)
		body, err := io.ReadAll(handle)
		require.NoError(t, err)
		require.NoError(t, handle.Close())
		records, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
		require.NoError(t, err, "%s must be valid CSV", file.Name)
		out[file.Name] = flatten(records)
	}
	return out
}

func flatten(records [][]string) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record...)
	}
	return out
}

func parseCSV(t *testing.T, payload []byte, name string) [][]string {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	require.NoError(t, err)
	for _, file := range reader.File {
		if file.Name != name {
			continue
		}
		handle, err := file.Open()
		require.NoError(t, err)
		defer handle.Close()
		records, err := csv.NewReader(handle).ReadAll()
		require.NoError(t, err)
		return records
	}
	t.Fatalf("%s missing from archive", name)
	return nil
}

func TestStaticZipRequiredFilesPresent(t *testing.T) {
	payload, etag, err := BuildStaticZip(testRegistry())
	require.NoError(t, err)
	require.NotEmpty(t, etag)
	files := unzip(t, payload)
	for _, required := range []string{
		"agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt",
	} {
		require.Contains(t, files, required, "required GTFS file %s", required)
		require.NotEmpty(t, files[required], "%s must not be empty", required)
	}
}

func TestStaticZipDeterministic(t *testing.T) {
	first, etagFirst, err := BuildStaticZip(testRegistry())
	require.NoError(t, err)
	second, etagSecond, err := BuildStaticZip(testRegistry())
	require.NoError(t, err)
	require.Equal(t, etagFirst, etagSecond)
	require.Equal(t, first, second, "identical registry must produce identical bytes (stable ETag)")
}

func TestStaticZipReferentialIntegrityAndMonotonicTimes(t *testing.T) {
	payload, _, err := BuildStaticZip(testRegistry())
	require.NoError(t, err)

	stops := parseCSV(t, payload, "stops.txt")
	stopIDs := map[string]bool{}
	for _, row := range stops[1:] {
		stopIDs[row[0]] = true
	}
	trips := parseCSV(t, payload, "trips.txt")
	tripIDs := map[string]bool{}
	for _, row := range trips[1:] {
		tripIDs[row[2]] = true
	}
	stopTimes := parseCSV(t, payload, "stop_times.txt")
	require.Equal(t, []string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"}, stopTimes[0])
	previousSeconds := -1
	for _, row := range stopTimes[1:] {
		require.True(t, tripIDs[row[0]], "stop_times trip %s must exist in trips.txt", row[0])
		require.True(t, stopIDs[row[3]], "stop_times stop %s must exist in stops.txt", row[3])
		arrival := parseHMS(t, row[1])
		departure := parseHMS(t, row[2])
		require.GreaterOrEqual(t, departure, arrival, "departure must not precede arrival")
		require.GreaterOrEqual(t, arrival, previousSeconds, "times must be monotonic per trip")
		previousSeconds = arrival
	}
	routes := parseCSV(t, payload, "routes.txt")
	require.Equal(t, "4", routes[1][4], "ferry routes are GTFS route_type 4")
	calendar := parseCSV(t, payload, "calendar.txt")
	require.Equal(t, []string{"WEEKDAY", "1", "1", "1", "1", "1", "0", "0", "20260101", "20271231"}, calendar[1])
	stopsRow := stops[1]
	require.Equal(t, "6.451830", stopsRow[2], "fixed-point coordinates render exactly")
	require.Equal(t, "3.400100", stopsRow[3])
}

func parseHMS(t *testing.T, value string) int {
	t.Helper()
	var hours, minutes, seconds int
	n, err := strconv.Atoi(value[0:2])
	require.NoError(t, err)
	hours = n
	n, err = strconv.Atoi(value[3:5])
	require.NoError(t, err)
	minutes = n
	n, err = strconv.Atoi(value[6:8])
	require.NoError(t, err)
	seconds = n
	return hours*3600 + minutes*60 + seconds
}

func TestStaticZipFailsClosedOnBrokenRegistry(t *testing.T) {
	broken := testRegistry()
	broken.StopTimes[0].StopID = "GHOST"
	_, _, err := BuildStaticZip(broken)
	require.Error(t, err, "stop_times referencing unknown stops must fail the build")

	broken = testRegistry()
	broken.StopTimes = broken.StopTimes[:1] // trip T1 now has 1 visit; remove all and check empty
	broken.StopTimes = nil
	_, _, err = BuildStaticZip(broken)
	require.Error(t, err, "a trip without stop_times must fail the build")

	broken = testRegistry()
	broken.StopTimes[0].ArrivalSeconds = 40000 // after the second stop — non-monotonic
	_, _, err = BuildStaticZip(broken)
	require.Error(t, err, "non-monotonic stop_times must fail the build")

	broken = testRegistry()
	broken.Routes[0].AgencyID = "GHOST"
	_, _, err = BuildStaticZip(broken)
	require.Error(t, err, "routes referencing unknown agencies must fail the build")
}
