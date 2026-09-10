package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

func TestDensityGridValidation(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)

	// Missing bbox → 400.
	req := httptest.NewRequest("GET", "/v1/geo/vessels/density", nil).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Bad bbox shape → 400.
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=1,2,3", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// min >= max → 400.
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=5000000,-5000000,4000000,-4000000", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Cell size out of range → 400.
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=-5000000,39000000,-4000000,40000000&cellSizeMeters=10", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Cell count guard: whole world at 100m cells → 400 (fail closed).
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=-180000000,-90000000,180000000,90000000&cellSizeMeters=100", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestDensityGridHonestCells(t *testing.T) {
	asOf := time.Now().Add(-time.Minute)
	fake := &fakeGeoV2Store{
		density: []store.DensityCellRow{
			{CellLatMicros: -4_100_000, CellLonMicros: 39_600_000, VesselCount: 3, LatestObservedAt: asOf},
		},
		densityAsOf: asOf,
	}
	g := newTestGeoV2(fake)

	req := httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=-5000000,39000000,-4000000,40000000&cellSizeMeters=5000", nil).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"vesselCount":3`)
	require.Contains(t, rec.Body.String(), `"cellCount":1`)
	require.Contains(t, rec.Body.String(), "OMITTED") // honest empty-cell contract

	// Empty grid is a 200 with zero cells, never fabricated density.
	fake.density = nil
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=-5000000,39000000,-4000000,40000000", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"cellCount":0`)

	// Store failure → 503, never an empty 200 masquerading as data.
	fake.failReads = true
	req = httptest.NewRequest("GET", "/v1/geo/vessels/density?bbox=-5000000,39000000,-4000000,40000000", nil).WithContext(testPrincipalCtx())
	rec = httptest.NewRecorder()
	g.densityGrid(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
