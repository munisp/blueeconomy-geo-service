package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// densityGrid: GET /v1/geo/vessels/density?bbox=minLon,minLat,maxLon,maxLat
// (micro-degrees, same convention as /v1/geo/vessels) &cellSizeMeters=
// (100..100000, default 5000).
//
// Server-side grid aggregation over latest_positions (G3): the honest map
// feed for heatmaps without fabricating or interpolating anything. Only
// occupied cells are returned; an absent cell means "no recorded vessel at
// the caller's clearance", never a synthesized zero-density claim beyond
// what the data supports. The cell size is converted meters → micro-degrees
// at the equatorial reference (1° ≈ 111 320 m); cells are axis-aligned
// micro-degree squares, so they narrow toward the poles in ground meters —
// this approximation is documented on the response.
func (g *GeoV2) densityGrid(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var minLon, minLat, maxLon, maxLat int32
	bbox := strings.TrimSpace(request.URL.Query().Get("bbox"))
	if bbox == "" {
		writeError(writer, http.StatusBadRequest, "bbox=minLon,minLat,maxLon,maxLat (micro-degrees) is required")
		return
	}
	parts := strings.Split(bbox, ",")
	if len(parts) != 4 {
		writeError(writer, http.StatusBadRequest, "bbox must be minLon,minLat,maxLon,maxLat in micro-degrees")
		return
	}
	values := make([]int32, 4)
	for i, part := range parts {
		value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 32)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "bbox values must be fixed-point micro-degrees")
			return
		}
		values[i] = int32(value)
	}
	minLon, minLat, maxLon, maxLat = values[0], values[1], values[2], values[3]
	if minLon >= maxLon || minLat >= maxLat {
		writeError(writer, http.StatusBadRequest, "bbox min must be below max")
		return
	}
	cellMeters := 5000.0
	if raw := strings.TrimSpace(request.URL.Query().Get("cellSizeMeters")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 100 || parsed > 100_000 {
			writeError(writer, http.StatusBadRequest, "cellSizeMeters must be in [100, 100000]")
			return
		}
		cellMeters = parsed
	}
	// meters → micro-degrees at the equatorial reference meridian.
	cellMicros := int64(cellMeters * 1e6 / 111_320.0)
	if cellMicros <= 0 {
		cellMicros = 1
	}
	cells, asOf, err := g.Store.DensityGrid(request.Context(), minLon, minLat, maxLon, maxLat,
		cellMicros, clearedLabels(principal.Clearance))
	if err != nil {
		if strings.Contains(err.Error(), "cells (max") {
			writeError(writer, http.StatusBadRequest, err.Error())
			return
		}
		writeError(writer, http.StatusServiceUnavailable, "DENSITY_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"cells": cells, "cellCount": len(cells),
		"cellSizeMeters": cellMeters, "cellSizeMicros": cellMicros,
		"emptyCells":     "OMITTED: only occupied cells are returned; absence means no recorded vessel at the caller clearance",
		"projection":     "axis-aligned micro-degree grid; cellSizeMeters converts at the equatorial reference (1 deg = 111320 m), ground size narrows toward the poles",
		"provenance":     g.prov("latest_positions", asOf, 15*time.Minute),
	})
}
