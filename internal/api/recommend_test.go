package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/mlstack"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// fakeScorer is a PolicyScorer stub (mock ml-stack allowed in tests only).
type fakeScorer struct {
	berthResp *mlstack.PolicyResponse
	routeResp *mlstack.PolicyResponse
	err       error
	lastReq   any
}

func (fake *fakeScorer) ScoreBerthAllocation(_ context.Context, request mlstack.BerthAllocationRequest) (*mlstack.PolicyResponse, error) {
	fake.lastReq = request
	if fake.err != nil {
		return nil, fake.err
	}
	return fake.berthResp, nil
}

func (fake *fakeScorer) ScoreRouteAdvice(_ context.Context, request mlstack.RouteAdviceRequest) (*mlstack.PolicyResponse, error) {
	fake.lastReq = request
	if fake.err != nil {
		return nil, fake.err
	}
	return fake.routeResp, nil
}

// fakeLogStore records recommendation_log inserts.
type fakeLogStore struct {
	rows []store.RecommendationLogRow
	fail bool
}

func (fake *fakeLogStore) InsertRecommendationLog(_ context.Context, row store.RecommendationLogRow) error {
	if fake.fail {
		return errors.New("db down")
	}
	fake.rows = append(fake.rows, row)
	return nil
}

func newTestRecommendServer(t *testing.T, scorer PolicyScorer, logStore RecommendationLogStore) *Server {
	t.Helper()
	// No store wiring needed: the recommendation handlers persist through
	// the RecommendationLogStore boundary, never through Server.Store.
	server := &Server{Metrics: metrics.NewRegistry()}
	if scorer != nil {
		recommendations, err := NewRecommendations(scorer, logStore, server.Metrics)
		require.NoError(t, err)
		server.Recommend = recommendations
	}
	return server
}

func TestNewRecommendationsFailClosed(t *testing.T) {
	registry := metrics.NewRegistry()
	_, err := NewRecommendations(nil, &fakeLogStore{}, registry)
	require.Error(t, err)
	_, err = NewRecommendations(&fakeScorer{}, nil, registry)
	require.Error(t, err)
	_, err = NewRecommendations(&fakeScorer{}, &fakeLogStore{}, nil)
	require.Error(t, err)
}

func TestBerthRecommendationUnconfigured(t *testing.T) {
	server := newTestRecommendServer(t, nil, nil)
	req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation", strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789"}]}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.berthRecommendation(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "RECOMMENDATION_UNCONFIGURED")
	require.Equal(t, int64(1), server.Metrics.Snapshot()[`geo_ml_recommendations_total{kind="berth",result="unconfigured"}`])
}

func TestBerthRecommendationValidation(t *testing.T) {
	server := newTestRecommendServer(t, &fakeScorer{}, &fakeLogStore{})
	cases := []string{
		`{"portCode":"ngapp","vessels":[{"mmsi":"123456789"}]}`, // lower-case port
		`{"portCode":"NGAPP","vessels":[]}`,                     // no vessels
		`{"portCode":"NGAPP","vessels":[{"mmsi":"123"}]}`,       // bad mmsi
		`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789","eta":"not-a-time"}]}`,
		`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789"}],"berths":[{"berthId":""}]}`,
	}
	for _, body := range cases {
		req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation", strings.NewReader(body)).WithContext(testPrincipalCtx())
		rec := httptest.NewRecorder()
		server.berthRecommendation(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func TestBerthRecommendationServedShadowLogged(t *testing.T) {
	scorer := &fakeScorer{berthResp: &mlstack.PolicyResponse{
		Status: "OK", Mode: "shadow", PolicyVersion: "berth-v3",
		Suggestion: []byte(`{"assignments":[{"mmsi":"123456789","berthId":"B4"}]}`),
	}}
	logStore := &fakeLogStore{}
	server := newTestRecommendServer(t, scorer, logStore)
	req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation",
		strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789","eta":"2026-01-01T00:00:00Z"}],"berths":[{"berthId":"B4"}]}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.berthRecommendation(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"mode":"shadow"`)
	require.Contains(t, rec.Body.String(), `"policyVersion":"berth-v3"`)
	require.Contains(t, rec.Body.String(), `"berthId":"B4"`)
	require.Contains(t, rec.Body.String(), "never auto-applied")
	// Logged BEFORE serving: exactly one durable row with the reward-trail
	// fields (request hash, policy version, verbatim suggestion).
	require.Len(t, logStore.rows, 1)
	row := logStore.rows[0]
	require.Equal(t, "berth_allocation", row.Kind)
	require.Len(t, row.RequestHash, 64)
	require.Equal(t, "berth-v3", row.PolicyVersion)
	require.Equal(t, "shadow", row.Mode)
	require.Contains(t, string(row.Suggestion), "B4")
	require.NotEmpty(t, row.RequestedBy)
	require.False(t, row.CreatedAt.IsZero())
	require.Equal(t, int64(1), server.Metrics.Snapshot()[`geo_ml_recommendations_total{kind="berth",result="served"}`])
	// Deterministic request hash: same validated payload → same digest.
	scorer2 := &fakeScorer{berthResp: scorer.berthResp}
	logStore2 := &fakeLogStore{}
	server2 := newTestRecommendServer(t, scorer2, logStore2)
	req2 := httptest.NewRequest("POST", "/v1/geo/berths/recommendation",
		strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789","eta":"2026-01-01T00:00:00Z"}],"berths":[{"berthId":"B4"}]}`)).WithContext(testPrincipalCtx())
	rec2 := httptest.NewRecorder()
	server2.berthRecommendation(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Len(t, logStore2.rows, 1)
	require.Equal(t, row.RequestHash, logStore2.rows[0].RequestHash)
}

func TestBerthRecommendationUntrained(t *testing.T) {
	scorer := &fakeScorer{err: &mlstack.UntrainedError{Model: "berth-allocation", Detail: "policy not promoted"}}
	logStore := &fakeLogStore{}
	server := newTestRecommendServer(t, scorer, logStore)
	req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation",
		strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789"}]}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.berthRecommendation(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "POLICY_UNTRAINED")
	require.Contains(t, rec.Body.String(), "policy not promoted")
	require.Empty(t, logStore.rows, "untrained rejections must never be logged as served recommendations")
	require.Equal(t, int64(1), server.Metrics.Snapshot()[`geo_ml_recommendations_total{kind="berth",result="untrained"}`])
}

func TestBerthRecommendationUnavailable(t *testing.T) {
	scorer := &fakeScorer{err: &mlstack.UnavailableError{Model: "berth-allocation", Detail: "HTTP 503"}}
	server := newTestRecommendServer(t, scorer, &fakeLogStore{})
	req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation",
		strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789"}]}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.berthRecommendation(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "SCORING_UNAVAILABLE")
}

func TestRecommendationLogFailureFailsClosed(t *testing.T) {
	scorer := &fakeScorer{berthResp: &mlstack.PolicyResponse{Status: "OK", Mode: "shadow", PolicyVersion: "v1"}}
	server := newTestRecommendServer(t, scorer, &fakeLogStore{fail: true})
	req := httptest.NewRequest("POST", "/v1/geo/berths/recommendation",
		strings.NewReader(`{"portCode":"NGAPP","vessels":[{"mmsi":"123456789"}]}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.berthRecommendation(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "RECOMMENDATION_LOG_FAILED")
	require.Equal(t, int64(1), server.Metrics.Snapshot()[`geo_ml_recommendations_total{kind="berth",result="log_failed"}`])
}

func TestRouteAdviceServed(t *testing.T) {
	scorer := &fakeScorer{routeResp: &mlstack.PolicyResponse{
		Status: "OK", Mode: "shadow", PolicyVersion: "route-v1",
		Suggestion: []byte(`{"options":[{"via":"channel-A","rank":1}]}`),
	}}
	logStore := &fakeLogStore{}
	server := newTestRecommendServer(t, scorer, logStore)
	req := httptest.NewRequest("POST", "/v1/geo/routes/advice",
		strings.NewReader(`{"origin":"NGAPP","destination":"NGLOS","departureTime":"2026-01-01T06:00:00Z"}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.routeAdvice(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"policyVersion":"route-v1"`)
	require.Contains(t, rec.Body.String(), "channel-A")
	require.Len(t, logStore.rows, 1)
	require.Equal(t, "route_advice", logStore.rows[0].Kind)
	require.Equal(t, int64(1), server.Metrics.Snapshot()[`geo_ml_recommendations_total{kind="route",result="served"}`])
}

func TestRouteAdviceValidation(t *testing.T) {
	server := newTestRecommendServer(t, &fakeScorer{}, &fakeLogStore{})
	cases := []string{
		`{"origin":"ngapp","destination":"NGLOS"}`,
		`{"origin":"NGAPP","destination":""}`,
		`{"origin":"NGAPP","destination":"NGLOS","vessel":{"mmsi":"12"}}`,
		`{"origin":"NGAPP","destination":"NGLOS","departureTime":"yesterday"}`,
	}
	for _, body := range cases {
		req := httptest.NewRequest("POST", "/v1/geo/routes/advice", strings.NewReader(body)).WithContext(testPrincipalCtx())
		rec := httptest.NewRecorder()
		server.routeAdvice(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
	}
}

func TestRouteAdviceUntrained(t *testing.T) {
	scorer := &fakeScorer{err: &mlstack.UntrainedError{Model: "route-advice", Detail: "untrained"}}
	server := newTestRecommendServer(t, scorer, &fakeLogStore{})
	req := httptest.NewRequest("POST", "/v1/geo/routes/advice",
		strings.NewReader(`{"origin":"NGAPP","destination":"NGLOS"}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.routeAdvice(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "POLICY_UNTRAINED")
}

// end-to-end against a mock ml-stack HTTP server (allowed in tests only):
// proves the wired path client → handler → log → response.
func TestRecommendationEndToEndMockMLStack(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/score/route-advice", r.URL.Path)
		require.Equal(t, "Bearer svc-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"OK","mode":"shadow","policy_version":"route-v2","suggestion":{"options":[{"via":"channel-B","etaMinutes":40}]}}`))
	}))
	defer mock.Close()
	client, err := mlstack.NewClient(mlstack.Config{BaseURL: mock.URL, ServiceToken: "svc-token", Timeout: 2 * time.Second})
	require.NoError(t, err)
	logStore := &fakeLogStore{}
	server := newTestRecommendServer(t, client, logStore)
	req := httptest.NewRequest("POST", "/v1/geo/routes/advice",
		strings.NewReader(`{"origin":"NGAPP","destination":"NGLOS"}`)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	server.routeAdvice(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"policyVersion":"route-v2"`)
	require.Len(t, logStore.rows, 1)
	require.Contains(t, string(logStore.rows[0].Request), "NGAPP")
}
