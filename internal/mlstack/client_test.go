package mlstack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewClientFailClosed(t *testing.T) {
	_, err := NewClient(Config{BaseURL: "not-a-url", ServiceToken: "tok"})
	require.Error(t, err)
	_, err = NewClient(Config{BaseURL: "http://ml:8100", ServiceToken: ""})
	require.Error(t, err, "missing service token must fail closed")
	_, err = NewClient(Config{BaseURL: "http://ml:8100?x=1", ServiceToken: "tok"})
	require.Error(t, err)
	client, err := NewClient(Config{BaseURL: "http://ml:8100", ServiceToken: "tok"})
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestScoreBerthAllocationOK(t *testing.T) {
	var authed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/score/berth-allocation", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		authed.Store(r.Header.Get("Authorization") == "Bearer svc-token")
		var body BerthAllocationRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "NGAPP", body.PortCode)
		require.Len(t, body.Vessels, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"OK","mode":"shadow","policy_version":"berth-v3","suggestion":{"assignments":[{"mmsi":"123456789","berth_id":"B4"}]},"latency_ms":12.5}`))
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "svc-token", Timeout: 2 * time.Second})
	require.NoError(t, err)
	resp, err := client.ScoreBerthAllocation(context.Background(), BerthAllocationRequest{
		RequestID: "req-1", PortCode: "NGAPP",
		Vessels: []VesselInput{{MMSI: "123456789", ETA: "2026-01-01T00:00:00Z"}},
	})
	require.NoError(t, err)
	require.True(t, authed.Load(), "service token must be sent")
	require.Equal(t, "shadow", resp.Mode)
	require.Equal(t, "berth-v3", resp.PolicyVersion)
	require.Contains(t, string(resp.Suggestion), "B4")
}

func TestScoreUntrained409(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"detail":"policy berth-allocation not promoted"}`))
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "tok"})
	require.NoError(t, err)
	_, err = client.ScoreBerthAllocation(context.Background(), BerthAllocationRequest{RequestID: "r", PortCode: "NGAPP", Vessels: []VesselInput{{MMSI: "123456789"}}})
	require.Error(t, err)
	require.True(t, IsUntrained(err))
	require.Contains(t, err.Error(), "not promoted")
}

func TestScoreBodyUntrainedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"SCORING_UNAVAILABLE","mode":"rules_only","detail":"no policy registered"}`))
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "tok"})
	require.NoError(t, err)
	_, err = client.ScoreRouteAdvice(context.Background(), RouteAdviceRequest{RequestID: "r", Origin: "NGAPP", Destination: "NGLOS"})
	require.True(t, IsUntrained(err))
}

func TestScoreUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "tok"})
	require.NoError(t, err)
	_, err = client.ScoreRouteAdvice(context.Background(), RouteAdviceRequest{RequestID: "r", Origin: "NGAPP", Destination: "NGLOS"})
	require.True(t, IsUnavailable(err))
	require.Contains(t, err.Error(), "HTTP 503")
}

func TestScoreContractViolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"OK","mode":"shadow"}`))
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "tok"})
	require.NoError(t, err)
	_, err = client.ScoreBerthAllocation(context.Background(), BerthAllocationRequest{RequestID: "r", PortCode: "NGAPP", Vessels: []VesselInput{{MMSI: "123456789"}}})
	require.True(t, IsUnavailable(err), "missing policy_version must be treated as contract violation")
}

func TestScoreRedirectRefused(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"OK","mode":"shadow","policy_version":"v1"}`))
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()
	client, err := NewClient(Config{BaseURL: redirector.URL, ServiceToken: "tok"})
	require.NoError(t, err)
	_, err = client.ScoreRouteAdvice(context.Background(), RouteAdviceRequest{RequestID: "r", Origin: "NGAPP", Destination: "NGLOS"})
	require.Error(t, err, "redirects must never be followed")
	require.True(t, IsUnavailable(err))
}

func TestScoreTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte(`{"status":"OK","policy_version":"v1"}`))
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ServiceToken: "tok", Timeout: 50 * time.Millisecond})
	require.NoError(t, err)
	_, err = client.ScoreBerthAllocation(context.Background(), BerthAllocationRequest{RequestID: "r", PortCode: "NGAPP", Vessels: []VesselInput{{MMSI: "123456789"}}})
	require.True(t, IsUnavailable(err))
}
