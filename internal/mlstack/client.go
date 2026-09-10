// Package mlstack is the typed, config-gated client for the
// blueeconomy-ml-stack inference service (Phase 18 contract):
//
//	POST {ML_STACK_HTTP_URL}/score/berth-allocation
//	POST {ML_STACK_HTTP_URL}/score/route-advice
//
// Doctrine (fail-closed, mirroring singlewindow's polyglot ml-stack
// scorer): the ml-stack endpoints are Keycloak-JWKS authenticated, so every
// call carries the env-only service token; an untrained policy answers 409
// (and a degraded service 5xx), which surface here as typed errors the API
// layer maps to honest 503s (POLICY_UNTRAINED / SCORING_UNAVAILABLE) —
// never a fabricated suggestion. Redirects are refused and the timeout is
// bounded so a wedged scorer can never hang the geo hot path.
package mlstack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// UntrainedError is the honest "policy not promoted yet" signal: ml-stack
// answered 409 (or a 200 body without an OK status). The API layer maps it
// to 503 POLICY_UNTRAINED.
type UntrainedError struct {
	Model  string
	Detail string
}

func (err *UntrainedError) Error() string {
	return fmt.Sprintf("POLICY_UNTRAINED: %s: %s", err.Model, err.Detail)
}

// IsUntrained reports whether err is an untrained-policy signal.
func IsUntrained(err error) bool {
	var target *UntrainedError
	return errors.As(err, &target)
}

// UnavailableError is any other scoring failure: transport error, refused
// redirect, 5xx, or a malformed/contract-violating response body. The API
// layer maps it to 503 SCORING_UNAVAILABLE.
type UnavailableError struct {
	Model  string
	Detail string
}

func (err *UnavailableError) Error() string {
	return fmt.Sprintf("SCORING_UNAVAILABLE: %s: %s", err.Model, err.Detail)
}

// IsUnavailable reports whether err is a scorer-unavailability signal.
func IsUnavailable(err error) bool {
	var target *UnavailableError
	return errors.As(err, &target)
}

// Config gates the client. Empty BaseURL disables the client entirely
// (NewClient is not called); set-but-incomplete configuration fails
// closed at startup.
type Config struct {
	// BaseURL is ML_STACK_HTTP_URL (http/https, no query).
	BaseURL string
	// ServiceToken is ML_STACK_SERVICE_TOKEN (env-only; sent as a Bearer
	// token — the ml-stack scoring endpoints are Keycloak-authed).
	ServiceToken string
	// Timeout bounds each scoring call (GEO_ML_STACK_TIMEOUT, default 5s).
	Timeout time.Duration
}

// Client scores against the ml-stack policy endpoints.
type Client struct {
	base   *url.URL
	token  string
	http   *http.Client
	models map[string]string
}

// NewClient validates the configuration fail-closed and refuses redirects.
func NewClient(cfg Config) (*Client, error) {
	base, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("ML_STACK_HTTP_URL %q must be an absolute http(s) URL", cfg.BaseURL)
	}
	if base.RawQuery != "" {
		return nil, errors.New("ML_STACK_HTTP_URL must not carry a query string")
	}
	token := strings.TrimSpace(cfg.ServiceToken)
	if token == "" {
		return nil, errors.New("ML_STACK_SERVICE_TOKEN must be set when ML_STACK_HTTP_URL is configured (the scoring endpoints are authenticated)")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		base:  base,
		token: token,
		http: &http.Client{
			Timeout: timeout,
			// Never follow redirects: a redirected scorer is a
			// misconfiguration, and following it could leak the service
			// token to a different origin.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		models: map[string]string{
			"berth-allocation": "/score/berth-allocation",
			"route-advice":     "/score/route-advice",
		},
	}, nil
}

// VesselInput describes one incoming vessel for the berth-allocation policy.
type VesselInput struct {
	MMSI        string  `json:"mmsi"`
	ETA         string  `json:"eta,omitempty"` // RFC 3339
	LOAMeters   float64 `json:"loa_meters,omitempty"`
	DraftMeters float64 `json:"draft_meters,omitempty"`
}

// BerthInput describes one candidate berth slot.
type BerthInput struct {
	BerthID       string  `json:"berth_id"`
	LengthMeters  float64 `json:"length_meters,omitempty"`
	DepthMeters   float64 `json:"depth_meters,omitempty"`
	OccupiedUntil string  `json:"occupied_until,omitempty"` // RFC 3339
}

// BerthAllocationRequest is the POST body for /score/berth-allocation.
type BerthAllocationRequest struct {
	RequestID string        `json:"request_id"`
	PortCode  string        `json:"port_code"`
	Vessels   []VesselInput `json:"vessels"`
	Berths    []BerthInput  `json:"berths,omitempty"`
}

// RouteAdviceRequest is the POST body for /score/route-advice.
type RouteAdviceRequest struct {
	RequestID     string       `json:"request_id"`
	Origin        string       `json:"origin"`
	Destination   string       `json:"destination"`
	Vessel        *VesselInput `json:"vessel,omitempty"`
	DepartureTime string       `json:"departure_time,omitempty"` // RFC 3339
}

// PolicyResponse is the typed ml-stack policy response. Suggestion carries
// the policy output verbatim (berth assignment / ranked route options); the
// geo-service never interprets or mutates it — shadow mode.
type PolicyResponse struct {
	Status        string          `json:"status"`
	Mode          string          `json:"mode"`
	PolicyVersion string          `json:"policy_version"`
	Detail        string          `json:"detail,omitempty"`
	Suggestion    json.RawMessage `json:"suggestion,omitempty"`
	LatencyMs     float64         `json:"latency_ms,omitempty"`
}

// ScoreBerthAllocation calls POST /score/berth-allocation.
func (client *Client) ScoreBerthAllocation(ctx context.Context, request BerthAllocationRequest) (*PolicyResponse, error) {
	return client.score(ctx, "berth-allocation", request)
}

// ScoreRouteAdvice calls POST /score/route-advice.
func (client *Client) ScoreRouteAdvice(ctx context.Context, request RouteAdviceRequest) (*PolicyResponse, error) {
	return client.score(ctx, "route-advice", request)
}

// score posts one policy request and classifies the outcome fail-closed:
// 200 with a contract-valid body (policy_version present) is the only
// success; 409 is untrained; everything else is unavailable.
func (client *Client) score(ctx context.Context, model string, payload any) (*PolicyResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, &UnavailableError{Model: model, Detail: "encode request: " + err.Error()}
	}
	endpoint := client.base.ResolveReference(&url.URL{Path: client.models[model]})
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, &UnavailableError{Model: model, Detail: "build request: " + err.Error()}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+client.token)
	httpRequest.Header.Set("Accept", "application/json")
	response, err := client.http.Do(httpRequest)
	if err != nil {
		return nil, &UnavailableError{Model: model, Detail: "request failed: " + err.Error()}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, &UnavailableError{Model: model, Detail: "read response: " + err.Error()}
	}
	if response.StatusCode == http.StatusConflict {
		detail := http.StatusText(response.StatusCode)
		var decoded struct {
			Detail string `json:"detail"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(raw, &decoded); err == nil {
			if decoded.Detail != "" {
				detail = decoded.Detail
			} else if decoded.Error != "" {
				detail = decoded.Error
			}
		}
		return nil, &UntrainedError{Model: model, Detail: detail}
	}
	if response.StatusCode != http.StatusOK {
		return nil, &UnavailableError{Model: model,
			Detail: fmt.Sprintf("ml-stack answered HTTP %d", response.StatusCode)}
	}
	var decoded PolicyResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, &UnavailableError{Model: model, Detail: "response body is not valid JSON"}
	}
	// Contract enforcement: a scorer body without status/policy_version is a
	// contract violation — never present it to callers as a suggestion.
	if strings.EqualFold(decoded.Status, "SCORING_UNAVAILABLE") || decoded.Status == "POLICY_UNTRAINED" {
		detail := decoded.Detail
		if detail == "" {
			detail = "policy not trained/promoted"
		}
		return nil, &UntrainedError{Model: model, Detail: detail}
	}
	if decoded.PolicyVersion == "" {
		return nil, &UnavailableError{Model: model, Detail: "response violates contract: policy_version missing"}
	}
	return &decoded, nil
}
