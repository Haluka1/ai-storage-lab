package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ai-inference-storage-showcase/router/internal/common"
)

const contractTraceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"

func TestAPIContractChatCompletionsNonStreaming(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	rt := &captureRoundTripper{
		body:        `{"id":"cmpl-contract","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`,
		contentType: "application/json",
	}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hello contract"}],"max_tokens":4}`))
	req.Header.Set("traceparent", contractTraceparent)
	req.Header.Set("X-Request-ID", "contract-raw-request")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("unexpected content-type %q", got)
	}
	if got := rec.Header().Get("X-Trace-Id"); got != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("missing trace id header, got %q", got)
	}
	if got := rec.Header().Get("X-Router-Request-Hash"); got == "" || got == "contract-raw-request" {
		t.Fatalf("missing or raw router request hash %q", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if _, ok := payload["choices"].([]any); !ok {
		t.Fatalf("OpenAI response missing choices: %v", payload)
	}
	if rt.lastHeader.Get("traceparent") != contractTraceparent {
		t.Fatalf("traceparent not forwarded to worker")
	}
}

func TestAPIContractChatCompletionsStreamingSSE(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	rt := &captureRoundTripper{
		body:        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n",
		contentType: "text/event-stream",
	}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"stream contract"}],"max_tokens":4,"stream":true}`))
	req.Header.Set("traceparent", contractTraceparent)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("unexpected streaming content-type %q", got)
	}
	if !strings.Contains(rec.Body.String(), "data:") || !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("SSE body contract not satisfied: %q", rec.Body.String())
	}
	if rec.Header().Get("X-Trace-Id") == "" {
		t.Fatalf("missing trace id header")
	}
}

func TestAPIContractRouterHealthReadyMetricsCarryCorrelation(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("traceparent", contractTraceparent)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("X-Trace-Id"); got != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("%s missing trace id header, got %q", path, got)
		}
	}
}

func TestAPIContractCompleteBlockLocationFeedsLiveTopologyStrategy(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "topology_aware_cost_aware")
	rt := &captureRoundTripper{body: `{"choices":[{"text":"ok"}]}`, contentType: "application/json"}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()

	prompt := "complete block location contract"
	block := common.BlockHash(firstBlockForPrompt(t, prompt))
	updatedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	expiresAt := updatedAt.Add(time.Minute)
	location := common.BlockLocation{
		BlockHash: block,
		WorkerID:  "worker-b",
		Tier:      common.TierS3,
		Topology: common.Topology{
			Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", Rack: "rack-2", NodeID: "object-tier",
		},
		Locality:               common.LocalitySameZone,
		Transport:              common.TransportS3HTTPDefault,
		EstimatedLoadP95Ms:     3.5,
		EstimatedTransferP95Ms: 2.25,
		EgressCostClass:        "intra_zone",
		Bytes:                  4096,
		Tokens:                 16,
		SeqNo:                  41,
		Confidence:             0.83,
		UpdatedAt:              updatedAt,
		ExpiresAt:              expiresAt,
	}
	eventReq := httptest.NewRequest(http.MethodPost, "/admin/events", strings.NewReader(mustJSON(t, cacheEvent{
		EventID:   "complete-location-41",
		EventType: "block_stored",
		Location:  &location,
	})))
	eventRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(eventRec, eventReq)
	if eventRec.Code != http.StatusOK {
		t.Fatalf("event status=%d body=%s", eventRec.Code, eventRec.Body.String())
	}

	locations := handler.index.LocationsByWorker([]common.BlockHash{block})["worker-b"]
	if len(locations) != 1 {
		t.Fatalf("stored locations=%+v", locations)
	}
	got := locations[0]
	if got.BlockHash != location.BlockHash || got.WorkerID != location.WorkerID || got.Tier != location.Tier ||
		got.Topology != location.Topology || got.Locality != location.Locality || got.Transport != location.Transport ||
		got.EstimatedLoadP95Ms != location.EstimatedLoadP95Ms || got.EstimatedTransferP95Ms != location.EstimatedTransferP95Ms ||
		got.EgressCostClass != location.EgressCostClass || got.Bytes != location.Bytes || got.Tokens != location.Tokens ||
		got.SeqNo != location.SeqNo || got.Confidence != location.Confidence || !got.UpdatedAt.Equal(location.UpdatedAt) || !got.ExpiresAt.Equal(location.ExpiresAt) {
		t.Fatalf("complete location was not preserved:\n got=%+v\nwant=%+v", got, location)
	}

	proxyReq := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"`+prompt+`","max_tokens":1}`))
	proxyRec := httptest.NewRecorder()
	handler.ServeHTTP(proxyRec, proxyReq)
	if proxyRec.Code != http.StatusOK {
		t.Fatalf("proxy status=%d body=%s", proxyRec.Code, proxyRec.Body.String())
	}
	if gotWorker := rt.lastHeader.Get("X-Router-Worker-ID"); gotWorker != "worker-b" {
		t.Fatalf("topology strategy chose %q, want worker-b", gotWorker)
	}
	if gotStrategy := rt.lastHeader.Get("X-Router-Strategy"); gotStrategy != "topology_aware_cost_aware" {
		t.Fatalf("live proxy strategy=%q", gotStrategy)
	}
}

func TestAPIContractRejectsInvalidBlockLocations(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "mismatched identity",
			body: `{"event_type":"block_stored","block_hash":"a","worker_id":"worker-a","location":{"block_hash":"b","worker_id":"worker-a"}}`,
			want: "location.block_hash must match block_hash",
		},
		{
			name: "invalid transport",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","transport":"teleport"}}`,
			want: "invalid location.transport",
		},
		{
			name: "negative transfer estimate",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","estimated_transfer_p95_ms":-1}}`,
			want: "latency estimates must be non-negative",
		},
		{
			name: "confidence above one",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","confidence":1.1}}`,
			want: "location.confidence must be between 0 and 1",
		},
		{
			name: "invalid egress class",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","egress_cost_class":"free_money"}}`,
			want: "invalid location.egress_cost_class",
		},
		{
			name: "invalid lifetime",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","updated_at":"2026-08-01T01:00:00Z","expires_at":"2026-08-01T00:00:00Z"}}`,
			want: "location.expires_at must be after location.updated_at",
		},
		{
			name: "partial lifetime",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","expires_at":"2026-08-01T01:00:00Z"}}`,
			want: "location.updated_at and location.expires_at must be provided together",
		},
		{
			name: "location on eviction",
			body: `{"event_type":"block_evicted","block_hash":"a","worker_id":"worker-a","location":{"block_hash":"a","worker_id":"worker-a"}}`,
			want: "location is not valid for block_evicted",
		},
		{
			name: "unknown location field",
			body: `{"event_type":"block_stored","location":{"block_hash":"a","worker_id":"worker-a","estimated_transfer_p95_milliseconds":1}}`,
			want: "invalid event json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/admin/events", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			handler.AdminHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tt.want) {
				t.Fatalf("status=%d body=%s, want %q", rec.Code, rec.Body.String(), tt.want)
			}
		})
	}
}
