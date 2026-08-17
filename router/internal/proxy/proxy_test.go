package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/blockhash"
	"github.com/Haluka1/ai-storage-lab/router/internal/cacheindex"
	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

func TestProxyStreamsToSelectedWorkerAndLogsRedactedDecision(t *testing.T) {
	td := t.TempDir()
	logPath := filepath.Join(td, "router_decisions.jsonl")
	rt := &captureRoundTripper{body: "data: {\"choices\":[{\"text\":\"hello\"}]}\n\ndata: [DONE]\n\n"}
	handler := newTestHandler(t, logPath, "cost_aware")
	handler.SetHTTPClient(&http.Client{Transport: rt})

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"shared prompt words","max_tokens":8,"stream":true}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Request-ID", "req-secret")
	req.Header.Set("X-Tenant-ID", "tenant-secret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	handler.Close()

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "data:") {
		t.Fatalf("stream body was not proxied: %q", rec.Body.String())
	}
	if rec.Header().Get("X-Router-Worker-ID") == "" {
		t.Fatalf("missing response router worker header")
	}
	for _, name := range []string{
		"X-Router-Timing-Router-Parse-Ms",
		"X-Router-Timing-Block-Hash-Ms",
		"X-Router-Timing-Cache-State-Lookup-Ms",
		"X-Router-Timing-Route-Decision-Ms",
	} {
		value := rec.Header().Get(name)
		if value == "" {
			t.Fatalf("missing response timing header %s", name)
		}
		if parsed, err := strconv.ParseFloat(value, 64); err != nil || parsed < 0 {
			t.Fatalf("invalid timing header %s=%q", name, value)
		}
	}
	if got := rec.Header().Get("X-Router-Timing-Estimated-Fields"); !strings.Contains(got, "cache_state_lookup_ms") {
		t.Fatalf("missing estimated timing field marker, got %q", got)
	}
	if rec.Header().Get("X-Router-Request-Hash") == "" || rec.Header().Get("X-Router-Request-Hash") == "req-secret" {
		t.Fatalf("response router request hash is missing or raw")
	}
	if rt.lastHeader.Get("X-Router-Worker-ID") == "" {
		t.Fatalf("missing router worker header")
	}
	if rt.lastHeader.Get("X-Router-Request-Hash") == "" || rt.lastHeader.Get("X-Router-Request-Hash") == "req-secret" {
		t.Fatalf("router request hash is missing or raw")
	}
	if !strings.Contains(string(rt.lastBody), "shared prompt words") {
		t.Fatalf("upstream body was not preserved")
	}
	logRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logRaw)
	for _, forbidden := range []string{"shared prompt words", "tenant-secret", "req-secret"} {
		if strings.Contains(logText, forbidden) {
			t.Fatalf("decision log leaked %q: %s", forbidden, logText)
		}
	}
	if !strings.Contains(logText, `"decision_type":"router_route"`) {
		t.Fatalf("decision log missing router decision: %s", logText)
	}
	metrics := handler.metrics.PrometheusText()
	if !strings.Contains(metrics, "router_tenant_requests_total") || !strings.Contains(metrics, "tenant_hash=") {
		t.Fatalf("metrics missing tenant hash aggregate: %s", metrics)
	}
	for _, forbidden := range []string{"shared prompt words", "tenant-secret", "tenant_id", "request_id", "req-secret"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics leaked high-cardinality value %q: %s", forbidden, metrics)
		}
	}
}

func TestRouterTraceSpansIncludeRequiredStagesAndRedactInputs(t *testing.T) {
	td := t.TempDir()
	cfg := testConfig(td, "cost_aware")
	cfg.Router.DecisionLogPath = filepath.Join(td, "decisions.jsonl")
	cfg.Router.TraceLogPath = filepath.Join(td, "router_spans.jsonl")
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rt := &captureRoundTripper{
		body:        "data: {\"choices\":[{\"text\":\"ok\"}]}\n\ndata: [DONE]\n\n",
		contentType: "text/event-stream",
	}
	handler.SetHTTPClient(&http.Client{Transport: rt})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"trace secret prompt"}],"max_tokens":4,"stream":true}`))
	req.Header.Set("traceparent", contractTraceparent)
	req.Header.Set("X-Request-ID", "trace-raw-request")
	req.Header.Set("X-Tenant-ID", "trace-raw-tenant")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	records := readTraceRecords(t, cfg.Router.TraceLogPath)
	if got, want := len(records), 7; got != want {
		t.Fatalf("span count=%d, want %d: %+v", got, want, records)
	}
	byName := make(map[string]map[string]any)
	for _, record := range records {
		name, _ := record["name"].(string)
		byName[name] = record
		if got := record["trace_id"]; got != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("%s trace_id=%v", name, got)
		}
		if got := record["component"]; got != "router" {
			t.Fatalf("%s component=%v", name, got)
		}
		assertNoForbiddenTraceAttributes(t, record)
	}
	for _, name := range []string{"router_receive", "parse_request", "build_block_hash", "cache_state_lookup", "route_decision", "proxy_to_worker", "response_stream"} {
		if byName[name] == nil {
			t.Fatalf("missing span %s in %+v", name, byName)
		}
	}
	routerSpanID := byName["router_receive"]["span_id"]
	if got := byName["route_decision"]["parent_span_id"]; got != routerSpanID {
		t.Fatalf("route_decision parent=%v, want router span %v", got, routerSpanID)
	}
	routeSpanID := byName["route_decision"]["span_id"]
	if got := byName["proxy_to_worker"]["parent_span_id"]; got != routeSpanID {
		t.Fatalf("proxy_to_worker parent=%v, want route span %v", got, routeSpanID)
	}
	proxySpanID := byName["proxy_to_worker"]["span_id"]
	if got := byName["response_stream"]["parent_span_id"]; got != proxySpanID {
		t.Fatalf("response_stream parent=%v, want proxy span %v", got, proxySpanID)
	}
	traceRaw, err := os.ReadFile(cfg.Router.TraceLogPath)
	if err != nil {
		t.Fatal(err)
	}
	traceText := string(traceRaw)
	for _, forbidden := range []string{"trace secret prompt", "trace-raw-tenant", "trace-raw-request"} {
		if strings.Contains(traceText, forbidden) {
			t.Fatalf("trace log leaked %q: %s", forbidden, traceText)
		}
	}
}

func TestNoHealthyWorkerReturnsUnavailable(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[0].Health = common.WorkerUnhealthy
	cfg.Workers[1].Health = common.WorkerDraining
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"hello","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdminBlockStoredEventCanInfluencePrefixRouting(t *testing.T) {
	td := t.TempDir()
	handler := newTestHandler(t, filepath.Join(td, "decisions.jsonl"), "prefix_hash")
	rt := &captureRoundTripper{body: `{"choices":[{"text":"ok"}]}`}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()

	prompt := "hot prefix token reuse"
	block := firstBlockForPrompt(t, prompt)
	eventBody := `{"event_type":"block_stored","worker_id":"worker-b","block_hash":"` + block + `","tier":"gpu","tokens":16,"seq_no":1}`
	eventReq := httptest.NewRequest(http.MethodPost, "/admin/events", strings.NewReader(eventBody))
	eventRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(eventRec, eventReq)
	if eventRec.Code != http.StatusOK {
		t.Fatalf("event status=%d body=%s", eventRec.Code, eventRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"`+prompt+`","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("proxy status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rt.lastHeader.Get("X-Router-Worker-ID"); got != "worker-b" {
		t.Fatalf("expected prefix_hash to choose worker-b, got %q", got)
	}
}

func TestAdminEventRejectsZeroSequence(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	req := httptest.NewRequest(
		http.MethodPost,
		"/admin/events",
		strings.NewReader(`{"event_type":"block_stored","worker_id":"worker-a","block_hash":"abc","tier":"gpu","tokens":16,"seq_no":0}`),
	)
	rec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "seq_no must be positive") {
		t.Fatalf("zero sequence status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGeneratedRequestIdentityIsConsistentAcrossOutputs(t *testing.T) {
	td := t.TempDir()
	cfg := testConfig(td, "cost_aware")
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rt := &captureRoundTripper{body: `{"choices":[{"text":"ok"}]}`}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"generated identity","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	responseHash := rec.Header().Get("X-Request-Hash")
	for label, got := range map[string]string{
		"response router hash": rec.Header().Get("X-Router-Request-Hash"),
		"upstream router hash": rt.lastHeader.Get("X-Router-Request-Hash"),
	} {
		if responseHash == "" || got != responseHash {
			t.Fatalf("%s=%q, response correlation hash=%q", label, got, responseHash)
		}
	}
	raw, err := os.ReadFile(cfg.Router.DecisionLogPath)
	if err != nil {
		t.Fatal(err)
	}
	var decision map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &decision); err != nil {
		t.Fatal(err)
	}
	if got := decision["request_id_hash"]; got != responseHash {
		t.Fatalf("decision request hash=%v, want %q", got, responseHash)
	}
	for _, record := range readTraceRecords(t, cfg.Router.TraceLogPath) {
		attributes, _ := record["attributes"].(map[string]any)
		if got, ok := attributes["request_id_hash"]; ok && got != responseHash {
			t.Fatalf("trace request hash=%v, want %q in %+v", got, responseHash, record)
		}
	}
}

func TestUpstreamTransportErrorDoesNotExposeWorkerEndpoint(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	handler.SetHTTPClient(&http.Client{Transport: errorRoundTripper{
		err: errors.New("dial tcp 203.0.113.8:9000 for http://worker.example.invalid"),
	}})
	defer handler.Close()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"transport error","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || rec.Body.String() != "upstream request failed\n" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	for _, forbidden := range []string{"203.0.113.8", "worker.example.invalid", "worker-a.local", "worker-b.local"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("public error leaked %q: %q", forbidden, rec.Body.String())
		}
	}
}

func TestProxyRemovesDynamicHopByHopHeaders(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	rt := &captureRoundTripper{
		body: `{"choices":[]}`,
		headers: http.Header{
			"Connection":     []string{"X-Upstream-Hop"},
			"X-Upstream-Hop": []string{"private"},
		},
	}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"headers","max_tokens":1}`))
	req.Header.Set("Connection", "X-Client-Hop")
	req.Header.Set("X-Client-Hop", "private")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rt.lastHeader.Get("X-Client-Hop"); got != "" {
		t.Fatalf("request hop-by-hop header reached upstream: %q", got)
	}
	if got := rec.Header().Get("X-Upstream-Hop"); got != "" {
		t.Fatalf("response hop-by-hop header reached client: %q", got)
	}
}

func TestProxyDoesNotFollowWorkerRedirect(t *testing.T) {
	redirected := make(chan struct{}, 1)
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer redirectTarget.Close()

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer worker.Close()

	cfg := testConfig(t.TempDir(), "round_robin")
	cfg.Workers = cfg.Workers[:1]
	cfg.Workers[0].URL = worker.URL
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	// The Router owns redirect policy even when a caller injects a client that
	// explicitly requests the default follow behavior.
	handler.SetHTTPClient(&http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return nil }})

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"must stay at selected worker","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("redirect status=%d body=%s", rec.Code, rec.Body.String())
	}
	if location := rec.Header().Get("Location"); location != "" {
		t.Fatalf("rejected upstream redirect leaked Location %q", location)
	}
	select {
	case <-redirected:
		t.Fatal("Router followed a Worker redirect to an unconfigured origin")
	default:
	}
}

func TestCachePredictionMetricsComparePredictedAndWorkerActuals(t *testing.T) {
	td := t.TempDir()
	cfg := testConfig(td, "prefix_hash")
	cfg.Router.BlockSizeTokens = 1
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	rt := &captureRoundTripper{
		body: `{"choices":[{"text":"ok"}]}`,
		headers: http.Header{
			actualHitBlocksHeader:  []string{"6"},
			actualMissBlocksHeader: []string{"4"},
		},
	}
	handler.SetHTTPClient(&http.Client{Transport: rt})

	prompt := "b0 b1 b2 b3 b4 b5 b6 b7 b8 b9"
	blocks := handler.hasher.ComputeBlocks(blockhash.ApproxTokenize(prompt), blockhash.IsolationKey{
		TenantID:          "metric-raw-tenant",
		TenantSalt:        "default-salt",
		ModelID:           "m",
		ModelRevision:     "default-revision",
		TokenizerRevision: blockhash.ApproxTokenizerRevision,
		LoRAID:            "none",
		ModalityKey:       "text",
		CacheSalt:         "cache-v1",
	})
	if got, want := len(blocks), 10; got != want {
		t.Fatalf("block count=%d, want %d", got, want)
	}
	for _, block := range blocks {
		handler.index.Store(common.BlockHash(block), common.WorkerID("worker-b"), common.TierGPU, 1)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"`+prompt+`","max_tokens":1}`))
	req.Header.Set("X-Request-ID", "metric-raw-request")
	req.Header.Set("X-Tenant-ID", "metric-raw-tenant")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proxy status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rt.lastHeader.Get("X-Router-Worker-ID"); got != "worker-b" {
		t.Fatalf("expected stale cache route to worker-b, got %q", got)
	}
	metrics := handler.metrics.PrometheusText()
	for _, want := range []string{
		"router_cache_prediction_precision_count 1",
		`router_cache_prediction_precision{quantile="0.50"} 0.600000`,
		"router_cache_prediction_recall_count 1",
		`router_cache_prediction_recall{quantile="0.50"} 0.600000`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	for _, forbidden := range []string{"block_hash", "request_id", "tenant_id", "metric-raw-request", "metric-raw-tenant"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics leaked high-cardinality value/key %q:\n%s", forbidden, metrics)
		}
	}
}

func TestPublicHandlerRejectsAdminEndpoints(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()

	req := httptest.NewRequest(http.MethodGet, "/admin/workers", nil)
	rec := httptest.NewRecorder()
	handler.PublicHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("public admin status=%d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestConfigRejectsExposedOrSharedAdminListener(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Server.AdminAddr = "0.0.0.0:9090"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected exposed admin listener to be rejected")
	}
	cfg.Server.AdminAddr = cfg.Server.ListenAddr
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected shared public/admin listener to be rejected")
	}
}

func TestConfigAndAdminRejectDuplicateWorkerIDs(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[1].ID = cfg.Workers[0].ID
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate worker id") {
		t.Fatalf("expected duplicate config worker id error, got %v", err)
	}

	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	workers := handler.snapshotWorkers()
	workers[1].ID = workers[0].ID
	req := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(mustJSON(t, workers)))
	rec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "duplicate worker id") {
		t.Fatalf("duplicate admin worker status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestConfigAndAdminRejectUnsafeWorkerURLs(t *testing.T) {
	for _, workerURL := range []string{
		"ftp://worker.example.invalid",
		"http://placeholder@127.0.0.1",
		"http://worker.example.invalid/#fragment",
	} {
		cfg := testConfig(t.TempDir(), "cost_aware")
		cfg.Workers[0].URL = workerURL
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected unsafe Worker URL %q to be rejected", workerURL)
		}
	}

	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	workers := handler.snapshotWorkers()
	workers = append(workers, common.WorkerState{
		ID:          "worker-unsafe",
		URL:         "ftp://worker.example.invalid",
		Health:      common.WorkerReady,
		LastUpdated: time.Now().UTC(),
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(mustJSON(t, workers)))
	rec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unsafe admin Worker URL status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSameWorkerIDCannotMutateIdentityInPlace(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	workers := handler.snapshotWorkers()
	workers[0].URL = "http://replacement-worker.local"

	req := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(mustJSON(t, workers)))
	rec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "identity_change_requires_remove_then_register") {
		t.Fatalf("identity mutation status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := requireWorker(t, handler.snapshotWorkers(), "worker-a").URL; got != "http://worker-a.local" {
		t.Fatalf("rejected identity mutation changed URL to %q", got)
	}
}

func TestSafeRemovalPurgesWorkerCacheBeforeIDReuse(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[0].QueueDepth = 0
	cfg.Workers[0].ActiveDecodeBlocks = 0
	cfg.Workers[0].InflightRequests = 0
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	handler.index.Store("cached-block", "worker-a", common.TierGPU, 16)

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}
	removeReq := httptest.NewRequest(
		http.MethodPost,
		"/admin/workers",
		strings.NewReader(mustJSON(t, []common.WorkerState{cfg.Workers[1]})),
	)
	removeRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(removeRec, removeReq)
	if removeRec.Code != http.StatusOK {
		t.Fatalf("removal status=%d body=%s", removeRec.Code, removeRec.Body.String())
	}
	if got := handler.index.OverlapByWorker([]common.BlockHash{"cached-block"}); got["worker-a"] != 0 {
		t.Fatalf("retired Worker cache survived removal: %v", got)
	}

	replacement := cfg.Workers[0]
	replacement.URL = "http://worker-a-generation-2.local"
	readdReq := httptest.NewRequest(
		http.MethodPost,
		"/admin/workers",
		strings.NewReader(mustJSON(t, []common.WorkerState{cfg.Workers[1], replacement})),
	)
	readdRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(readdRec, readdReq)
	if readdRec.Code != http.StatusOK {
		t.Fatalf("explicit re-registration status=%d body=%s", readdRec.Code, readdRec.Body.String())
	}
	if got := handler.index.OverlapByWorker([]common.BlockHash{"cached-block"}); got["worker-a"] != 0 {
		t.Fatalf("reused Worker ID inherited stale cache: %v", got)
	}
}

func TestConfigRejectsInjectedRouterInflightAndNegativeWorkerCounts(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[0].RouterInflightRequests = 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "router_inflight_requests is router-managed") {
		t.Fatalf("expected Router inflight ownership error, got %v", err)
	}
	cfg.Workers[0].RouterInflightRequests = 0
	cfg.Workers[0].InflightRequests = -1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must be non-negative") {
		t.Fatalf("expected negative Worker count error, got %v", err)
	}
}

func TestReadinessAndAdminWorkers(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()

	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	handler.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusOK {
		t.Fatalf("ready status=%d", readyRec.Code)
	}

	workersReq := httptest.NewRequest(http.MethodGet, "/admin/workers", nil)
	workersRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(workersRec, workersReq)
	if workersRec.Code != http.StatusOK || !strings.Contains(workersRec.Body.String(), "worker-a") {
		t.Fatalf("workers response invalid: status=%d body=%s", workersRec.Code, workersRec.Body.String())
	}
}

func TestAdminDrainUndrainStopsNewRoutingToWorker(t *testing.T) {
	td := t.TempDir()
	handler := newTestHandler(t, filepath.Join(td, "decisions.jsonl"), "prefix_hash")
	rt := &captureRoundTripper{body: `{"choices":[{"text":"ok"}]}`}
	handler.SetHTTPClient(&http.Client{Transport: rt})
	defer handler.Close()

	prompt := "hot prefix token reuse"
	block := firstBlockForPrompt(t, prompt)
	eventBody := `{"event_type":"block_stored","worker_id":"worker-a","block_hash":"` + block + `","tier":"gpu","tokens":16,"seq_no":1}`
	eventReq := httptest.NewRequest(http.MethodPost, "/admin/events", strings.NewReader(eventBody))
	eventRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(eventRec, eventReq)
	if eventRec.Code != http.StatusOK {
		t.Fatalf("event status=%d body=%s", eventRec.Code, eventRec.Body.String())
	}

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK || !strings.Contains(drainRec.Body.String(), `"draining":true`) {
		t.Fatalf("drain response invalid: status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"`+prompt+`","max_tokens":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("proxy status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rt.lastHeader.Get("X-Router-Worker-ID"); got == "worker-a" {
		t.Fatalf("draining worker received a new request")
	}

	undrainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/undrain", nil)
	undrainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(undrainRec, undrainReq)
	if undrainRec.Code != http.StatusOK || !strings.Contains(undrainRec.Body.String(), `"draining":false`) {
		t.Fatalf("undrain response invalid: status=%d body=%s", undrainRec.Code, undrainRec.Body.String())
	}

	readyReq := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"`+prompt+`","max_tokens":1}`))
	readyRec := httptest.NewRecorder()
	handler.ServeHTTP(readyRec, readyReq)
	if got := rt.lastHeader.Get("X-Router-Worker-ID"); got != "worker-a" {
		t.Fatalf("undrained worker should receive prefix route again, got %q", got)
	}
}

func TestDrainingWorkerCannotBeRemovedUntilInflightCompletes(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[0].ActiveDecodeBlocks = 2
	cfg.Workers[0].InflightRequests = 1
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK || !strings.Contains(drainRec.Body.String(), `"safe_to_remove":false`) {
		t.Fatalf("expected unsafe drain response, status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}

	replaceBody := mustJSON(t, []common.WorkerState{cfg.Workers[1]})
	replaceReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(replaceBody))
	replaceRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(replaceRec, replaceReq)
	if replaceRec.Code != http.StatusConflict {
		t.Fatalf("expected conflict while draining worker has inflight work, status=%d body=%s", replaceRec.Code, replaceRec.Body.String())
	}

	completedA := cfg.Workers[0]
	completedA.Health = common.WorkerDraining
	completedA.ReadinessState = common.ReadinessDraining
	completedA.Draining = true
	completedA.ActiveDecodeBlocks = 0
	completedA.InflightRequests = 0
	completedA.QueueDepth = 0
	updateReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(mustJSON(t, []common.WorkerState{completedA, cfg.Workers[1]})))
	updateRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected inflight completion update to pass, status=%d body=%s", updateRec.Code, updateRec.Body.String())
	}

	finalReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(replaceBody))
	finalRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(finalRec, finalReq)
	if finalRec.Code != http.StatusOK {
		t.Fatalf("expected safe removal after inflight completes, status=%d body=%s", finalRec.Code, finalRec.Body.String())
	}
}

func TestIdleWorkerMustDrainBeforeRemoval(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	for index := range cfg.Workers {
		cfg.Workers[index].QueueDepth = 0
		cfg.Workers[index].ActiveDecodeBlocks = 0
		cfg.Workers[index].InflightRequests = 0
	}
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()

	replaceBody := mustJSON(t, []common.WorkerState{cfg.Workers[1]})
	replaceReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(replaceBody))
	replaceRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(replaceRec, replaceReq)
	if replaceRec.Code != http.StatusConflict {
		t.Fatalf("expected conflict before explicit drain, status=%d body=%s", replaceRec.Code, replaceRec.Body.String())
	}

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK || !strings.Contains(drainRec.Body.String(), `"safe_to_remove":true`) {
		t.Fatalf("expected idle worker to become removable after drain, status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}

	finalReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(replaceBody))
	finalRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(finalRec, finalReq)
	if finalRec.Code != http.StatusOK {
		t.Fatalf("expected removal after explicit drain, status=%d body=%s", finalRec.Code, finalRec.Body.String())
	}
}

func TestDrainBetweenSelectionAndReservationRejectsRequest(t *testing.T) {
	handler := newTestHandler(t, filepath.Join(t.TempDir(), "decisions.jsonl"), "cost_aware")
	defer handler.Close()
	selectionEntered := make(chan struct{})
	continueSelection := make(chan struct{})
	var continueOnce sync.Once
	continueRequest := func() { continueOnce.Do(func() { close(continueSelection) }) }
	defer continueRequest()
	handler.strategy = &selectionBarrierStrategy{
		workerID: "worker-a",
		entered:  selectionEntered,
		proceed:  continueSelection,
	}
	upstreamCalled := make(chan struct{}, 1)
	handler.SetHTTPClient(&http.Client{Transport: &notifyingRoundTripper{called: upstreamCalled}})

	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"reservation race","max_tokens":1}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		result <- rec
	}()
	awaitSignal(t, selectionEntered, "strategy selection")

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}
	continueRequest()

	var rec *httptest.ResponseRecorder
	select {
	case rec = <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy request did not finish")
	}
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "no longer routable") {
		t.Fatalf("proxy status=%d body=%s", rec.Code, rec.Body.String())
	}
	select {
	case <-upstreamCalled:
		t.Fatal("request reached Worker after drain won the reservation race")
	default:
	}
	worker, ok := findWorker(handler.snapshotWorkers(), "worker-a")
	if !ok || worker.RouterInflightRequests != 0 {
		t.Fatalf("failed reservation leaked Router inflight state: %+v", worker)
	}
}

func TestRouterReservationBlocksRemovalUntilResponseStreamCompletes(t *testing.T) {
	cfg := testConfig(t.TempDir(), "cost_aware")
	cfg.Workers[0].QueueDepth = 0
	cfg.Workers[0].InflightRequests = 2
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	handler.strategy = &selectionBarrierStrategy{workerID: "worker-a"}
	streamEntered := make(chan struct{})
	releaseStream := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStream) }) }
	defer release()
	handler.SetHTTPClient(&http.Client{Transport: &gatedRoundTripper{
		body: &gatedReadCloser{
			reader:  bytes.NewBufferString(`{"choices":[{"text":"ok"}]}`),
			entered: streamEntered,
			release: releaseStream,
		},
	}})

	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"m","prompt":"long stream","max_tokens":1}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		result <- rec
	}()
	awaitSignal(t, streamEntered, "upstream response stream")

	workerA := requireWorker(t, handler.snapshotWorkers(), "worker-a")
	if workerA.InflightRequests != 2 || workerA.RouterInflightRequests != 1 || workerA.EffectiveInflightRequests() != 3 {
		t.Fatalf("external and Router inflight counts were not kept separate: %+v", workerA)
	}

	removeBody := mustJSON(t, []common.WorkerState{cfg.Workers[1]})
	removeReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(removeBody))
	removeRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(removeRec, removeReq)
	if removeRec.Code != http.StatusConflict || !strings.Contains(removeRec.Body.String(), `"router_inflight_requests":1`) {
		t.Fatalf("active Router reservation did not block removal: status=%d body=%s", removeRec.Code, removeRec.Body.String())
	}

	drainReq := httptest.NewRequest(http.MethodPost, "/admin/workers/worker-a/drain", nil)
	drainRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(drainRec, drainReq)
	if drainRec.Code != http.StatusOK || !strings.Contains(drainRec.Body.String(), `"effective_inflight_requests":3`) || !strings.Contains(drainRec.Body.String(), `"safe_to_remove":false`) {
		t.Fatalf("drain response did not expose effective inflight state: status=%d body=%s", drainRec.Code, drainRec.Body.String())
	}

	completedA := cfg.Workers[0]
	completedA.Health = common.WorkerDraining
	completedA.ReadinessState = common.ReadinessDraining
	completedA.Draining = true
	completedA.InflightRequests = 0
	updateReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(mustJSON(t, []common.WorkerState{completedA, cfg.Workers[1]})))
	updateRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("external completion update failed: status=%d body=%s", updateRec.Code, updateRec.Body.String())
	}
	workerA = requireWorker(t, handler.snapshotWorkers(), "worker-a")
	if workerA.InflightRequests != 0 || workerA.RouterInflightRequests != 1 || workerA.SafeToRemove() {
		t.Fatalf("external update overwrote Router reservation: %+v", workerA)
	}

	release()
	select {
	case rec := <-result:
		if rec.Code != http.StatusOK {
			t.Fatalf("proxy status=%d body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy stream did not finish")
	}
	workerA = requireWorker(t, handler.snapshotWorkers(), "worker-a")
	if workerA.InflightRequests != 0 || workerA.RouterInflightRequests != 0 || !workerA.SafeToRemove() {
		t.Fatalf("Router reservation was not released after stream completion: %+v", workerA)
	}

	finalReq := httptest.NewRequest(http.MethodPost, "/admin/workers", strings.NewReader(removeBody))
	finalRec := httptest.NewRecorder()
	handler.AdminHandler().ServeHTTP(finalRec, finalReq)
	if finalRec.Code != http.StatusOK {
		t.Fatalf("safe removal failed after stream completion: status=%d body=%s", finalRec.Code, finalRec.Body.String())
	}
}

func newTestHandler(t *testing.T, logPath string, strategy string) *Handler {
	t.Helper()
	cfg := testConfig(filepath.Dir(logPath), strategy)
	cfg.Router.DecisionLogPath = logPath
	handler, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testConfig(td string, strategy string) Config {
	now := time.Now().UTC()
	return Config{
		Server: ServerConfig{ListenAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:1"},
		Router: RouterConfig{
			RunID:           "router_proxy_test",
			Strategy:        strategy,
			BlockSizeTokens: 16,
			CacheTTLMS:      30000,
			DecisionLogPath: filepath.Join(td, "router_decisions.jsonl"),
			TraceLogPath:    filepath.Join(td, "router_spans.jsonl"),
			EntryTopology: common.Topology{
				Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", NodeID: "router",
			},
		},
		Workers: []common.WorkerState{
			{ID: "worker-a", URL: "http://worker-a.local", Health: common.WorkerReady, QueueDepth: 4, Topology: common.Topology{Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", NodeID: "worker-a"}, LastUpdated: now},
			{ID: "worker-b", URL: "http://worker-b.local", Health: common.WorkerReady, QueueDepth: 0, Topology: common.Topology{Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", NodeID: "worker-b"}, LastUpdated: now},
		},
	}
}

func firstBlockForPrompt(t *testing.T, prompt string) string {
	t.Helper()
	hasher := blockhash.New(16)
	blocks := hasher.ComputeBlocks(blockhash.ApproxTokenize(prompt), blockhash.IsolationKey{
		TenantID:          "default-tenant",
		TenantSalt:        "default-salt",
		ModelID:           "m",
		ModelRevision:     "default-revision",
		TokenizerRevision: blockhash.ApproxTokenizerRevision,
		LoRAID:            "none",
		ModalityKey:       "text",
		CacheSalt:         "cache-v1",
	})
	if len(blocks) == 0 {
		t.Fatal("no block generated")
	}
	return blocks[0]
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type captureRoundTripper struct {
	body        string
	contentType string
	headers     http.Header
	lastHeader  http.Header
	lastBody    []byte
}

type selectionBarrierStrategy struct {
	workerID common.WorkerID
	entered  chan struct{}
	proceed  chan struct{}
}

func (s *selectionBarrierStrategy) Name() string { return "selection_barrier" }

func (s *selectionBarrierStrategy) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, _ *cacheindex.Index) (common.RouteDecision, error) {
	if s.entered != nil {
		close(s.entered)
	}
	if s.proceed != nil {
		select {
		case <-s.proceed:
		case <-ctx.Done():
			return common.RouteDecision{}, ctx.Err()
		}
	}
	return common.RouteDecision{
		RequestIDHash:  req.RequestIDHash,
		Strategy:       s.Name(),
		WorkerID:       s.workerID,
		CandidateCount: len(workers),
		Reason:         "deterministic_test_selection",
	}, nil
}

type notifyingRoundTripper struct {
	called chan<- struct{}
}

type errorRoundTripper struct {
	err error
}

func (rt errorRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, rt.err
}

func (rt *notifyingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case rt.called <- struct{}{}:
	default:
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"choices":[]}`)),
		Request:    req,
	}, nil
}

type gatedRoundTripper struct {
	body io.ReadCloser
}

func (rt *gatedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       rt.body,
		Request:    req,
	}, nil
}

type gatedReadCloser struct {
	reader  io.Reader
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (r *gatedReadCloser) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.reader.Read(p)
}

func (r *gatedReadCloser) Close() error { return nil }

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func requireWorker(t *testing.T, workers []common.WorkerState, id common.WorkerID) common.WorkerState {
	t.Helper()
	worker, ok := findWorker(workers, id)
	if !ok {
		t.Fatalf("worker %q not found in %+v", id, workers)
	}
	return worker
}

func (rt *captureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	rt.lastHeader = req.Header.Clone()
	rt.lastBody = append([]byte(nil), raw...)
	contentType := rt.contentType
	if contentType == "" {
		contentType = "text/event-stream"
	}
	headers := rt.headers.Clone()
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Content-Type", contentType)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     headers,
		Body:       io.NopCloser(bytes.NewBufferString(rt.body)),
		Request:    req,
	}, nil
}

func readTraceRecords(t *testing.T, path string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid trace JSONL line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func assertNoForbiddenTraceAttributes(t *testing.T, record map[string]any) {
	t.Helper()
	attrs, _ := record["attributes"].(map[string]any)
	for key := range attrs {
		switch key {
		case "raw_prompt", "prompt", "tenant_id", "block_hash", "block_hashes":
			t.Fatalf("trace attributes contain forbidden key %q in %+v", key, record)
		}
	}
}

func TestMetricsUseBoundedQuantileWindows(t *testing.T) {
	metrics := NewMetrics()
	observations := metricWindowCapacity + 17
	for i := 0; i < observations; i++ {
		metrics.ObserveRoute("cost_aware", "ok", "worker", time.Duration(i)*time.Microsecond)
		metrics.ObserveProxy(time.Duration(i)*time.Millisecond, time.Duration(i+1)*time.Millisecond)
		metrics.ObserveCachePrediction(10, 8, 2)
	}
	if got := len(metrics.routeLatencyUS.values); got != metricWindowCapacity {
		t.Fatalf("route window size=%d want=%d", got, metricWindowCapacity)
	}
	if got := metrics.routeLatencyUS.count; got != uint64(observations) {
		t.Fatalf("route observation count=%d want=%d", got, observations)
	}
	output := metrics.PrometheusText()
	if !strings.Contains(output, "router_route_latency_microseconds_count "+strconv.Itoa(observations)) {
		t.Fatalf("total count missing from metrics: %s", output)
	}
}

func TestTenantMetricSeriesAreBounded(t *testing.T) {
	metrics := NewMetrics()
	for i := 0; i < maxTenantMetricSeries+100; i++ {
		metrics.ObserveTenantRoute("cost_aware", "tenant-"+strconv.Itoa(i), "ok")
	}
	if got := len(metrics.tenantRequests); got > maxTenantMetricSeries+1 {
		t.Fatalf("tenant metric series=%d exceeds bound", got)
	}
	if !strings.Contains(metrics.PrometheusText(), `tenant_hash="overflow"`) {
		t.Fatal("overflow tenant series missing")
	}
}
