package proxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/blockhash"
	"github.com/Haluka1/ai-storage-lab/router/internal/cacheindex"
	"github.com/Haluka1/ai-storage-lab/router/internal/common"
	"github.com/Haluka1/ai-storage-lab/router/internal/routing"
)

const (
	maxRequestBodyBytes     = 16 << 20
	actualHitBlocksHeader   = "X-KV-Actual-Hit-Blocks"
	actualMissBlocksHeader  = "X-KV-Actual-Miss-Blocks"
	actualHitBlocksJSONKey  = "actual_hit_blocks"
	actualMissBlocksJSONKey = "actual_miss_blocks"
)

var generatedRequestCounter atomic.Uint64

type Handler struct {
	runID          string
	strategy       routing.Strategy
	index          *cacheindex.Index
	hasher         *blockhash.Hasher
	entryTopology  common.Topology
	workersMu      sync.RWMutex
	workers        []common.WorkerState
	routerInflight map[common.WorkerID]int
	client         *http.Client
	metrics        *Metrics
	decisionLog    *DecisionLogger
	traceLog       *TraceLogger
}

type proxyTimings struct {
	RouterParse      time.Duration
	BlockHash        time.Duration
	CacheStateLookup time.Duration
	RouteDecision    time.Duration
	EstimatedFields  []string
}

func NewHandler(cfg Config) (*Handler, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	strategy, err := strategyByName(cfg.Router.Strategy)
	if err != nil {
		return nil, err
	}
	logger, err := NewDecisionLogger(cfg.Router.DecisionLogPath)
	if err != nil {
		return nil, err
	}
	traceLogger, err := NewTraceLogger(cfg.Router.TraceLogPath)
	if err != nil {
		if logger != nil {
			_ = logger.Close()
		}
		return nil, err
	}
	return &Handler{
		runID:          cfg.Router.RunID,
		strategy:       strategy,
		index:          cacheindex.New(time.Duration(cfg.Router.CacheTTLMS) * time.Millisecond),
		hasher:         blockhash.New(cfg.Router.BlockSizeTokens),
		entryTopology:  cfg.Router.EntryTopology,
		workers:        append([]common.WorkerState(nil), cfg.Workers...),
		routerInflight: make(map[common.WorkerID]int),
		client:         &http.Client{Timeout: 0},
		metrics:        NewMetrics(),
		decisionLog:    logger,
		traceLog:       traceLogger,
	}, nil
}

func (h *Handler) Close() error {
	if h == nil {
		return nil
	}
	var err error
	if h.decisionLog != nil {
		err = h.decisionLog.Close()
	}
	if h.traceLog != nil {
		if traceErr := h.traceLog.Close(); err == nil {
			err = traceErr
		}
	}
	return err
}

func (h *Handler) SetHTTPClient(client *http.Client) {
	if client != nil {
		h.client = client
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.servePublicHTTP(w, r)
}

// PublicHandler exposes only read-only health/metrics endpoints and the
// OpenAI-compatible inference API. Mutating control-plane endpoints are kept
// on AdminHandler so deployments can bind them to loopback.
func (h *Handler) PublicHandler() http.Handler {
	return http.HandlerFunc(h.servePublicHTTP)
}

func (h *Handler) AdminHandler() http.Handler {
	return http.HandlerFunc(h.serveAdminHTTP)
}

func (h *Handler) servePublicHTTP(w http.ResponseWriter, r *http.Request) {
	setRequestCorrelationHeaders(w.Header(), r)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeText(w, http.StatusOK, "ok\n")
	case r.Method == http.MethodGet && r.URL.Path == "/readyz":
		if h.ready() {
			writeText(w, http.StatusOK, "ready\n")
		} else {
			writeText(w, http.StatusServiceUnavailable, "no routable workers\n")
		}
	case r.Method == http.MethodGet && r.URL.Path == "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, h.metrics.PrometheusText())
	case r.Method == http.MethodPost && isOpenAIPath(r.URL.Path):
		h.handleProxy(w, r)
	default:
		writeText(w, http.StatusNotFound, "not found\n")
	}
}

func (h *Handler) serveAdminHTTP(w http.ResponseWriter, r *http.Request) {
	setRequestCorrelationHeaders(w.Header(), r)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeText(w, http.StatusOK, "ok\n")
	case r.Method == http.MethodGet && r.URL.Path == "/readyz":
		if h.ready() {
			writeText(w, http.StatusOK, "ready\n")
		} else {
			writeText(w, http.StatusServiceUnavailable, "no routable workers\n")
		}
	case r.Method == http.MethodGet && r.URL.Path == "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, h.metrics.PrometheusText())
	case r.Method == http.MethodGet && r.URL.Path == "/admin/workers":
		writeJSON(w, http.StatusOK, h.snapshotWorkers())
	case r.Method == http.MethodPost && r.URL.Path == "/admin/workers":
		h.handleReplaceWorkers(w, r)
	case r.Method == http.MethodPost && isWorkerAdminAction(r.URL.Path, "drain"):
		h.handleWorkerDrain(w, r, true)
	case r.Method == http.MethodPost && isWorkerAdminAction(r.URL.Path, "undrain"):
		h.handleWorkerDrain(w, r, false)
	case r.Method == http.MethodPost && r.URL.Path == "/admin/events":
		h.handleEvent(w, r)
	default:
		writeText(w, http.StatusNotFound, "not found\n")
	}
}

func (h *Handler) ready() bool {
	for _, worker := range h.snapshotWorkers() {
		if worker.Routable() {
			return true
		}
	}
	return false
}

func (h *Handler) snapshotWorkers() []common.WorkerState {
	h.workersMu.RLock()
	defer h.workersMu.RUnlock()
	workers := make([]common.WorkerState, len(h.workers))
	for i, worker := range h.workers {
		workers[i] = h.workerSnapshotLocked(worker)
	}
	return workers
}

func (h *Handler) handleReplaceWorkers(w http.ResponseWriter, r *http.Request) {
	var workers []common.WorkerState
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes)).Decode(&workers); err != nil {
		writeText(w, http.StatusBadRequest, "invalid workers json\n")
		return
	}
	if len(workers) == 0 {
		writeText(w, http.StatusBadRequest, "workers must not be empty\n")
		return
	}
	workerIDs := make(map[common.WorkerID]struct{}, len(workers))
	for i := range workers {
		if workers[i].ID == "" || workers[i].URL == "" {
			writeText(w, http.StatusBadRequest, "worker id and url are required\n")
			return
		}
		if _, duplicate := workerIDs[workers[i].ID]; duplicate {
			writeText(w, http.StatusBadRequest, "duplicate worker id\n")
			return
		}
		workerIDs[workers[i].ID] = struct{}{}
		if workers[i].RouterInflightRequests != 0 {
			writeText(w, http.StatusBadRequest, "router_inflight_requests is read-only\n")
			return
		}
		if workers[i].QueueDepth < 0 || workers[i].ActiveDecodeBlocks < 0 || workers[i].InflightRequests < 0 {
			writeText(w, http.StatusBadRequest, "worker queue, decode, and inflight counts must be non-negative\n")
			return
		}
		normalizeWorkerDefaults(&workers[i], h.entryTopology)
		workers[i].LastUpdated = time.Now().UTC()
	}
	h.workersMu.Lock()
	if blocked := unsafeRemovedWorkers(h.workers, workers, h.routerInflight); len(blocked) > 0 {
		h.workersMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "worker cannot be removed while work remains", "blocked_workers": blocked})
		return
	}
	h.workers = append([]common.WorkerState(nil), workers...)
	for workerID, count := range h.routerInflight {
		if count == 0 {
			if _, exists := workerIDs[workerID]; !exists {
				delete(h.routerInflight, workerID)
			}
		}
	}
	h.workersMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"updated_workers": len(workers)})
}

func (h *Handler) handleWorkerDrain(w http.ResponseWriter, r *http.Request, drain bool) {
	workerID, ok := workerIDFromAdminAction(r.URL.Path)
	if !ok {
		writeText(w, http.StatusNotFound, "not found\n")
		return
	}
	h.workersMu.Lock()
	defer h.workersMu.Unlock()
	for i := range h.workers {
		if h.workers[i].ID != common.WorkerID(workerID) {
			continue
		}
		if drain {
			h.workers[i].Health = common.WorkerDraining
			h.workers[i].ReadinessState = common.ReadinessDraining
			h.workers[i].Draining = true
		} else {
			h.workers[i].Health = common.WorkerReady
			h.workers[i].ReadinessState = common.ReadinessReady
			h.workers[i].Draining = false
		}
		h.workers[i].LastUpdated = time.Now().UTC()
		writeJSON(w, http.StatusOK, workerAdminResponse(h.workerSnapshotLocked(h.workers[i])))
		return
	}
	writeText(w, http.StatusNotFound, "worker not found\n")
}

func (h *Handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	var event cacheEvent
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		writeText(w, http.StatusBadRequest, "invalid event json\n")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeText(w, http.StatusBadRequest, "invalid event json\n")
		return
	}
	indexEvent, err := h.cacheIndexEvent(event)
	if err != nil {
		writeText(w, http.StatusBadRequest, err.Error()+"\n")
		return
	}
	if err := h.index.ApplyEvent(indexEvent); err != nil {
		writeText(w, http.StatusBadRequest, err.Error()+"\n")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *Handler) handleProxy(w http.ResponseWriter, r *http.Request) {
	requestNonce := ensureRequestID(r)
	traceCtx := traceContextFromRequest(r.Header.Get("traceparent"), requestNonce)
	routerStart := time.Now()
	routerSpan := h.traceLog.WriteSpan(h.runID, traceCtx, "router_receive", "", routerStart, "ok", map[string]any{
		"method":          r.Method,
		"path_kind":       openAIPathKind(r.URL.Path),
		"request_id_hash": hashPrefix(requestNonce),
	})
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes))
	if err != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "parse_request", routerSpan, time.Now(), "error", map[string]any{
			"request_id_hash": hashPrefix(requestNonce),
			"error_class":     "read_body",
		})
		writeText(w, http.StatusBadRequest, "failed to read request body\n")
		return
	}
	if len(body) >= maxRequestBodyBytes {
		h.traceLog.WriteSpan(h.runID, traceCtx, "parse_request", routerSpan, time.Now(), "error", map[string]any{
			"request_id_hash": hashPrefix(requestNonce),
			"error_class":     "body_too_large",
			"body_bytes":      len(body),
		})
		writeText(w, http.StatusRequestEntityTooLarge, "request body too large\n")
		return
	}
	reqCtx, timings, err := h.buildRequestContext(r, body)
	if err != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "parse_request", routerSpan, time.Now().Add(-timings.RouterParse), "error", map[string]any{
			"request_id_hash": hashPrefix(requestNonce),
			"error_class":     "invalid_openai_request",
		})
		writeText(w, http.StatusBadRequest, err.Error()+"\n")
		return
	}
	h.traceLog.WriteSpan(h.runID, traceCtx, "parse_request", routerSpan, time.Now().Add(-timings.RouterParse), "ok", map[string]any{
		"request_id_hash":     reqCtx.RequestIDHash,
		"prompt_token_count":  reqCtx.TotalPromptTokens,
		"max_output_tokens":   reqCtx.MaxOutputTokens,
		"openai_request_kind": openAIPathKind(r.URL.Path),
	})
	h.traceLog.WriteSpan(h.runID, traceCtx, "build_block_hash", routerSpan, time.Now().Add(-timings.BlockHash), "ok", map[string]any{
		"request_id_hash":   reqCtx.RequestIDHash,
		"block_count":       len(reqCtx.BlockHashes),
		"block_size_tokens": h.hasher.BlockSizeTokens,
	})
	workers := h.snapshotWorkers()
	lookupStart := time.Now()
	overlapByWorker := h.index.OverlapByWorker(reqCtx.BlockHashes)
	timings.CacheStateLookup = time.Since(lookupStart)
	h.traceLog.WriteSpan(h.runID, traceCtx, "cache_state_lookup", routerSpan, lookupStart, "ok", map[string]any{
		"request_id_hash":      reqCtx.RequestIDHash,
		"block_count":          len(reqCtx.BlockHashes),
		"worker_count":         len(workers),
		"workers_with_hits":    countPositiveOverlaps(overlapByWorker),
		"estimated_field":      false,
		"full_hashes_redacted": true,
	})
	routeStart := time.Now()
	decision, routeErr := h.strategy.Pick(r.Context(), reqCtx, workers, h.index)
	timings.RouteDecision = time.Since(routeStart)
	decision.RouteLatencyMicros = timings.RouteDecision.Microseconds()
	if routeErr != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "route_decision", routerSpan, routeStart, "error", map[string]any{
			"request_id_hash": reqCtx.RequestIDHash,
			"strategy":        h.strategy.Name(),
			"error_class":     "route_error",
		})
		_ = h.decisionLog.Write(h.runID, reqCtx, decision, routeErr)
		h.metrics.ObserveRoute(h.strategy.Name(), "route_error", "", time.Since(routeStart))
		h.metrics.ObserveTenantRoute(h.strategy.Name(), reqCtx.TenantHash, "route_error")
		writeText(w, http.StatusServiceUnavailable, routeErr.Error()+"\n")
		return
	}
	if predictedOverlap, ok := overlapByWorker[decision.WorkerID]; ok {
		decision.OverlappedBlocks = predictedOverlap
	}
	routeSpan := h.traceLog.WriteSpan(h.runID, traceCtx, "route_decision", routerSpan, routeStart, "ok", routeDecisionTraceAttributes(reqCtx, decision))
	worker, reserveErr := h.reserveRoutableWorker(decision.WorkerID)
	if reserveErr != nil {
		routeErr = reserveErr
		h.traceLog.WriteSpan(h.runID, traceCtx, "proxy_to_worker", routeSpan, time.Now(), "error", map[string]any{
			"request_id_hash": reqCtx.RequestIDHash,
			"strategy":        decision.Strategy,
			"error_class":     "worker_unavailable_after_selection",
		})
		_ = h.decisionLog.Write(h.runID, reqCtx, decision, routeErr)
		h.metrics.ObserveRoute(h.strategy.Name(), "route_error", "", time.Since(routeStart))
		h.metrics.ObserveTenantRoute(h.strategy.Name(), reqCtx.TenantHash, "route_error")
		writeText(w, http.StatusServiceUnavailable, routeErr.Error()+"\n")
		return
	}
	defer h.releaseWorker(decision.WorkerID)
	_ = h.decisionLog.Write(h.runID, reqCtx, decision, nil)
	h.metrics.ObserveRoute(decision.Strategy, "ok", hashPrefix(string(decision.WorkerID)), time.Since(routeStart))
	h.metrics.ObserveTenantRoute(decision.Strategy, reqCtx.TenantHash, "ok")
	if err := h.proxyToWorker(w, r, body, reqCtx, decision, worker, timings, traceCtx, routeSpan); err != nil {
		h.metrics.ObserveTenantRoute(decision.Strategy, reqCtx.TenantHash, "proxy_error")
		return
	}
}

func (h *Handler) proxyToWorker(w http.ResponseWriter, r *http.Request, body []byte, reqCtx common.RequestContext, decision common.RouteDecision, worker common.WorkerState, timings proxyTimings, traceCtx traceContext, parentSpanID string) error {
	upstreamURL, err := joinWorkerURL(worker.URL, r.URL)
	if err != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "proxy_to_worker", parentSpanID, time.Now(), "error", map[string]any{
			"request_id_hash": reqCtx.RequestIDHash,
			"strategy":        decision.Strategy,
			"worker_id_hash":  hashPrefix(string(decision.WorkerID)),
			"error_class":     "invalid_worker_url",
		})
		writeText(w, http.StatusBadGateway, "invalid worker url\n")
		return err
	}
	upReq, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, bytes.NewReader(body))
	if err != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "proxy_to_worker", parentSpanID, time.Now(), "error", map[string]any{
			"request_id_hash": reqCtx.RequestIDHash,
			"strategy":        decision.Strategy,
			"worker_id_hash":  hashPrefix(string(decision.WorkerID)),
			"error_class":     "build_upstream_request",
		})
		writeText(w, http.StatusBadGateway, "failed to build upstream request\n")
		return err
	}
	upReq.Header = cloneProxyHeaders(r.Header)
	upReq.Header.Set("X-Router-Worker-ID", string(decision.WorkerID))
	upReq.Header.Set("X-Router-Strategy", decision.Strategy)
	upReq.Header.Set("X-Router-Request-Hash", reqCtx.RequestIDHash)
	started := time.Now()
	resp, err := h.client.Do(upReq)
	if err != nil {
		h.traceLog.WriteSpan(h.runID, traceCtx, "proxy_to_worker", parentSpanID, started, "error", map[string]any{
			"request_id_hash": reqCtx.RequestIDHash,
			"strategy":        decision.Strategy,
			"worker_id_hash":  hashPrefix(string(decision.WorkerID)),
			"error_class":     "upstream_round_trip",
		})
		h.metrics.ObserveRoute(decision.Strategy, "proxy_error", hashPrefix(string(decision.WorkerID)), 0)
		writeText(w, http.StatusBadGateway, "upstream request failed\n")
		return err
	}
	defer resp.Body.Close()
	if actualHit, actualMiss, ok := workerCacheActuals(resp.Header); ok {
		h.metrics.ObserveCachePrediction(decision.OverlappedBlocks, actualHit, actualMiss)
	}
	proxySpan := h.traceLog.WriteSpan(h.runID, traceCtx, "proxy_to_worker", parentSpanID, started, "ok", map[string]any{
		"request_id_hash": reqCtx.RequestIDHash,
		"strategy":        decision.Strategy,
		"worker_id_hash":  hashPrefix(string(decision.WorkerID)),
		"status_code":     resp.StatusCode,
	})
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set("X-Router-Worker-ID", string(decision.WorkerID))
	w.Header().Set("X-Router-Strategy", decision.Strategy)
	w.Header().Set("X-Router-Request-Hash", reqCtx.RequestIDHash)
	setRouterTimingHeaders(w.Header(), timings)
	w.WriteHeader(resp.StatusCode)
	streamStart := time.Now()
	ttft, total, bytesCopied, copyErr := copyStreaming(w, resp.Body, started)
	result := "ok"
	if copyErr != nil {
		result = "error"
	}
	h.traceLog.WriteSpan(h.runID, traceCtx, "response_stream", proxySpan, streamStart, result, map[string]any{
		"request_id_hash": reqCtx.RequestIDHash,
		"status_code":     resp.StatusCode,
		"ttft_ms":         float64(ttft.Microseconds()) / 1000.0,
		"total_ms":        float64(total.Microseconds()) / 1000.0,
		"bytes":           bytesCopied,
	})
	h.metrics.ObserveProxy(ttft, total)
	if copyErr != nil {
		return copyErr
	}
	return nil
}

func (h *Handler) buildRequestContext(r *http.Request, body []byte) (common.RequestContext, proxyTimings, error) {
	timings := proxyTimings{
		EstimatedFields: []string{"cache_state_lookup_ms"},
	}
	var req openAIRequest
	parseStart := time.Now()
	if err := json.Unmarshal(body, &req); err != nil {
		return common.RequestContext{}, timings, fmt.Errorf("invalid OpenAI-compatible JSON")
	}
	text := req.PromptText()
	if text == "" {
		return common.RequestContext{}, timings, fmt.Errorf("request prompt/messages content is empty")
	}
	modelID := req.Model
	if modelID == "" {
		modelID = "unknown-model"
	}
	tokenizerRevision := headerOrDefault(
		r,
		"X-Tokenizer-Revision",
		blockhash.ApproxTokenizerRevision,
	)
	tokens := blockhash.ApproxTokenizeWithRevision(text, tokenizerRevision)
	key := blockhash.IsolationKey{
		TenantID:          headerOrDefault(r, "X-Tenant-ID", "default-tenant"),
		TenantSalt:        headerOrDefault(r, "X-Tenant-Salt", "default-salt"),
		ModelID:           modelID,
		ModelRevision:     headerOrDefault(r, "X-Model-Revision", "default-revision"),
		TokenizerRevision: tokenizerRevision,
		LoRAID:            headerOrDefault(r, "X-LoRA-ID", "none"),
		ModalityKey:       headerOrDefault(r, "X-Modality-Key", "text"),
		CacheSalt:         headerOrDefault(r, "X-Cache-Salt", "cache-v1"),
	}
	timings.RouterParse = time.Since(parseStart)
	hashStart := time.Now()
	blocks := h.hasher.ComputeBlocks(tokens, key)
	blockHashes := make([]common.BlockHash, 0, len(blocks))
	for _, block := range blocks {
		blockHashes = append(blockHashes, common.BlockHash(block))
	}
	timings.BlockHash = time.Since(hashStart)
	requestID := ensureRequestID(r)
	tenantID := headerOrDefault(r, "X-Tenant-ID", "default-tenant")
	return common.RequestContext{
		RequestID:            requestID,
		RequestIDHash:        hashPrefix(requestID),
		TenantHash:           hashPrefix(tenantID),
		BlockHashes:          blockHashes,
		BlockSizeTokens:      h.hasher.BlockSizeTokens,
		TotalPromptTokens:    len(tokens),
		MissingPrefillTokens: len(tokens),
		MaxOutputTokens:      req.MaxTokens,
		ArrivalMS:            time.Now().UnixMilli(),
		EntryTopology:        h.entryTopology,
	}, timings, nil
}

func strategyByName(name string) (routing.Strategy, error) {
	switch name {
	case "", "cost_aware":
		return routing.NewCostAwareVariant("cost_aware"), nil
	case "round_robin":
		return &routing.RoundRobin{}, nil
	case "p2c":
		return routing.P2C{}, nil
	case "prefix_hash":
		return routing.PrefixHash{}, nil
	case "prefix_hash_bounded_load":
		return routing.PrefixHashBoundedLoad{}, nil
	case "tenant_aware_bounded_load":
		return &routing.TenantAwareBoundedLoad{}, nil
	case "topology_aware_cost_aware":
		strategy := routing.NewTopologyAwareCostAware()
		return strategy, nil
	case "costaware_0_overlap_only", "costaware_1_plus_queue", "costaware_2_plus_decode", "costaware_3_plus_memory", "costaware_4_plus_staleness", "costaware_5_plus_slo":
		return routing.NewCostAwareVariant(name), nil
	default:
		return nil, fmt.Errorf("unknown strategy %s", name)
	}
}

type openAIRequest struct {
	Model     string        `json:"model"`
	Prompt    any           `json:"prompt"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens"`
	Stream    bool          `json:"stream"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

func (r openAIRequest) PromptText() string {
	parts := make([]string, 0, len(r.Messages)+1)
	parts = appendPromptValue(parts, r.Prompt)
	for _, message := range r.Messages {
		parts = appendPromptValue(parts, message.Content)
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func appendPromptValue(parts []string, value any) []string {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			parts = append(parts, v)
		}
	case []any:
		for _, item := range v {
			parts = appendPromptValue(parts, item)
		}
	case map[string]any:
		if text, ok := v["text"].(string); ok && strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	return parts
}

func isOpenAIPath(path string) bool {
	return path == "/v1/completions" || path == "/v1/chat/completions"
}

func openAIPathKind(path string) string {
	switch path {
	case "/v1/chat/completions":
		return "chat_completions"
	case "/v1/completions":
		return "completions"
	default:
		return "other"
	}
}

func countPositiveOverlaps(overlaps map[common.WorkerID]int) int {
	count := 0
	for _, overlap := range overlaps {
		if overlap > 0 {
			count++
		}
	}
	return count
}

func routeDecisionTraceAttributes(reqCtx common.RequestContext, decision common.RouteDecision) map[string]any {
	return map[string]any{
		"request_id_hash":          reqCtx.RequestIDHash,
		"strategy":                 decision.Strategy,
		"worker_id_hash":           hashPrefix(string(decision.WorkerID)),
		"candidate_count":          decision.CandidateCount,
		"fallback":                 decision.Fallback,
		"reason":                   decision.Reason,
		"predicted_overlap_blocks": decision.OverlappedBlocks,
		"local_overlap_blocks":     decision.LocalOverlapBlocks,
		"shared_overlap_blocks":    decision.SharedOverlapBlocks,
		"topology_unknown":         decision.TopologyUnknown,
		"egress_cost_class":        normalizeEgressCostClass(decision.EgressCostClass),
		"selected_transport":       normalizeTransport(decision.SelectedTransport),
	}
}

func isWorkerAdminAction(path string, action string) bool {
	_, ok := workerIDFromAdminAction(path)
	return ok && strings.HasSuffix(path, "/"+action)
}

func workerIDFromAdminAction(path string) (string, bool) {
	rest := strings.TrimPrefix(path, "/admin/workers/")
	if rest == path || rest == "" {
		return "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || (parts[1] != "drain" && parts[1] != "undrain") {
		return "", false
	}
	id, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(id) == "" {
		return "", false
	}
	return id, true
}

func unsafeRemovedWorkers(oldWorkers []common.WorkerState, newWorkers []common.WorkerState, routerInflight map[common.WorkerID]int) []map[string]any {
	next := make(map[common.WorkerID]bool, len(newWorkers))
	for _, worker := range newWorkers {
		next[worker.ID] = true
	}
	blocked := make([]map[string]any, 0)
	for _, worker := range oldWorkers {
		if next[worker.ID] {
			continue
		}
		worker.RouterInflightRequests = routerInflight[worker.ID]
		// Removal is a two-part contract: the control plane must first drain
		// the worker, and both worker-reported and Router-owned work must reach
		// zero.  Do not infer that an idle but non-draining worker is removable.
		if worker.SafeToRemove() {
			continue
		}
		blocked = append(blocked, map[string]any{
			"id":                          string(worker.ID),
			"queue_depth":                 worker.QueueDepth,
			"active_decode_blocks":        worker.ActiveDecodeBlocks,
			"inflight_requests":           worker.InflightRequests,
			"router_inflight_requests":    worker.RouterInflightRequests,
			"effective_inflight_requests": worker.EffectiveInflightRequests(),
		})
	}
	return blocked
}

func workerAdminResponse(worker common.WorkerState) map[string]any {
	return map[string]any{
		"id":                          string(worker.ID),
		"health":                      string(worker.Health),
		"readiness_state":             string(worker.ReadinessState),
		"draining":                    worker.Draining,
		"queue_depth":                 worker.QueueDepth,
		"active_decode_blocks":        worker.ActiveDecodeBlocks,
		"inflight_requests":           worker.InflightRequests,
		"router_inflight_requests":    worker.RouterInflightRequests,
		"effective_inflight_requests": worker.EffectiveInflightRequests(),
		"safe_to_remove":              worker.SafeToRemove(),
	}
}

func (h *Handler) workerSnapshotLocked(worker common.WorkerState) common.WorkerState {
	worker.RouterInflightRequests = h.routerInflight[worker.ID]
	return worker
}

func (h *Handler) reserveRoutableWorker(id common.WorkerID) (common.WorkerState, error) {
	h.workersMu.Lock()
	defer h.workersMu.Unlock()
	for _, worker := range h.workers {
		if worker.ID != id {
			continue
		}
		if !worker.Routable() {
			return common.WorkerState{}, fmt.Errorf("selected worker %q is no longer routable", id)
		}
		h.routerInflight[id]++
		return h.workerSnapshotLocked(worker), nil
	}
	return common.WorkerState{}, fmt.Errorf("selected worker %q is no longer registered", id)
}

func (h *Handler) releaseWorker(id common.WorkerID) {
	h.workersMu.Lock()
	defer h.workersMu.Unlock()
	if h.routerInflight[id] <= 1 {
		delete(h.routerInflight, id)
		return
	}
	h.routerInflight[id]--
}

func findWorker(workers []common.WorkerState, id common.WorkerID) (common.WorkerState, bool) {
	for _, worker := range workers {
		if worker.ID == id {
			return worker, true
		}
	}
	return common.WorkerState{}, false
}

func joinWorkerURL(workerURL string, original *url.URL) (string, error) {
	base, err := url.Parse(workerURL)
	if err != nil {
		return "", err
	}
	if base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("worker url must be absolute")
	}
	joined := *base
	prefix := strings.TrimRight(base.Path, "/")
	joined.Path = prefix + original.Path
	joined.RawQuery = original.RawQuery
	return joined.String(), nil
}

func cloneProxyHeaders(headers http.Header) http.Header {
	out := headers.Clone()
	removeHopByHopHeaders(out)
	return out
}

func setRequestCorrelationHeaders(headers http.Header, r *http.Request) {
	requestID := ensureRequestID(r)
	if traceID := traceIDFromTraceparent(r.Header.Get("traceparent")); traceID != "" {
		headers.Set("X-Trace-Id", traceID)
	}
	headers.Set("X-Request-Hash", hashPrefix(requestID))
}

func ensureRequestID(r *http.Request) string {
	if requestID := strings.TrimSpace(r.Header.Get("X-Request-ID")); requestID != "" {
		return requestID
	}
	random := make([]byte, 16)
	requestID := ""
	if _, err := rand.Read(random); err == nil {
		requestID = hex.EncodeToString(random)
	} else {
		requestID = fmt.Sprintf(
			"%d-%d", time.Now().UnixNano(), generatedRequestCounter.Add(1),
		)
	}
	r.Header.Set("X-Request-ID", requestID)
	return requestID
}

func removeHopByHopHeaders(headers http.Header) {
	for _, connectionValue := range headers.Values("Connection") {
		for _, token := range strings.Split(connectionValue, ",") {
			if name := strings.TrimSpace(token); name != "" {
				headers.Del(name)
			}
		}
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		headers.Del(name)
	}
}

func traceIDFromTraceparent(value string) string {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) < 4 {
		return ""
	}
	traceID := parts[1]
	if len(traceID) != 32 {
		return ""
	}
	for _, ch := range traceID {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return ""
		}
	}
	if traceID == "00000000000000000000000000000000" {
		return ""
	}
	return traceID
}

func copyResponseHeaders(dst http.Header, src http.Header) {
	clean := src.Clone()
	removeHopByHopHeaders(clean)
	for key, values := range clean {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func workerCacheActuals(headers http.Header) (int, int, bool) {
	actualHit, hitOK := parseNonNegativeHeader(headers, actualHitBlocksHeader, actualHitBlocksJSONKey)
	actualMiss, missOK := parseNonNegativeHeader(headers, actualMissBlocksHeader, actualMissBlocksJSONKey)
	return actualHit, actualMiss, hitOK && missOK
}

func parseNonNegativeHeader(headers http.Header, names ...string) (int, bool) {
	for _, name := range names {
		value := firstHeaderValue(headers, name)
		if value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, false
		}
		return parsed, true
	}
	return 0, false
}

func firstHeaderValue(headers http.Header, name string) string {
	if value := strings.TrimSpace(headers.Get(name)); value != "" {
		return value
	}
	for key, values := range headers {
		if !strings.EqualFold(key, name) || len(values) == 0 {
			continue
		}
		return strings.TrimSpace(values[0])
	}
	return ""
}

func setRouterTimingHeaders(headers http.Header, timings proxyTimings) {
	headers.Set("X-Router-Timing-Router-Parse-Ms", formatDurationMS(timings.RouterParse))
	headers.Set("X-Router-Timing-Block-Hash-Ms", formatDurationMS(timings.BlockHash))
	headers.Set("X-Router-Timing-Cache-State-Lookup-Ms", formatDurationMS(timings.CacheStateLookup))
	headers.Set("X-Router-Timing-Route-Decision-Ms", formatDurationMS(timings.RouteDecision))
	if len(timings.EstimatedFields) > 0 {
		headers.Set("X-Router-Timing-Estimated-Fields", strings.Join(timings.EstimatedFields, ","))
	}
}

func formatDurationMS(value time.Duration) string {
	if value < 0 {
		value = 0
	}
	return fmt.Sprintf("%.3f", float64(value.Microseconds())/1000.0)
}

func copyStreaming(w http.ResponseWriter, src io.Reader, started time.Time) (time.Duration, time.Duration, int64, error) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	first := time.Duration(-1)
	var totalBytes int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if first < 0 {
				first = time.Since(started)
			}
			written, err := w.Write(buf[:n])
			totalBytes += int64(written)
			if err != nil {
				return first, time.Since(started), totalBytes, err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return first, time.Since(started), totalBytes, readErr
		}
	}
	if first < 0 {
		first = time.Since(started)
	}
	return first, time.Since(started), totalBytes, nil
}

func headerOrDefault(r *http.Request, name string, fallback string) string {
	if value := strings.TrimSpace(r.Header.Get(name)); value != "" {
		return value
	}
	return fallback
}

func hashPrefix(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
