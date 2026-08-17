package proxy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

type DecisionLogger struct {
	mu   sync.Mutex
	file *os.File
}

func NewDecisionLogger(path string) (*DecisionLogger, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &DecisionLogger{file: file}, nil
}

func (l *DecisionLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}

func (l *DecisionLogger) Write(runID string, req common.RequestContext, decision common.RouteDecision, routeErr error) error {
	if l == nil || l.file == nil {
		return nil
	}
	record := map[string]any{
		"run_id":                   runID,
		"decision_type":            "router_route",
		"timestamp_ms":             time.Now().UnixMilli(),
		"request_id_hash":          req.RequestIDHash,
		"strategy":                 decision.Strategy,
		"chosen_worker":            string(decision.WorkerID),
		"chosen_topology":          decision.ChosenTopology,
		"candidate_count":          decision.CandidateCount,
		"decision":                 "route",
		"reason":                   decision.Reason,
		"fallback_reason":          nil,
		"predicted_overlap_blocks": decision.OverlappedBlocks,
		"topology_penalty":         nullableFloat(decision.TopologyPenalty),
		"estimated_kv_transfer_ms": nullableFloat(decision.EstimatedKVTransferMS),
		"egress_cost_class":        normalizeEgressCostClass(decision.EgressCostClass),
		"selected_transport":       normalizeTransport(decision.SelectedTransport),
		"topology_unknown":         decision.TopologyUnknown,
		"route_latency_micros":     decision.RouteLatencyMicros,
		"top_candidates":           decisionTopCandidates(decision),
	}
	if decision.FallbackReason != "" {
		record["fallback_reason"] = decision.FallbackReason
	}
	if routeErr != nil {
		record["decision"] = "skip"
		record["reason"] = routeErr.Error()
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return l.file.Sync()
}

func decisionTopCandidates(decision common.RouteDecision) []map[string]any {
	if len(decision.TopCandidates) == 0 {
		if decision.WorkerID == "" {
			return nil
		}
		return []map[string]any{{
			"worker":                   string(decision.WorkerID),
			"score":                    decision.EstimatedScore,
			"predicted_overlap_blocks": decision.OverlappedBlocks,
		}}
	}
	out := make([]map[string]any, 0, len(decision.TopCandidates))
	for _, candidate := range decision.TopCandidates {
		out = append(out, map[string]any{
			"worker":                   string(candidate.WorkerID),
			"score":                    candidate.Score,
			"predicted_overlap_blocks": candidate.PredictedOverlap,
			"local_overlap_blocks":     candidate.LocalOverlapBlocks,
			"shared_overlap_blocks":    candidate.SharedOverlapBlocks,
			"queue_depth":              candidate.QueueDepth,
			"active_decode_blocks":     candidate.ActiveDecodeBlocks,
			"memory_pressure":          candidate.MemoryPressure,
			"topology_penalty":         nullableFloat(candidate.TopologyPenalty),
			"estimated_kv_transfer_ms": nullableFloat(candidate.EstimatedKVTransferMS),
			"egress_cost_class":        normalizeEgressCostClass(candidate.EgressCostClass),
			"selected_transport":       normalizeTransport(candidate.SelectedTransport),
			"topology_unknown":         candidate.TopologyUnknown,
		})
	}
	return out
}

func nullableFloat(value float64) any {
	if value == 0 {
		return nil
	}
	return value
}

func normalizeEgressCostClass(class string) string {
	switch class {
	case "", "none":
		return "none"
	case "intra_zone", "cross_zone", "cross_region", "cross_cloud", "unknown":
		return class
	default:
		return "unknown"
	}
}

func normalizeTransport(transport common.TransportKind) string {
	if transport == "" {
		return string(common.TransportUnknown)
	}
	return string(transport)
}
