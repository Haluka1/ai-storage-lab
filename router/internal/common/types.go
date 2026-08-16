package common

import "time"

type WorkerID string
type BlockHash string
type Tier string
type WorkerHealthState string
type WorkerReadinessState string
type LocalityKind string
type TransportKind string

const (
	TierGPU     Tier = "gpu"
	TierCPU     Tier = "cpu"
	TierNVMe    Tier = "nvme"
	TierS3      Tier = "s3"
	TierUnknown Tier = "unknown"

	WorkerReady     WorkerHealthState = "ready"
	WorkerDraining  WorkerHealthState = "draining"
	WorkerUnhealthy WorkerHealthState = "unhealthy"

	ReadinessReady     WorkerReadinessState = "ready"
	ReadinessNotReady  WorkerReadinessState = "not_ready"
	ReadinessDraining  WorkerReadinessState = "draining"
	ReadinessUnhealthy WorkerReadinessState = "unhealthy"

	LocalityLocal       LocalityKind = "local"
	LocalitySameNode    LocalityKind = "same_node"
	LocalitySameZone    LocalityKind = "same_zone"
	LocalityCrossZone   LocalityKind = "cross_zone"
	LocalityCrossRegion LocalityKind = "cross_region"
	LocalityCrossCloud  LocalityKind = "cross_cloud"
	LocalityUnknown     LocalityKind = "unknown"

	TransportLocalMemory   TransportKind = "local_memory"
	TransportLocalNVMe     TransportKind = "local_nvme"
	TransportS3HTTPDefault TransportKind = "s3_http_default"
	TransportFilePOSIX     TransportKind = "file_posix_default"
	TransportUnknown       TransportKind = "unknown"
)

type Topology struct {
	Cloud     string `json:"cloud" yaml:"cloud"`
	Region    string `json:"region" yaml:"region"`
	Zone      string `json:"zone" yaml:"zone"`
	ClusterID string `json:"cluster_id" yaml:"cluster_id"`
	Rack      string `json:"rack,omitempty" yaml:"rack,omitempty"`
	NodeID    string `json:"node_id" yaml:"node_id"`
}

func (t Topology) Unknown() bool {
	return t.Cloud == "" || t.Region == "" || t.Zone == "" || t.ClusterID == "" || t.NodeID == ""
}

type WorkerState struct {
	ID                 WorkerID             `json:"id"`
	URL                string               `json:"url"`
	Health             WorkerHealthState    `json:"health"`
	ReadinessState     WorkerReadinessState `json:"readiness_state,omitempty"`
	Draining           bool                 `json:"draining"`
	QueueDepth         int                  `json:"queue_depth"`
	ActiveDecodeBlocks int                  `json:"active_decode_blocks"`
	// InflightRequests is the count reported by the Worker/control plane. The
	// Router keeps its own reservations separately so it never overwrites or
	// misrepresents externally supplied telemetry.
	InflightRequests int `json:"inflight_requests,omitempty"`
	// RouterInflightRequests is populated by the Router in state snapshots. It
	// is read-only at the admin/config boundary.
	RouterInflightRequests int       `json:"router_inflight_requests,omitempty"`
	MemoryPressure         float64   `json:"memory_pressure"`
	GPUMemoryPressure      float64   `json:"gpu_memory_pressure"`
	KVCachePressure        float64   `json:"kv_cache_pressure"`
	RecentErrorRate        float64   `json:"recent_error_rate"`
	CacheStateStalenessMS  float64   `json:"cache_state_staleness_ms"`
	Weight                 float64   `json:"weight"`
	Topology               Topology  `json:"topology"`
	LastHeartbeat          time.Time `json:"last_heartbeat"`
	LastUpdated            time.Time `json:"last_updated"`
}

func (w WorkerState) Routable() bool {
	if w.Draining || w.Health == WorkerDraining || w.Health == WorkerUnhealthy {
		return false
	}
	switch w.ReadinessState {
	case "", ReadinessReady:
		return w.Health == "" || w.Health == WorkerReady
	case ReadinessDraining, ReadinessUnhealthy, ReadinessNotReady:
		return false
	default:
		return false
	}
}

func (w WorkerState) ResourcePressure() float64 {
	pressure := w.MemoryPressure
	if w.GPUMemoryPressure > pressure {
		pressure = w.GPUMemoryPressure
	}
	if w.KVCachePressure > pressure {
		pressure = w.KVCachePressure
	}
	return pressure
}

func (w WorkerState) SafeToRemove() bool {
	return w.Draining && w.ActiveDecodeBlocks == 0 && w.EffectiveInflightRequests() == 0 && w.QueueDepth == 0
}

func (w WorkerState) EffectiveInflightRequests() int {
	return w.InflightRequests + w.RouterInflightRequests
}

type RequestContext struct {
	RequestID            string      `json:"request_id"`
	RequestIDHash        string      `json:"request_id_hash"`
	TenantHash           string      `json:"tenant_hash,omitempty"`
	BlockHashes          []BlockHash `json:"block_hashes"`
	BlockSizeTokens      int         `json:"block_size_tokens,omitempty"`
	TotalPromptTokens    int         `json:"total_prompt_tokens"`
	MissingPrefillTokens int         `json:"missing_prefill_tokens"`
	MaxOutputTokens      int         `json:"max_output_tokens"`
	ArrivalMS            int64       `json:"arrival_ms"`
	EntryTopology        Topology    `json:"entry_topology,omitempty"`
}

type BlockLocation struct {
	BlockHash              BlockHash     `json:"block_hash"`
	WorkerID               WorkerID      `json:"worker_id"`
	Tier                   Tier          `json:"tier"`
	Topology               Topology      `json:"topology"`
	Locality               LocalityKind  `json:"locality"`
	Transport              TransportKind `json:"transport"`
	EstimatedLoadP95Ms     float64       `json:"estimated_load_p95_ms"`
	EstimatedTransferP95Ms float64       `json:"estimated_transfer_p95_ms"`
	EgressCostClass        string        `json:"egress_cost_class"`
	Bytes                  int64         `json:"bytes"`
	Tokens                 int           `json:"tokens"`
	SeqNo                  int64         `json:"seq_no"`
	Confidence             float64       `json:"confidence"`
	UpdatedAt              time.Time     `json:"updated_at"`
	ExpiresAt              time.Time     `json:"expires_at"`
}

type CandidateScore struct {
	WorkerID              WorkerID      `json:"worker"`
	Score                 float64       `json:"score"`
	PredictedOverlap      int           `json:"predicted_overlap_blocks"`
	LocalOverlapBlocks    int           `json:"local_overlap_blocks,omitempty"`
	SharedOverlapBlocks   int           `json:"shared_overlap_blocks,omitempty"`
	QueueDepth            int           `json:"queue_depth"`
	ActiveDecodeBlocks    int           `json:"active_decode_blocks"`
	MemoryPressure        float64       `json:"memory_pressure"`
	TopologyPenalty       float64       `json:"topology_penalty,omitempty"`
	EstimatedKVTransferMS float64       `json:"estimated_kv_transfer_ms,omitempty"`
	EgressCostClass       string        `json:"egress_cost_class,omitempty"`
	SelectedTransport     TransportKind `json:"selected_transport,omitempty"`
	TopologyUnknown       bool          `json:"topology_unknown,omitempty"`
}

type RouteDecision struct {
	RequestIDHash         string           `json:"request_id_hash"`
	Strategy              string           `json:"strategy"`
	WorkerID              WorkerID         `json:"worker_id"`
	ChosenTopology        Topology         `json:"chosen_topology,omitempty"`
	OverlappedBlocks      int              `json:"overlapped_blocks"`
	LocalOverlapBlocks    int              `json:"local_overlap_blocks,omitempty"`
	SharedOverlapBlocks   int              `json:"shared_overlap_blocks,omitempty"`
	EstimatedScore        float64          `json:"estimated_score"`
	TopologyPenalty       float64          `json:"topology_penalty,omitempty"`
	EstimatedKVTransferMS float64          `json:"estimated_kv_transfer_ms,omitempty"`
	EgressCostClass       string           `json:"egress_cost_class,omitempty"`
	SelectedTransport     TransportKind    `json:"selected_transport,omitempty"`
	TopologyUnknown       bool             `json:"topology_unknown,omitempty"`
	Fallback              bool             `json:"fallback"`
	FallbackReason        string           `json:"fallback_reason,omitempty"`
	CandidateCount        int              `json:"candidate_count"`
	RouteLatencyMicros    int64            `json:"route_latency_micros"`
	Reason                string           `json:"reason"`
	TopCandidates         []CandidateScore `json:"top_candidates,omitempty"`
}
