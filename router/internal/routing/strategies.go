package routing

import (
	"context"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/cacheindex"
	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

type RoundRobin struct {
	mu   sync.Mutex
	next int
}

func (r *RoundRobin) Name() string { return "round_robin" }

func (r *RoundRobin) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	_ = idx
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	r.mu.Lock()
	worker := candidates[r.next%len(candidates)]
	r.next++
	r.mu.Unlock()
	return common.RouteDecision{RequestIDHash: req.RequestIDHash, Strategy: r.Name(), WorkerID: worker.ID, CandidateCount: len(candidates), Reason: "round_robin"}, nil
}

type P2C struct{}

func (p P2C) Name() string { return "p2c" }

func (p P2C) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	_ = idx
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	if len(candidates) == 1 {
		return common.RouteDecision{RequestIDHash: req.RequestIDHash, Strategy: p.Name(), WorkerID: candidates[0].ID, CandidateCount: 1, Reason: "only_healthy_worker"}, nil
	}
	i, j := deterministicPair(req.RequestIDHash, len(candidates))
	a, b := candidates[i], candidates[j]
	chosen := a
	if loadScore(b) < loadScore(a) {
		chosen = b
	}
	return common.RouteDecision{RequestIDHash: req.RequestIDHash, Strategy: p.Name(), WorkerID: chosen.ID, CandidateCount: len(candidates), Reason: "lower_queue_power_of_two"}, nil
}

type PrefixHash struct{}

func (p PrefixHash) Name() string { return "prefix_hash" }

func (p PrefixHash) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	overlap := overlapFor(idx, req)
	best := candidates[0]
	bestOverlap := overlap[best.ID]
	for _, worker := range candidates[1:] {
		workerOverlap := overlap[worker.ID]
		if workerOverlap > bestOverlap || (workerOverlap == bestOverlap && loadScore(worker) < loadScore(best)) {
			best = worker
			bestOverlap = workerOverlap
		}
	}
	return common.RouteDecision{
		RequestIDHash:    req.RequestIDHash,
		Strategy:         p.Name(),
		WorkerID:         best.ID,
		OverlappedBlocks: bestOverlap,
		CandidateCount:   len(candidates),
		Reason:           "highest_overlap",
	}, nil
}

type PrefixHashBoundedLoad struct {
	MaxQueueRatio float64
	MaxQueueSlack int
	P2C           P2C
}

func (p PrefixHashBoundedLoad) Name() string { return "prefix_hash_bounded_load" }

func (p PrefixHashBoundedLoad) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	ratio := p.MaxQueueRatio
	if ratio <= 0 {
		ratio = 1.5
	}
	slack := p.MaxQueueSlack
	if slack == 0 {
		slack = 4
	}
	prefixDecision, err := (PrefixHash{}).Pick(ctx, req, workers, idx)
	if err != nil {
		return prefixDecision, err
	}
	avg := averageQueue(healthy(workers))
	chosen := findWorker(workers, prefixDecision.WorkerID)
	threshold := int(math.Ceil(avg*ratio)) + slack
	if chosen.QueueDepth > threshold {
		fallback, err := p.P2C.Pick(ctx, req, workers, idx)
		if err != nil {
			return fallback, err
		}
		fallback.Strategy = p.Name()
		fallback.Fallback = true
		fallback.FallbackReason = "bounded_load_spillover"
		fallback.OverlappedBlocks = idx.OverlapByWorker(req.BlockHashes)[fallback.WorkerID]
		fallback.Reason = "bounded_load_spillover"
		return fallback, nil
	}
	prefixDecision.Strategy = p.Name()
	return prefixDecision, nil
}

type TenantAwareBoundedLoad struct {
	mu                  sync.Mutex
	assignments         map[string]map[common.WorkerID]int
	lastSeen            map[string]time.Time
	MaxTenantQueueSlack int
	MaxGlobalQueueSlack int
	MaxTenantStates     int
	TenantStateTTL      time.Duration
	now                 func() time.Time
}

func (t *TenantAwareBoundedLoad) pruneTenantState(now time.Time) {
	ttl := t.TenantStateTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	for tenant, seen := range t.lastSeen {
		if now.Sub(seen) > ttl {
			delete(t.lastSeen, tenant)
			delete(t.assignments, tenant)
		}
	}
}

func (t *TenantAwareBoundedLoad) evictOldestTenant() {
	var oldestTenant string
	var oldest time.Time
	for tenant, seen := range t.lastSeen {
		if oldestTenant == "" || seen.Before(oldest) {
			oldestTenant = tenant
			oldest = seen
		}
	}
	if oldestTenant != "" {
		delete(t.lastSeen, oldestTenant)
		delete(t.assignments, oldestTenant)
	}
}

func (t *TenantAwareBoundedLoad) Name() string { return "tenant_aware_bounded_load" }

func (t *TenantAwareBoundedLoad) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	tenantHash := req.TenantHash
	if tenantHash == "" {
		tenantHash = "unknown"
	}
	overlap := overlapFor(idx, req)
	avgQueue := averageQueue(candidates)
	globalSlack := t.MaxGlobalQueueSlack
	if globalSlack == 0 {
		globalSlack = 4
	}
	tenantSlack := t.MaxTenantQueueSlack
	if tenantSlack == 0 {
		tenantSlack = 2
	}
	t.mu.Lock()
	if t.assignments == nil {
		t.assignments = make(map[string]map[common.WorkerID]int)
		t.lastSeen = make(map[string]time.Time)
	}
	now := time.Now()
	if t.now != nil {
		now = t.now()
	}
	t.pruneTenantState(now)
	tenantLoad := t.assignments[tenantHash]
	if tenantLoad == nil {
		maxStates := t.MaxTenantStates
		if maxStates <= 0 {
			maxStates = 4096
		}
		if len(t.assignments) >= maxStates {
			t.evictOldestTenant()
		}
		tenantLoad = make(map[common.WorkerID]int)
		t.assignments[tenantHash] = tenantLoad
	}
	t.lastSeen[tenantHash] = now
	tenantTotal := 0
	for _, worker := range candidates {
		tenantTotal += tenantLoad[worker.ID]
	}
	if tenantTotal > 1_000_000 {
		tenantTotal = 0
		for workerID, count := range tenantLoad {
			tenantLoad[workerID] = count / 2
			tenantTotal += tenantLoad[workerID]
		}
	}
	tenantLimit := tenantTotal/len(candidates) + tenantSlack
	best := candidates[0]
	bestScore := math.Inf(-1)
	bestOverlap := overlap[best.ID]
	top := make([]common.CandidateScore, 0, len(candidates))
	for _, worker := range candidates {
		ov := overlap[worker.ID]
		tenantOverLimit := tenantLoad[worker.ID] > tenantLimit
		globalOverLimit := float64(worker.QueueDepth) > avgQueue+float64(globalSlack)
		score := float64(ov)*8.0 - float64(worker.QueueDepth)*1.5 - float64(tenantLoad[worker.ID])*3.0 - worker.ResourcePressure()*2.0
		if tenantOverLimit {
			score -= 25.0
		}
		if globalOverLimit {
			score -= 15.0
		}
		top = append(top, common.CandidateScore{
			WorkerID:           worker.ID,
			Score:              score,
			PredictedOverlap:   ov,
			QueueDepth:         worker.QueueDepth,
			ActiveDecodeBlocks: worker.ActiveDecodeBlocks,
			MemoryPressure:     worker.ResourcePressure(),
		})
		if score > bestScore {
			best = worker
			bestScore = score
			bestOverlap = ov
		}
	}
	tenantLoad[best.ID]++
	t.mu.Unlock()
	sort.SliceStable(top, func(i, j int) bool {
		return top[i].Score > top[j].Score
	})
	reason := "tenant_aware_bounded_load"
	if bestOverlap == 0 {
		reason = "tenant_no_reuse_degrade_to_load_aware"
	}
	return common.RouteDecision{
		RequestIDHash:    req.RequestIDHash,
		Strategy:         t.Name(),
		WorkerID:         best.ID,
		ChosenTopology:   best.Topology,
		OverlappedBlocks: bestOverlap,
		EstimatedScore:   bestScore,
		CandidateCount:   len(candidates),
		Reason:           reason,
		TopCandidates:    top,
	}, nil
}

type CostAwareWeights struct {
	Hit      float64
	Shared   float64
	Prefill  float64
	Decode   float64
	Queue    float64
	Memory   float64
	Stale    float64
	SLO      float64
	Topology float64
	Transfer float64
	Egress   float64
}

type CostAware struct {
	StrategyName    string
	Weights         CostAwareWeights
	UsePrefillCost  bool
	UseQueueCost    bool
	UseDecodeCost   bool
	UseMemoryCost   bool
	UseStalePenalty bool
	UseSLORisk      bool
}

func (c CostAware) Name() string {
	if c.StrategyName != "" {
		return c.StrategyName
	}
	return "cost_aware"
}

func NewCostAwareVariant(name string) CostAware {
	switch name {
	case "costaware_0_overlap_only":
		return CostAware{StrategyName: name}
	case "costaware_1_plus_queue":
		return CostAware{StrategyName: name, UseQueueCost: true}
	case "costaware_2_plus_decode":
		return CostAware{StrategyName: name, UseQueueCost: true, UseDecodeCost: true}
	case "costaware_3_plus_memory":
		return CostAware{StrategyName: name, UseQueueCost: true, UseDecodeCost: true, UseMemoryCost: true}
	case "costaware_4_plus_staleness":
		return CostAware{StrategyName: name, UseQueueCost: true, UseDecodeCost: true, UseMemoryCost: true, UseStalePenalty: true}
	case "costaware_5_plus_slo":
		return CostAware{StrategyName: name, UsePrefillCost: true, UseQueueCost: true, UseDecodeCost: true, UseMemoryCost: true, UseStalePenalty: true, UseSLORisk: true}
	default:
		return CostAware{StrategyName: name, UsePrefillCost: true, UseQueueCost: true, UseDecodeCost: true, UseMemoryCost: true, UseStalePenalty: true, UseSLORisk: true}
	}
}

func requestBlockSize(req common.RequestContext) int {
	if req.BlockSizeTokens > 0 {
		return req.BlockSizeTokens
	}
	return 16
}

func (c CostAware) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	c = c.normalized()
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	w := c.weights()
	overlap := overlapFor(idx, req)
	best := candidates[0]
	bestScore := math.Inf(-1)
	top := make([]common.CandidateScore, 0, len(candidates))
	for _, worker := range candidates {
		ov := overlap[worker.ID]
		missingBlocks := len(req.BlockHashes) - ov
		missingTokens := missingBlocks * requestBlockSize(req)
		score := w.Hit * float64(ov)
		if c.UsePrefillCost {
			score -= w.Prefill * float64(missingTokens)
		}
		if c.UseDecodeCost {
			score -= w.Decode * float64(worker.ActiveDecodeBlocks)
		}
		if c.UseQueueCost {
			score -= w.Queue * float64(worker.QueueDepth)
		}
		if c.UseMemoryCost {
			score -= w.Memory * worker.ResourcePressure()
		}
		if c.UseStalePenalty {
			score -= w.Stale * (worker.CacheStateStalenessMS / 1000.0)
		}
		if c.UseSLORisk {
			score -= w.SLO * predictedSLORisk(float64(missingTokens), worker)
		}
		top = append(top, common.CandidateScore{
			WorkerID:           worker.ID,
			Score:              score,
			PredictedOverlap:   ov,
			QueueDepth:         worker.QueueDepth,
			ActiveDecodeBlocks: worker.ActiveDecodeBlocks,
			MemoryPressure:     worker.ResourcePressure(),
		})
		if score > bestScore {
			best = worker
			bestScore = score
		}
	}
	reason := "highest_score"
	if overlap[best.ID] == 0 {
		reason = "no_reuse_degrade_to_load_aware"
	}
	return common.RouteDecision{
		RequestIDHash:    req.RequestIDHash,
		Strategy:         c.Name(),
		WorkerID:         best.ID,
		ChosenTopology:   best.Topology,
		OverlappedBlocks: overlap[best.ID],
		EstimatedScore:   bestScore,
		CandidateCount:   len(candidates),
		Reason:           reason,
		TopCandidates:    top,
	}, nil
}

func (c CostAware) normalized() CostAware {
	if c.StrategyName == "" && !c.UsePrefillCost && !c.UseQueueCost && !c.UseDecodeCost && !c.UseMemoryCost && !c.UseStalePenalty && !c.UseSLORisk {
		return NewCostAwareVariant("cost_aware")
	}
	return c
}

func (c CostAware) weights() CostAwareWeights {
	w := c.Weights
	if w.Hit == 0 {
		w.Hit = 4.0
	}
	if w.Prefill == 0 {
		w.Prefill = 0.02
	}
	if w.Decode == 0 {
		w.Decode = 0.5
	}
	if w.Queue == 0 {
		w.Queue = 1.0
	}
	if w.Memory == 0 {
		w.Memory = 3.0
	}
	if w.Stale == 0 {
		w.Stale = 0.5
	}
	if w.SLO == 0 {
		w.SLO = 2.0
	}
	return w
}

func predictedSLORisk(missingTokens float64, worker common.WorkerState) float64 {
	predictedTTFT := missingTokens*0.08 + float64(worker.QueueDepth)*1.5 + float64(worker.ActiveDecodeBlocks)*0.2
	if predictedTTFT <= 100 {
		return 0
	}
	return (predictedTTFT - 100) / 100
}

func loadScore(w common.WorkerState) float64 {
	return float64(w.QueueDepth) + float64(w.ActiveDecodeBlocks)*0.25 + w.ResourcePressure()*4
}

func averageQueue(workers []common.WorkerState) float64 {
	if len(workers) == 0 {
		return 0
	}
	sum := 0
	for _, worker := range workers {
		sum += worker.QueueDepth
	}
	return float64(sum) / float64(len(workers))
}

func findWorker(workers []common.WorkerState, id common.WorkerID) common.WorkerState {
	for _, worker := range workers {
		if worker.ID == id {
			return worker
		}
	}
	return common.WorkerState{}
}

type TopologyAwareCostAware struct {
	Weights CostAwareWeights
}

func (t TopologyAwareCostAware) Name() string { return "topology_aware_cost_aware" }

func NewTopologyAwareCostAware() TopologyAwareCostAware {
	return TopologyAwareCostAware{}
}

func (t TopologyAwareCostAware) Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error) {
	_ = ctx
	candidates := healthy(workers)
	if len(candidates) == 0 {
		return common.RouteDecision{}, ErrNoHealthyWorker
	}
	w := t.normalizedWeights()
	locations := idx.LocationsByWorker(req.BlockHashes)
	best := candidates[0]
	bestScore := math.Inf(-1)
	var bestCandidate common.CandidateScore
	top := make([]common.CandidateScore, 0, len(candidates))
	for _, worker := range candidates {
		score, candidate := t.scoreWorker(req, worker, locations[worker.ID], w)
		top = append(top, candidate)
		if score > bestScore {
			best = worker
			bestScore = score
			bestCandidate = candidate
		}
	}
	sort.SliceStable(top, func(i, j int) bool {
		return top[i].Score > top[j].Score
	})
	reason := topologyDecisionReason(top, bestCandidate)
	return common.RouteDecision{
		RequestIDHash:         req.RequestIDHash,
		Strategy:              t.Name(),
		WorkerID:              best.ID,
		ChosenTopology:        best.Topology,
		OverlappedBlocks:      bestCandidate.LocalOverlapBlocks + bestCandidate.SharedOverlapBlocks,
		LocalOverlapBlocks:    bestCandidate.LocalOverlapBlocks,
		SharedOverlapBlocks:   bestCandidate.SharedOverlapBlocks,
		EstimatedScore:        bestScore,
		TopologyPenalty:       bestCandidate.TopologyPenalty,
		EstimatedKVTransferMS: bestCandidate.EstimatedKVTransferMS,
		EgressCostClass:       bestCandidate.EgressCostClass,
		SelectedTransport:     bestCandidate.SelectedTransport,
		TopologyUnknown:       bestCandidate.TopologyUnknown,
		CandidateCount:        len(candidates),
		Reason:                reason,
		TopCandidates:         top,
	}, nil
}

func (t TopologyAwareCostAware) scoreWorker(req common.RequestContext, worker common.WorkerState, locations []common.BlockLocation, w CostAwareWeights) (float64, common.CandidateScore) {
	localOverlap := 0
	sharedOverlap := 0
	transferMS := 0.0
	selectedTransport := common.TransportUnknown
	egressClass := "none"
	for _, loc := range locations {
		if isLocalLocation(loc) {
			localOverlap++
			if selectedTransport == common.TransportUnknown {
				selectedTransport = loc.Transport
			}
			continue
		}
		sharedOverlap++
		transferMS += estimatedTransferMS(loc)
		selectedTransport = loc.Transport
		egressClass = worseEgressClass(egressClass, loc.EgressCostClass)
	}
	missingBlocks := len(req.BlockHashes) - localOverlap - sharedOverlap
	if missingBlocks < 0 {
		missingBlocks = 0
	}
	missingTokens := missingBlocks * requestBlockSize(req)
	topologyPenalty, topologyUnknown := topologyPenalty(req.EntryTopology, worker.Topology)
	egressPenalty := egressPenalty(egressClass)
	score := w.Hit*float64(localOverlap) +
		w.Shared*float64(sharedOverlap) -
		w.Prefill*float64(missingTokens) -
		w.Decode*float64(worker.ActiveDecodeBlocks) -
		w.Queue*float64(worker.QueueDepth) -
		w.Memory*worker.ResourcePressure() -
		w.Stale*(worker.CacheStateStalenessMS/1000.0) -
		w.SLO*predictedSLORisk(float64(missingTokens), worker) -
		w.Topology*topologyPenalty -
		w.Transfer*transferMS -
		w.Egress*egressPenalty
	return score, common.CandidateScore{
		WorkerID:              worker.ID,
		Score:                 score,
		PredictedOverlap:      localOverlap + sharedOverlap,
		LocalOverlapBlocks:    localOverlap,
		SharedOverlapBlocks:   sharedOverlap,
		QueueDepth:            worker.QueueDepth,
		ActiveDecodeBlocks:    worker.ActiveDecodeBlocks,
		MemoryPressure:        worker.ResourcePressure(),
		TopologyPenalty:       topologyPenalty,
		EstimatedKVTransferMS: transferMS,
		EgressCostClass:       egressClass,
		SelectedTransport:     selectedTransport,
		TopologyUnknown:       topologyUnknown,
	}
}

func (t TopologyAwareCostAware) normalizedWeights() CostAwareWeights {
	w := t.Weights
	if w.Hit == 0 {
		w.Hit = 4.0
	}
	if w.Shared == 0 {
		w.Shared = 3.2
	}
	if w.Prefill == 0 {
		w.Prefill = 0.02
	}
	if w.Decode == 0 {
		w.Decode = 0.5
	}
	if w.Queue == 0 {
		w.Queue = 1.0
	}
	if w.Memory == 0 {
		w.Memory = 3.0
	}
	if w.Stale == 0 {
		w.Stale = 0.5
	}
	if w.SLO == 0 {
		w.SLO = 2.0
	}
	if w.Topology == 0 {
		w.Topology = 1.0
	}
	if w.Transfer == 0 {
		w.Transfer = 0.12
	}
	if w.Egress == 0 {
		w.Egress = 1.0
	}
	return w
}

func isLocalLocation(loc common.BlockLocation) bool {
	switch loc.Locality {
	case common.LocalityLocal, common.LocalitySameNode:
		return true
	}
	switch loc.Transport {
	case common.TransportLocalMemory, common.TransportLocalNVMe:
		return true
	}
	switch loc.Tier {
	case common.TierGPU, common.TierCPU, common.TierNVMe:
		return loc.Locality == "" || loc.Locality == common.LocalityUnknown
	default:
		return false
	}
}

func estimatedTransferMS(loc common.BlockLocation) float64 {
	if loc.EstimatedTransferP95Ms > 0 {
		return loc.EstimatedTransferP95Ms
	}
	if loc.EstimatedLoadP95Ms > 0 {
		return loc.EstimatedLoadP95Ms
	}
	return 0
}

func topologyPenalty(req common.Topology, worker common.Topology) (float64, bool) {
	if req.Unknown() || worker.Unknown() {
		return 0, true
	}
	if req.Cloud != worker.Cloud {
		return 10.0, false
	}
	if req.Region != worker.Region {
		return 4.0, false
	}
	if req.Zone != worker.Zone {
		return 1.5, false
	}
	if req.ClusterID != worker.ClusterID {
		return 0.5, false
	}
	if req.NodeID == worker.NodeID {
		return 0, false
	}
	return 0.1, false
}

func egressPenalty(class string) float64 {
	switch class {
	case "", "none":
		return 0
	case "intra_zone":
		return 0.1
	case "cross_zone":
		return 1.5
	case "cross_region":
		return 4.0
	case "cross_cloud":
		return 12.0
	default:
		return 0.5
	}
}

func worseEgressClass(a, b string) string {
	if egressRank(b) > egressRank(a) {
		return b
	}
	return a
}

func egressRank(class string) int {
	switch class {
	case "", "none":
		return 0
	case "intra_zone":
		return 1
	case "cross_zone":
		return 2
	case "cross_region":
		return 3
	case "cross_cloud":
		return 4
	default:
		return 1
	}
}

func topologyDecisionReason(top []common.CandidateScore, best common.CandidateScore) string {
	if best.TopologyUnknown {
		return "topology_unknown_score"
	}
	if best.SharedOverlapBlocks > 0 && best.LocalOverlapBlocks == 0 && best.EgressCostClass == "cross_cloud" {
		return "cross_cloud_hit_penalized"
	}
	for _, candidate := range top {
		if candidate.WorkerID == best.WorkerID {
			continue
		}
		if candidate.LocalOverlapBlocks > best.LocalOverlapBlocks && candidate.QueueDepth > best.QueueDepth+8 && best.SharedOverlapBlocks > 0 {
			return "same_zone_remote_load_lower_than_hot_local_queue"
		}
		if candidate.EgressCostClass == "cross_cloud" && candidate.SharedOverlapBlocks > best.SharedOverlapBlocks && best.EstimatedKVTransferMS < candidate.EstimatedKVTransferMS {
			return "cross_cloud_hit_skipped_by_transfer_cost"
		}
	}
	return "topology_aware_highest_score"
}

func deterministicPair(key string, n int) (int, int) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	x := int(h.Sum64() % uint64(n))
	y := int((h.Sum64()/uint64(n) + 1) % uint64(n))
	if x == y {
		y = (y + 1) % n
	}
	return x, y
}
