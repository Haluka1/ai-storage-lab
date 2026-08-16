package routing

import (
	"context"
	"testing"
	"time"

	"ai-inference-storage-showcase/router/internal/cacheindex"
	"ai-inference-storage-showcase/router/internal/common"
)

func TestPrefixHashChoosesHighestOverlap(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	idx.Store("a", "worker-1", common.TierGPU, 16)
	idx.Store("b", "worker-1", common.TierGPU, 16)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	workers := testWorkers(2)
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := (PrefixHash{}).Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-1" {
		t.Fatalf("worker=%s want worker-1", decision.WorkerID)
	}
}

func TestBoundedLoadFallsBack(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-0", common.TierGPU, 16)
	workers := testWorkers(2)
	workers[0].QueueDepth = 100
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := (PrefixHashBoundedLoad{MaxQueueRatio: 1.1, MaxQueueSlack: 0}).Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Fallback {
		t.Fatalf("expected bounded-load fallback")
	}
}

func TestCostAwareTradesOverlapForQueue(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-0", common.TierGPU, 16)
	workers := testWorkers(2)
	workers[0].QueueDepth = 50
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := NewCostAwareVariant("cost_aware").Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-1" {
		t.Fatalf("worker=%s want worker-1 under high queue", decision.WorkerID)
	}
}

func TestCostAwareOverlapOnlyKeepsHotWorker(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-0", common.TierGPU, 16)
	workers := testWorkers(2)
	workers[0].QueueDepth = 50
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := NewCostAwareVariant("costaware_0_overlap_only").Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-0" {
		t.Fatalf("worker=%s want overlap-only worker-0", decision.WorkerID)
	}
}

func TestDrainingAndNotReadyWorkersAreExcluded(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-0", common.TierGPU, 16)
	workers := testWorkers(3)
	workers[0].Draining = true
	workers[0].Health = common.WorkerDraining
	workers[0].ReadinessState = common.ReadinessDraining
	workers[1].ReadinessState = common.ReadinessNotReady
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := (PrefixHash{}).Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-2" {
		t.Fatalf("worker=%s want only routable worker-2", decision.WorkerID)
	}
}

func TestCostAwareUsesGPUAndKVCachePressure(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	workers := testWorkers(2)
	workers[0].KVCachePressure = 0.95
	workers[1].KVCachePressure = 0.05
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := NewCostAwareVariant("cost_aware").Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-1" {
		t.Fatalf("worker=%s want lower KV pressure worker-1", decision.WorkerID)
	}
	if decision.TopCandidates[0].MemoryPressure < 0.05 {
		t.Fatalf("candidate memory pressure did not include new pressure fields: %#v", decision.TopCandidates[0])
	}
}

func TestTenantAwareBoundedLoadSpreadsTenantAssignments(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	workers := testWorkers(3)
	strategy := &TenantAwareBoundedLoad{}
	seen := map[common.WorkerID]bool{}
	for i := 0; i < 6; i++ {
		req := common.RequestContext{
			RequestIDHash: "tenant-spread",
			TenantHash:    "tenant-hash-a",
			BlockHashes:   []common.BlockHash{"a", "b"},
		}
		decision, err := strategy.Pick(context.Background(), req, workers, idx)
		if err != nil {
			t.Fatal(err)
		}
		seen[decision.WorkerID] = true
	}
	if len(seen) < 3 {
		t.Fatalf("tenant-aware strategy did not spread assignments across workers: %#v", seen)
	}
}

func TestTopologyAwareChoosesSameZoneSharedWhenLocalWorkerOverloaded(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	workers := topologyTestWorkers()
	workers[0].QueueDepth = 48
	req := topologyTestRequest()
	for _, block := range req.BlockHashes {
		idx.StoreLocation(common.BlockLocation{
			BlockHash:              block,
			WorkerID:               "worker-a0",
			Tier:                   common.TierGPU,
			Topology:               workers[0].Topology,
			Locality:               common.LocalityLocal,
			Transport:              common.TransportLocalMemory,
			EstimatedTransferP95Ms: 0,
			EgressCostClass:        "none",
			Tokens:                 16,
		})
	}
	for _, block := range req.BlockHashes[:3] {
		idx.StoreLocation(common.BlockLocation{
			BlockHash:              block,
			WorkerID:               "worker-a1",
			Tier:                   common.TierS3,
			Topology:               workers[1].Topology,
			Locality:               common.LocalitySameZone,
			Transport:              common.TransportS3HTTPDefault,
			EstimatedTransferP95Ms: 5,
			EgressCostClass:        "intra_zone",
			Tokens:                 16,
		})
	}
	decision, err := NewTopologyAwareCostAware().Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if decision.WorkerID != "worker-a1" {
		t.Fatalf("worker=%s want same-zone shared worker-a1", decision.WorkerID)
	}
	if decision.LocalOverlapBlocks != 0 || decision.SharedOverlapBlocks != 3 {
		t.Fatalf("unexpected overlap local=%d shared=%d", decision.LocalOverlapBlocks, decision.SharedOverlapBlocks)
	}
	if decision.Reason != "same_zone_remote_load_lower_than_hot_local_queue" {
		t.Fatalf("reason=%s", decision.Reason)
	}
}

func TestTopologyAwareMissingTopologyDoesNotCrash(t *testing.T) {
	idx := cacheindex.New(time.Minute)
	workers := testWorkers(2)
	req := common.RequestContext{RequestIDHash: "r1", BlockHashes: []common.BlockHash{"a", "b"}}
	decision, err := NewTopologyAwareCostAware().Pick(context.Background(), req, workers, idx)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.TopologyUnknown {
		t.Fatalf("expected topology_unknown")
	}
}

func testWorkers(n int) []common.WorkerState {
	out := make([]common.WorkerState, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, common.WorkerState{
			ID:     common.WorkerID("worker-" + string(rune('0'+i))),
			Health: common.WorkerReady,
			Weight: 1.0,
		})
	}
	return out
}

func topologyTestRequest() common.RequestContext {
	return common.RequestContext{
		RequestIDHash: "topology-test",
		BlockHashes:   []common.BlockHash{"a", "b", "c", "d"},
		EntryTopology: common.Topology{
			Cloud:     "local",
			Region:    "local",
			Zone:      "zone-a",
			ClusterID: "cluster-a",
			NodeID:    "router",
		},
	}
}

func topologyTestWorkers() []common.WorkerState {
	return []common.WorkerState{
		{
			ID:         "worker-a0",
			Health:     common.WorkerReady,
			QueueDepth: 1,
			Topology:   common.Topology{Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", NodeID: "node-a0"},
		},
		{
			ID:         "worker-a1",
			Health:     common.WorkerReady,
			QueueDepth: 1,
			Topology:   common.Topology{Cloud: "local", Region: "local", Zone: "zone-a", ClusterID: "cluster-a", NodeID: "node-a1"},
		},
		{
			ID:         "worker-b0",
			Health:     common.WorkerReady,
			QueueDepth: 1,
			Topology:   common.Topology{Cloud: "local", Region: "local", Zone: "zone-b", ClusterID: "cluster-a", NodeID: "node-b0"},
		},
	}
}

func TestRequestBlockSizeUsesConfiguredValue(t *testing.T) {
	if got := requestBlockSize(common.RequestContext{BlockSizeTokens: 64}); got != 64 {
		t.Fatalf("block size=%d want=64", got)
	}
	if got := requestBlockSize(common.RequestContext{}); got != 16 {
		t.Fatalf("default block size=%d want=16", got)
	}
}

func TestTenantAwareStateExpiresAndIsBounded(t *testing.T) {
	now := time.Unix(100, 0)
	strategy := &TenantAwareBoundedLoad{
		MaxTenantStates: 2,
		TenantStateTTL:  time.Second,
		now:             func() time.Time { return now },
	}
	workers := testWorkers(2)
	idx := cacheindex.New(time.Minute)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		_, err := strategy.Pick(context.Background(), common.RequestContext{TenantHash: tenant}, workers, idx)
		if err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Second)
	if _, err := strategy.Pick(context.Background(), common.RequestContext{TenantHash: "tenant-c"}, workers, idx); err != nil {
		t.Fatal(err)
	}
	if len(strategy.assignments) != 1 {
		t.Fatalf("tenant states=%d want=1 after TTL prune", len(strategy.assignments))
	}
	if _, exists := strategy.assignments["tenant-c"]; !exists {
		t.Fatal("current tenant state missing")
	}
}
