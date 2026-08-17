package cacheindex

import (
	"math"
	"sync"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

type Index struct {
	mu              sync.RWMutex
	byBlock         map[common.BlockHash]map[common.WorkerID]common.BlockLocation
	ttl             time.Duration
	now             func() time.Time
	nextSeqNo       int64
	appliedEventIDs map[string]struct{}
	lastSeqByWorker map[common.WorkerID]int64
}

func New(ttl time.Duration) *Index {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Index{
		byBlock:         make(map[common.BlockHash]map[common.WorkerID]common.BlockLocation),
		ttl:             ttl,
		now:             time.Now,
		appliedEventIDs: make(map[string]struct{}),
		lastSeqByWorker: make(map[common.WorkerID]int64),
	}
}

func (idx *Index) Store(block common.BlockHash, worker common.WorkerID, tier common.Tier, tokens int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	now := idx.now()
	idx.storeLocationLocked(common.BlockLocation{
		BlockHash:              block,
		WorkerID:               worker,
		Tier:                   tier,
		Locality:               common.LocalityLocal,
		Transport:              common.TransportLocalMemory,
		EstimatedLoadP95Ms:     0,
		EstimatedTransferP95Ms: 0,
		EgressCostClass:        "none",
		Tokens:                 tokens,
		SeqNo:                  0,
		Confidence:             1.0,
		UpdatedAt:              now,
		ExpiresAt:              now.Add(idx.ttl),
	}, false, false)
}

func (idx *Index) StoreLocation(loc common.BlockLocation) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.storeLocationLocked(loc, true, loc.SeqNo > 0)
}

func (idx *Index) storeLocationLocked(loc common.BlockLocation, rejectStale bool, producerOrdered bool) bool {
	if idx.nextSeqNo < math.MaxInt64 {
		idx.nextSeqNo++
	}
	now := idx.now()
	if loc.SeqNo == 0 {
		loc.SeqNo = idx.nextSeqNo
	} else if loc.SeqNo > idx.nextSeqNo {
		idx.nextSeqNo = loc.SeqNo
	}
	if loc.UpdatedAt.IsZero() {
		loc.UpdatedAt = now
	}
	if loc.ExpiresAt.IsZero() {
		loc.ExpiresAt = now.Add(idx.ttl)
	}
	if loc.Confidence == 0 {
		loc.Confidence = 1.0
	}
	if loc.Locality == "" {
		loc.Locality = common.LocalityUnknown
	}
	if loc.Transport == "" {
		loc.Transport = common.TransportUnknown
	}
	if loc.EgressCostClass == "" {
		loc.EgressCostClass = "unknown"
	}
	if rejectStale && producerOrdered && !idx.seqFreshLocked(loc.WorkerID, loc.SeqNo) {
		return false
	}
	locations := idx.byBlock[loc.BlockHash]
	if locations == nil {
		locations = make(map[common.WorkerID]common.BlockLocation)
		idx.byBlock[loc.BlockHash] = locations
	}
	locations[loc.WorkerID] = loc
	if producerOrdered {
		idx.markWorkerSeqLocked(loc.WorkerID, loc.SeqNo)
	}
	return true
}

func (idx *Index) Evict(block common.BlockHash, worker common.WorkerID, seqNo int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.evictLocked(block, worker, seqNo, true)
}

// EvictWorker removes every observation and producer watermark owned by a
// retired Worker ID. A control plane must quiesce the old event producer
// before reusing that ID; the public prototype does not implement Worker
// generations.
func (idx *Index) EvictWorker(worker common.WorkerID) int {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	removed := 0
	for block, locations := range idx.byBlock {
		if _, ok := locations[worker]; !ok {
			continue
		}
		delete(locations, worker)
		removed++
		if len(locations) == 0 {
			delete(idx.byBlock, block)
		}
	}
	delete(idx.lastSeqByWorker, worker)
	return removed
}

func (idx *Index) evictLocked(block common.BlockHash, worker common.WorkerID, seqNo int64, rejectStale bool) bool {
	if rejectStale && !idx.seqFreshLocked(worker, seqNo) {
		return false
	}
	locations := idx.byBlock[block]
	if locations == nil {
		idx.markWorkerSeqLocked(worker, seqNo)
		return true
	}
	loc, ok := locations[worker]
	if !ok || seqNo < loc.SeqNo {
		idx.markWorkerSeqLocked(worker, seqNo)
		return false
	}
	delete(locations, worker)
	if len(locations) == 0 {
		delete(idx.byBlock, block)
	}
	idx.markWorkerSeqLocked(worker, seqNo)
	return true
}

func (idx *Index) markWorkerSeqLocked(worker common.WorkerID, seqNo int64) {
	if worker == "" || seqNo <= 0 {
		return
	}
	if idx.lastSeqByWorker == nil {
		idx.lastSeqByWorker = make(map[common.WorkerID]int64)
	}
	if seqNo > idx.lastSeqByWorker[worker] {
		idx.lastSeqByWorker[worker] = seqNo
	}
}

func (idx *Index) seqFreshLocked(worker common.WorkerID, seqNo int64) bool {
	if worker == "" || seqNo <= 0 {
		return true
	}
	if idx.lastSeqByWorker == nil {
		idx.lastSeqByWorker = make(map[common.WorkerID]int64)
	}
	return seqNo >= idx.lastSeqByWorker[worker]
}

func (idx *Index) OverlapByWorker(blocks []common.BlockHash) map[common.WorkerID]int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	now := idx.now()
	out := make(map[common.WorkerID]int)
	for _, block := range blocks {
		for worker, loc := range idx.byBlock[block] {
			if loc.ExpiresAt.After(now) {
				out[worker]++
			}
		}
	}
	return out
}

func (idx *Index) LocationsByWorker(blocks []common.BlockHash) map[common.WorkerID][]common.BlockLocation {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	now := idx.now()
	out := make(map[common.WorkerID][]common.BlockLocation)
	for _, block := range blocks {
		for worker, loc := range idx.byBlock[block] {
			if loc.ExpiresAt.After(now) {
				out[worker] = append(out[worker], loc)
			}
		}
	}
	return out
}

func (idx *Index) Stats() (blocks int, locations int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, locs := range idx.byBlock {
		blocks++
		locations += len(locs)
	}
	return blocks, locations
}
