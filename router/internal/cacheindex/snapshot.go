package cacheindex

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"ai-inference-storage-showcase/router/internal/common"
)

type Snapshot struct {
	SchemaVersion int                    `json:"schema_version"`
	CreatedAt     time.Time              `json:"created_at"`
	TTLMS         int64                  `json:"ttl_ms"`
	NextSeqNo     int64                  `json:"next_seq_no"`
	Locations     []common.BlockLocation `json:"locations"`
}

func (idx *Index) Snapshot() Snapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	now := idx.now()
	locations := make([]common.BlockLocation, 0)
	for _, byWorker := range idx.byBlock {
		for _, loc := range byWorker {
			if loc.ExpiresAt.IsZero() || loc.ExpiresAt.After(now) {
				locations = append(locations, loc)
			}
		}
	}
	sort.Slice(locations, func(i, j int) bool {
		if locations[i].BlockHash == locations[j].BlockHash {
			return locations[i].WorkerID < locations[j].WorkerID
		}
		return locations[i].BlockHash < locations[j].BlockHash
	})
	return Snapshot{
		SchemaVersion: 1,
		CreatedAt:     now.UTC(),
		TTLMS:         idx.ttl.Milliseconds(),
		NextSeqNo:     idx.nextSeqNo,
		Locations:     locations,
	}
}

func (idx *Index) DumpSnapshot(path string) error {
	raw, err := json.MarshalIndent(idx.Snapshot(), "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func LoadSnapshot(path string, fallbackTTL time.Duration) (*Index, Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, Snapshot{}, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, Snapshot{}, err
	}
	ttl := fallbackTTL
	if snapshot.TTLMS > 0 {
		ttl = time.Duration(snapshot.TTLMS) * time.Millisecond
	}
	idx := New(ttl)
	idx.RestoreSnapshot(snapshot)
	return idx, snapshot, nil
}

func (idx *Index) RestoreSnapshot(snapshot Snapshot) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.byBlock = make(map[common.BlockHash]map[common.WorkerID]common.BlockLocation)
	idx.appliedEventIDs = make(map[string]struct{})
	idx.lastSeqByWorker = make(map[common.WorkerID]int64)
	idx.nextSeqNo = snapshot.NextSeqNo
	for _, loc := range snapshot.Locations {
		normalizeLocation(&loc, idx.now(), idx.ttl)
		if loc.SeqNo > idx.nextSeqNo {
			idx.nextSeqNo = loc.SeqNo
		}
		idx.markWorkerSeqLocked(loc.WorkerID, loc.SeqNo)
		locations := idx.byBlock[loc.BlockHash]
		if locations == nil {
			locations = make(map[common.WorkerID]common.BlockLocation)
			idx.byBlock[loc.BlockHash] = locations
		}
		locations[loc.WorkerID] = loc
	}
}

func normalizeLocation(loc *common.BlockLocation, now time.Time, ttl time.Duration) {
	if loc.UpdatedAt.IsZero() {
		loc.UpdatedAt = now
	}
	if loc.ExpiresAt.IsZero() {
		loc.ExpiresAt = now.Add(ttl)
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
}
