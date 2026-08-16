package cacheindex

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"ai-inference-storage-showcase/router/internal/common"
)

type Event struct {
	SchemaVersion int                   `json:"schema_version"`
	EventID       string                `json:"event_id,omitempty"`
	EventType     string                `json:"event_type"`
	BlockHash     common.BlockHash      `json:"block_hash"`
	WorkerID      common.WorkerID       `json:"worker_id"`
	Tier          common.Tier           `json:"tier"`
	Tokens        int                   `json:"tokens"`
	SeqNo         int64                 `json:"seq_no"`
	Location      *common.BlockLocation `json:"location,omitempty"`
	TimestampMS   int64                 `json:"timestamp_ms"`
}

func AppendEvent(path string, event Event) error {
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 1
	}
	if event.TimestampMS == 0 {
		event.TimestampMS = time.Now().UnixMilli()
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	_, err = fh.Write(append(raw, '\n'))
	return err
}

func ReplayEventLog(path string, limit int, idx *Index) (int, error) {
	events, err := ReadEvents(path)
	if err != nil {
		return 0, err
	}
	if limit > 0 && limit < len(events) {
		events = events[len(events)-limit:]
	}
	for _, event := range events {
		if err := idx.ApplyEvent(event); err != nil {
			return 0, err
		}
	}
	return len(events), nil
}

func ReadEvents(path string) ([]Event, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	events := make([]Event, 0)
	for lineno := 1; scanner.Scan(); lineno++ {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineno, err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (idx *Index) ApplyEvent(event Event) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if !idx.acceptEventLocked(event) {
		return nil
	}
	switch event.EventType {
	case "block_stored":
		if event.Location != nil {
			loc := *event.Location
			if loc.SeqNo == 0 {
				loc.SeqNo = event.SeqNo
			}
			if loc.WorkerID == "" {
				loc.WorkerID = event.WorkerID
			}
			if loc.BlockHash == "" {
				loc.BlockHash = event.BlockHash
			}
			idx.storeLocationLocked(loc, true)
			return nil
		}
		loc := common.BlockLocation{
			BlockHash:          event.BlockHash,
			WorkerID:           event.WorkerID,
			Tier:               event.Tier,
			Tokens:             event.Tokens,
			SeqNo:              event.SeqNo,
			Locality:           common.LocalityLocal,
			Transport:          common.TransportLocalMemory,
			EgressCostClass:    "none",
			Confidence:         1.0,
			UpdatedAt:          time.Now().UTC(),
			ExpiresAt:          time.Now().UTC().Add(idx.ttl),
			EstimatedLoadP95Ms: 0,
		}
		if loc.Tier == "" {
			loc.Tier = common.TierGPU
		}
		if loc.Tokens <= 0 {
			loc.Tokens = 16
		}
		idx.storeLocationLocked(loc, true)
		return nil
	case "block_evicted":
		idx.evictLocked(event.BlockHash, event.WorkerID, event.SeqNo, true)
		return nil
	default:
		return fmt.Errorf("unknown cache event type %q", event.EventType)
	}
}

func (idx *Index) acceptEventLocked(event Event) bool {
	if event.EventID != "" {
		if idx.appliedEventIDs == nil {
			idx.appliedEventIDs = make(map[string]struct{})
		}
		if _, ok := idx.appliedEventIDs[event.EventID]; ok {
			return false
		}
		idx.appliedEventIDs[event.EventID] = struct{}{}
	}
	if event.WorkerID != "" && event.SeqNo > 0 {
		if idx.lastSeqByWorker == nil {
			idx.lastSeqByWorker = make(map[common.WorkerID]int64)
		}
		if event.SeqNo <= idx.lastSeqByWorker[event.WorkerID] {
			return false
		}
	}
	return idx.seqFreshLocked(event.WorkerID, event.SeqNo)
}
