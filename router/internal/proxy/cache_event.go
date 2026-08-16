package proxy

import (
	"errors"
	"fmt"

	"ai-inference-storage-showcase/router/internal/cacheindex"
	"ai-inference-storage-showcase/router/internal/common"
)

type cacheEvent struct {
	EventID   string                `json:"event_id"`
	EventType string                `json:"event_type"`
	WorkerID  string                `json:"worker_id"`
	BlockHash string                `json:"block_hash"`
	Tier      string                `json:"tier"`
	Tokens    int                   `json:"tokens"`
	SeqNo     int64                 `json:"seq_no"`
	Location  *common.BlockLocation `json:"location,omitempty"`
}

func (h *Handler) cacheIndexEvent(event cacheEvent) (cacheindex.Event, error) {
	if event.SeqNo < 0 {
		return cacheindex.Event{}, errors.New("seq_no must be non-negative")
	}
	indexEvent := cacheindex.Event{
		EventID:   event.EventID,
		EventType: event.EventType,
		BlockHash: common.BlockHash(event.BlockHash),
		WorkerID:  common.WorkerID(event.WorkerID),
		Tier:      common.Tier(event.Tier),
		Tokens:    event.Tokens,
		SeqNo:     event.SeqNo,
	}
	switch event.EventType {
	case "block_evicted":
		if event.Location != nil {
			return cacheindex.Event{}, errors.New("location is not valid for block_evicted")
		}
		if indexEvent.BlockHash == "" || indexEvent.WorkerID == "" {
			return cacheindex.Event{}, errors.New("block_hash and worker_id are required")
		}
		return indexEvent, nil
	case "block_stored":
		if event.Tokens < 0 {
			return cacheindex.Event{}, errors.New("tokens must be non-negative")
		}
		if event.Location == nil {
			if indexEvent.BlockHash == "" || indexEvent.WorkerID == "" {
				return cacheindex.Event{}, errors.New("block_hash and worker_id are required")
			}
			if indexEvent.Tier == "" {
				indexEvent.Tier = common.TierGPU
			}
			if !validTier(indexEvent.Tier) {
				return cacheindex.Event{}, fmt.Errorf("invalid tier %q", indexEvent.Tier)
			}
			if indexEvent.Tokens == 0 {
				indexEvent.Tokens = h.hasher.BlockSizeTokens
			}
			return indexEvent, nil
		}
		location := *event.Location
		if err := mergeCacheEventLocation(&indexEvent, &location, h.hasher.BlockSizeTokens); err != nil {
			return cacheindex.Event{}, err
		}
		if err := validateBlockLocation(location); err != nil {
			return cacheindex.Event{}, err
		}
		indexEvent.Location = &location
		return indexEvent, nil
	default:
		return cacheindex.Event{}, errors.New("unknown event_type")
	}
}

func mergeCacheEventLocation(event *cacheindex.Event, location *common.BlockLocation, blockSizeTokens int) error {
	if location.BlockHash != "" && event.BlockHash != "" && location.BlockHash != event.BlockHash {
		return errors.New("location.block_hash must match block_hash")
	}
	if location.WorkerID != "" && event.WorkerID != "" && location.WorkerID != event.WorkerID {
		return errors.New("location.worker_id must match worker_id")
	}
	if location.Tier != "" && event.Tier != "" && location.Tier != event.Tier {
		return errors.New("location.tier must match tier")
	}
	if location.Tokens > 0 && event.Tokens > 0 && location.Tokens != event.Tokens {
		return errors.New("location.tokens must match tokens")
	}
	if location.SeqNo > 0 && event.SeqNo > 0 && location.SeqNo != event.SeqNo {
		return errors.New("location.seq_no must match seq_no")
	}
	if location.BlockHash == "" {
		location.BlockHash = event.BlockHash
	}
	if location.WorkerID == "" {
		location.WorkerID = event.WorkerID
	}
	if location.Tier == "" {
		location.Tier = event.Tier
	}
	if location.Tier == "" {
		location.Tier = common.TierGPU
	}
	if location.Tokens == 0 {
		location.Tokens = event.Tokens
	}
	if location.Tokens == 0 {
		location.Tokens = blockSizeTokens
	}
	if location.SeqNo == 0 {
		location.SeqNo = event.SeqNo
	}
	event.BlockHash = location.BlockHash
	event.WorkerID = location.WorkerID
	event.Tier = location.Tier
	event.Tokens = location.Tokens
	event.SeqNo = location.SeqNo
	return nil
}

func validateBlockLocation(location common.BlockLocation) error {
	if location.BlockHash == "" || location.WorkerID == "" {
		return errors.New("location.block_hash and location.worker_id are required")
	}
	if !validTier(location.Tier) {
		return fmt.Errorf("invalid location.tier %q", location.Tier)
	}
	if !validLocality(location.Locality) {
		return fmt.Errorf("invalid location.locality %q", location.Locality)
	}
	if !validTransport(location.Transport) {
		return fmt.Errorf("invalid location.transport %q", location.Transport)
	}
	if location.Tokens <= 0 {
		return errors.New("location.tokens must be positive")
	}
	if location.Bytes < 0 {
		return errors.New("location.bytes must be non-negative")
	}
	if location.SeqNo < 0 {
		return errors.New("location.seq_no must be non-negative")
	}
	if location.EstimatedLoadP95Ms < 0 || location.EstimatedTransferP95Ms < 0 {
		return errors.New("location latency estimates must be non-negative")
	}
	if location.Confidence < 0 || location.Confidence > 1 {
		return errors.New("location.confidence must be between 0 and 1")
	}
	if !validEgressCostClass(location.EgressCostClass) {
		return fmt.Errorf("invalid location.egress_cost_class %q", location.EgressCostClass)
	}
	if location.UpdatedAt.IsZero() != location.ExpiresAt.IsZero() {
		return errors.New("location.updated_at and location.expires_at must be provided together")
	}
	if !location.UpdatedAt.IsZero() && !location.ExpiresAt.IsZero() && !location.ExpiresAt.After(location.UpdatedAt) {
		return errors.New("location.expires_at must be after location.updated_at")
	}
	return nil
}

func validTier(tier common.Tier) bool {
	switch tier {
	case common.TierGPU, common.TierCPU, common.TierNVMe, common.TierS3, common.TierUnknown:
		return true
	default:
		return false
	}
}

func validLocality(locality common.LocalityKind) bool {
	switch locality {
	case "", common.LocalityLocal, common.LocalitySameNode, common.LocalitySameZone, common.LocalityCrossZone, common.LocalityCrossRegion, common.LocalityCrossCloud, common.LocalityUnknown:
		return true
	default:
		return false
	}
}

func validTransport(transport common.TransportKind) bool {
	switch transport {
	case "", common.TransportLocalMemory, common.TransportLocalNVMe, common.TransportS3HTTPDefault, common.TransportFilePOSIX, common.TransportUnknown:
		return true
	default:
		return false
	}
}

func validEgressCostClass(class string) bool {
	switch class {
	case "", "none", "intra_zone", "cross_zone", "cross_region", "cross_cloud", "unknown":
		return true
	default:
		return false
	}
}
