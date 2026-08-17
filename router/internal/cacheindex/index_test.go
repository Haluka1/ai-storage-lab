package cacheindex

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

func TestOverlapByWorker(t *testing.T) {
	idx := New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-0", common.TierGPU, 16)
	idx.Store("b", "worker-1", common.TierGPU, 16)

	got := idx.OverlapByWorker([]common.BlockHash{"a", "b", "c"})
	if got["worker-0"] != 2 {
		t.Fatalf("worker-0 overlap=%d want 2", got["worker-0"])
	}
	if got["worker-1"] != 1 {
		t.Fatalf("worker-1 overlap=%d want 1", got["worker-1"])
	}
}

func TestEvictIgnoresStaleSeqNo(t *testing.T) {
	idx := New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.Evict("a", "worker-0", 0)
	if got := idx.OverlapByWorker([]common.BlockHash{"a"}); got["worker-0"] != 1 {
		t.Fatalf("stale evict removed block")
	}
	idx.Evict("a", "worker-0", 1)
	if got := idx.OverlapByWorker([]common.BlockHash{"a"}); got["worker-0"] != 0 {
		t.Fatalf("fresh evict did not remove block")
	}
}

func TestEvictWorkerPurgesObservationsAndProducerWatermark(t *testing.T) {
	idx := New(time.Minute)
	idx.StoreLocation(common.BlockLocation{
		BlockHash: "a", WorkerID: "worker-0", Tier: common.TierGPU, SeqNo: 10,
	})
	idx.StoreLocation(common.BlockLocation{
		BlockHash: "b", WorkerID: "worker-0", Tier: common.TierGPU, SeqNo: 11,
	})
	idx.StoreLocation(common.BlockLocation{
		BlockHash: "b", WorkerID: "worker-1", Tier: common.TierGPU, SeqNo: 20,
	})

	if removed := idx.EvictWorker("worker-0"); removed != 2 {
		t.Fatalf("removed=%d, want 2", removed)
	}
	got := idx.OverlapByWorker([]common.BlockHash{"a", "b"})
	if got["worker-0"] != 0 || got["worker-1"] != 1 {
		t.Fatalf("worker purge produced overlap=%v", got)
	}

	// A deliberately reused ID starts a fresh producer sequence. The caller
	// must already have quiesced the retired producer because there is no
	// generation field in this prototype.
	idx.StoreLocation(common.BlockLocation{
		BlockHash: "c", WorkerID: "worker-0", Tier: common.TierGPU, SeqNo: 1,
	})
	if got := idx.OverlapByWorker([]common.BlockHash{"c"}); got["worker-0"] != 1 {
		t.Fatalf("fresh sequence was rejected after Worker retirement: %v", got)
	}
}

func TestSnapshotRoundTripPreservesOverlap(t *testing.T) {
	idx := New(time.Minute)
	idx.Store("a", "worker-0", common.TierGPU, 16)
	idx.StoreLocation(common.BlockLocation{
		BlockHash:          "b",
		WorkerID:           "worker-1",
		Tier:               common.TierGPU,
		Tokens:             16,
		SeqNo:              99,
		Locality:           common.LocalitySameZone,
		Transport:          common.TransportS3HTTPDefault,
		EgressCostClass:    "intra_zone",
		EstimatedLoadP95Ms: 3.5,
		Confidence:         0.9,
		UpdatedAt:          time.Now().UTC(),
		ExpiresAt:          time.Now().UTC().Add(time.Minute),
	})
	path := filepath.Join(t.TempDir(), "cacheindex.snapshot.json")
	if err := idx.DumpSnapshot(path); err != nil {
		t.Fatal(err)
	}
	restored, snapshot, err := LoadSnapshot(path, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SchemaVersion != 2 {
		t.Fatalf("snapshot schema version=%d", snapshot.SchemaVersion)
	}
	got := restored.OverlapByWorker([]common.BlockHash{"a", "b"})
	if got["worker-0"] != 1 || got["worker-1"] != 1 {
		t.Fatalf("restored overlap=%v", got)
	}
	locations := restored.LocationsByWorker([]common.BlockHash{"b"})
	if locations["worker-1"][0].Transport != common.TransportS3HTTPDefault {
		t.Fatalf("transport was not preserved: %#v", locations["worker-1"][0])
	}
}

func TestZeroSequenceDoesNotAdvanceProducerWatermark(t *testing.T) {
	idx := New(time.Minute)
	if err := idx.ApplyEvent(Event{EventType: "block_stored", BlockHash: "local", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 0}); err != nil {
		t.Fatal(err)
	}
	if err := idx.ApplyEvent(Event{EventType: "block_stored", BlockHash: "ordered", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 32, SeqNo: 1}); err != nil {
		t.Fatal(err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"ordered"}); got["worker-0"] != 1 {
		t.Fatalf("producer sequence 1 was rejected after unordered local store: %v", got)
	}
	if got := idx.Snapshot().LastSeqByWorker["worker-0"]; got != 1 {
		t.Fatalf("producer watermark=%d, want 1", got)
	}
}

func TestSnapshotKeepsLocalRevisionOutOfProducerWatermark(t *testing.T) {
	idx := New(time.Minute)
	if err := idx.ApplyEvent(Event{EventType: "block_stored", BlockHash: "local", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16}); err != nil {
		t.Fatal(err)
	}
	restored := New(time.Minute)
	restored.RestoreSnapshot(idx.Snapshot())
	if err := restored.ApplyEvent(Event{EventType: "block_stored", BlockHash: "ordered", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 1}); err != nil {
		t.Fatal(err)
	}
	if got := restored.OverlapByWorker([]common.BlockHash{"ordered"}); got["worker-0"] != 1 {
		t.Fatalf("restored local revision polluted producer watermark: %v", got)
	}
}

func TestEventLogReplayRebuildsRecentState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := AppendEvent(path, Event{EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 10}); err != nil {
		t.Fatal(err)
	}
	if err := AppendEvent(path, Event{EventType: "block_stored", BlockHash: "b", WorkerID: "worker-1", Tier: common.TierGPU, Tokens: 16, SeqNo: 20}); err != nil {
		t.Fatal(err)
	}
	idx := New(time.Minute)
	replayed, err := ReplayEventLog(path, 0, idx)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != 2 {
		t.Fatalf("replayed=%d", replayed)
	}
	got := idx.OverlapByWorker([]common.BlockHash{"a", "b"})
	if got["worker-0"] != 1 || got["worker-1"] != 1 {
		t.Fatalf("replayed overlap=%v", got)
	}
	limited := New(time.Minute)
	replayed, err = ReplayEventLog(path, 1, limited)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != 1 {
		t.Fatalf("limited replayed=%d", replayed)
	}
	limitedOverlap := limited.OverlapByWorker([]common.BlockHash{"a", "b"})
	if limitedOverlap["worker-0"] != 0 || limitedOverlap["worker-1"] != 1 {
		t.Fatalf("limited replay overlap=%v", limitedOverlap)
	}
}

func TestApplyEventDeduplicatesEventIDAndRejectsStaleWorkerSeq(t *testing.T) {
	idx := New(time.Minute)
	if err := idx.ApplyEvent(Event{EventID: "evt-store-a", EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 10}); err != nil {
		t.Fatal(err)
	}
	if err := idx.ApplyEvent(Event{EventID: "evt-store-a", EventType: "block_evicted", BlockHash: "a", WorkerID: "worker-0", SeqNo: 11}); err != nil {
		t.Fatal(err)
	}
	got := idx.OverlapByWorker([]common.BlockHash{"a"})
	if got["worker-0"] != 1 {
		t.Fatalf("duplicate event_id changed index: %v", got)
	}
	if err := idx.ApplyEvent(Event{EventID: "evt-store-new", EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 32, SeqNo: 20}); err != nil {
		t.Fatal(err)
	}
	if err := idx.ApplyEvent(Event{EventID: "evt-store-stale", EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", Tier: common.TierCPU, Tokens: 4, SeqNo: 15}); err != nil {
		t.Fatal(err)
	}
	locations := idx.LocationsByWorker([]common.BlockHash{"a"})["worker-0"]
	if len(locations) != 1 || locations[0].Tokens != 32 || locations[0].Tier != common.TierGPU {
		t.Fatalf("stale store overwrote newer location: %+v", locations)
	}
	if err := idx.ApplyEvent(Event{EventID: "evt-evict-stale", EventType: "block_evicted", BlockHash: "a", WorkerID: "worker-0", SeqNo: 19}); err != nil {
		t.Fatal(err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"a"}); got["worker-0"] != 1 {
		t.Fatalf("stale evict removed block: %v", got)
	}
	if err := idx.ApplyEvent(Event{EventID: "evt-evict-fresh", EventType: "block_evicted", BlockHash: "a", WorkerID: "worker-0", SeqNo: 21}); err != nil {
		t.Fatal(err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"a"}); got["worker-0"] != 0 {
		t.Fatalf("fresh evict did not remove block: %v", got)
	}
}

func TestInvalidEventDoesNotConsumeEventIDOrProducerSequence(t *testing.T) {
	idx := New(time.Minute)
	invalid := Event{
		EventID:   "evt-corrected-retry",
		EventType: "unknown",
		BlockHash: "a",
		WorkerID:  "worker-0",
		Tier:      common.TierGPU,
		Tokens:    16,
		SeqNo:     1,
	}
	if err := idx.ApplyEvent(invalid); err == nil {
		t.Fatal("expected unknown event type to fail")
	}
	invalid.EventType = "block_stored"
	if err := idx.ApplyEvent(invalid); err != nil {
		t.Fatalf("corrected retry failed: %v", err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"a"}); got["worker-0"] != 1 {
		t.Fatalf("invalid event consumed EventID or sequence: %v", got)
	}

	negative := Event{
		EventID:   "evt-negative-retry",
		EventType: "block_stored",
		BlockHash: "b",
		WorkerID:  "worker-0",
		Tier:      common.TierGPU,
		Tokens:    16,
		SeqNo:     -1,
	}
	if err := idx.ApplyEvent(negative); err == nil {
		t.Fatal("expected negative producer sequence to fail")
	}
	negative.SeqNo = 2
	if err := idx.ApplyEvent(negative); err != nil {
		t.Fatalf("corrected sequence retry failed: %v", err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"b"}); got["worker-0"] != 1 {
		t.Fatalf("invalid sequence consumed EventID or watermark: %v", got)
	}

	missingIdentity := Event{
		EventID:   "evt-missing-identity",
		EventType: "block_evicted",
		WorkerID:  "worker-0",
		SeqNo:     3,
	}
	if err := idx.ApplyEvent(missingIdentity); err == nil {
		t.Fatal("expected missing block identity to fail")
	}
	if got := idx.Snapshot().LastSeqByWorker["worker-0"]; got != 2 {
		t.Fatalf("invalid event advanced producer watermark to %d", got)
	}
}

func TestValidStaleEventIDRemainsConsumed(t *testing.T) {
	idx := New(time.Minute)
	if err := idx.ApplyEvent(Event{EventID: "evt-new", EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 10}); err != nil {
		t.Fatal(err)
	}
	stale := Event{EventID: "evt-stale", EventType: "block_stored", BlockHash: "b", WorkerID: "worker-0", Tier: common.TierGPU, Tokens: 16, SeqNo: 9}
	if err := idx.ApplyEvent(stale); err != nil {
		t.Fatal(err)
	}
	stale.SeqNo = 11
	if err := idx.ApplyEvent(stale); err != nil {
		t.Fatal(err)
	}
	if got := idx.OverlapByWorker([]common.BlockHash{"b"}); got["worker-0"] != 0 {
		t.Fatalf("immutable EventID was incorrectly reusable after a valid stale event: %v", got)
	}
}

func TestMaximumProducerSequenceCannotOverflowLocalRevision(t *testing.T) {
	idx := New(time.Minute)
	err := idx.ApplyEvent(Event{
		EventID:   "evt-max-sequence",
		EventType: "block_stored",
		BlockHash: "a",
		WorkerID:  "worker-0",
		Tier:      common.TierGPU,
		Tokens:    16,
		SeqNo:     math.MaxInt64,
	})
	if err == nil {
		t.Fatal("expected maximum producer sequence to be rejected")
	}
	idx.Store("local", "worker-0", common.TierGPU, 16)
	snapshot := idx.Snapshot()
	if snapshot.NextSeqNo <= 0 {
		t.Fatalf("local revision overflowed after rejected producer sequence: %d", snapshot.NextSeqNo)
	}
}

func TestInMemoryBusPublishesToSubscribersAndHonorsAck(t *testing.T) {
	bus := NewInMemoryBus(1)
	defer bus.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := bus.Subscribe(ctx, "router")
	if err != nil {
		t.Fatal(err)
	}
	event := Event{EventID: "evt-1", EventType: "block_stored", BlockHash: "a", WorkerID: "worker-0", SeqNo: 1}
	if err := bus.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-ch:
		if msg.Event.EventID != event.EventID {
			t.Fatalf("event_id=%q, want %q", msg.Event.EventID, event.EventID)
		}
		if err := msg.Ack(context.Background()); err != nil {
			t.Fatalf("ack failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("subscriber channel should close after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscriber close")
	}
}

func TestInMemoryBusConcurrentPublishAndCancel(t *testing.T) {
	bus := NewInMemoryBus(4)
	publishCtx, stopPublish := context.WithCancel(context.Background())
	var publishers sync.WaitGroup
	for publisher := 0; publisher < 4; publisher++ {
		publishers.Add(1)
		go func(id int) {
			defer publishers.Done()
			for sequence := 0; ; sequence++ {
				err := bus.Publish(publishCtx, Event{EventID: "race", SeqNo: int64(id*1000 + sequence)})
				if err != nil {
					return
				}
			}
		}(publisher)
	}

	for iteration := 0; iteration < 100; iteration++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := bus.Subscribe(ctx, "router")
		if err != nil {
			t.Fatal(err)
		}
		drained := make(chan struct{})
		go func() {
			for range ch {
			}
			close(drained)
		}()
		cancel()
		select {
		case <-drained:
		case <-time.After(time.Second):
			t.Fatal("subscriber did not close")
		}
	}
	stopPublish()
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	publishers.Wait()
}
