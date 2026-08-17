package routing

import (
	"context"
	"errors"

	"github.com/Haluka1/ai-storage-lab/router/internal/cacheindex"
	"github.com/Haluka1/ai-storage-lab/router/internal/common"
)

var ErrNoHealthyWorker = errors.New("no healthy worker available")

type Strategy interface {
	Name() string
	Pick(ctx context.Context, req common.RequestContext, workers []common.WorkerState, idx *cacheindex.Index) (common.RouteDecision, error)
}

func healthy(workers []common.WorkerState) []common.WorkerState {
	out := make([]common.WorkerState, 0, len(workers))
	for _, worker := range workers {
		if worker.Routable() {
			out = append(out, worker)
		}
	}
	return out
}

func overlapFor(idx *cacheindex.Index, req common.RequestContext) map[common.WorkerID]int {
	return idx.OverlapByWorker(req.BlockHashes)
}
