package glfs

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs/bigblob"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/semaphore"
)

type GetListDeleter interface {
	schema.RO
	cadata.Lister
	Delete(ctx context.Context, cids []blobcache.CID) error
}

type GCResult struct {
	Reachable uint64
	Scanned   uint64
	Deleted   uint64
}

type gcConfig struct{}

type GCOption func(*gcConfig)

// GC will remove objects from store which are not referenced by any of the refs in keep.
// If GC does not successfully complete, referential integrity may be violated, and GC will need
// to be run to completion before it is safe to call Sync on the store again.
func (ag *Machine) GC(ctx context.Context, store GetListDeleter, keep []Ref, opts ...GCOption) (*GCResult, error) {
	// compute reachable
	reachable := &idSet{}
	for _, ref := range keep {
		if err := ag.Populate(ctx, store, ref, reachable); err != nil {
			return nil, err
		}
	}

	// iterate through prefix and delete
	var scanned, deleted atomic.Uint64
	// TODO: parallelize
	if err := cadata.ForEach(ctx, store, cadata.Span{}, func(id cadata.ID) error {
		scanned.Add(1)
		if _, exists := reachable.m[id]; !exists {
			if err := store.Delete(ctx, []blobcache.CID{id}); err != nil {
				return err
			}
			deleted.Add(1)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &GCResult{
		Reachable: uint64(reachable.Len()),
		Scanned:   scanned.Load(),
		Deleted:   deleted.Load(),
	}, nil
}

type AddExister interface {
	cadata.Adder
	cadata.Exister
}

// Populate adds everything reachable form x to dst
func (ag *Machine) Populate(ctx context.Context, store schema.RO, x Ref, dst AddExister) error {
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	return ag.Traverse(ctx, store, sem, x, Traverser{
		Enter: func(ctx context.Context, id cadata.ID) (bool, error) {
			exists, err := dst.Exists(ctx, id)
			if err != nil {
				return false, err
			}
			return !exists, nil
		},
		Exit: func(ctx context.Context, ty Type, level int, ref bigblob.Ref) error {
			return dst.Add(ctx, ref.CID)
		},
	})
}

type idSet struct {
	mu sync.RWMutex
	m  map[cadata.ID]struct{}
}

func (s *idSet) Add(ctx context.Context, id cadata.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[cadata.ID]struct{})
	}
	s.m[id] = struct{}{}
	return nil
}

func (s *idSet) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.m[id]
	return exists, nil
}

func (s *idSet) Len() int {
	return len(s.m)
}
