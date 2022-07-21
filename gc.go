package glfs

import (
	"bytes"
	"context"
	"log"

	"github.com/brendoncarroll/go-state/cadata"
)

type GCStore interface {
	cadata.Store
}

type GCStats struct {
	Reachable int
	Scanned   int
	Deleted   int
}

type idSet map[cadata.ID]struct{}

// GC will remove objects from store which are not referenced by any of the refs in keep.
// If GC does not successfully complete, referential integrity may be violated, and GC will need
// to be run to completion before it is safe to call Sync on the store again.
func (o *Operator) GC(ctx context.Context, store GCStore, prefix []byte, keep []Ref) (*GCStats, error) {
	// compute reachable
	reachable, reachableTrees := idSet{}, idSet{}
	for _, ref := range keep {
		if err := o.gcCollect(ctx, store, prefix, reachable, reachableTrees, ref); err != nil {
			return nil, err
		}
	}
	log.Println("computed reachable", len(reachable))
	// iterate through prefix and delete
	scanned := 0
	deleted := 0
	if err := cadata.ForEach(ctx, store, cadata.Span{}, func(id cadata.ID) error {
		if !bytes.HasPrefix(id[:], prefix) {
			return nil
		}
		scanned++
		if _, exists := reachable[id]; !exists {
			if err := store.Delete(ctx, id); err != nil {
				return err
			}
			deleted++
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &GCStats{
		Reachable: len(reachable),
		Scanned:   scanned,
		Deleted:   deleted,
	}, nil
}

func (o *Operator) gcCollect(ctx context.Context, store GCStore, prefix []byte, reachable, trees idSet, x Ref) error {
	switch x.Type {
	case TypeTree:
		if _, exists := trees[x.Ref.CID]; exists {
			return nil
		}
		tree, err := o.GetTree(ctx, store, x)
		if err != nil {
			return err
		}
		for _, ent := range tree.Entries {
			if err := o.gcCollect(ctx, store, prefix, reachable, trees, ent.Ref); err != nil {
				return err
			}
		}
		if bytes.HasPrefix(x.Ref.CID[:], prefix) {
			reachable[x.Ref.CID] = struct{}{}
			trees[x.Ref.CID] = struct{}{}
		}
	default:
		if bytes.HasPrefix(x.Ref.CID[:], prefix) {
			reachable[x.Ref.CID] = struct{}{}
		}
	}
	return nil
}
