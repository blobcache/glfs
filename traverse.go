package glfs

import (
	"context"

	"github.com/blobcache/glfs/bigblob"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Traverser struct {
	// Enter is called before visiting a node, if false is returned the node is skipped.
	Enter func(ctx context.Context, id cadata.ID) (bool, error)
	// Exit is called before leaving a node.  After all it's children have been visited.
	Exit func(ctx context.Context, ty Type, level int, ref bigblob.Ref) error
}

func (ag *Agent) Traverse(ctx context.Context, s cadata.Getter, sem *semaphore.Weighted, x Ref, tr Traverser) error {
	if sem == nil {
		sem = semaphore.NewWeighted(1)
	}
	if yes, err := tr.Enter(ctx, x.CID); err != nil {
		return err
	} else if !yes {
		return nil
	}

	switch x.Type {
	case TypeTree:
		tree, err := ag.GetTree(ctx, s, x)
		if err != nil {
			return err
		}
		eg, ctx := errgroup.WithContext(ctx)
		for _, ent := range tree.Entries {
			ent := ent
			fn := func() error {
				return ag.Traverse(ctx, s, sem, ent.Ref, tr)
			}
			if sem.TryAcquire(1) {
				eg.Go(func() error {
					defer sem.Release(1)
					return fn()
				})
			} else {
				if err := fn(); err != nil {
					return err
				}
			}
		}
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	return ag.bbag.Traverse(ctx, s, sem, x.Root, bigblob.Traverser{
		Enter: tr.Enter,
		Exit: func(ctx context.Context, level int, ref bigblob.Ref) error {
			return tr.Exit(ctx, x.Type, level, ref)
		},
	})
}
