package glfs

import (
	"context"
	"fmt"

	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/errgroup"
)

// Sync ensures that all data referenced by x exists in dst, copying from src if necessary.
// Sync assumes there are no dangling references, and skips copying data when its existence is implied.
func (ag *Machine) Sync(ctx context.Context, dst cadata.PostExister, src cadata.Getter, x Ref) error {
	switch x.Type {
	case TypeBlob:
		return ag.bbag.Sync(ctx, dst, src, x.Root, func(r *Reader) error { return nil })
	case TypeTree:
		return ag.bbag.Sync(ctx, dst, src, x.Root, func(r *Reader) error {
			tr := ag.ReadTreeFrom(r)
			group, ctx2 := errgroup.WithContext(ctx)
			for {
				ent, err := streams.Next(ctx, tr)
				if err != nil {
					if streams.IsEOS(err) {
						break
					}
					return err
				}
				group.Go(func() error {
					return ag.Sync(ctx2, dst, src, ent.Ref)
				})
			}
			return group.Wait()
		})
	default:
		return fmt.Errorf("can't sync unrecognized type %s", x.Type)
	}
}

// syncTreeEntries is a convenience function for syncing tree entries.
// Most callers should prefer Sync
func (ag *Machine) syncTreeEntries(ctx context.Context, dst cadata.PostExister, src cadata.Getter, ents []TreeEntry) error {
	for _, ent := range ents {
		if err := ag.Sync(ctx, dst, src, ent.Ref); err != nil {
			return err
		}
	}
	return nil
}
