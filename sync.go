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
func (ag *Agent) Sync(ctx context.Context, dst cadata.Store, src cadata.Getter, x Ref) error {
	switch x.Type {
	case TypeBlob:
		return ag.bbag.Sync(ctx, dst, src, x.Root, func(r *Reader) error { return nil })
	case TypeTree:
		return ag.bbag.Sync(ctx, dst, src, x.Root, func(r *Reader) error {
			tr := ag.ReadTreeFrom(r)
			group, ctx2 := errgroup.WithContext(ctx)
			if err := streams.ForEach(ctx, tr, func(x TreeEntry) error {
				ref := x.Ref
				group.Go(func() error {
					return ag.Sync(ctx2, dst, src, ref)
				})
				return nil
			}); err != nil {
				return err
			}
			return group.Wait()
		})
	default:
		return fmt.Errorf("can't sync unrecognized type %s", x.Type)
	}
}
