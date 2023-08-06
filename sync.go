package glfs

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/sync/errgroup"
)

// Sync ensures that all data referenced by x exists in dst, copying from src if necessary.
// Sync assumes there are no dangling references, and skips copying data when its existence is implied.
func (o *Operator) Sync(ctx context.Context, dst cadata.Store, src cadata.Getter, x Ref) error {
	switch x.Type {
	case TypeBlob:
		return o.bfop.Sync(ctx, dst, src, x.Root, func(r *Reader) error { return nil })
	case TypeTree:
		return o.bfop.Sync(ctx, dst, src, x.Root, func(r *Reader) error {
			tree, err := readTree(r)
			if err != nil {
				return err
			}
			group, ctx2 := errgroup.WithContext(ctx)
			for _, ent := range tree.Entries {
				ref := ent.Ref
				group.Go(func() error {
					return o.Sync(ctx2, dst, src, ref)
				})
			}
			return group.Wait()
		})
	default:
		return fmt.Errorf("can't sync unrecognized type %s", x.Type)
	}
}
