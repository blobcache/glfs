package glfs

import (
	"context"
	"io"

	"github.com/blobcache/glfs/bigfile"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Sync ensures that all data referenced by x exists in dst, copying from src if necessary.
// Sync assumes there are no dangling references, and skips copying data when its existence is implied.
func Sync(ctx context.Context, dst, src cadata.Store, x Ref) error {
	switch x.Type {
	case TypeBlob:
		return bigfile.Sync(ctx, dst, src, x.Root, func(io.Reader) error { return nil })
	case TypeTree:
		return bigfile.Sync(ctx, dst, src, x.Root, func(r io.Reader) error {
			tree, err := readTree(r)
			if err != nil {
				return err
			}
			group, ctx2 := errgroup.WithContext(ctx)
			for _, ent := range tree.Entries {
				ref := ent.Ref
				group.Go(func() error {
					return Sync(ctx2, dst, src, ref)
				})
			}
			return group.Wait()
		})
	default:
		return errors.Errorf("can't sync unrecognized type %s", x.Type)
	}
}
