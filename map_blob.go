package glfs

import (
	"context"
	"io"
	"path"

	"go.brendoncarroll.net/state/cadata"
)

type BlobMapper func(p string, in io.Reader, out io.Writer) error

func (ag *Machine) MapBlobs(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, f BlobMapper) (*Ref, error) {
	return ag.MapLeaves(ctx, dst, src, root, func(p string, x Ref) (*Ref, error) {
		switch x.Type {
		case TypeBlob:
			r := ag.bbag.NewReader(ctx, src, x.Root)
			w := ag.NewBlobWriter(dst)
			w.SetWriteContext(ctx)
			if err := f(p, r, w); err != nil {
				return nil, err
			}
			return w.Finish(ctx)
		default:
			return &x, nil
		}
	})
}

type RefMapper func(p string, ref Ref) (*Ref, error)

func (ag *Machine) MapLeaves(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, f RefMapper) (*Ref, error) {
	return ag.mapLeaves(ctx, dst, src, root, "", f)
}

func (ag *Machine) mapLeaves(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, p string, f RefMapper) (*Ref, error) {
	switch root.Type {
	case TypeTree:
		// TODO: use TreeReader
		tree, err := ag.GetTreeSlice(ctx, src, root, 1e6)
		if err != nil {
			return nil, err
		}
		tree2 := []TreeEntry{}
		for _, ent := range tree {
			p2 := path.Join(p, ent.Name)
			ref, err := ag.mapLeaves(ctx, dst, src, ent.Ref, p2, f)
			if err != nil {
				return nil, err
			}
			if ref != nil {
				tree2 = append(tree2, TreeEntry{
					Name:     ent.Name,
					FileMode: ent.FileMode,
					Ref:      *ref,
				})
			}
		}
		return ag.PostTreeSlice(ctx, dst, tree2)
	default:
		return f(p, root)
	}
}
