package glfs

import (
	"context"
	"io"
	"path"
)

type BlobMapper func(p string, in io.Reader, out io.Writer) error

func (o *Operator) MapBlobs(ctx context.Context, s GetPoster, root Ref, f BlobMapper) (*Ref, error) {
	return o.MapLeaves(ctx, s, root, func(p string, x Ref) (*Ref, error) {
		switch x.Type {
		case TypeBlob:
			r := o.bfop.NewReader(ctx, s, x.Root)
			w := o.NewBlobWriter(ctx, s)
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

func (o *Operator) MapLeaves(ctx context.Context, s GetPoster, root Ref, f RefMapper) (*Ref, error) {
	return o.mapLeaves(ctx, s, root, "", f)
}

func (o *Operator) mapLeaves(ctx context.Context, s GetPoster, root Ref, p string, f RefMapper) (*Ref, error) {
	switch root.Type {
	case TypeTree:
		tree, err := o.GetTree(ctx, s, root)
		if err != nil {
			return nil, err
		}
		tree2 := Tree{}
		for _, ent := range tree.Entries {
			p2 := path.Join(p, ent.Name)
			ref, err := o.mapLeaves(ctx, s, ent.Ref, p2, f)
			if err != nil {
				return nil, err
			}
			if ref != nil {
				tree2.Entries = append(tree2.Entries, TreeEntry{
					Name:     ent.Name,
					FileMode: ent.FileMode,
					Ref:      *ref,
				})
			}
		}
		return o.PostTree(ctx, s, tree2)
	default:
		return f(p, root)
	}
}
