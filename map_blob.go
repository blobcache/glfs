package glfs

import (
	"context"
	"io"
	"path"

	"github.com/blobcache/glfs/bigfile"
	"github.com/brendoncarroll/go-state/cadata"
)

type BlobMapper func(p string, in io.Reader, out io.Writer) error

func MapBlobs(ctx context.Context, s cadata.Store, root Ref, f BlobMapper) (*Ref, error) {
	return MapLeaves(ctx, s, root, func(p string, x Ref) (*Ref, error) {
		switch x.Type {
		case TypeBlob:
			r := bigfile.NewReader(ctx, s, x.Root)
			w := NewBlobWriter(ctx, s)
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

func MapLeaves(ctx context.Context, s cadata.Store, root Ref, f RefMapper) (*Ref, error) {
	return mapLeaves(ctx, s, root, "", f)
}

func mapLeaves(ctx context.Context, s cadata.Store, root Ref, p string, f RefMapper) (*Ref, error) {
	switch root.Type {
	case TypeTree:
		tree, err := GetTree(ctx, s, root)
		if err != nil {
			return nil, err
		}
		tree2 := Tree{}
		for _, ent := range tree.Entries {
			p2 := path.Join(p, ent.Name)
			ref, err := mapLeaves(ctx, s, ent.Ref, p2, f)
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
		return PostTree(ctx, s, tree2)
	default:
		return f(p, root)
	}
}
