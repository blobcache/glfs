package glfs

import (
	"context"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
)

type TreeEntryMapper func(ent TreeEntry) (*TreeEntry, error)

func (o *Operator) MapEntries(ctx context.Context, s cadata.Store, root Ref, f TreeEntryMapper) (*Ref, error) {
	switch root.Type {
	case TypeBlob:
		return &root, nil
	case TypeTree:
		tree, err := o.GetTree(ctx, s, root)
		if err != nil {
			return nil, err
		}
		tree2 := Tree{Entries: make([]TreeEntry, len(tree.Entries))}
		for i, ent := range tree.Entries {
			ent2, err := f(ent)
			if err != nil {
				return nil, err
			}
			tree2.Entries[i] = *ent2
		}
		return o.PostTree(ctx, s, tree2)
	default:
		panic(root.Type)
	}
}

func (o *Operator) MapEntryAt(ctx context.Context, s cadata.Store, root Ref, p string, f TreeEntryMapper) (*Ref, error) {
	if p == "" {
		return nil, errors.New("MapEntryAt cannot operate on empty path")
	}
	parts := strings.SplitN(p, "/", 2)
	switch root.Type {
	case TypeTree:
		tree, err := o.GetTree(ctx, s, root)
		if err != nil {
			return nil, err
		}

		ent := tree.Lookup(parts[0])
		if len(parts) == 1 {
			var ent2 *TreeEntry
			if ent == nil {
				return nil, errors.Errorf("name %s not found in tree", parts[0])
			}
			ent2, err = f(*ent)
			if err != nil {
				return nil, err
			}
			tree.Replace(*ent2)
		} else {
			ref2, err := o.MapEntryAt(ctx, s, ent.Ref, parts[1], f)
			if err != nil {
				return nil, err
			}
			ent2 := *ent
			ent2.Ref = *ref2
			tree.Replace(ent2)
		}
		return o.PostTree(ctx, s, *tree)
	default:
		return nil, errors.Errorf("MapEntry cannot traverse object type: %s", root.Type)
	}
}
