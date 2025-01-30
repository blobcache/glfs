package glfs

import (
	"context"
	"fmt"
	"strings"

	"errors"
)

type TreeEntryMapper func(ent TreeEntry) (*TreeEntry, error)

func (ag *Agent) MapEntries(ctx context.Context, s GetPoster, root Ref, f TreeEntryMapper) (*Ref, error) {
	switch root.Type {
	case TypeBlob:
		return &root, nil
	case TypeTree:
		// TODO: use TreeReader
		tree, err := ag.GetTreeSlice(ctx, s, root, 1e6)
		if err != nil {
			return nil, err
		}
		tree2 := make([]TreeEntry, len(tree))
		for i, ent := range tree2 {
			ent2, err := f(ent)
			if err != nil {
				return nil, err
			}
			tree2[i] = *ent2
		}
		return ag.PostTreeSlice(ctx, s, tree2)
	default:
		panic(root.Type)
	}
}

func (ag *Agent) MapEntryAt(ctx context.Context, s GetPoster, root Ref, p string, f TreeEntryMapper) (*Ref, error) {
	if p == "" {
		return nil, errors.New("MapEntryAt cannot operate on empty path")
	}
	parts := strings.SplitN(p, "/", 2)
	switch root.Type {
	case TypeTree:
		// TODO: use TreeReader
		tree, err := ag.GetTreeSlice(ctx, s, root, 1e6)
		if err != nil {
			return nil, err
		}

		ent := Lookup(tree, parts[0])
		if len(parts) == 1 {
			var ent2 *TreeEntry
			if ent == nil {
				return nil, fmt.Errorf("name %s not found in tree", parts[0])
			}
			ent2, err = f(*ent)
			if err != nil {
				return nil, err
			}
			Replace(tree, *ent2)
		} else {
			ref2, err := ag.MapEntryAt(ctx, s, ent.Ref, parts[1], f)
			if err != nil {
				return nil, err
			}
			ent2 := *ent
			ent2.Ref = *ref2
			Replace(tree, ent2)
		}
		return ag.PostTreeSlice(ctx, s, tree)
	default:
		return nil, fmt.Errorf("MapEntry cannot traverse object type: %s", root.Type)
	}
}
