package glfs

import (
	"context"

	"github.com/blobcache/glfs/bigfile"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
)

// Merge merges the refs in layers with increasing prescedence.
// layer[i+1] is higher prescendence than layer[i]
// Merging is associative, but not commutative
// Merge(tree, blob) -> blob
// Merge(blob, tree) -> tree
// Merge(tree1, tree2) -> set of entry names given by tree1 + tree2. value at entry x given by Merge(tree1[x], tree2[x])
// Although not written as such for performance reasons:
// Merging(1, 2, 3, 4, 5) == Merge(Merge(Merge(Merge(1, 2), 3), 4), 5)
func Merge(ctx context.Context, store cadata.Store, layers ...Ref) (*Ref, error) {
	switch {
	case len(layers) == 0:
		panic("merging 0 layers")
	case len(layers) == 1:
		return &layers[0], nil
	case layers[len(layers)-1].Type == TypeBlob:
		return &layers[len(layers)-1], nil
	}

	m := map[string][]TreeEntry{}
	for _, layer := range layers {
		switch layer.Type {
		case TypeTree:
			tree, err := GetTree(ctx, store, layer)
			if err != nil {
				return nil, err
			}
			for _, ent := range tree.Entries {
				m[ent.Name] = append(m[ent.Name], ent)
			}
		case TypeBlob:
			// clear
			for k := range m {
				delete(m, k)
			}
		}
	}

	tree := Tree{}
	for key, entries := range m {
		layers2 := []Ref{}
		for _, ent := range entries {
			layers2 = append(layers2, ent.Ref)
		}
		ref, err := Merge(ctx, store, layers2...)
		if err != nil {
			return nil, err
		}
		lastEnt := entries[len(entries)-1]
		tree.Entries = append(tree.Entries, TreeEntry{
			Name:     key,
			Ref:      *ref,
			FileMode: lastEnt.FileMode,
		})
	}
	return PostTree(ctx, store, tree)
}

func Concat(ctx context.Context, store cadata.Store, layers ...Ref) (*Ref, error) {
	switch {
	case len(layers) == 0:
		return nil, errors.New("concat 0 refs")
	case len(layers) == 1:
		return &layers[0], nil
	case len(layers) == 2:
		left, right := layers[0], layers[1]
		return concat2(ctx, store, left, right)
	default:
		left, err := Concat(ctx, store, layers[:2]...)
		if err != nil {
			return nil, err
		}
		right, err := Concat(ctx, store, layers[2:]...)
		if err != nil {
			return nil, err
		}
		return Concat(ctx, store, *left, *right)
	}
}

func concat2(ctx context.Context, store cadata.Store, left, right Ref) (*Ref, error) {
	switch {
	case left.Type == TypeBlob && right.Type == TypeBlob:
		return concatBlobs(ctx, store, left, right)
	case left.Type == TypeTree && right.Type == TypeTree:
		return concat2Trees(ctx, store, left, right)
	default:
		return nil, errors.Errorf("can't concat types %s %s", left.Type, right.Type)
	}
}

func concat2Trees(ctx context.Context, store cadata.Store, left, right Ref) (*Ref, error) {
	leftTree, err := GetTree(ctx, store, left)
	if err != nil {
		return nil, err
	}
	rightTree, err := GetTree(ctx, store, left)
	if err != nil {
		return nil, err
	}
	m := map[string]TreeEntry{}
	for _, ent := range leftTree.Entries {
		m[ent.Name] = ent
	}
	for _, ent2 := range rightTree.Entries {
		if ent1, exists := m[ent2.Name]; exists {
			ref, err := Concat(ctx, store, ent1.Ref, ent2.Ref)
			if err != nil {
				return nil, err
			}
			m[ent2.Name] = TreeEntry{
				Name:     ent2.Name,
				Ref:      *ref,
				FileMode: ent2.FileMode,
			}
		} else {
			m[ent2.Name] = ent2
		}
	}
	tree := Tree{}
	for _, ent := range m {
		tree.Entries = append(tree.Entries, ent)
	}
	return PostTree(ctx, store, tree)
}

func concatBlobs(ctx context.Context, s cadata.Store, refs ...Ref) (*Ref, error) {
	var roots []bigfile.Root
	for _, ref := range refs {
		roots = append(roots, ref.Root)
	}
	yRoot, err := bigfile.Concat(ctx, s, s.MaxSize(), makeSalt(nil, TypeBlob), roots...)
	if err != nil {
		return nil, err
	}
	r := bigfile.NewReader(ctx, s, *yRoot)
	fp, err := FPReader(r)
	if err != nil {
		return nil, err
	}
	return &Ref{
		Type:        TypeBlob,
		Root:        *yRoot,
		Fingerprint: fp,
	}, nil
}
