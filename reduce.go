package glfs

import (
	"context"
	"errors"
	"fmt"

	"go.brendoncarroll.net/state/cadata"

	"github.com/blobcache/glfs/bigblob"
)

// Merge merges the refs in layers with increasing prescedence.
// layer[i+1] is higher prescendence than layer[i]
// Merging is associative, but not commutative
// Merge(tree, blob) -> blob
// Merge(blob, tree) -> tree
// Merge(tree1, tree2) -> set of entry names given by tree1 + tree2. value at entry x given by Merge(tree1[x], tree2[x])
// Although not written as such for performance reasons:
// Merging(1, 2, 3, 4, 5) == Merge(Merge(Merge(Merge(1, 2), 3), 4), 5)
func (ag *Agent) Merge(ctx context.Context, dst cadata.Poster, src cadata.Getter, layers ...Ref) (*Ref, error) {
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
			// TODO: use TreeReader
			tree, err := ag.GetTreeSlice(ctx, src, layer, 1e6)
			if err != nil {
				return nil, err
			}
			for _, ent := range tree {
				m[ent.Name] = append(m[ent.Name], ent)
			}
		case TypeBlob:
			// clear
			for k := range m {
				delete(m, k)
			}
		}
	}

	tree := []TreeEntry{}
	for key, entries := range m {
		layers2 := []Ref{}
		for _, ent := range entries {
			layers2 = append(layers2, ent.Ref)
		}
		ref, err := ag.Merge(ctx, dst, src, layers2...)
		if err != nil {
			return nil, err
		}
		lastEnt := entries[len(entries)-1]
		tree = append(tree, TreeEntry{
			Name:     key,
			Ref:      *ref,
			FileMode: lastEnt.FileMode,
		})
	}
	return ag.PostTreeSlice(ctx, dst, tree)
}

func (ag *Agent) Concat(ctx context.Context, store cadata.Store, layers ...Ref) (*Ref, error) {
	switch {
	case len(layers) == 0:
		return nil, errors.New("concat 0 refs")
	case len(layers) == 1:
		return &layers[0], nil
	case len(layers) == 2:
		left, right := layers[0], layers[1]
		return ag.concat2(ctx, store, left, right)
	default:
		left, err := ag.Concat(ctx, store, layers[:2]...)
		if err != nil {
			return nil, err
		}
		right, err := ag.Concat(ctx, store, layers[2:]...)
		if err != nil {
			return nil, err
		}
		return ag.Concat(ctx, store, *left, *right)
	}
}

func (ag *Agent) concat2(ctx context.Context, store cadata.Store, left, right Ref) (*Ref, error) {
	switch {
	case left.Type == TypeBlob && right.Type == TypeBlob:
		return ag.concatBlobs(ctx, store, left, right)
	case left.Type == TypeTree && right.Type == TypeTree:
		return ag.concat2Trees(ctx, store, left, right)
	default:
		return nil, fmt.Errorf("can't concat types %s %s", left.Type, right.Type)
	}
}

func (ag *Agent) concat2Trees(ctx context.Context, store cadata.Store, left, right Ref) (*Ref, error) {
	// TODO: use TreeReader
	leftTree, err := ag.GetTreeSlice(ctx, store, left, 1e6)
	if err != nil {
		return nil, err
	}
	rightTree, err := ag.GetTreeSlice(ctx, store, left, 1e6)
	if err != nil {
		return nil, err
	}
	m := map[string]TreeEntry{}
	for _, ent := range leftTree {
		m[ent.Name] = ent
	}
	for _, ent2 := range rightTree {
		if ent1, exists := m[ent2.Name]; exists {
			ref, err := ag.Concat(ctx, store, ent1.Ref, ent2.Ref)
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
	tree := []TreeEntry{}
	for _, ent := range m {
		tree = append(tree, ent)
	}
	return ag.PostTreeSlice(ctx, store, tree)
}

func (ag *Agent) concatBlobs(ctx context.Context, s cadata.Store, refs ...Ref) (*Ref, error) {
	var roots []bigblob.Root
	for _, ref := range refs {
		roots = append(roots, ref.Root)
	}
	yRoot, err := ag.bbag.Concat(ctx, s, s.MaxSize(), ag.makeSalt(TypeBlob), roots...)
	if err != nil {
		return nil, err
	}
	return &Ref{
		Type: TypeBlob,
		Root: *yRoot,
	}, nil
}
