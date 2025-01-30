package glfs

import (
	"context"
	"hash/fnv"
	"math"
	"path"

	"golang.org/x/sync/errgroup"
)

// FilterPaths returns a version of root with paths filtered using f as a predicate.
// If f returns true for a path it will be included in the output, otherwise it will not.
func (ag *Agent) FilterPaths(ctx context.Context, s GetPoster, root Ref, f func(string) bool) (*Ref, error) {
	ref, err := ag.filterPaths(ctx, s, root, "", f)
	if err != nil {
		return nil, err
	}
	if ref != nil {
		return ref, nil
	}
	return ag.PostTreeSlice(ctx, s, nil)
}

func (ag *Agent) filterPaths(ctx context.Context, s GetPoster, root Ref, p string, f func(string) bool) (*Ref, error) {
	switch root.Type {
	case TypeTree:
		// TODO: use TreeReader
		ents, err := ag.GetTreeSlice(ctx, s, root, 1e6)
		if err != nil {
			return nil, err
		}
		var ents2 []TreeEntry
		for _, ent := range ents {
			p2 := path.Join(p, ent.Name)
			p2 = CleanPath(p2)
			ref, err := ag.filterPaths(ctx, s, ent.Ref, p2, f)
			if err != nil {
				return nil, err
			}
			if ref == nil {
				continue
			}
			ent2 := ent
			ent2.Ref = *ref
			ents2 = append(ents2, ent2)
		}
		if len(ents2) > 0 || len(ents) == 0 {
			if _, err := ag.PostTreeSlice(ctx, s, ents2); err != nil {
				panic(err)
			}
			return ag.PostTreeSlice(ctx, s, ents2)
		}
		return nil, nil
	default:
		if f(p) {
			return &root, nil
		}
		return nil, nil
	}
}

func (ag *Agent) ShardLeaves(ctx context.Context, s GetPoster, root Ref, n int) ([]Ref, error) {
	hashFunc := func(p string) uint32 {
		h := fnv.New32()
		h.Write([]byte(p))
		return h.Sum32()
	}
	shards := make([]Ref, n)
	eg, ctx2 := errgroup.WithContext(ctx)
	for i := 0; i < n; i++ {
		i := i
		eg.Go(func() error {
			shard, err := ag.FilterPaths(ctx2, s, root, func(p string) bool {
				x := hashFunc(p)
				return int(x)/(math.MaxUint32/n) == i
			})
			if err != nil {
				return err
			}
			shards[i] = *shard
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return shards, nil
}
