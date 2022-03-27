package glfs

import (
	"context"
	"hash/fnv"
	"log"
	"math"
	"path"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/sync/errgroup"
)

// FilterPaths returns a version of root with paths filtered using f as a predicate.
// If f returns true for a path it will be included in the output, otherwise it will not.
func FilterPaths(ctx context.Context, s cadata.Store, root Ref, f func(string) bool) (*Ref, error) {
	ref, err := filterPaths(ctx, s, root, "", f)
	if err != nil {
		return nil, err
	}
	if ref != nil {
		return ref, nil
	}
	return PostTree(ctx, s, Tree{})
}

func filterPaths(ctx context.Context, s cadata.Store, root Ref, p string, f func(string) bool) (*Ref, error) {
	switch root.Type {
	case TypeTree:
		tree, err := GetTree(ctx, s, root)
		if err != nil {
			return nil, err
		}
		tree2 := Tree{}
		for _, ent := range tree.Entries {
			p2 := path.Join(p, ent.Name)
			p2 = CleanPath(p2)
			ref, err := filterPaths(ctx, s, ent.Ref, p2, f)
			if err != nil {
				return nil, err
			}
			if ref == nil {
				continue
			}
			ent2 := ent
			ent2.Ref = *ref
			tree2.Entries = append(tree2.Entries, ent2)
		}
		if len(tree2.Entries) > 0 || len(tree.Entries) == 0 {
			if _, err := PostTree(ctx, s, tree2); err != nil {
				log.Println(tree)
				log.Println(tree2)
				log.Println(err)
				panic(err)
			}
			return PostTree(ctx, s, tree2)
		}
		return nil, nil
	default:
		if f(p) {
			return &root, nil
		}
		return nil, nil
	}
}

func ShardLeaves(ctx context.Context, s cadata.Store, root Ref, n int) ([]Ref, error) {
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
			shard, err := FilterPaths(ctx2, s, root, func(p string) bool {
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
