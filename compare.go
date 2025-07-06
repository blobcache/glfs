package glfs

import (
	"context"

	"go.brendoncarroll.net/state/cadata"
)

// Diff contains the result of a Compare
// Left contains only elements in left
// Right contains only elements in right
// Both contains elements common to left and right
type Diff struct {
	Left  *Ref
	Right *Ref
	Both  *Ref
}

// Compare compares left and right and returns a diff.
// Left and right must both point only to data in s.
func (ag *Machine) Compare(ctx context.Context, dst cadata.PostExister, src cadata.Getter, left, right Ref) (*Diff, error) {
	if left.Type != right.Type {
		return &Diff{
			Left:  &left,
			Right: &right,
		}, nil
	}
	switch left.Type {
	case TypeTree:
		// TODO: use TreeReader
		lTree, err := ag.GetTreeSlice(ctx, src, left, 1e6)
		if err != nil {
			return nil, err
		}
		rTree, err := ag.GetTreeSlice(ctx, src, right, 1e6)
		if err != nil {
			return nil, err
		}
		return ag.compareTrees(ctx, dst, src, lTree, rTree)
	default:
		if left.Equals(right) {
			return &Diff{Both: &left}, nil
		} else {
			return &Diff{
				Left:  &left,
				Right: &right,
			}, nil
		}
	}
}

func (ag *Machine) compareTrees(ctx context.Context, dst cadata.PostExister, src cadata.Getter, lTree, rTree []TreeEntry) (*Diff, error) {
	onlyLeft := onlyInFirst(lTree, rTree)
	onlyRight := onlyInFirst(rTree, lTree)
	var both []TreeEntry
	if err := forEachInBoth(rTree, lTree, func(lEnt, rEnt TreeEntry) error {
		diff, err := ag.Compare(ctx, dst, src, lEnt.Ref, rEnt.Ref)
		if err != nil {
			return err
		}
		if diff.Left != nil {
			onlyLeft = append(onlyLeft, TreeEntry{
				FileMode: lEnt.FileMode,
				Name:     lEnt.Name,
				Ref:      *diff.Left,
			})
		}
		if diff.Right != nil {
			onlyRight = append(onlyRight, TreeEntry{
				FileMode: rEnt.FileMode,
				Name:     rEnt.Name,
				Ref:      *diff.Right,
			})
		}
		if diff.Both != nil {
			both = append(both, TreeEntry{
				FileMode: lEnt.FileMode,
				Name:     lEnt.Name,
				Ref:      *diff.Both,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var err error
	var diff Diff
	if len(onlyLeft) > 0 {
		if diff.Left, err = ag.PostTreeSlice(ctx, dst, onlyLeft); err != nil {
			return nil, err
		}
	}
	if len(onlyRight) > 0 {
		if diff.Right, err = ag.PostTreeSlice(ctx, dst, onlyRight); err != nil {
			return nil, err
		}
	}
	if len(both) > 0 {
		if diff.Both, err = ag.PostTreeSlice(ctx, dst, both); err != nil {
			return nil, err
		}
	}
	return &diff, nil
}

func onlyInFirst(a, b []TreeEntry) (ret []TreeEntry) {
	for _, aEnt := range a {
		if bEnt := Lookup(b, aEnt.Name); bEnt == nil {
			ret = append(ret, aEnt)
		}
	}
	return ret
}

func forEachInBoth(a, b []TreeEntry, fn func(l, r TreeEntry) error) error {
	for _, aEnt := range a {
		if bEnt := Lookup(b, aEnt.Name); bEnt != nil {
			if err := fn(aEnt, *bEnt); err != nil {
				return err
			}
		}
	}
	return nil
}
