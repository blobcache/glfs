package glfs

import (
	"context"
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
func (ag *Agent) Compare(ctx context.Context, s GetPoster, left, right Ref) (*Diff, error) {
	if left.Type != right.Type {
		return &Diff{
			Left:  &left,
			Right: &right,
		}, nil
	}
	switch left.Type {
	case TypeTree:
		lTree, err := ag.GetTree(ctx, s, left)
		if err != nil {
			return nil, err
		}
		rTree, err := ag.GetTree(ctx, s, right)
		if err != nil {
			return nil, err
		}
		return ag.compareTrees(ctx, s, *lTree, *rTree)
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

func (ag *Agent) compareTrees(ctx context.Context, s GetPoster, lTree, rTree Tree) (*Diff, error) {
	onlyLeft := onlyInFirst(lTree, rTree)
	onlyRight := onlyInFirst(rTree, lTree)
	var both []TreeEntry
	if err := forEachInBoth(rTree, lTree, func(lEnt, rEnt TreeEntry) error {
		diff, err := ag.Compare(ctx, s, lEnt.Ref, rEnt.Ref)
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
		if diff.Left, err = ag.PostTreeEntries(ctx, s, onlyLeft); err != nil {
			return nil, err
		}
	}
	if len(onlyRight) > 0 {
		if diff.Right, err = ag.PostTreeEntries(ctx, s, onlyRight); err != nil {
			return nil, err
		}
	}
	if len(both) > 0 {
		if diff.Both, err = ag.PostTreeEntries(ctx, s, both); err != nil {
			return nil, err
		}
	}
	return &diff, nil
}

func onlyInFirst(a, b Tree) (ret []TreeEntry) {
	for _, aEnt := range a.Entries {
		if bEnt := b.Lookup(aEnt.Name); bEnt == nil {
			ret = append(ret, aEnt)
		}
	}
	return ret
}

func forEachInBoth(a, b Tree, fn func(l, r TreeEntry) error) error {
	for _, aEnt := range a.Entries {
		if bEnt := b.Lookup(aEnt.Name); bEnt != nil {
			if err := fn(aEnt, *bEnt); err != nil {
				return err
			}
		}
	}
	return nil
}
