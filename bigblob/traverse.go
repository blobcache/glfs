package bigblob

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/sync/semaphore"
)

type Traverser struct {
	// If Enter returns false the node is skipped
	Enter func(ctx context.Context, id cadata.ID) (bool, error)
	Exit  func(ctx context.Context, level int, ref Ref) error
}

func (ag *Agent) Traverse(ctx context.Context, s cadata.Getter, sem *semaphore.Weighted, root Root, tr Traverser) error {
	return ag.traverse(ctx, s, sem, root.BlockSize, depth(root.Size, root.BlockSize), root.Ref, tr)
}

func (ag *Agent) traverse(ctx context.Context, s cadata.Getter, sem *semaphore.Weighted, blockSize uint64, level int, x Ref, tr Traverser) error {
	if yes, err := tr.Enter(ctx, x.CID); err != nil {
		return err
	} else if !yes {
		return nil
	}
	if level > 0 {
		if err := ag.getF(ctx, s, x, func(data []byte) error {
			idx, err := newIndexUsing(data, int(blockSize))
			if err != nil {
				return err
			}
			for i := 0; uint64(i) < blockSize/maxRefSize; i++ {
				ref2 := idx.Get(i)
				if ref2.CID.IsZero() {
					break
				}
				if err := ag.traverse(ctx, s, sem, blockSize, level-1, ref2, tr); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return tr.Exit(ctx, level, x)
}
