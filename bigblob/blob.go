package bigblob

import (
	"context"
	"fmt"
	"io"
	"math/bits"

	"github.com/brendoncarroll/go-state/cadata"
)

// Root is the root of a blob represented as a tree of fixed sized content-addressed blocks
type Root struct {
	Ref
	Size      uint64 `json:"size"`
	BlockSize uint64 `json:"block_size"`
}

func (r Root) String() string {
	return fmt.Sprintf("{%s %s}", r.Ref.CID.String()[:8], "chacha20")
}

func (r1 Root) Equals(r2 Root) bool {
	return r1.Size == r2.Size && r1.BlockSize == r2.BlockSize && r1.Ref.Equals(r2.Ref)
}

func (o *Operator) ReadAt(ctx context.Context, s cadata.Getter, x Root, offset int64, buf []byte) (n int, err error) {
	level := depth(x.Size, x.BlockSize)
	bf := branchingFactor(x.BlockSize)
	blockIndex := uint64(offset) / x.BlockSize
	relOffset := uint64(offset) % x.BlockSize
	ref, err := o.getPiece(ctx, s, x.Ref, int(bf), level, int(blockIndex))
	if err != nil {
		return n, err
	}
	if err := o.getF(ctx, s, *ref, func(data []byte) error {
		n = copy(buf[n:], data[relOffset:])
		offset += int64(n)
		return nil
	}); err != nil {
		return n, err
	}
	if uint64(offset) == x.Size {
		return n, io.EOF
	}
	return n, nil
}

func (o *Operator) getPiece(ctx context.Context, s cadata.Getter, root Ref, bf, level, blockIndex int) (*Ref, error) {
	if level == 0 {
		return &root, nil
	}
	var ref Ref
	if err := o.getF(ctx, s, root, func(data []byte) error {
		idx, err := newIndexUsing(data, bf*maxRefSize)
		if err != nil {
			return err
		}
		ref = idx.Get(blockIndex / powInt(bf, level-1))
		return nil
	}); err != nil {
		return nil, err
	}
	return o.getPiece(ctx, s, ref, bf, level-1, blockIndex%powInt(bf, level-1))
}

type Writer struct {
	ctx                context.Context
	o                  *Operator
	s                  cadata.Poster
	blockSize          int
	indexSalt, rawSalt *[32]byte
	branchingFactor    int

	indexes []Index
	counts  []int
	size    uint64
	buf     []byte
}

func (o *Operator) NewWriter(ctx context.Context, s cadata.Poster, salt *[32]byte) *Writer {
	blockSize := s.MaxSize()
	if o.blockSize > 0 {
		blockSize = o.blockSize
	}
	if blockSize > s.MaxSize() {
		panic(fmt.Sprintf("blockSize %d > maxSize %d", blockSize, s.MaxSize()))
	}
	if blockSize < 2*maxRefSize {
		panic(fmt.Sprintf("blockSize cannot be < %d", 2*maxRefSize))
	}
	if salt == nil {
		salt = new([32]byte)
	}
	var indexSalt, rawSalt [32]byte
	DeriveKey(indexSalt[:], salt, []byte("index"))
	DeriveKey(rawSalt[:], salt, []byte("raw"))
	return &Writer{
		ctx:             ctx,
		o:               o,
		s:               s,
		blockSize:       blockSize,
		branchingFactor: blockSize / maxRefSize,
		rawSalt:         &rawSalt,
		indexSalt:       &indexSalt,

		indexes: []Index{newIndex(blockSize)},
		counts:  []int{0},
	}
}

func (w *Writer) Write(data []byte) (int, error) {
	if len(w.buf)+len(data) < w.blockSize {
		w.buf = append(w.buf, data...)
		n := len(data)
		return n, nil
	}
	n := w.blockSize - len(w.buf)
	w.buf = append(w.buf, data[:n]...)
	if err := w.postBuf(w.ctx); err != nil {
		return 0, err
	}
	n2, err := w.Write(data[n:])
	return n + n2, err
}

func (w *Writer) Finish(ctx context.Context) (*Root, error) {
	if len(w.buf) > 0 {
		if err := w.postBuf(ctx); err != nil {
			return nil, err
		}
	}
	ref, err := w.finishIndexes(ctx)
	if err != nil {
		return nil, err
	}
	return &Root{
		Size:      w.size,
		Ref:       *ref,
		BlockSize: uint64(w.blockSize),
	}, nil
}

func (w *Writer) postBuf(ctx context.Context) error {
	ref, err := w.o.post(w.ctx, w.s, w.rawSalt, w.buf)
	if err != nil {
		return err
	}
	if err := w.addRef(w.ctx, 0, *ref); err != nil {
		return err
	}
	w.size += uint64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

func (w *Writer) addRef(ctx context.Context, i int, ref Ref) error {
	if len(w.indexes) <= i {
		w.indexes = append(w.indexes, newIndex(w.blockSize))
		w.counts = append(w.counts, 0)
	}
	w.indexes[i].Set(w.counts[i], ref)
	w.counts[i]++
	if w.counts[i] < w.branchingFactor {
		return nil
	}
	ref2, err := w.o.post(ctx, w.s, w.indexSalt, w.indexes[i].x)
	if err != nil {
		return err
	}
	w.counts[i] = 0
	w.indexes[i].Clear()
	return w.addRef(ctx, i+1, *ref2)
}

func (w *Writer) finishIndexes(ctx context.Context) (*Ref, error) {
	for i := 0; i < len(w.indexes); i++ {
		if i == len(w.indexes)-1 {
			if w.counts[i] == 0 {
				return w.o.post(w.ctx, w.s, w.indexSalt, nil)
			}
			if w.counts[i] == 1 {
				ref := w.indexes[i].Get(0)
				return &ref, nil
			}
		}
		if w.counts[i] > 0 {
			ref, err := w.o.post(ctx, w.s, w.indexSalt, w.indexes[i].x)
			if err != nil {
				return nil, err
			}
			if err := w.addRef(ctx, i+1, *ref); err != nil {
				return nil, err
			}
		}
	}
	panic("should not happen")
}

// Create creates a Blob and returns it's Root.
func (o *Operator) Create(ctx context.Context, s cadata.Poster, salt *[32]byte, r io.Reader) (*Root, error) {
	w := o.NewWriter(ctx, s, salt)
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	return w.Finish(ctx)
}

// Integer power: compute a**b using binary powering algorithm
// See Donald Knuth, The Art of Computer Programming, Volume 2, Section 4.6.3
func pow(a, b uint64) uint64 {
	p := uint64(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

func powInt(a, b int) int {
	return int(pow(uint64(a), uint64(b)))
}

func log2Ceil(x uint64) uint64 {
	if x == 0 {
		panic("log2 0")
	}
	l := 64 - bits.LeadingZeros64(x)
	if bits.OnesCount64(x) > 1 {
		l++
	}
	return uint64(l) - 1
}

func divCeil(a, b uint64) uint64 {
	q := a / b
	if a%b > 0 {
		q++
	}
	return q
}

func depth(size, blockSize uint64) int {
	if size == 0 {
		return 0
	}
	blocks := divCeil(size, blockSize)
	bf := branchingFactor(blockSize)
	d := divCeil(log2Ceil(blocks), log2Ceil(bf))
	return int(d)
}

func branchingFactor(blockSize uint64) uint64 {
	return blockSize / maxRefSize
}

func (o *Operator) Sync(ctx context.Context, dst, src cadata.Store, x Root, fn func(r *Reader) error) error {
	if exists, err := cadata.Exists(ctx, dst, x.Ref.CID); err != nil {
		return err
	} else if exists {
		return nil
	}
	r := o.NewReader(ctx, src, x)
	if err := fn(r); err != nil {
		return err
	}
	return o.sync(ctx, dst, src, x.BlockSize, x.Ref, depth(x.Size, x.BlockSize))
}

func (o *Operator) sync(ctx context.Context, dst, src cadata.Store, blockSize uint64, ref Ref, level int) error {
	if level > 0 {
		if err := o.getF(ctx, src, ref, func(data []byte) error {
			idx, err := newIndexUsing(data, int(blockSize))
			if err != nil {
				return err
			}
			for i := 0; uint64(i) < blockSize/maxRefSize; i++ {
				ref2 := idx.Get(i)
				if ref2.CID.IsZero() {
					break
				}
				if err := o.sync(ctx, dst, src, blockSize, ref2, level-1); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return cadata.Copy(ctx, dst, src, ref.CID)
}

func (o *Operator) Concat(ctx context.Context, s cadata.Store, blockSize int, salt *[32]byte, roots ...Root) (*Root, error) {
	rs := make([]io.Reader, len(roots))
	for i := range roots {
		rs[i] = o.NewReader(ctx, s, roots[i])
	}
	mr := io.MultiReader(rs...)
	w := o.NewWriter(ctx, s, salt)
	if _, err := io.Copy(w, mr); err != nil {
		return nil, err
	}
	return w.Finish(ctx)
}
