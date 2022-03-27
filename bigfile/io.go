package bigfile

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
)

var _ io.ReadSeeker = &Reader{}

type Reader struct {
	ctx    context.Context
	store  cadata.Store
	root   Root
	offset uint64
}

func NewReader(ctx context.Context, s cadata.Store, root Root) *Reader {
	return &Reader{
		ctx:   ctx,
		store: s,
		root:  root,
	}
}

func (r *Reader) Read(data []byte) (int, error) {
	n, err := ReadAt(r.ctx, r.store, r.root, r.offset, data)
	r.offset += uint64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.offset = uint64(offset)
	case io.SeekCurrent:
		r.offset += uint64(offset)
	case io.SeekEnd:
		r.offset = r.root.Size - uint64(offset)
	default:
		panic("invalid whence")
	}
	return int64(r.offset), nil
}
