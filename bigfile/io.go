package bigfile

import (
	"context"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
)

var (
	_ io.ReadSeeker = &Reader{}
	_ io.ReaderAt   = &Reader{}
)

type Reader struct {
	ctx    context.Context
	store  cadata.Store
	root   Root
	offset int64
}

func NewReader(ctx context.Context, s cadata.Store, root Root) *Reader {
	return &Reader{
		ctx:   ctx,
		store: s,
		root:  root,
	}
}

func (r *Reader) ReadAt(data []byte, at int64) (int, error) {
	return ReadAt(r.ctx, r.store, r.root, at, data)
}

func (r *Reader) Read(data []byte) (int, error) {
	n, err := ReadAt(r.ctx, r.store, r.root, r.offset, data)
	r.offset += int64(n)
	return n, err
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		r.offset = int64(r.root.Size) - offset
	default:
		panic("invalid whence")
	}
	return int64(r.offset), nil
}
