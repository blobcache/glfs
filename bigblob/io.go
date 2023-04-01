package bigblob

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
	o      *Operator
	ctx    context.Context
	store  cadata.Getter
	root   Root
	offset int64
}

func (o *Operator) NewReader(ctx context.Context, s cadata.Getter, root Root) *Reader {
	return &Reader{
		o:     o,
		ctx:   ctx,
		store: s,
		root:  root,
	}
}

func (r *Reader) ReadAt(data []byte, at int64) (int, error) {
	return r.o.ReadAt(r.ctx, r.store, r.root, at, data)
}

func (r *Reader) Read(data []byte) (int, error) {
	n, err := r.o.ReadAt(r.ctx, r.store, r.root, r.offset, data)
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
