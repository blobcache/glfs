package glfs

import (
	"context"
	"io"

	"github.com/blobcache/glfs/bigblob"
	"github.com/brendoncarroll/go-state/cadata"
)

type Reader = bigblob.Reader

// PostBlob creates a new blob with data from r, and returns a Ref to it.
func (o *Operator) PostBlob(ctx context.Context, s cadata.Poster, r io.Reader) (*Ref, error) {
	return o.PostRaw(ctx, s, TypeBlob, r)
}

// GetBlob returns an io.ReadSeeker for accessing data from the blob at x
func (o *Operator) GetBlob(ctx context.Context, s cadata.Getter, x Ref) (*Reader, error) {
	return o.GetRaw(ctx, s, TypeBlob, x)
}

// GetBlobBytes reads the entire contents of the blob at x into memory and returns the slice of bytes.
func (o *Operator) GetBlobBytes(ctx context.Context, s cadata.Getter, x Ref) ([]byte, error) {
	r, err := o.GetBlob(ctx, s, x)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// BlobWriter writes a blob
type BlobWriter struct {
	inner *bigblob.Writer
}

func (o *Operator) NewBlobWriter(ctx context.Context, s cadata.Poster) *BlobWriter {
	return &BlobWriter{
		inner: o.bfop.NewWriter(ctx, s, o.makeSalt(TypeBlob)),
	}
}

// Write adds data to the blob being written.
func (bw *BlobWriter) Write(data []byte) (int, error) {
	return bw.inner.Write(data)
}

// Finish completes the blob and returns a reference to it.
func (bw *BlobWriter) Finish(ctx context.Context) (*Ref, error) {
	root, err := bw.inner.Finish(ctx)
	if err != nil {
		return nil, err
	}
	return &Ref{
		Type: TypeBlob,
		Root: *root,
	}, nil
}
