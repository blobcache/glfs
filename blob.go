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
	return o.PostTyped(ctx, s, TypeBlob, r)
}

// GetBlob returns an io.ReadSeeker for accessing data from the blob at x
func (o *Operator) GetBlob(ctx context.Context, s cadata.Getter, x Ref) (*Reader, error) {
	return o.GetTyped(ctx, s, TypeBlob, x)
}

// GetBlobBytes reads the entire contents of the blob at x into memory and returns the slice of bytes.
func (o *Operator) GetBlobBytes(ctx context.Context, s cadata.Getter, x Ref) ([]byte, error) {
	r, err := o.GetBlob(ctx, s, x)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

func (o *Operator) NewBlobWriter(ctx context.Context, s cadata.Poster) *TypedWriter {
	return o.NewTypedWriter(ctx, s, TypeBlob)
}
