package glfs

import (
	"bytes"
	"context"
	"io"

	"github.com/blobcache/glfs/bigblob"
	"go.brendoncarroll.net/state/cadata"
)

type Reader = bigblob.Reader

// PostBlob creates a new blob with data from r, and returns a Ref to it.
func (ag *Agent) PostBlob(ctx context.Context, s cadata.Poster, r io.Reader) (*Ref, error) {
	return ag.PostTyped(ctx, s, TypeBlob, r)
}

// GetBlob returns an io.ReadSeeker for accessing data from the blob at x
func (ag *Agent) GetBlob(ctx context.Context, s cadata.Getter, x Ref) (*Reader, error) {
	return ag.NewBlobReader(ctx, s, x)
}

// GetBlobBytes reads the entire contents of the blob at x into memory and returns the slice of bytes.
func (ag *Agent) GetBlobBytes(ctx context.Context, s cadata.Getter, x Ref, maxSize int) ([]byte, error) {
	r, err := ag.GetBlob(ctx, s, x)
	if err != nil {
		return nil, err
	}
	return readAtMost(r, maxSize)
}

func (ag *Agent) NewBlobReader(ctx context.Context, s cadata.Getter, x Ref) (*Reader, error) {
	return ag.GetTyped(ctx, s, TypeBlob, x)
}

func (ag *Agent) NewBlobWriter(s cadata.Poster) *TypedWriter {
	return ag.NewTypedWriter(s, TypeBlob)
}

func readAtMost(r io.Reader, maxSize int) ([]byte, error) {
	var buf bytes.Buffer
	r = io.LimitReader(r, int64(maxSize))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
