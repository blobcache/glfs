package glfs

import (
	"context"
	"fmt"
	"io"

	"github.com/blobcache/glfs/bigblob"
	"github.com/brendoncarroll/go-state/cadata"
)

const DefaultBlockSize = 1 << 21

// Type is the type of data pointed to by a Ref
type Type string

const (
	TypeBlob = Type("blob")
	TypeTree = Type("tree")
)

func ParseType(x []byte) (Type, error) {
	ty := Type(x)
	switch ty {
	case TypeBlob, TypeTree:
		return ty, nil
	default:
		return "", fmt.Errorf("%q is not a valid type", x)
	}
}

// Ref is a reference to a glfs Object, which could be:
// - Tree
// - Blob
type Ref struct {
	Type Type `json:"type"`
	bigblob.Root
}

func (r Ref) String() string {
	return fmt.Sprintf("%s %s", r.Type, r.Root.CID.String()[:8])
}

func (a Ref) Equals(b Ref) bool {
	return a.Type == b.Type && a.Root.Equals(b.Root)
}

// PostTyped posts data with an arbitrary type.
// This can be used to extend the types provided by glfs, without interfering with syncing.
func (ag *Agent) PostTyped(ctx context.Context, s cadata.Poster, ty Type, r io.Reader) (*Ref, error) {
	tw := ag.NewTypedWriter(ctx, s, ty)
	if _, err := io.Copy(tw, r); err != nil {
		return nil, err
	}
	return tw.Finish(ctx)
}

// GetTyped retrieves the object in s at x.
// If x.Type != ty, ErrRefType is returned.
func (ag *Agent) GetTyped(ctx context.Context, s cadata.Getter, ty Type, x Ref) (*Reader, error) {
	if ty != "" && x.Type != ty {
		return nil, ErrRefType{Have: x.Type, Want: TypeBlob}
	}
	return ag.bbag.NewReader(ctx, s, x.Root), nil
}

type TypedWriter struct {
	ty Type
	bw *bigblob.Writer
}

func (tw *TypedWriter) Write(data []byte) (int, error) {
	return tw.bw.Write(data)
}

func (tw *TypedWriter) Finish(ctx context.Context) (*Ref, error) {
	root, err := tw.bw.Finish(ctx)
	if err != nil {
		return nil, err
	}
	return &Ref{Root: *root, Type: tw.ty}, nil
}

// NewTypedWriter returns a new writer for ty.
func (ag *Agent) NewTypedWriter(ctx context.Context, s cadata.Poster, ty Type) *TypedWriter {
	return &TypedWriter{ty: ty, bw: ag.bbag.NewWriter(ctx, s, ag.makeSalt(ty))}
}

// SizeOf returns the size of the data at x
func SizeOf(x Ref) uint64 {
	return x.Size
}

type GetPoster = cadata.GetPoster

type PostLister interface {
	cadata.Poster
	cadata.Lister
}
