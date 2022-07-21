package glfs

import (
	"context"
	"fmt"
	"io"

	"github.com/blobcache/glfs/bigfile"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/pkg/errors"
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
		return "", errors.Errorf("%q is not a valid type", x)
	}
}

// Ref is a reference to a glfs Object, which could be:
// - Tree
// - Blob
type Ref struct {
	Type Type `json:"type"`
	bigfile.Root
	Fingerprint Fingerprint `json:"fp"`
}

func (r Ref) String() string {
	fp := r.Fingerprint
	return fmt.Sprintf("%s %s", r.Type, fp.String()[:8])
}

func (a Ref) Equals(b Ref) bool {
	return a.Type == b.Type && a.Fingerprint == b.Fingerprint
}

// PostRaw posts data with an arbitrary type.
// This can be used to extend the types provided by glfs, without interfering with syncing.
func (o *Operator) PostRaw(ctx context.Context, s cadata.Store, ty Type, r io.Reader) (*Ref, error) {
	fpw := NewFPWriter()
	bw := o.bfop.NewWriter(ctx, s, s.MaxSize(), o.makeSalt(ty))
	mw := io.MultiWriter(bw, fpw)
	if _, err := io.Copy(mw, r); err != nil {
		return nil, err
	}
	root, err := bw.Finish(ctx)
	if err != nil {
		return nil, err
	}
	return &Ref{
		Type:        ty,
		Root:        *root,
		Fingerprint: fpw.Finish(),
	}, nil
}

// GetRaw retrieves the object in s at x.
// If x.Type != ty, ErrRefType is returned.
func (o *Operator) GetRaw(ctx context.Context, s cadata.Store, ty Type, x Ref) (*Reader, error) {
	if ty != "" && x.Type != ty {
		return nil, ErrRefType{Have: x.Type, Want: TypeBlob}
	}
	return o.bfop.NewReader(ctx, s, x.Root), nil
}

// SizeOf returns the size of the data at x
func SizeOf(x Ref) uint64 {
	return x.Size
}
