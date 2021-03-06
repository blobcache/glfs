package glfs

import (
	"context"
	"io"

	"github.com/blobcache/glfs/bigfile"
	"github.com/brendoncarroll/go-state/cadata"
)

type Option func(*Operator)

// WithSalt sets the salt used for deriving encryption keys.
func WithSalt(salt [32]byte) Option {
	return func(o *Operator) {
		o.salt = &salt
	}
}

// WithCompression sets the compression algorithm to use when creating blobs.
func WithCompression(cc bigfile.CompressionCodec) Option {
	return func(o *Operator) {
		o.compCodec = cc
	}
}

type Operator struct {
	salt      *[32]byte
	compCodec bigfile.CompressionCodec

	bfop bigfile.Operator
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		salt:      new([32]byte),
		compCodec: bigfile.CompressSnappy,
	}
	o.bfop = bigfile.NewOperator(bigfile.WithCompression(o.compCodec))
	return o
}

func (o *Operator) makeSalt(ty Type) *[32]byte {
	var out [32]byte
	bigfile.DeriveKey(out[:], o.salt, []byte(ty))
	return &out
}

var defaultOp = NewOperator()

// PostRaw calls PostRaw on the default Operator
func PostRaw(ctx context.Context, s cadata.Store, ty Type, r io.Reader) (*Ref, error) {
	return defaultOp.PostRaw(ctx, s, ty, r)
}

// PostBlob creates a new blob with data from r, and returns a Ref to it.
func PostBlob(ctx context.Context, s cadata.Store, r io.Reader) (*Ref, error) {
	return defaultOp.PostBlob(ctx, s, r)
}

// GetBlob returns an io.ReadSeeker for accessing data from the blob at x
func GetBlob(ctx context.Context, s cadata.Store, x Ref) (*Reader, error) {
	return defaultOp.GetBlob(ctx, s, x)
}

// GetBlobBytes reads the entire contents of the blob at x into memory and returns the slice of bytes.
func GetBlobBytes(ctx context.Context, s cadata.Store, x Ref) ([]byte, error) {
	return defaultOp.GetBlobBytes(ctx, s, x)
}

// PostTree writes a tree to CA storage and returns a Ref pointing to it.
func PostTree(ctx context.Context, store cadata.Store, t Tree) (*Ref, error) {
	return defaultOp.PostTree(ctx, store, t)
}

func PostTreeFromEntries(ctx context.Context, s cadata.Store, ents []TreeEntry) (*Ref, error) {
	return defaultOp.PostTreeFromEntries(ctx, s, ents)
}

func PostTreeFromMap(ctx context.Context, s cadata.Store, m map[string]Ref) (*Ref, error) {
	return defaultOp.PostTreeFromMap(ctx, s, m)
}

// GetTree retreives the tree in store at Ref if it exists.
// If ref.Type != TypeTree ErrRefType is returned.
func GetTree(ctx context.Context, store cadata.Store, ref Ref) (*Tree, error) {
	return defaultOp.GetTree(ctx, store, ref)
}

// GetAtPath returns a ref to the object under ref at subpath.
// ErrNoEnt is returned if there is no entry at that path.
func GetAtPath(ctx context.Context, store cadata.Store, ref Ref, subpath string) (*Ref, error) {
	return defaultOp.GetAtPath(ctx, store, ref, subpath)
}

// WalkTree walks the tree and calls f with tree entries in lexigraphical order
// file1.txt comes before file2.txt
// dir1/ comes before dir1/file1.txt
func WalkTree(ctx context.Context, store cadata.Store, ref Ref, f WalkTreeFunc) error {
	return defaultOp.WalkTree(ctx, store, ref, f)
}

// WalkRefs calls fn with every Ref reacheable from ref, including Ref. The only guarentee about order is bottom up.
// if a tree is encoutered the child refs will be visited first.
func WalkRefs(ctx context.Context, s cadata.Store, ref Ref, fn RefWalker) error {
	return defaultOp.WalkRefs(ctx, s, ref, fn)
}

// Sync ensures that all data referenced by x exists in dst, copying from src if necessary.
// Sync assumes there are no dangling references, and skips copying data when its existence is implied.
func Sync(ctx context.Context, dst, src cadata.Store, x Ref) error {
	return defaultOp.Sync(ctx, dst, src, x)
}

// FilterPaths returns a version of root with paths filtered using f as a predicate.
// If f returns true for a path it will be included in the output, otherwise it will not.
func FilterPaths(ctx context.Context, s cadata.Store, root Ref, f func(string) bool) (*Ref, error) {
	return defaultOp.FilterPaths(ctx, s, root, f)
}

// ShardLeaves calls ShardLeaves on the default Operator
func ShardLeaves(ctx context.Context, s cadata.Store, root Ref, n int) ([]Ref, error) {
	return defaultOp.ShardLeaves(ctx, s, root, n)
}

// MapBlobs calls MapBlobs on the default Operator
func MapBlobs(ctx context.Context, s cadata.Store, root Ref, f BlobMapper) (*Ref, error) {
	return defaultOp.MapBlobs(ctx, s, root, f)
}

// MapLeaves calls MapLeaves on the default Operator
func MapLeaves(ctx context.Context, s cadata.Store, root Ref, f RefMapper) (*Ref, error) {
	return defaultOp.MapLeaves(ctx, s, root, f)
}

// MapEntryAt calls MapEntryAt on the default Operator
func MapEntryAt(ctx context.Context, s cadata.Store, root Ref, p string, f TreeEntryMapper) (*Ref, error) {
	return defaultOp.MapEntryAt(ctx, s, root, p, f)
}

// Merge calls Merge on the default Operator
func Merge(ctx context.Context, store cadata.Store, layers ...Ref) (*Ref, error) {
	return defaultOp.Merge(ctx, store, layers...)
}

// GC will remove objects from store which are not referenced by any of the refs in keep.
// If GC does not successfully complete, referential integrity may be violated, and GC will need
// to be run to completion before it is safe to call Sync on the store again.
func GC(ctx context.Context, store GCStore, prefix []byte, keep ...Ref) (*GCStats, error) {
	return defaultOp.GC(ctx, store, prefix, keep)
}
