package glfs

import (
	"context"
	"io"
	"iter"

	"blobcache.io/glfs/bigblob"
	"go.brendoncarroll.net/state/cadata"
)

type Option func(*Machine)

// WithSalt sets the salt used for deriving encryption keys.
func WithSalt(salt [32]byte) Option {
	return func(ag *Machine) {
		ag.salt = &salt
	}
}

// Machine holds a configuration, and caches.
// Machine configuration is immutable once it is created.
// Any cache state should be transparent to the user, so the Machine
// should appear stateless.
// Machines are thread-safe.
//
// Creating a new Machine will perform better than using the default Machine when
// there are many concurrent operations being performed on unrelated filesystems,
// since the unreleated tasks won't be affecting the same cache.
//
// Repeated lookups within a given filesystem will be much faster when rerun on the same machine.
// This is because each hop must be read from the store and decrypted, the decrypted plaintext
// will be cached by the machine.
type Machine struct {
	salt      *[32]byte
	blockSize int

	bbag *bigblob.Machine
}

func NewMachine(opts ...Option) *Machine {
	o := &Machine{
		salt:      new([32]byte),
		blockSize: DefaultBlockSize,
	}
	o.bbag = bigblob.NewMachine(bigblob.WithBlockSize(o.blockSize))
	return o
}

func (ag *Machine) makeSalt(ty Type) *[32]byte {
	var out [32]byte
	bigblob.DeriveKey(out[:], ag.salt, []byte(ty))
	return &out
}

var defaultOp = NewMachine()

// PostRaw calls PostRaw on the default Machine
func PostTyped(ctx context.Context, s cadata.Poster, ty Type, r io.Reader) (*Ref, error) {
	return defaultOp.PostTyped(ctx, s, ty, r)
}

// PostBlob creates a new blob with data from r, and returns a Ref to it.
func PostBlob(ctx context.Context, s cadata.Poster, r io.Reader) (*Ref, error) {
	return defaultOp.PostBlob(ctx, s, r)
}

// GetBlob returns an io.ReadSeeker for accessing data from the blob at x
func GetBlob(ctx context.Context, s cadata.Getter, x Ref) (*Reader, error) {
	return defaultOp.GetBlob(ctx, s, x)
}

// GetBlobBytes reads the entire contents of the blob at x into memory and returns the slice of bytes.
func GetBlobBytes(ctx context.Context, s cadata.Getter, x Ref, maxSize int) ([]byte, error) {
	return defaultOp.GetBlobBytes(ctx, s, x, maxSize)
}

func PostTree(ctx context.Context, s cadata.PostExister, ents iter.Seq[TreeEntry]) (*Ref, error) {
	return defaultOp.PostTree(ctx, s, ents)
}

func PostTreeSlice(ctx context.Context, s cadata.PostExister, ents []TreeEntry) (*Ref, error) {
	return defaultOp.PostTreeSlice(ctx, s, ents)
}

func PostTreeMap(ctx context.Context, s cadata.PostExister, m map[string]Ref) (*Ref, error) {
	return defaultOp.PostTreeMap(ctx, s, m)
}

// GetTreeSlice retreives the tree in store at Ref if it exists.
// If ref.Type != TypeTree ErrRefType is returned.
func GetTreeSlice(ctx context.Context, store cadata.Getter, ref Ref, maxEnts int) ([]TreeEntry, error) {
	return defaultOp.GetTreeSlice(ctx, store, ref, maxEnts)
}

// GetAtPath returns a ref to the object under ref at subpath.
// ErrNoEnt is returned if there is no entry at that path.
func GetAtPath(ctx context.Context, store cadata.Getter, ref Ref, subpath string) (*Ref, error) {
	return defaultOp.GetAtPath(ctx, store, ref, subpath)
}

// WalkTree walks the tree and calls f with tree entries in lexigraphical order
// file1.txt comes before file2.txt
// dir1/ comes before dir1/file1.txt
func WalkTree(ctx context.Context, store cadata.Getter, ref Ref, f WalkTreeFunc) error {
	return defaultOp.WalkTree(ctx, store, ref, f)
}

// WalkRefs calls fn with every Ref reacheable from ref, including Ref. The only guarentee about order is bottom up.
// if a tree is encoutered the child refs will be visited first.
func WalkRefs(ctx context.Context, s cadata.Getter, ref Ref, fn RefWalker) error {
	return defaultOp.WalkRefs(ctx, s, ref, fn)
}

// Sync ensures that all data referenced by x exists in dst, copying from src if necessary.
// Sync assumes there are no dangling references, and skips copying data when its existence is implied.
func Sync(ctx context.Context, dst cadata.PostExister, src cadata.Getter, x Ref) error {
	return defaultOp.Sync(ctx, dst, src, x)
}

// FilterPaths returns a version of root with paths filtered using f as a predicate.
// If f returns true for a path it will be included in the output, otherwise it will not.
func FilterPaths(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, f func(string) bool) (*Ref, error) {
	return defaultOp.FilterPaths(ctx, dst, src, root, f)
}

// ShardLeaves calls ShardLeaves on the default Machine
func ShardLeaves(ctx context.Context, dst cadata.PostExister, s cadata.Getter, root Ref, n int) ([]Ref, error) {
	return defaultOp.ShardLeaves(ctx, dst, s, root, n)
}

// MapBlobs calls MapBlobs on the default Machine
func MapBlobs(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, f BlobMapper) (*Ref, error) {
	return defaultOp.MapBlobs(ctx, dst, src, root, f)
}

// MapLeaves calls MapLeaves on the default Machine
func MapLeaves(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, f RefMapper) (*Ref, error) {
	return defaultOp.MapLeaves(ctx, dst, src, root, f)
}

// MapEntryAt calls MapEntryAt on the default Machine
func MapEntryAt(ctx context.Context, dst cadata.PostExister, src cadata.Getter, root Ref, p string, f TreeEntryMapper) (*Ref, error) {
	return defaultOp.MapEntryAt(ctx, dst, src, root, p, f)
}

// Merge calls Merge on the default Machine
func Merge(ctx context.Context, dst cadata.PostExister, src cadata.Getter, layers ...Ref) (*Ref, error) {
	return defaultOp.Merge(ctx, dst, src, layers...)
}

// GC will remove objects from store which are not referenced by any of the refs in keep.
// If GC does not successfully complete, referential integrity may be violated, and GC will need
// to be run to completion before it is safe to call Sync on the store again.
func GC(ctx context.Context, store GetListDeleter, keep []Ref, opts ...GCOption) (*GCResult, error) {
	return defaultOp.GC(ctx, store, keep, opts...)
}
