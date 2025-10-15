package bigblob

import (
	"context"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
	lru "github.com/hashicorp/golang-lru/v2"
	"go.brendoncarroll.net/state/cadata"
)

type Option func(*Machine)

func WithCacheSize(n int) Option {
	return func(ag *Machine) {
		ag.cacheSize = n
	}
}

// WithBlockSize sets the block size used when writing files.
// If n < 0 then WithBlockSize panics
// If n == 0 then the store's MaxBlobSize will be used as a default.
func WithBlockSize(n int) Option {
	if n < 0 {
		panic(n)
	}
	return func(ag *Machine) {
		ag.blockSize = n
	}
}

// Machine contains configuration options and caches.
type Machine struct {
	cacheSize int
	blockSize int

	cache   *lru.Cache[blobcache.CID, []byte]
	bufPool sync.Pool
}

func NewMachine(opts ...Option) *Machine {
	o := Machine{
		cacheSize: 64,
		bufPool: sync.Pool{
			New: func() any {
				buf := []byte(nil)
				return &buf
			},
		},
	}
	for _, opt := range opts {
		opt(&o)
	}
	o.cache = newCache(o.cacheSize)
	return &o
}

func (ag *Machine) acquireBuffer(n int) *[]byte {
	x := ag.bufPool.Get().(*[]byte)
	if len(*x) < n {
		*x = append(*x, make([]byte, n-len(*x))...)
	}
	return x
}

func (ag *Machine) releaseBuffer(x *[]byte) {
	ag.bufPool.Put(x)
}

func newCache(size int) *lru.Cache[cadata.ID, []byte] {
	cache, err := lru.New[cadata.ID, []byte](size)
	if err != nil {
		panic(err)
	}
	return cache
}

type Exister interface {
	Exists(ctx context.Context, cids []blobcache.CID, exists []bool) error
}

type AddExister interface {
	Exister
	Add(ctx context.Context, cid blobcache.CID) error
}

func ExistsUnit(ctx context.Context, ex Exister, cid blobcache.CID) (bool, error) {
	var exists [1]bool
	if err := ex.Exists(ctx, []blobcache.CID{cid}, exists[:]); err != nil {
		return false, err
	}
	return exists[0], nil
}
