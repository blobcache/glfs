package bigblob

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.brendoncarroll.net/state/cadata"
)

type Option func(*Agent)

func WithCacheSize(n int) Option {
	return func(ag *Agent) {
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
	return func(ag *Agent) {
		ag.blockSize = n
	}
}

type Agent struct {
	cacheSize int
	blockSize int

	cache   *lru.Cache[cadata.ID, []byte]
	bufPool sync.Pool
}

func NewAgent(opts ...Option) *Agent {
	o := Agent{
		cacheSize: 64,
		bufPool: sync.Pool{
			New: func() interface{} {
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

func (ag *Agent) acquireBuffer(n int) *[]byte {
	x := ag.bufPool.Get().(*[]byte)
	if len(*x) < n {
		*x = append(*x, make([]byte, n-len(*x))...)
	}
	return x
}

func (ag *Agent) releaseBuffer(x *[]byte) {
	ag.bufPool.Put(x)
}

func newCache(size int) *lru.Cache[cadata.ID, []byte] {
	cache, err := lru.New[cadata.ID, []byte](size)
	if err != nil {
		panic(err)
	}
	return cache
}

type AddExister interface {
	cadata.Adder
	cadata.Exister
}
