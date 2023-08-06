package bigblob

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

type Option func(*Operator)

func WithCacheSize(n int) Option {
	return func(o *Operator) {
		o.cacheSize = n
	}
}

// WithBlockSize sets the block size used when writing files.
// If n < 0 then WithBlockSize panics
// If n == 0 then the stores MaxBlobSize will be used as a default.
func WithBlockSize(n int) Option {
	if n < 0 {
		panic(n)
	}
	return func(o *Operator) {
		o.blockSize = n
	}
}

type Operator struct {
	cacheSize int
	blockSize int

	cache   *lru.Cache
	bufPool sync.Pool
}

func NewOperator(opts ...Option) *Operator {
	o := Operator{
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

func (o *Operator) acquireBuffer(n int) *[]byte {
	x := o.bufPool.Get().(*[]byte)
	if len(*x) < n {
		*x = append(*x, make([]byte, n-len(*x))...)
	}
	return x
}

func (o *Operator) releaseBuffer(x *[]byte) {
	o.bufPool.Put(x)
}

func newCache(size int) *lru.Cache {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}
	return cache
}
