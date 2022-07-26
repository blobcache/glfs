package bigfile

import (
	"bytes"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

type Option func(*Operator)

func WithCacheSize(n int) Option {
	return func(o *Operator) {
		o.cacheSize = n
	}
}

func WithCompression(cc CompressionCodec) Option {
	if len(cc) > 4 {
		panic(cc)
	}
	return func(o *Operator) {
		o.compCodec = cc
	}
}

type Operator struct {
	cacheSize int
	compCodec CompressionCodec

	cache   *lru.Cache
	bufPool sync.Pool
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		cacheSize: 64,
		compCodec: CompressSnappy,
		bufPool: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
	for _, opt := range opts {
		opt(&o)
	}
	o.cache = newCache(o.cacheSize)
	return o
}

func (o *Operator) acquireBuffer() *bytes.Buffer {
	x := o.bufPool.Get().(*bytes.Buffer)
	x.Reset()
	return x
}

func (o *Operator) releaseBuffer(x *bytes.Buffer) {
	x.Reset()
	o.bufPool.Put(x)
}

func newCache(size int) *lru.Cache {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}
	return cache
}
