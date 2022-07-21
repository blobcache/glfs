package bigfile

import (
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

	cache *lru.Cache
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		cacheSize: 64,
		compCodec: CompressSnappy,
	}
	for _, opt := range opts {
		opt(&o)
	}
	o.cache = newCache(o.cacheSize)
	return o
}

func newCache(size int) *lru.Cache {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}
	return cache
}
