package bigfile

import lru "github.com/hashicorp/golang-lru"

type Option func(*Operator)

func WithCacheSize(n int) Option {
	return func(o *Operator) {
		o.cacheSize = n
	}
}

type Operator struct {
	cacheSize int
	cache     *lru.Cache
}

func NewOperator(opts ...Option) Operator {
	o := Operator{
		cacheSize: 64,
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
