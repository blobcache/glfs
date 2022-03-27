package bigfile

import "github.com/pkg/errors"

// maxRefSize is the size of a slot in an index
const maxRefSize = 128

type Index struct {
	x []byte
}

func newIndex(blockSize int) Index {
	return Index{x: make([]byte, blockSize)}
}

func newIndexUsing(x []byte, blockSize int) (Index, error) {
	if len(x) != blockSize {
		return Index{}, errors.Errorf("data is not correct size for index")
	}
	return Index{x: x}, nil
}

func (idx Index) Get(i int) Ref {
	start := i * maxRefSize
	end := (i + 1) * maxRefSize
	ref, err := RefFromBytes(idx.x[start:end])
	if err != nil {
		panic(err)
	}
	return *ref
}

func (idx Index) Set(i int, ref Ref) {
	start := i * maxRefSize
	end := (i + 1) * maxRefSize
	buf := idx.x[start:end]
	copy(buf, marshalRef(ref))
}

func (idx Index) Len() int {
	return len(idx.x) / maxRefSize
}

func (idx Index) Clear() {
	for i := range idx.x {
		idx.x[i] = 0
	}
}
