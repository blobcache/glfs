package glfs

import (
	"bytes"
	"context"

	"go.brendoncarroll.net/state/cadata"
)

func MustPostBlob(s cadata.Poster, x []byte) Ref {
	ref, err := PostBlob(context.Background(), s, bytes.NewReader(x))
	if err != nil {
		panic(err)
	}
	return *ref
}

func MustPostTreeSlice(s cadata.PostExister, ents []TreeEntry) Ref {
	ref, err := PostTreeSlice(context.Background(), s, ents)
	if err != nil {
		panic(err)
	}
	return *ref
}

func MustPostTreeMap(s cadata.PostExister, m map[string]Ref) Ref {
	ref, err := PostTreeMap(context.Background(), s, m)
	if err != nil {
		panic(err)
	}
	return *ref
}
