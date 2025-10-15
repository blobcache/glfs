package glfs

import (
	"bytes"
	"context"

	"blobcache.io/blobcache/src/schema"
)

func MustPostBlob(s schema.WO, x []byte) Ref {
	ref, err := PostBlob(context.Background(), s, bytes.NewReader(x))
	if err != nil {
		panic(err)
	}
	return *ref
}

func MustPostTreeSlice(s schema.WO, ents []TreeEntry) Ref {
	ref, err := PostTreeSlice(context.Background(), s, ents)
	if err != nil {
		panic(err)
	}
	return *ref
}

func MustPostTreeMap(s schema.WO, m map[string]Ref) Ref {
	ref, err := PostTreeMap(context.Background(), s, m)
	if err != nil {
		panic(err)
	}
	return *ref
}
