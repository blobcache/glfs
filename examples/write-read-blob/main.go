package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
)

func main() {
	ctx := context.Background()
	s := schema.NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), 1<<20)
	ref, err := glfs.PostBlob(ctx, s, strings.NewReader("test data"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ref)
	r, err := glfs.GetBlob(ctx, s, *ref)
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(data)
}
