package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/blobcache/glfs"
	"go.brendoncarroll.net/state/cadata"
)

func main() {
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, glfs.DefaultBlockSize)
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
