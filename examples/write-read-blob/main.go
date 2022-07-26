package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
)

func main() {
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	ref, err := glfs.PostBlob(ctx, s, strings.NewReader("test data"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ref)
	r, err := glfs.GetBlob(ctx, s, *ref)
	if err != nil {
		log.Fatal(err)
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(data)
}
