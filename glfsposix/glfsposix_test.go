package glfsposix

import (
	"context"
	"strings"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"blobcache.io/glfs"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/posixfs"
	"golang.org/x/sync/semaphore"
)

func TestImport(t *testing.T) {
	ctx := context.Background()
	ag := glfs.NewMachine()
	s := schema.NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), glfs.DefaultBlockSize)
	fs := posixfs.NewTestFS(t)
	require.NoError(t, posixfs.PutFile(ctx, fs, "test.txt", 0o644, strings.NewReader("hello world")))

	ref, err := Import(ctx, ag, semaphore.NewWeighted(1), s, fs, "")
	require.NoError(t, err)
	require.Equal(t, glfs.TypeTree, ref.Type)

	tree, err := ag.GetTreeSlice(ctx, s, *ref, 1)
	require.NoError(t, err)
	require.Len(t, tree, 1)
}

func TestExport(t *testing.T) {
	ctx := context.Background()
	op := glfs.NewMachine()
	s := schema.NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), glfs.DefaultBlockSize)
	fs := posixfs.NewTestFS(t)
	sem := semaphore.NewWeighted(10)

	ref, err := glfs.PostBlob(ctx, s, strings.NewReader("hello world"))
	require.NoError(t, err)

	ref, err = glfs.PostTreeMap(ctx, s, map[string]glfs.Ref{
		"hw.txt": *ref,
	})
	require.NoError(t, err)

	err = Export(ctx, op, sem, s, *ref, fs, "export_root")
	require.NoError(t, err)
}
