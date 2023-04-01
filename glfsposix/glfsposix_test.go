package glfsposix

import (
	"context"
	"strings"
	"testing"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/posixfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestImport(t *testing.T) {
	ctx := context.Background()
	op := glfs.NewOperator()
	s := cadata.NewMem(cadata.DefaultHash, glfs.DefaultBlockSize)
	fs := posixfs.NewTestFS(t)
	require.NoError(t, posixfs.PutFile(ctx, fs, "test.txt", 0o644, strings.NewReader("hello world")))

	ref, err := Import(ctx, &op, semaphore.NewWeighted(0), s, fs, "")
	require.NoError(t, err)
	require.Equal(t, glfs.TypeTree, ref.Type)

	tree, err := op.GetTree(ctx, s, *ref)
	require.NoError(t, err)
	require.Len(t, tree.Entries, 1)
}
