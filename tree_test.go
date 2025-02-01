package glfs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestPostTreeFromEntries(t *testing.T) {
	ctx := context.TODO()
	s := newStore(t)
	m1 := map[string]Ref{
		"dir1/file1.1": blobRef(t, s),
		"dir1/file1.2": blobRef(t, s),
		"dir2/file2.1": blobRef(t, s),
	}
	ref, err := PostTreeMap(ctx, s, m1)
	require.Nil(t, err)

	for k := range m1 {
		assertBlobAtPath(t, s, *ref, k)
	}
}

func TestTreeNoEnt(t *testing.T) {
	ctx := context.TODO()
	s := newStore(t)
	m1 := map[string]Ref{
		"dir1/file1.1": blobRef(t, s),
		"dir1/file1.2": blobRef(t, s),
		"dir2/file2.1": blobRef(t, s),
	}
	ref, err := PostTreeMap(ctx, s, m1)
	require.NoError(t, err)

	_, err = GetAtPath(ctx, s, *ref, "should-not-exist")
	require.True(t, IsErrNoEnt(err))
}

func TestMergeSubtrees(t *testing.T) {
	ctx := context.TODO()
	s := newStore(t)
	ms := []map[string]Ref{
		{
			"dir1/file1.1": blobRef(t, s),
		},
		{
			"dir1/file1.2": blobRef(t, s),
			"dir1/file1.3": blobRef(t, s),
		},
		{
			"dir1/file1.1": blobRef(t, s),
			"dir2/file2.1": blobRef(t, s),
		},
	}

	layers := []Ref{}
	for _, m := range ms {
		ref, err := PostTreeMap(ctx, s, m)
		require.Nil(t, err)
		layers = append(layers, *ref)
	}

	ref, err := Merge(ctx, s, s, layers...)
	require.Nil(t, err)
	tree, err := GetTreeSlice(ctx, s, *ref, 2)
	require.Nil(t, err)

	if assert.Len(t, tree, 2) {
		assertTreeExists(t, s, tree[0].Ref)
		assertTreeExists(t, s, tree[1].Ref)
	}

	assertBlobAtPath(t, s, *ref, "dir2/file2.1")
	assertBlobAtPath(t, s, *ref, "dir2/file2.1")
}

func TestDataNotFound(t *testing.T) {
	ctx := context.TODO()
	s := newStore(t)

	ref := mustPostTree(t, s, map[string]Ref{
		"a": mustPostBlob(t, s, []byte("hello a")),
		"b": mustPostBlob(t, s, []byte("hello b")),
		"c": mustPostBlob(t, s, []byte("hello c")),
	})
	require.NoError(t, s.Delete(ctx, ref.CID))
	ref2, err := GetAtPath(ctx, s, ref, "a")
	require.ErrorIs(t, err, cadata.ErrNotFound{Key: ref.CID})
	require.Nil(t, ref2)
}

func mustPostTree(t testing.TB, s cadata.PostExister, m map[string]Ref) Ref {
	ctx := context.TODO()
	ref, err := PostTreeMap(ctx, s, m)
	require.NoError(t, err)
	return *ref
}

func mustPostBlob(t testing.TB, s cadata.Poster, data []byte) Ref {
	ctx := context.TODO()
	ref, err := PostBlob(ctx, s, bytes.NewReader(data))
	require.NoError(t, err)
	return *ref
}

func assertTreeExists(t *testing.T, s cadata.Store, ref Ref) bool {
	ctx := context.TODO()
	_, err := GetTreeSlice(ctx, s, ref, 1e6)
	if err != nil {
		logRaw(t, s, ref)
	}
	return assert.Nil(t, err)
}

func assertBlobAtPath(t *testing.T, s cadata.Store, root Ref, p string) bool {
	ctx := context.TODO()
	ref, err := GetAtPath(ctx, s, root, p)
	return assert.Nil(t, err) &&
		assert.NotNil(t, *ref) &&
		assert.Equal(t, ref.Type, TypeBlob)
}

func logTree(ctx context.Context, t *testing.T, s cadata.Store, ref Ref) {
	tree, err := GetTreeSlice(ctx, s, ref, 1e6)
	require.Nil(t, err)
	t.Log(tree)
}

func logRaw(t *testing.T, s cadata.Store, ref Ref) {
	ctx := context.TODO()
	r := defaultOp.bbag.NewReader(ctx, s, ref.Root)
	data, _ := io.ReadAll(r)
	t.Log(string(data))
}

func blobRef(t testing.TB, s cadata.Poster) Ref {
	ref, err := PostBlob(context.TODO(), s, bytes.NewReader(nil))
	require.NoError(t, err)
	return *ref
}
