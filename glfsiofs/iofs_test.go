package glfsiofs

import (
	"context"
	"fmt"
	"path"
	"testing"
	"testing/fstest"

	"github.com/blobcache/glfs"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

func TestFS(t *testing.T) {
	s := newStore()
	tcs := []struct {
		Ref glfs.Ref
	}{
		{Ref: glfs.MustPostTreeMap(s, map[string]glfs.Ref{})},
		{
			Ref: glfs.MustPostTreeMap(s, map[string]glfs.Ref{
				"a": glfs.MustPostBlob(s, []byte("hello world")),
				"b": glfs.MustPostBlob(s, []byte("hello world")),
				"c": glfs.MustPostBlob(s, nil),
			}),
		},
		{
			Ref: glfs.MustPostTreeMap(s, map[string]glfs.Ref{
				"a": glfs.MustPostBlob(s, []byte("hello world")),
				"b-dir": glfs.MustPostTreeMap(s, map[string]glfs.Ref{
					"b.0": glfs.MustPostBlob(s, []byte("000000")),
					"b.1": glfs.MustPostBlob(s, []byte("11111")),
					"b.2": glfs.MustPostBlob(s, []byte("222222222222222")),
				}),
				"c": glfs.MustPostBlob(s, []byte("hello world")),
			}),
		},
	}
	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			fsys := New(s, tc.Ref)
			require.NoError(t, fstest.TestFS(fsys, listPaths(t, s, tc.Ref)...))
		})
	}
}

func listPaths(t testing.TB, s cadata.Getter, x glfs.Ref) (ret []string) {
	ctx := context.TODO()
	require.NoError(t, glfs.WalkTree(ctx, s, x, func(prefix string, tree glfs.TreeEntry) error {
		ret = append(ret, path.Join(prefix, tree.Name))
		return nil
	}))
	return ret
}

func newStore() cadata.Store {
	return cadata.NewMem(cadata.DefaultHash, 1<<21)
}
