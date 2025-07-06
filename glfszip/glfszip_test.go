package glfszip

import (
	"archive/zip"
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"blobcache.io/glfs"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

var corpus = []string{
	"protoc.zip",
}

func TestImport(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	for _, tc := range corpus {
		tc := tc
		t.Run(path.Base(tc), func(t *testing.T) {
			ctx := context.Background()
			op := glfs.NewMachine()
			s := newStore(t)

			zr := newZipReader(t, tc)
			ref, err := Import(ctx, op, s, zr)
			require.NoError(t, err)
			t.Log(ref)
			err = glfs.WalkTree(ctx, s, *ref, func(prefix string, ent glfs.TreeEntry) error {
				t.Log(prefix, ent.Name, ent.FileMode)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func newZipReader(t testing.TB, u string) *zip.Reader {
	f, err := os.Open(filepath.Join("testdata", u))
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	finfo, err := f.Stat()
	require.NoError(t, err)

	zr, err := zip.NewReader(f, finfo.Size())
	require.NoError(t, err)

	return zr
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(cadata.DefaultHash, glfs.DefaultBlockSize)
}
