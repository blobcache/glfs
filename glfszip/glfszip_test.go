package glfszip

import (
	"archive/zip"
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

var corpus = []string{
	"https://github.com/protocolbuffers/protobuf/releases/download/v22.0-rc1/protoc-22.0-rc-1-osx-x86_64.zip",
}

func TestImport(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	for _, tc := range corpus {
		tc := tc
		t.Run(path.Base(tc), func(t *testing.T) {
			ctx := context.Background()
			op := glfs.NewOperator()
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
	f, err := ensureData(u)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	finfo, err := f.Stat()
	require.NoError(t, err)

	zr, err := zip.NewReader(f, finfo.Size())
	require.NoError(t, err)

	return zr
}

func ensureData(u string) (*os.File, error) {
	if err := os.MkdirAll("./testdata", 0755); err != nil {
		return nil, err
	}
	p := filepath.Join("testdata", path.Base(u))
	f, err := os.Open(p)
	if !os.IsNotExist(err) {
		return f, err
	}
	f, err = os.Create(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if _, err = io.Copy(f, res.Body); err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	return os.Open(p)
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(cadata.DefaultHash, glfs.DefaultBlockSize)
}
