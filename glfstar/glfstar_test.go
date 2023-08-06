package glfstar

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/blobcache/glfs"
)

var corpus = []string{
	//"https://cdimage.ubuntu.com/ubuntu-base/releases/20.04/release/ubuntu-base-20.04-base-amd64.tar.gz",
	"https://dl-cdn.alpinelinux.org/alpine/latest-stable/releases/x86_64/alpine-minirootfs-3.17.0-x86_64.tar.gz",
}

func TestReadTAR(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	tcs := corpus
	for _, tc := range tcs {
		tc := tc
		t.Run(path.Base(tc), func(t *testing.T) {
			ctx := context.Background()
			s := newStore(t)
			op := glfs.NewOperator()
			withTARStream(t, tc, func(tr *tar.Reader) {
				ref, err := ReadTAR(ctx, op, s, tr)
				require.NoError(t, err)
				require.NotNil(t, ref)
				err = glfs.WalkRefs(ctx, s, *ref, func(ref glfs.Ref) error {
					return nil
				})
				require.NoError(t, err)
			})
		})
	}
}

func TestWriteRead(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	i := 0
	ctx := context.Background()
	s := newStore(t)
	op := glfs.NewOperator()
	withTARStream(t, corpus[i], func(tr *tar.Reader) {
		expected, err := ReadTAR(ctx, op, s, tr)
		require.NoError(t, err)
		require.NotNil(t, expected)

		eg := errgroup.Group{}
		r, w := io.Pipe()
		eg.Go(func() error {
			tw := tar.NewWriter(w)
			defer tw.Close()
			if err := WriteTAR(ctx, op, s, *expected, tw); err != nil {
				return err
			}
			return tw.Close()
		})
		var actual *glfs.Ref
		eg.Go(func() error {
			tr := tar.NewReader(r)
			actual, err = ReadTAR(ctx, op, s, tr)
			return err
		})
		require.NoError(t, eg.Wait())
		require.Equal(t, expected, actual)
	})
}

func withTARStream(t *testing.T, u string, fn func(r *tar.Reader)) {
	f, err := ensureData(u)
	require.NoError(t, err)
	defer f.Close()
	r, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer r.Close()
	tr := tar.NewReader(r)
	fn(tr)
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
