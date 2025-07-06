package glfstar

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/sync/errgroup"

	"blobcache.io/glfs"
)

var corpus = []string{
	"alpine-minirootfs.tar",
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
			op := glfs.NewMachine()
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
	op := glfs.NewMachine()
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

func withTARStream(t *testing.T, p string, fn func(r *tar.Reader)) {
	f, err := os.Open(filepath.Join("testdata", p))
	require.NoError(t, err)
	defer f.Close()
	tr := tar.NewReader(f)
	fn(tr)
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(cadata.DefaultHash, glfs.DefaultBlockSize)
}
