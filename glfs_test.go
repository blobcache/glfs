package glfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsErrNoEnt(t *testing.T) {
	err := ErrNoEnt{}
	require.True(t, IsErrNoEnt(err))
}

func TestSync(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	tcs := []struct {
		Ref Ref
		Err error
	}{
		{Ref: MustPostBlob(src, nil)},
		{Ref: MustPostTreeMap(src, map[string]Ref{
			"a": MustPostBlob(src, []byte("hello")),
			"b": MustPostBlob(src, []byte("world")),
			"c": MustPostBlob(src, nil),
		})},
	}
	for i, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			dst := newStore(t)
			err := Sync(ctx, dst, src, tc.Ref)
			if tc.Err != nil {
				require.ErrorIs(t, tc.Err, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
