package glfs

import (
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestShardLeaves(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	testCases := []map[string]Ref{
		{
			"dir1/file1.1": blobRef(),
			"dir2/file2.1": blobRef(),
		},
		generateTree(100),
	}

	for _, tc := range testCases {
		x, err := PostTreeFromMap(ctx, s, tc)
		require.Nil(t, err)
		logTree(ctx, t, s, *x)
		shards, err := ShardLeaves(ctx, s, *x, 4)
		require.Nil(t, err)
		t.Log(shards)
		y, err := Merge(ctx, s, shards...)
		require.Nil(t, err)
		logTree(ctx, t, s, *y)
		require.Equal(t, *x, *y)
	}
}

func generateTree(n int) map[string]Ref {
	m := map[string]Ref{}
	for i := 0; i < n; i++ {
		p := strconv.Itoa(i/10) + "/" + strconv.Itoa(i)
		m[p] = blobRef()
	}
	return m
}
