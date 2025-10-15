package glfs

import (
	"context"
	"strconv"
	"testing"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
	"github.com/stretchr/testify/require"
)

func TestShardLeaves(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	testCases := []map[string]Ref{
		{
			"dir1/file1.1": blobRef(t, s),
			"dir2/file2.1": blobRef(t, s),
		},
		generateTree(t, s, 100),
	}

	for _, tc := range testCases {
		x, err := PostTreeMap(ctx, s, tc)
		require.Nil(t, err)
		logTree(ctx, t, s, *x)
		shards, err := ShardLeaves(ctx, s, s, *x, 4)
		require.Nil(t, err)
		t.Log(shards)
		y, err := Merge(ctx, s, s, shards...)
		require.Nil(t, err)
		logTree(ctx, t, s, *y)
		require.Equal(t, *x, *y)
	}
}

func generateTree(t testing.TB, s schema.WO, n int) map[string]Ref {
	m := map[string]Ref{}
	for i := 0; i < n; i++ {
		p := strconv.Itoa(i/10) + "/" + strconv.Itoa(i)
		m[p] = blobRef(t, s)
	}
	return m
}

func newStore(_ testing.TB) *schema.MemStore {
	return schema.NewMem(blobcache.HashAlgo_BLAKE3_256.HashFunc(), DefaultBlockSize)
}
