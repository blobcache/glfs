package bigblob

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"go.brendoncarroll.net/state/cadata"
	"github.com/stretchr/testify/require"
)

func TestDepth(t *testing.T) {
	const blockSize = 1 << 10
	bf := int(branchingFactor(uint64(blockSize)))
	testCases := []struct {
		BlockSize, Size int
		Depth           int
	}{
		{1 << 10, 0, 0},
		{1 << 10, 1 << 10, 0},
		{1 << 10, 1<<10 + 1, 1},
		{1 << 10, 1 << 12, 1},
		{1 << 10, 8192, 1},

		{blockSize, blockSize*bf - 1, 1},
		{blockSize, blockSize * bf, 1},
		{blockSize, blockSize*bf + 1, 2},

		{blockSize, blockSize*bf*bf - 1, 2},
		{blockSize, blockSize * bf * bf, 2},
		{blockSize, blockSize*bf*bf + 1, 3},

		{blockSize, blockSize*bf*bf*bf - 1, 3},
		{blockSize, blockSize * bf * bf * bf, 3},
		{blockSize, blockSize*bf*bf*bf + 1, 4},
	}
	for i, tc := range testCases {
		d := depth(uint64(tc.Size), uint64(tc.BlockSize))
		require.Equal(t, tc.Depth, d, "%d) BlockSize=%d, Size=%d => Depth=%d, Expected=%d", i, tc.BlockSize, tc.Size, d, tc.Depth)
	}
}

func TestCreateFile(t *testing.T) {
	const defaultMaxSize = 1 << 20
	ctx := context.Background()
	ag := NewAgent()
	s := cadata.NewMem(cadata.DefaultHash, defaultMaxSize)

	const size = defaultMaxSize * 3
	rng := rand.New(rand.NewSource(0))
	r := io.LimitReader(rng, size)
	f, err := ag.Create(ctx, s, nil, r)
	require.Nil(t, err)
	require.NotNil(t, f)
	require.Equal(t, uint64(size), f.Size)

	exists, err := s.Exists(ctx, f.Ref.CID)
	require.Nil(t, err)
	require.True(t, exists)
	require.Equal(t, 4, s.Len())
}

func TestCreateRead(t *testing.T) {
	const blockSize = 1 << 10 // aritificially small
	bf := int(branchingFactor(blockSize))
	for _, size := range []int{
		0,
		1,
		100,
		blockSize / 2,
		blockSize,

		blockSize * 2,
		blockSize*2 - 1,
		blockSize*2 + 1,

		blockSize * bf,
		blockSize*bf + 1,
		blockSize*bf - 1,

		blockSize * bf * bf,
		blockSize*bf*bf + 1,
		blockSize*bf*bf - 1,
	} {
		size := size
		t.Run(fmt.Sprintf("CreateRead-%d", size), func(t *testing.T) {
			testCreateRead(t, size, blockSize)
		})
	}
}

func testCreateRead(t *testing.T, size, blockSize int) {
	ctx := context.Background()
	ag := NewAgent()
	s := cadata.NewMem(cadata.DefaultHash, blockSize)
	newRNG := func() io.Reader { return io.LimitReader(rand.New(rand.NewSource(0)), int64(size)) }

	root, err := ag.Create(ctx, s, nil, newRNG())
	require.NoError(t, err)
	r := ag.NewReader(ctx, s, *root)
	streamsEqual(t, newRNG(), r)
}

func streamsEqual(t *testing.T, a, b io.Reader) {
	brA := bufio.NewReader(a)
	brB := bufio.NewReader(b)
	for i := 0; true; i++ {
		aByte, errA := brA.ReadByte()
		bByte, errB := brB.ReadByte()
		if errA != nil || errB != nil {
			require.Equal(t, errA, errB)
			break
		}
		if aByte != bByte {
			t.Fatalf("streams differ at %d. a: 0x%02x, b: 0x%02x", i, aByte, bByte)
		}
	}
}
