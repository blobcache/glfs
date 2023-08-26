package bigblob

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestRefPostGet(t *testing.T) {
	ctx := context.TODO()
	ag := NewAgent()
	s := cadata.NewMem(cadata.DefaultHash, 1<<10)
	testData := "test data"
	ref, err := ag.post(ctx, s, new([32]byte), []byte(testData))
	require.NoError(t, err)
	err = ag.getF(ctx, s, *ref, func(data []byte) error {
		require.Equal(t, testData, string(data))
		return nil
	})
	require.NoError(t, err)
}

func TestRefMarshal(t *testing.T) {
	ctx := context.TODO()
	ag := NewAgent()
	s := cadata.NewMem(cadata.DefaultHash, 1<<10)
	testData := "test data"
	ref, err := ag.post(ctx, s, new([32]byte), []byte(testData))
	require.NoError(t, err)

	data := marshalRef(*ref)
	t.Log(hex.Dump(data))
	ref2, err := RefFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, *ref, *ref2)
}
