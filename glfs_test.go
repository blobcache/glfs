package glfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsErrNoEnt(t *testing.T) {
	err := ErrNoEnt{}
	require.True(t, IsErrNoEnt(err))
}
