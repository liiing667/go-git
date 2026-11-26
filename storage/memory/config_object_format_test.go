package memory_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/storage/memory"
)

func TestSetConfigAlignsObjectHasher(t *testing.T) {
	t.Parallel()

	st := memory.NewStorage()
	cfg, err := st.Config()
	require.NoError(t, err)
	cfg.Extensions.ObjectFormat = formatcfg.SHA256
	require.NoError(t, st.SetConfig(cfg))

	obj := st.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	writer, err := obj.Writer()
	require.NoError(t, err)
	_, err = writer.Write([]byte("object format"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	hash, err := st.SetEncodedObject(obj)
	require.NoError(t, err)
	assert.Len(t, hash.String(), formatcfg.SHA256.HexSize())
}

func TestSetConfigRejectsInvalidObjectFormat(t *testing.T) {
	t.Parallel()

	st := memory.NewStorage()
	bad := config.NewConfig()
	bad.Extensions.ObjectFormat = formatcfg.ObjectFormat("sha999")

	err := st.SetConfig(bad)
	require.ErrorIs(t, err, formatcfg.ErrInvalidObjectFormat)

	got, err := st.Config()
	require.NoError(t, err)
	assert.Equal(t, formatcfg.UnsetObjectFormat, got.Extensions.ObjectFormat)
}
