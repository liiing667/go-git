package transport

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/storage/memory"
)

func TestReconcileObjectFormatRejectsMixedAdvertisement(t *testing.T) {
	t.Parallel()

	st := memory.NewStorage()
	require.NoError(t, st.SetReference(
		plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Invalid),
	))

	var caps capability.List
	caps.Set(capability.ObjectFormat, "sha256", "sha999")

	err := ReconcileObjectFormat(st, caps)
	require.ErrorIs(t, err, formatcfg.ErrInvalidObjectFormat)

	cfg, err := st.Config()
	require.NoError(t, err)
	assert.Equal(t, formatcfg.UnsetObjectFormat, cfg.Extensions.ObjectFormat)
}

func TestReconcileObjectFormatRejectsConflictingAlgorithms(t *testing.T) {
	t.Parallel()

	var caps capability.List
	caps.Set(capability.ObjectFormat, "sha1", "sha256")

	err := ReconcileObjectFormatV2(memory.NewStorage(), caps)
	require.ErrorIs(t, err, formatcfg.ErrObjectFormatMismatch)
}

func TestNegotiatePackRejectsUnsupportedLocalFormatWithoutCapability(t *testing.T) {
	t.Parallel()

	st := memory.NewStorage()
	cfg, err := st.Config()
	require.NoError(t, err)
	cfg.Extensions.ObjectFormat = formatcfg.SHA256
	require.NoError(t, st.SetConfig(cfg))

	req := &FetchRequest{
		Wants: []plumbing.Hash{plumbing.NewHash("6ecf0ef2c2dffb796033e5a02219af86ec6584e5")},
	}

	_, err = NegotiatePack(
		context.Background(), st, capability.List{}, false,
		bytes.NewReader(nil), newMockWriteCloser(nil), req,
	)
	require.ErrorIs(t, err, formatcfg.ErrObjectFormatMismatch)
	assert.Contains(t, err.Error(), "does not support algorithm")
}
