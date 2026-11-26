package packp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-git/go-git/v6/plumbing"
)

func TestResolvedReferencesSkipsInvalidAdvertisedNames(t *testing.T) {
	t.Parallel()

	hash := plumbing.NewHash("5dc01c595e6c6ec9ccda4f6f69c131c0dd945f8c")
	peeled := plumbing.ReferenceName("refs/tags/v1.0^{}")
	adv := &AdvRefs{
		References: []*plumbing.Reference{
			plumbing.NewHashReference("refs/heads/ma*n", hash),
			plumbing.NewHashReference("refs/heads/main", hash),
			plumbing.NewHashReference(peeled, hash),
		},
	}

	refs, err := adv.ResolvedReferences()
	require.NoError(t, err)

	var names []plumbing.ReferenceName
	for _, ref := range refs {
		names = append(names, ref.Name())
	}
	assert.Equal(t, []plumbing.ReferenceName{
		"refs/heads/main",
		peeled,
	}, names)
}
