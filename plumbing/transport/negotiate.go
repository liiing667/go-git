package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/utils/ioutil"
	xstorage "github.com/go-git/go-git/v6/x/storage"
)

const (
	initialFlush  = 16
	pipeSafeFlush = 32
	largeFlush    = 16384
	maxInVein     = 256
)

func nextFlush(statelessRPC bool, count int) int {
	if statelessRPC {
		if count < largeFlush {
			return count << 1
		}
		return count * 11 / 10
	}

	if count < pipeSafeFlush {
		return count << 1
	}

	return count + pipeSafeFlush
}

func applyServerACKs(
	statelessRPC bool,
	acks []packp.ACK,
	common map[plumbing.Hash]struct{},
	statelessCommon *[]plumbing.Hash,
	gotContinue *bool,
	gotReady *bool,
	inVein *int,
) {
	for _, ack := range acks {
		if !*gotContinue && ack.Status > 0 {
			*gotContinue = true
		}

		switch ack.Status {
		case packp.ACKContinue:
			*inVein = 0
		case packp.ACKReady:
			*gotReady = true
			*inVein = 0
		case packp.ACKCommon:
			_, alreadyCommon := common[ack.Hash]
			common[ack.Hash] = struct{}{}
			if statelessRPC && !alreadyCommon {
				*statelessCommon = append(*statelessCommon, ack.Hash)
				*inVein = 0
			}
		}
	}
}

// NegotiatePack performs the pack negotiation phase of the fetch operation.
func NegotiatePack(
	ctx context.Context,
	st storage.Storer,
	caps capability.List,
	statelessRPC bool,
	reader io.Reader,
	writer io.WriteCloser,
	req *FetchRequest,
) (shallowInfo *packp.ShallowUpdate, err error) {
	reader = ioutil.NewContextReader(ctx, reader)
	writer = ioutil.NewContextWriteCloser(ctx, writer)

	upreq := &packp.UploadRequest{}
	multiAck := caps.Supports(capability.MultiACK)
	multiAckDetailed := caps.Supports(capability.MultiACKDetailed)
	if multiAckDetailed {
		upreq.Capabilities.Set(capability.MultiACKDetailed)
	} else if multiAck {
		upreq.Capabilities.Set(capability.MultiACK)
	}

	if req.Progress != nil {
		if caps.Supports(capability.Sideband64k) {
			upreq.Capabilities.Set(capability.Sideband64k)
		} else if caps.Supports(capability.Sideband) {
			upreq.Capabilities.Set(capability.Sideband)
		}
	} else if caps.Supports(capability.NoProgress) {
		upreq.Capabilities.Set(capability.NoProgress)
	}

	clientFormat, err := reconcileObjectFormat(st, caps)
	if err != nil {
		return nil, err
	}

	if caps.Supports(capability.ObjectFormat) {
		upreq.Capabilities.Set(capability.ObjectFormat, clientFormat.String())
	}

	if caps.Supports(capability.OFSDelta) {
		upreq.Capabilities.Set(capability.OFSDelta)
	}

	if caps.Supports(capability.Agent) {
		upreq.Capabilities.Set(capability.Agent, capability.DefaultAgent())
	}

	if req.IncludeTags && caps.Supports(capability.IncludeTag) {
		upreq.Capabilities.Set(capability.IncludeTag)
	}

	if req.Filter != "" {
		if caps.Supports(capability.Filter) {
			upreq.Filter = req.Filter
			upreq.Capabilities.Set(capability.Filter)
		} else {
			return nil, ErrFilterNotSupported
		}
	}

	upreq.Wants = req.Wants

	if req.Depth > 0 {
		if !caps.Supports(capability.Shallow) {
			return nil, ErrShallowNotSupported
		}
		upreq.Depth = packp.DepthRequest{Deepen: req.Depth}
		upreq.Shallows, err = st.Shallow()
		if err != nil {
			return nil, err
		}
	}

	if isSubset(req.Wants, req.Haves) && len(upreq.Shallows) == 0 {
		if err := pktline.WriteFlush(writer); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("closing writer: %w", err)
		}
		return nil, ErrNoChange
	}

	common := map[plumbing.Hash]struct{}{}
	var statelessCommon []plumbing.Hash
	var inVein int
	var done bool
	var gotContinue bool
	var gotReady bool
	var sendDoneAfterReady bool
	firstRound := true
	flushAt := initialFlush

	for !done {
		var uphav packp.UploadHaves
		if statelessRPC && !sendDoneAfterReady {
			uphav.Haves = append(uphav.Haves, statelessCommon...)
		}

		batchSize := 0
		if !sendDoneAfterReady {
			batchSize = flushAt
			if gotContinue {
				remaining := maxInVein - inVein
				if remaining <= 0 {
					batchSize = 0
				} else if batchSize > remaining {
					batchSize = remaining
				}
			}
		}

		for i := 0; i < batchSize && len(req.Haves) > 0; i++ {
			uphav.Haves = append(uphav.Haves, req.Haves[len(req.Haves)-1])
			req.Haves = req.Haves[:len(req.Haves)-1]
			inVein++
		}

		done = sendDoneAfterReady || len(req.Haves) == 0 || (gotContinue && inVein >= maxInVein)
		uphav.Done = done

		if isSubset(req.Wants, uphav.Haves) && len(upreq.Shallows) == 0 {
			if err := pktline.WriteFlush(writer); err != nil {
				return nil, err
			}
			if err := writer.Close(); err != nil && !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("closing writer: %w", err)
			}
			return nil, ErrNoChange
		}

		if firstRound || statelessRPC {
			if err := upreq.Encode(writer); err != nil {
				return nil, fmt.Errorf("sending upload-request: %w", err)
			}
		}

		// The server sends its shallow-update before it reads any haves
		// (upload-pack.c receive_needs), so read it first, as git's
		// fetch-pack does.
		if !statelessRPC {
			if err := readShallows(statelessRPC, reader, req, &shallowInfo, firstRound); err != nil {
				return nil, err
			}
		}

		if err := uphav.Encode(writer); err != nil {
			return nil, fmt.Errorf("sending upload-haves: %w", err)
		}

		if statelessRPC {
			if err := writer.Close(); err != nil {
				return nil, fmt.Errorf("closing writer: %w", err)
			}
			if err := readShallows(statelessRPC, reader, req, &shallowInfo, firstRound); err != nil {
				return nil, err
			}
		}

		if done || len(uphav.Haves) > 0 {
			var srvrs packp.ServerResponse
			if err := srvrs.Decode(reader); err != nil {
				return nil, fmt.Errorf("decoding server-response: %w", err)
			}
			applyServerACKs(statelessRPC, srvrs.ACKs, common, &statelessCommon, &gotContinue, &gotReady, &inVein)
		}
		if sendDoneAfterReady {
			break
		}
		if gotReady {
			sendDoneAfterReady = true
		}

		firstRound = false
		flushAt = nextFlush(statelessRPC, flushAt)
	}

	if !statelessRPC {
		if err := writer.Close(); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("closing writer: %w", err)
		}
	}

	return shallowInfo, nil
}

func isSubset(needle, haystack []plumbing.Hash) bool {
	for _, h := range needle {
		if !slices.Contains(haystack, h) {
			return false
		}
	}
	return true
}

func readShallows(
	statelessRPC bool,
	r io.Reader,
	req *FetchRequest,
	shallowInfo **packp.ShallowUpdate,
	firstRound bool,
) error {
	if (firstRound || statelessRPC) && req.Depth > 0 {
		var shupd packp.ShallowUpdate
		if err := shupd.Decode(r); err != nil {
			return fmt.Errorf("decoding shallow-update: %w", err)
		}
		if *shallowInfo == nil {
			*shallowInfo = &shupd
		}
	}
	return nil
}

// ReconcileObjectFormat aligns the storer's object format with the server's
// advertised object-format before any packfile is requested. On a fresh clone
// the storer's format is unset (HEAD still points at the refs/heads/.invalid
// placeholder) and the server's sha256 is adopted; otherwise a mismatch is a
// hard error, since indexing a sha256 pack as sha1 (or vice versa) corrupts
// the store and only surfaces later as a checksum failure. It follows git's
// fetch-pack.c, including the case where the server omits object-format (it
// only speaks sha1) but the client repository uses another algorithm.
func ReconcileObjectFormat(st storage.Storer, caps capability.List) error {
	_, err := reconcileObjectFormat(st, caps)
	return err
}

// ReconcileObjectFormatV2 is the Protocol v2 name for [ReconcileObjectFormat].
// Protocol v0/v1 negotiation applies the same reconciliation.
func ReconcileObjectFormatV2(st storage.Storer, caps capability.List) error {
	return ReconcileObjectFormat(st, caps)
}

func reconcileObjectFormat(st storage.Storer, caps capability.List) (config.ObjectFormat, error) {
	var clientFormat config.ObjectFormat
	if cfg, err := st.Config(); err == nil && cfg != nil {
		clientFormat = cfg.Extensions.ObjectFormat
	}

	advertised := caps.Get(capability.ObjectFormat)
	if len(advertised) == 0 {
		// The server advertised no object-format, so it only speaks sha1.
		// Upstream errors when the client repo uses a different algorithm
		// rather than letting it fail later on a checksum mismatch.
		if clientFormat != config.UnsetObjectFormat && clientFormat != config.SHA1 {
			return config.SHA1, fmt.Errorf("%w: the server does not support algorithm '%s'",
				config.ErrObjectFormatMismatch, clientFormat)
		}
		return config.SHA1, nil
	}

	var serverFormat config.ObjectFormat
	switch v := config.ObjectFormat(advertised[0]); v {
	case config.SHA1, config.SHA256:
		serverFormat = v
	case config.UnsetObjectFormat:
		for _, value := range advertised[1:] {
			switch extra := config.ObjectFormat(value); extra {
			case config.UnsetObjectFormat, config.SHA1:
			case config.SHA256:
				return config.UnsetObjectFormat, fmt.Errorf("%w: server advertised conflicting object-format values %q and %q",
					config.ErrObjectFormatMismatch, config.SHA1, extra)
			default:
				return config.UnsetObjectFormat, fmt.Errorf("%w: server advertised unsupported object-format %q",
					config.ErrInvalidObjectFormat, extra)
			}
		}

		// An empty value carries no algorithm; treat it exactly as an absent
		// object-format (the server only speaks sha1). Apply the same guard as
		// the len(advertised)==0 branch so a client repo on a different
		// algorithm is rejected rather than slipping past to fail later on a
		// checksum mismatch.
		if clientFormat != config.UnsetObjectFormat && clientFormat != config.SHA1 {
			return config.SHA1, fmt.Errorf("%w: the server does not support algorithm '%s'",
				config.ErrObjectFormatMismatch, clientFormat)
		}
		return config.SHA1, nil
	default:
		// An algorithm go-git does not speak. Fail fast rather than proceed
		// with the wrong hash format, matching NegotiatePack's v0/v1 handling.
		return config.UnsetObjectFormat, fmt.Errorf("%w: server advertised unsupported object-format %q",
			config.ErrInvalidObjectFormat, v)
	}

	// object-format takes exactly one algorithm. A malformed or mixed
	// advertisement must not be partially accepted and used to rewrite the
	// repository configuration. Repeated identical values are tolerated for
	// peers that duplicate the capability line.
	for _, value := range advertised[1:] {
		extra := config.ObjectFormat(value)
		if extra != serverFormat {
			if extra == config.SHA1 || extra == config.SHA256 {
				return config.UnsetObjectFormat, fmt.Errorf("%w: server advertised conflicting object-format values %q and %q",
					config.ErrObjectFormatMismatch, serverFormat, extra)
			}
			return config.UnsetObjectFormat, fmt.Errorf("%w: server advertised unsupported object-format %q",
				config.ErrInvalidObjectFormat, extra)
		}
	}

	// Adopt the server format on a fresh clone: unset client + sha256 server,
	// with HEAD still at the clone placeholder.
	if clientFormat == config.UnsetObjectFormat && serverFormat == config.SHA256 {
		if ref, err := st.Reference(plumbing.HEAD); err == nil && ref.Target().String() == "refs/heads/.invalid" {
			if setter, ok := st.(xstorage.ObjectFormatSetter); ok {
				if err := setter.SetObjectFormat(serverFormat); err != nil {
					return config.UnsetObjectFormat, fmt.Errorf("unable to set object format: %w", err)
				}
				clientFormat = serverFormat
			}
		}
	}

	if clientFormat == config.UnsetObjectFormat {
		clientFormat = config.SHA1
	}

	switch clientFormat {
	case config.SHA1, config.SHA256:
	default:
		return clientFormat, fmt.Errorf("%w: local repository uses unsupported object-format %q",
			config.ErrInvalidObjectFormat, clientFormat)
	}

	if serverFormat != clientFormat {
		return clientFormat, fmt.Errorf("%w: mismatched algorithms: client %s; server %s",
			config.ErrObjectFormatMismatch, clientFormat, serverFormat)
	}
	return clientFormat, nil
}
