//go:build linux

package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

func openMemberPublicSnapshot(ctx context.Context, path string, limit int64) (_ *os.Root, _ *os.File, _ int64, result error) {
	if limit < 1 || ctx.Err() != nil {
		return nil, nil, 0, errors.Join(ErrInvalidConfiguration, ctx.Err())
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, nil, 0, err
	}
	keep := false
	defer func() {
		if !keep {
			result = errors.Join(result, root.Close())
		}
	}()
	name := filepath.Base(path)
	before, err := root.Lstat(name)
	if err != nil || before == nil {
		return nil, nil, 0, errors.Join(ErrUnsupportedArtifact, err)
	}
	if before.Size() > limit {
		return nil, nil, 0, ErrArtifactTooLarge
	}
	file, err := openVerifiedArtifact(ctx, root, name, before)
	if err != nil {
		return nil, nil, 0, err
	}
	keep = true
	return root, file, before.Size(), nil
}
func decodeMemberPeerState(ctx context.Context, reader io.Reader, membership *linuxMemberMembershipView) (int, error) {
	if membership == nil || len(membership.endpoints) != membership.descriptor.Count {
		return 0, ErrInvalidSample
	}
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return 0, ErrInvalidSample
	}
	seen := map[string]bool{}
	count := 0
	version := 0
	for decoder.More() {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] || name != "version" && strings.EqualFold(name, "version") || name != "peers" && strings.EqualFold(name, "peers") {
			return 0, ErrInvalidSample
		}
		seen[name] = true
		switch name {
		case "version":
			if decoder.Decode(&version) != nil || version < 1 {
				return 0, ErrInvalidSample
			}
		case "peers":
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return 0, ErrInvalidSample
			}
			for decoder.More() {
				if count >= membership.descriptor.Count {
					return 0, ErrInvalidSample
				}
				if err := decodeMemberPeerRow(ctx, decoder, membership); err != nil {
					return 0, err
				}
				count++
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return 0, ErrInvalidSample
			}
		default:
			if validateJSONValue(decoder, 0) != nil {
				return 0, ErrInvalidSample
			}
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seen["version"] || !seen["peers"] {
		return 0, ErrInvalidSample
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, ErrInvalidSample
	}
	return count, ctx.Err()
}
func decodeMemberPeerRow(ctx context.Context, decoder *json.Decoder, membership *linuxMemberMembershipView) error {
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return ErrInvalidSample
	}
	seen := map[string]bool{}
	address := domain.PeerAddress("")
	for decoder.More() {
		if err = ctx.Err(); err != nil {
			return err
		}
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] || name != "address" && strings.EqualFold(name, "address") {
			return ErrInvalidSample
		}
		seen[name] = true
		if name == "address" {
			if decoder.Decode(&address) != nil || !membership.endpoints[address] {
				return ErrInvalidSample
			}
		} else if validateJSONValue(decoder, 0) != nil {
			return ErrInvalidSample
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seen["address"] {
		return ErrInvalidSample
	}
	return nil
}
func (r *linuxHelperRuntime) memberPeerState(ctx context.Context) (count int, result error) {
	if r.membership == nil || r.membership.descriptor.Count < 1 || int64(r.membership.descriptor.Count) > (1<<63-1)/processFileLimit {
		return 0, ErrInvalidConfiguration
	}
	// Reuse the existing per-artifact bound per actual planned member, while
	// streaming the final file instead of materializing all entries/large fields.
	limit := int64(r.membership.descriptor.Count) * processFileLimit
	root, file, size, err := openMemberPublicSnapshot(ctx, filepath.Join(r.init.Config.DataDir, "peers.json"), limit)
	if err != nil {
		return 0, err
	}
	defer func() { result = errors.Join(result, file.Close(), root.Close()) }()
	return decodeMemberPeerState(ctx, io.NewSectionReader(file, 0, size), r.membership)
}
