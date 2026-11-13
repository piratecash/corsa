//go:build s6_observation_probe && !d1_resource_probe && (darwin || linux)

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"

	corenode "github.com/piratecash/corsa/internal/core/node"
)

// Base64 expands each page by 4/3. Leave room for the envelope under
// responseLimit, which is still checked against the actual encoded bytes.
const pinPagePayloadLimit = 180 * 1024

type pinPageFrame struct {
	Version  int    `json:"v"`
	Nonce    string `json:"nonce"`
	Sequence int    `json:"seq"`
	Kind     string `json:"kind"`
	Index    int    `json:"index"`
	Data     []byte `json:"data"`
}

// The CUT terminates exactly one page sequence. Data is gzip-compressed JSON
// of the complete original pins observation, bound by this receipt and the
// page envelopes. Like its CUT, the receipt is provisional.
type pinPageReceipt struct {
	Count             int    `json:"count"`
	CompressedBytes   int    `json:"compressed_bytes"`
	UncompressedBytes int    `json:"uncompressed_bytes"`
	SHA256            string `json:"sha256"`
	BaseSHA256        string `json:"base_sha256,omitempty"`
	DeltaBytes        int    `json:"delta_bytes,omitempty"`
}

type pinDelta struct {
	// Each range is [offset in result, offset in base, length].
	Copy   [][3]int `json:"copy"`
	Insert []byte   `json:"insert"`
}

// Large arrays in an observation are immutable prefixes. Exact bytes are
// copied only while equal, so a changed or missing prefix can travel in full.
func pinPrefixDelta(base, current []byte) pinDelta {
	var delta pinDelta
	previousEnd := 0
	for _, marker := range []string{`"BaselineIdentities":[`, `"Transitions":[`, `"Events":[`} {
		start := bytes.Index(current, []byte(marker))
		from := bytes.Index(base, []byte(marker))
		if start < previousEnd || from < 0 {
			continue
		}
		length := 0
		for length < min(len(current)-start, len(base)-from) && current[start+length] == base[from+length] {
			length++
		}
		if length < 256 {
			continue
		}
		delta.Copy = append(delta.Copy, [3]int{start, from, length})
		delta.Insert = append(delta.Insert, current[previousEnd:start]...)
		previousEnd = start + length
	}
	if previousEnd != 0 {
		delta.Insert = append(delta.Insert, current[previousEnd:]...)
	}
	return delta
}

func pinDigest(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (c *channel) sendCut(ctx context.Context, returned *atomic.Bool, cut valueFrame) error {
	if cut.Sources == nil || cut.Sources.Pins.Snapshot.DurableWrites == nil {
		c.pinBase = nil
		return c.sendBound(ctx, returned, cut)
	}
	if err := alive(ctx, returned); err != nil {
		return err
	}
	pins, err := json.Marshal(cut.Sources.Pins)
	if err != nil {
		return entryError("pin-pages-encode")
	}
	lines, err := encodePagedPinCut(cut, pins, c.pinBase, channelLimit-c.written)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err := c.sendLine(ctx, returned, line); err != nil {
			return err
		}
	}
	c.pinBase = pins
	return nil
}

func encodePagedPinCut(cut valueFrame, pins, base []byte, budget int) ([][]byte, error) {
	body := pins
	receipt := pinPageReceipt{UncompressedBytes: len(pins), SHA256: pinDigest(pins)}
	if delta := pinPrefixDelta(base, pins); len(delta.Copy) != 0 {
		encoded, err := json.Marshal(delta)
		if err != nil {
			return nil, entryError("pin-pages-encode")
		}
		if len(encoded) < len(pins) {
			body = encoded
			receipt.BaseSHA256 = pinDigest(base)
			receipt.DeltaBytes = len(encoded)
		}
	}
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, entryError("pin-pages-encode")
	}
	_, writeErr := writer.Write(body)
	if closeErr := writer.Close(); writeErr != nil || closeErr != nil {
		return nil, entryError("pin-pages-encode")
	}
	payload := compressed.Bytes()
	count := (len(payload) + pinPagePayloadLimit - 1) / pinPagePayloadLimit

	// A CUT carries a locator for the original pins source while the page
	// stream carries all snapshot fields. Preserve the other seven readouts.
	sources := *cut.Sources
	sources.Pins.Snapshot = corenode.RawPinMembershipObservationSnapshot{Status: "PAGED"}
	cut.Sources = &sources
	receipt.Count, receipt.CompressedBytes = count, len(payload)
	cut.PinPages = &receipt
	stop := valueFrame{
		Version: cut.Version, Nonce: cut.Nonce, Sequence: cut.Sequence + 1,
		Kind: "STOP_REQUESTED", Mode: cut.Mode, IndependentReads: true,
	}
	stopJSON, err := json.Marshal(stop)
	if err != nil {
		return nil, entryError("pin-pages-encode")
	}
	budget -= len(stopJSON) + 1
	lines := make([][]byte, 0, count+1)
	appendFrame := func(frame any) error {
		raw, err := json.Marshal(frame)
		if err != nil {
			return entryError("pin-pages-encode")
		}
		raw = append(raw, '\n')
		if len(raw) > responseLimit || len(raw) > budget {
			return entryError("channel-encoded-limit")
		}
		budget -= len(raw)
		lines = append(lines, raw)
		return nil
	}
	for index := 0; index < count; index++ {
		start := index * pinPagePayloadLimit
		page := pinPageFrame{
			Version: cut.Version, Nonce: cut.Nonce, Sequence: cut.Sequence,
			Kind: "PIN_PAGE", Index: index, Data: payload[start:min(start+pinPagePayloadLimit, len(payload))],
		}
		if err := appendFrame(page); err != nil {
			return nil, err
		}
	}
	if err := appendFrame(cut); err != nil {
		return nil, err
	}
	return lines, nil
}
