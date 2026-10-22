//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func linuxTransferTestMembership(t *testing.T, count int) ([]byte, *linuxMemberMembershipView) {
	t.Helper()
	rows := make([]linuxMemberMembershipRow, count)
	for i := range rows {
		rows[i] = linuxMemberMembershipRow{ID: MemberID(fmt.Sprintf("full-%d", i)), Endpoint: domain.PeerAddress(fmt.Sprintf("%d.%d.0.1:64646", 240+i/256, i%256))}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	descriptor := linuxMemberMembershipDescriptor{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(raw)), Count: count}
	view, err := decodeLinuxMemberMembership(t.Context(), bytes.NewReader(raw), descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return raw, view
}
func TestLinuxMembershipBeyondPacketUsesExactFullSet(t *testing.T) {
	raw, view := linuxTransferTestMembership(t, 600)
	if len(raw) <= linuxPacketLimit {
		t.Fatal("test does not cross original private packet bound")
	}
	for _, reason := range []string{"digest", "size", "count", "duplicate-id", "foreign-address"} {
		t.Run(reason, func(t *testing.T) {
			descriptor := view.descriptor
			copy := append([]byte(nil), raw...)
			switch reason {
			case "digest":
				descriptor.SHA256 = strings.Repeat("a", 64)
			case "size":
				descriptor.Size--
			case "count":
				descriptor.Count--
			case "duplicate-id":
				copy = bytes.Replace(copy, []byte(`"ID":"full-1"`), []byte(`"ID":"full-0"`), 1)
				sum := sha256.Sum256(copy)
				descriptor.SHA256 = hex.EncodeToString(sum[:])
			case "foreign-address":
				copy = bytes.Replace(copy, []byte("240.0.0.1"), []byte("192.0.2.1"), 1)
				sum := sha256.Sum256(copy)
				descriptor.SHA256 = hex.EncodeToString(sum[:])
			}
			if _, err := decodeLinuxMemberMembership(t.Context(), bytes.NewReader(copy), descriptor); err == nil {
				t.Fatal("foreign/truncated membership accepted")
			}
		})
	}
}
func TestLinuxPeerStateStreamsBeyondOldWholeFileBound(t *testing.T) {
	_, view := linuxTransferTestMembership(t, 600)
	var body bytes.Buffer
	body.WriteString(`{"version":1,"peers":[`)
	first := true
	for endpoint := range view.endpoints {
		if !first {
			body.WriteByte(',')
		}
		first = false
		raw, err := json.Marshal(map[string]any{"address": endpoint, "metadata": strings.Repeat("x", 512)})
		if err != nil {
			t.Fatal(err)
		}
		body.Write(raw)
	}
	body.WriteString(`]}`)
	if body.Len() <= processFileLimit {
		t.Fatal("test does not cross old fullfile read bound")
	}
	count, err := decodeMemberPeerState(t.Context(), bytes.NewReader(body.Bytes()), view)
	if err != nil || count != 600 {
		t.Fatal("valid full stream refused", count, err)
	}
	invalid := bytes.Replace(body.Bytes(), []byte("240."), []byte("192."), 1)
	if _, err = decodeMemberPeerState(t.Context(), bytes.NewReader(invalid), view); err == nil {
		t.Fatal("foreign address late/fullfile accepted")
	}
	for _, bad := range []string{`{"version":1,"peers":[{"address":"240.0.0.1:64646","address":"240.0.0.1:64646"}]}`, `{"version":1,"peers":[{"Address":"240.0.0.1:64646"}]}`, `{"version":1,"peers":[{}]}`, `{"version":1,"peers":null}`} {
		if _, err = decodeMemberPeerState(t.Context(), strings.NewReader(bad), view); err == nil {
			t.Fatal("incomplete/duplicate peer stream accepted")
		}
	}
	raw, err := encodeLinuxPacket(linuxHelperJoined, 0, 1, linuxTerminal{Nonce: LaunchNonce{1}, Code: linuxResultReady, MemberFinal: &linuxMemberFinal{PeersKnown: true, PeerCount: count, MembershipSHA256: view.descriptor.SHA256}})
	if err != nil || len(raw) > linuxPacketLimit {
		t.Fatal("bounded final verdict retained full peer vector", err)
	}
}
func TestLinuxReceiptLargeStreamUsesOriginalWorkOwner(t *testing.T) {
	owner, work, facts := linuxRuntimeTestWire(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	binding, err := completedStartBinding(launchBinding{nonce: LaunchNonce{1}, fingerprint: LaunchFingerprint{2}}, StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	var file bytes.Buffer
	for i := range 1024 {
		row := Receipt{ID: domain.MessageID(fmt.Sprintf("11111111-1111-4111-8111-%012d", i)), Sender: domain.PeerIdentity{4}, Recipient: facts.Public.Identity, BodySHA256: strings.Repeat("a", 64)}
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		file.Write(raw)
		file.WriteByte('\n')
	}
	if file.Len() <= linuxPacketLimit {
		t.Fatal("test does not cross packet bound")
	}
	if err = os.WriteFile(filepath.Join(directory, "receipts.jsonl"), file.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	helper := &linuxHelperRuntime{work: work, init: linuxInit{Nonce: owner.correlation.nonce, Config: ChildConfig{DataDir: directory}, Planned: &linuxPlannedInit{Plan: PlannedChildConfig{Role: MemberSDK}}}, memberBinding: &binding}
	done := make(chan error, 1)
	go func() {
		packet, err := work.read(time.Now().Add(time.Second))
		if err != nil {
			done <- err
			return
		}
		request, err := decodeLinuxBody[linuxMemberReceiptsRequest](packet)
		if err != nil {
			done <- err
			return
		}
		done <- helper.writeMemberReceipts(ctx, packet, request)
	}()
	owner.mu.Lock()
	id, err := owner.correlation.allocate()
	owner.workBusy = true
	owner.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	x := linuxSampleExchange{ctx: ctx, id: id, receiptLimit: 1024, receiptsResult: make(chan linuxMemberReceiptsOutcome)}
	go owner.receiptsOnWorkOwner(x)
	result := <-x.receiptsResult
	if result.err != nil || len(result.receipts) != 1024 {
		t.Fatal("large receipts lost/truncated", len(result.receipts), result.err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	for i, row := range result.receipts {
		if row.ID != domain.MessageID(fmt.Sprintf("11111111-1111-4111-8111-%012d", i)) {
			t.Fatal("large receipt stream reordered/duplicated records")
		}
	}
}

func TestLinuxReceiptChunksRejectForeignGapEmptyAndOverflow(t *testing.T) {
	for _, reason := range []string{"valid-two-chunks", "bad-after-prefix", "sequence", "nonce", "request", "empty-more", "overflow"} {
		t.Run(reason, func(t *testing.T) {
			owner, work, facts := linuxRuntimeTestWire(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			owner.mu.Lock()
			id, err := owner.correlation.allocate()
			owner.workBusy = true
			owner.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			x := linuxSampleExchange{ctx: ctx, id: id, receiptLimit: 1, receiptsResult: make(chan linuxMemberReceiptsOutcome, 1)}
			if reason == "valid-two-chunks" || reason == "bad-after-prefix" {
				x.receiptLimit = 2
			}
			// A poisoned work owner may publish terminal stop instead of a direct reply,
			// just as the public Receipts caller observes stoppingDone. Join that exact
			// operation before inspecting its optional reply; never wait for reply alone.
			ownerDone := make(chan struct{})
			go func() { defer close(ownerDone); owner.receiptsOnWorkOwner(x) }()
			request, err := work.read(time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			row := Receipt{ID: "11111111-1111-4111-8111-111111111111", Sender: domain.PeerIdentity{4}, Recipient: facts.Public.Identity, BodySHA256: strings.Repeat("a", 64)}
			reply := linuxMemberReceiptsReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Sequence: 1, Receipts: []Receipt{row}}
			switch reason {
			case "valid-two-chunks", "bad-after-prefix":
				reply.More = true
			case "sequence":
				reply.Sequence = 2
			case "nonce":
				reply.Nonce = LaunchNonce{9}
			case "request":
				request.id++
			case "empty-more":
				reply.More = true
				reply.Receipts = nil
			case "overflow":
				reply.Receipts = append(reply.Receipts, row)
			}
			raw, err := encodeLinuxPacket(linuxMemberReceiptsResult, request.id, 1, reply)
			if err != nil {
				t.Fatal(err)
			}
			if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if reason == "valid-two-chunks" || reason == "bad-after-prefix" {
				reply.Sequence = 2
				reply.More = false
				if reason == "bad-after-prefix" {
					reply.Nonce = LaunchNonce{9}
				}
				reply.Receipts[0].ID = "22222222-2222-4222-8222-222222222222"
				raw, err = encodeLinuxPacket(linuxMemberReceiptsResult, request.id, 1, reply)
				if err != nil {
					t.Fatal(err)
				}
				if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-ownerDone:
			case <-ctx.Done():
				t.Fatal("original receipt work owner did not finish within its request bound", ctx.Err())
			}
			var result linuxMemberReceiptsOutcome
			published := false
			select {
			case result = <-x.receiptsResult:
				published = true
			default:
			}
			owner.mu.Lock()
			poisoned, stopping, busy := owner.correlation.poisoned, owner.stopping, owner.workBusy
			owner.mu.Unlock()
			stopped := false
			select {
			case <-owner.stoppingDone:
				stopped = true
			default:
			}
			if reason == "valid-two-chunks" {
				if !published || result.err != nil || len(result.receipts) != 2 || result.receipts[0].ID != row.ID || result.receipts[1].ID != "22222222-2222-4222-8222-222222222222" || poisoned || stopping || stopped || busy {
					t.Fatal("ordered complete stream lost records or retained work ownership", published, result.err)
				}
			} else {
				if published && (result.err == nil || result.receipts != nil) {
					t.Fatal("foreign/partial chunk stream published")
				}
				if !poisoned || !stopping || !stopped || busy {
					t.Fatal("invalid stream did not finish in original poisoned terminal-stop state", poisoned, stopping, stopped, busy)
				}
			}
		})
	}
}
