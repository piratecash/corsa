package node

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func newApplicationReceiveTransport(t *testing.T, svc *Service, inbound bool, id domain.ConnID, address domain.PeerAddress) (*netcore.NetCore, func(protocol.Frame)) {
	t.Helper()
	if inbound {
		id, core := newTrackedInboundHealthConn(t, svc, address)
		core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: idPeerB.String()}})
		return core, func(frame protocol.Frame) {
			line, err := protocol.MarshalFrameLine(frame)
			if err != nil {
				t.Fatal(err)
			}
			svc.dispatchNetworkFrame(id, line)
		}
	}
	session := newReceiveOwnerSession(t, svc, id, address)
	return session.netCore, func(frame protocol.Frame) { svc.dispatchPeerSessionFrame(address, session, frame) }
}

func TestPushMessageAndNoticeUseOriginalTransport(t *testing.T) {
	for _, direction := range []string{"inbound", "outbound"} {
		t.Run(direction, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeClient)
			svc.runCtx = t.Context()
			sender := registerSenderKey(t, svc)
			old, dispatchOld := newApplicationReceiveTransport(t, svc, direction == "inbound", 5950, "192.0.2.105:64646")
			_, dispatchLive := newApplicationReceiveTransport(t, svc, direction == "inbound", 5951, "192.0.2.106:64646")
			push := protocol.Frame{Type: "push_message", Topic: "dm", Item: &protocol.MessageFrame{
				ID: "push-transport", Sender: sender.Address, Recipient: svc.Address(),
				Flag: string(protocol.MessageFlagImmutable), CreatedAt: time.Now().UTC().Format(time.RFC3339),
				Body: sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey)),
			}}
			notice := protocol.Frame{Type: "push_notice", TTLSeconds: 30, Ciphertext: "transport-notice"}
			assertStored := func(want int) {
				t.Helper()
				svc.gossipMu.RLock()
				messages := len(svc.topics["dm"])
				svc.gossipMu.RUnlock()
				if notices := svc.fetchNoticesFrame().Count; messages != want || notices != want {
					t.Fatalf("stored %d messages and %d notices, want %d of each", messages, notices, want)
				}
			}
			old.ShutSendQueue()
			if svc.netCoreForID(old.ConnID()) != old {
				t.Fatal("test must exercise the interval before unregister")
			}
			dispatchOld(push)
			dispatchOld(notice)
			assertStored(0)
			dispatchLive(push)
			dispatchLive(notice)
			assertStored(1)
		})
	}
}

func TestGazetaSharesNoticeAdmissionAcrossLocalAndPeerPaths(t *testing.T) {
	s := &Service{cfg: config.Node{Type: domain.NodeTypeClient}}
	for _, frame := range []protocol.Frame{
		{TTLSeconds: 0, Ciphertext: "notice"},
		{TTLSeconds: -1, Ciphertext: "notice"},
		{TTLSeconds: maxNoticeTTLSeconds + 1, Ciphertext: "notice"},
		{TTLSeconds: math.MaxInt, Ciphertext: "notice"},
		{TTLSeconds: 30, Ciphertext: "  "},
		{TTLSeconds: 30, Ciphertext: strings.Repeat("a", protocol.MaxFrameLine+1)},
	} {
		if got := s.publishNoticeFrame(frame); got.Code != protocol.ErrCodeInvalidPublishNotice {
			t.Fatalf("invalid notice returned %q/%q", got.Type, got.Code)
		}
		s.handleInboundPushNotice(frame)
	}
	if got := s.fetchNoticesFrame().Count; got != 0 {
		t.Fatalf("retained %d invalid notices", got)
	}
	first := protocol.Frame{TTLSeconds: 30, Ciphertext: "local"}
	stored := s.publishNoticeFrame(first)
	if stored.Type != "notice_stored" {
		t.Fatalf("local publish returned %q/%q", stored.Type, stored.Code)
	}
	for i := 1; i < maxNoticeCount; i++ {
		s.handleInboundPushNotice(protocol.Frame{TTLSeconds: maxNoticeTTLSeconds, Ciphertext: strconv.Itoa(i)})
	}
	overflow := protocol.Frame{TTLSeconds: 30, Ciphertext: "extra"}
	s.handleInboundPushNotice(overflow)
	if got := s.publishNoticeFrame(overflow); got.Code != protocol.ErrCodeStoreDeferred {
		t.Fatalf("full store returned %q/%q", got.Type, got.Code)
	}
	first.TTLSeconds = maxNoticeTTLSeconds
	duplicate := s.publishNoticeFrame(first)
	if duplicate.Type != "notice_known" || duplicate.ID != stored.ID || duplicate.ExpiresAt != stored.ExpiresAt {
		t.Fatal("full store lost the original notice or its deadline")
	}
	if got := s.fetchNoticesFrame().Count; got != maxNoticeCount {
		t.Fatalf("retained %d notices, want %d", got, maxNoticeCount)
	}
}

func TestGazetaNoticeBytesReturnInDeadlineOrder(t *testing.T) {
	var n noticeStore
	now := time.Now()
	payload := strings.Repeat("a", maxNoticeBytes/2)
	if _, stored := n.put(now, "later", payload, time.Minute); !stored {
		t.Fatal("first half of the byte budget was refused")
	}
	if _, stored := n.put(now, "earlier", payload, time.Second); !stored {
		t.Fatal("second half of the byte budget was refused")
	}
	if expiresAt, stored := n.put(now, "extra", "a", time.Second); !expiresAt.IsZero() || stored {
		t.Fatal("a notice exceeded the byte budget")
	}
	if _, stored := n.put(now.Add(time.Second), "earlier", "a", time.Second); !stored {
		t.Fatal("bytes or ID were unavailable at the expiry boundary")
	}
	if n.bytes != len(payload)+1 || len(n.byID) != 2 || n.byID["later"].ExpiresAt != now.Add(time.Minute) {
		t.Fatal("expiration changed the wrong entry or lost byte accounting")
	}
	n.expire(now.Add(time.Minute))
	if n.bytes != 0 || len(n.byID) != 0 || len(n.deadlines) != 0 {
		t.Fatal("expiration left retained notices or accounting")
	}
}
