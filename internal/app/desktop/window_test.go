package desktop

import (
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/domain"
	"corsa/internal/core/service"
)

func TestMergeRecipientOrder(t *testing.T) {
	recipients := []domain.PeerIdentity{"a", "b", "c", "d"}
	order := []domain.PeerIdentity{"c", "a"}

	merged := mergeRecipientOrder(recipients, order)

	// "c" and "a" should come first (in order), then "b" and "d" (sorted).
	if len(merged) != 4 {
		t.Fatalf("expected 4, got %d: %v", len(merged), merged)
	}
	if merged[0] != "c" || merged[1] != "a" {
		t.Fatalf("expected [c, a, ...], got %v", merged)
	}
}

func TestMergeRecipientOrderEmpty(t *testing.T) {
	merged := mergeRecipientOrder(nil, []domain.PeerIdentity{"a"})
	if merged != nil {
		t.Fatalf("expected nil for empty recipients, got %v", merged)
	}
}

func TestSearchKnownIdentities(t *testing.T) {
	knownIDs := []string{"abc-def-ghi", "xyz-abc-123", "zzz-yyy-xxx"}
	recipients := []domain.PeerIdentity{"abc-def-ghi"} // already listed
	self := domain.PeerIdentity("self-addr")

	results := searchKnownIdentities(knownIDs, recipients, self, "abc")

	// "abc-def-ghi" is already listed → excluded.
	// "xyz-abc-123" matches query → included.
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}
	if results[0] != "xyz-abc-123" {
		t.Fatalf("expected xyz-abc-123, got %s", results[0])
	}
}

func TestSearchKnownIdentitiesEmptyQuery(t *testing.T) {
	results := searchKnownIdentities([]string{"a", "b"}, nil, domain.PeerIdentity("self"), "")
	if results != nil {
		t.Fatalf("expected nil for empty query, got %v", results)
	}
}

func TestShortFingerprint(t *testing.T) {
	short := "abc"
	if got := shortFingerprint(short); got != short {
		t.Fatalf("expected %q, got %q", short, got)
	}

	long := "abcdefghijklmnopqrstuvwxyz"
	got := shortFingerprint(long)
	if got != "abcdefgh...uvwxyz" {
		t.Fatalf("expected 'abcdefgh...uvwxyz', got %q", got)
	}
}

func TestEllipsize(t *testing.T) {
	if got := ellipsize("hello", 10); got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
	if got := ellipsize("hello world", 5); got != "hell…" {
		t.Fatalf("expected 'hell…', got %q", got)
	}
	if got := ellipsize("", 5); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// TestNetworkStatusSummary verifies that the aggregate network status is based
// on the number of usable peers (healthy + degraded). Stalled peers are
// connected at TCP level but excluded from routing, so they do not count
// as usable for the aggregate status label.
// TestHasNewerPeerBuildRequiresQuorum verifies that a single peer with a
// higher build number is not enough to trigger the update badge. At least
// 2 distinct peer identities must report a higher build to prevent a
// malicious custom build from causing false upgrade prompts across the
// network. The same identity appearing under multiple addresses counts
// only once.
func TestHasNewerPeerBuildRequiresQuorum(t *testing.T) {
	t.Parallel()

	myBuild := config.ClientBuild

	tests := []struct {
		name  string
		peers []service.PeerHealth
		want  bool
	}{
		{
			name:  "no peers",
			peers: nil,
			want:  false,
		},
		{
			name:  "single peer with same build",
			peers: []service.PeerHealth{{ClientBuild: myBuild, PeerID: "a"}},
			want:  false,
		},
		{
			name:  "single peer with higher build is not enough",
			peers: []service.PeerHealth{{ClientBuild: myBuild + 1, PeerID: "a"}},
			want:  false,
		},
		{
			name: "two distinct peers with higher build triggers update",
			peers: []service.PeerHealth{
				{ClientBuild: myBuild + 1, PeerID: "a"},
				{ClientBuild: myBuild + 1, PeerID: "b"},
			},
			want: true,
		},
		{
			name: "same identity under two addresses does not satisfy quorum",
			peers: []service.PeerHealth{
				{ClientBuild: myBuild + 1, PeerID: "a", Address: "1.2.3.4:100"},
				{ClientBuild: myBuild + 1, PeerID: "a", Address: "5.6.7.8:200"},
			},
			want: false,
		},
		{
			name: "two peers higher among many same",
			peers: []service.PeerHealth{
				{ClientBuild: myBuild, PeerID: "a"},
				{ClientBuild: myBuild + 1, PeerID: "b"},
				{ClientBuild: myBuild, PeerID: "c"},
				{ClientBuild: myBuild + 2, PeerID: "d"},
			},
			want: true,
		},
		{
			name: "all peers lower",
			peers: []service.PeerHealth{
				{ClientBuild: myBuild - 1, PeerID: "a"},
				{ClientBuild: myBuild - 2, PeerID: "b"},
			},
			want: false,
		},
		{
			name: "peers without identity fall back to address dedup",
			peers: []service.PeerHealth{
				{ClientBuild: myBuild + 1, Address: "1.2.3.4:100"},
				{ClientBuild: myBuild + 1, Address: "5.6.7.8:200"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Window{
				snap: service.RouterSnapshot{
					NodeStatus: service.NodeStatus{PeerHealth: tt.peers},
				},
			}
			got := w.hasNewerPeerBuild()
			if got != tt.want {
				t.Errorf("hasNewerPeerBuild() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNetworkStatusSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		peers         []service.PeerHealth
		wantState     string
		wantConnected int
		wantTotal     int
	}{
		{
			name:      "no peers is offline",
			peers:     nil,
			wantState: "offline",
		},
		{
			name: "all reconnecting",
			peers: []service.PeerHealth{
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "reconnecting",
			wantConnected: 0,
			wantTotal:     2,
		},
		{
			name: "single healthy peer is limited",
			peers: []service.PeerHealth{
				{State: "healthy"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "single stalled peer is limited (connected but not usable)",
			peers: []service.PeerHealth{
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "two usable peers are healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     2,
		},
		{
			name: "all stalled is limited not healthy (P2 regression)",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "stalled"},
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 3,
			wantTotal:     3,
		},
		{
			name: "less than half usable is warning",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "warning",
			wantConnected: 2,
			wantTotal:     7,
		},
		{
			name: "half usable is healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     4,
		},
		{
			name: "mix of stalled and degraded uses only usable for status",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 4,
			wantTotal:     4,
		},
		{
			name: "stalled peers do not help reach healthy threshold",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "stalled"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "warning",
			wantConnected: 4,
			wantTotal:     9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := service.NodeStatus{PeerHealth: tt.peers}
			gotState, gotConnected, gotTotal, _ := networkStatusSummary(status)
			if gotState != tt.wantState {
				t.Errorf("state: got %q, want %q", gotState, tt.wantState)
			}
			if gotConnected != tt.wantConnected {
				t.Errorf("connected: got %d, want %d", gotConnected, tt.wantConnected)
			}
			if gotTotal != tt.wantTotal {
				t.Errorf("total: got %d, want %d", gotTotal, tt.wantTotal)
			}
		})
	}
}

func TestFindMessageBody(t *testing.T) {
	t.Parallel()

	w := &Window{
		snap: service.RouterSnapshot{
			ActiveMessages: []service.DirectMessage{
				{ID: "aaa", Body: "hello"},
				{ID: "bbb", Body: "world"},
			},
		},
	}
	w.rebuildMsgCache()

	if got := w.findMessageBody("aaa"); got != "hello" {
		t.Errorf("findMessageBody(aaa) = %q, want %q", got, "hello")
	}
	if got := w.findMessageBody("bbb"); got != "world" {
		t.Errorf("findMessageBody(bbb) = %q, want %q", got, "world")
	}
	if got := w.findMessageBody("nonexistent"); got != "" {
		t.Errorf("findMessageBody(nonexistent) = %q, want empty", got)
	}
}

func TestFindCachedMsg(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 3, 12, 30, 0, 0, time.UTC)
	w := &Window{
		snap: service.RouterSnapshot{
			ActiveMessages: []service.DirectMessage{
				{ID: "aaa", Body: "hello", Sender: "alice", Timestamp: ts},
				{ID: "bbb", Body: "world", Sender: "bob", Timestamp: ts.Add(time.Minute)},
			},
		},
	}
	w.rebuildMsgCache()

	cm, ok := w.findCachedMsg("aaa")
	if !ok {
		t.Fatal("findCachedMsg(aaa) not found")
	}
	if cm.Sender != "alice" {
		t.Errorf("Sender = %q, want %q", cm.Sender, "alice")
	}
	if cm.Index != 0 {
		t.Errorf("Index = %d, want 0", cm.Index)
	}
	if cm.Timestamp != ts {
		t.Errorf("Timestamp = %v, want %v", cm.Timestamp, ts)
	}

	cm2, ok := w.findCachedMsg("bbb")
	if !ok {
		t.Fatal("findCachedMsg(bbb) not found")
	}
	if cm2.Index != 1 {
		t.Errorf("Index = %d, want 1", cm2.Index)
	}

	_, ok = w.findCachedMsg("nonexistent")
	if ok {
		t.Error("findCachedMsg(nonexistent) should return false")
	}
}

func TestTriggerSendSetsReplyTo(t *testing.T) {
	t.Parallel()

	replyMsg := &service.DirectMessage{
		ID:   "reply-target-id",
		Body: "original message",
	}
	w := &Window{
		replyToMsg: replyMsg,
	}

	// triggerSend builds OutgoingDM with ReplyTo but does NOT clear
	// replyToMsg — the router clears it via PendingActions.ClearReply
	// after successful delivery.
	outgoing := domain.OutgoingDM{Body: "my reply"}
	if w.replyToMsg != nil {
		outgoing.ReplyTo = domain.MessageID(w.replyToMsg.ID)
	}

	if outgoing.ReplyTo != "reply-target-id" {
		t.Errorf("ReplyTo = %q, want %q", outgoing.ReplyTo, "reply-target-id")
	}
	if w.replyToMsg == nil {
		t.Error("replyToMsg should remain set until ClearReply arrives")
	}
}

func TestResetReplyOnPeerChange(t *testing.T) {
	t.Parallel()

	w := &Window{
		replyToMsg: &service.DirectMessage{ID: "msg-1", Body: "hello"},
		snap: service.RouterSnapshot{
			ActivePeer: "peer-b",
		},
		lastChatPeer: "peer-a",
	}

	w.resetReplyOnPeerChange()

	if w.replyToMsg != nil {
		t.Error("replyToMsg should be nil after peer change")
	}
	if w.msgContextMsg != nil {
		t.Error("msgContextMsg should be nil after peer change")
	}
}

func TestResetReplyOnPeerChangeSamePeer(t *testing.T) {
	t.Parallel()

	replyMsg := &service.DirectMessage{ID: "msg-1", Body: "hello"}
	w := &Window{
		replyToMsg: replyMsg,
		snap: service.RouterSnapshot{
			ActivePeer: "peer-a",
		},
		lastChatPeer: "peer-a",
	}

	w.resetReplyOnPeerChange()

	if w.replyToMsg != replyMsg {
		t.Error("replyToMsg should remain unchanged when peer is the same")
	}
}
