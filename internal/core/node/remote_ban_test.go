package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// normalizeRemoteBanUntil — pure function, exhaustive matrix.
// ---------------------------------------------------------------------------

// TestNormalizeRemoteBanUntil_ZeroInputFallsBackToDefault pins the contract
// that a notice without `until` (e.g. minimal {"code":"peer-banned"}) still
// produces a bounded record. peerBanIncompatible is reused as the fallback
// because it is the local policy's natural "we don't know when" value.
func TestNormalizeRemoteBanUntil_ZeroInputFallsBackToDefault(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	got := normalizeRemoteBanUntil(time.Time{}, now)

	want := now.Add(peerBanIncompatible)
	if !got.Equal(want) {
		t.Fatalf("zero input must fall back to default; got %v, want %v", got, want)
	}
}

// TestNormalizeRemoteBanUntil_LongWindowPreserved pins the policy shift
// from the earlier cap-at-24h design: a peer that asks for a very long
// ban (decades) must be recorded verbatim. Fighting the remote by downgrading
// the window only wastes connect attempts against a node that has already
// decided it will not talk to us.
func TestNormalizeRemoteBanUntil_LongWindowPreserved(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	tenYears := now.AddDate(10, 0, 0)

	got := normalizeRemoteBanUntil(tenYears, now)

	if !got.Equal(tenYears) {
		t.Fatalf("long window must be preserved; got %v, want %v", got, tenYears)
	}
}

// TestNormalizeRemoteBanUntil_PastInputNormalisedToNow prevents the dialler
// from recording a negative window that downstream code (`now.Before(*until)`)
// would treat as "not banned" the moment it lands. A past `until` is
// nonsense from the wire, but mapping it to `now` keeps the contract that
// recordRemoteBanLocked never produces an immediately-stale entry.
func TestNormalizeRemoteBanUntil_PastInputNormalisedToNow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	past := now.Add(-1 * time.Hour)

	got := normalizeRemoteBanUntil(past, now)

	if !got.Equal(now) {
		t.Fatalf("past input must normalise to now; got %v, want %v", got, now)
	}
}

// TestNormalizeRemoteBanUntil_ShortFutureInputUnchanged confirms the happy
// path — a short, sane wire value passes through untouched. Without this
// we'd silently downgrade reasonable bans.
func TestNormalizeRemoteBanUntil_ShortFutureInputUnchanged(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	good := now.Add(6 * time.Hour)

	got := normalizeRemoteBanUntil(good, now)

	if !got.Equal(good) {
		t.Fatalf("short future input must pass through unchanged; got %v, want %v", got, good)
	}
}

// ---------------------------------------------------------------------------
// recordRemoteBanLocked — state mutation under unknown-peer safety.
// ---------------------------------------------------------------------------

// TestRecordRemoteBanLocked_UnknownPeerIgnored guards the unbounded-growth
// path: if we honoured bans for arbitrary addresses, a hostile peer could
// inflate persistedMeta by inventing addresses inside its notice. The
// dialler only honours bans for peers it already knows about.
func TestRecordRemoteBanLocked_UnknownPeerIgnored(t *testing.T) {
	t.Parallel()

	svc := &Service{
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
	}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	addr := domain.PeerAddress("10.0.0.1:64646")

	got := svc.recordRemoteBanLocked(addr, protocol.PeerBannedReasonPeerBan, now.Add(time.Hour), now)

	if !got.IsZero() {
		t.Fatalf("unknown peer must return zero effective time, got %v", got)
	}
	if _, exists := svc.persistedMeta[addr]; exists {
		t.Fatal("unknown peer must not be inserted into persistedMeta")
	}
}

// TestRecordRemoteBanLocked_KnownPeerStoresEffectiveExpiry covers the happy
// path: a known peer gets RemoteBannedUntil and RemoteBanReason populated,
// the returned effective time matches the persisted value, and the persisted
// value is the clamped (not raw) expiration.
func TestRecordRemoteBanLocked_KnownPeerStoresEffectiveExpiry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	addr := domain.PeerAddress("10.0.0.1:64646")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
	}

	remoteUntil := now.Add(2 * time.Hour)
	got := svc.recordRemoteBanLocked(addr, protocol.PeerBannedReasonPeerBan, remoteUntil, now)

	// In-range input — effective must equal raw.
	if !got.Equal(remoteUntil) {
		t.Fatalf("effective expiry mismatch: got %v, want %v", got, remoteUntil)
	}

	entry := svc.persistedMeta[addr]
	if entry.RemoteBannedUntil == nil {
		t.Fatal("RemoteBannedUntil must be set after record")
	}
	if !entry.RemoteBannedUntil.Equal(remoteUntil) {
		t.Errorf("persisted RemoteBannedUntil = %v, want %v", *entry.RemoteBannedUntil, remoteUntil)
	}
	if entry.RemoteBanReason != string(protocol.PeerBannedReasonPeerBan) {
		t.Errorf("RemoteBanReason = %q, want %q", entry.RemoteBanReason, string(protocol.PeerBannedReasonPeerBan))
	}
}

// TestRecordRemoteBanLocked_LongExpiryPreserved pins the no-cap policy: a
// remote that asks for a decade-long ban must be recorded verbatim rather
// than trimmed to a local maximum. A peer that doesn't want to talk to us
// is authoritative about when it will — fighting that decision by
// downgrading the window just wastes connect attempts on a node that has
// already told us to go away.
func TestRecordRemoteBanLocked_LongExpiryPreserved(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	addr := domain.PeerAddress("10.0.0.1:64646")

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr},
		},
	}

	tenYears := now.AddDate(10, 0, 0)
	got := svc.recordRemoteBanLocked(addr, protocol.PeerBannedReasonPeerBan, tenYears, now)

	if !got.Equal(tenYears) {
		t.Fatalf("returned effective expiry = %v, want %v (verbatim)", got, tenYears)
	}

	entry := svc.persistedMeta[addr]
	if entry.RemoteBannedUntil == nil {
		t.Fatal("RemoteBannedUntil must be set after record")
	}
	if !entry.RemoteBannedUntil.Equal(tenYears) {
		t.Errorf("persisted RemoteBannedUntil = %v, want %v (verbatim)", *entry.RemoteBannedUntil, tenYears)
	}
}

// TestRecordRemoteBanLocked_ZeroPersistedMetaSafe guards the early-init path.
// The notice handler may run before persistedMeta is allocated (very early
// boot, or a Service constructed in a non-standard test harness); the helper
// must not panic, just no-op.
func TestRecordRemoteBanLocked_ZeroPersistedMetaSafe(t *testing.T) {
	t.Parallel()

	svc := &Service{} // persistedMeta is nil
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	got := svc.recordRemoteBanLocked("10.0.0.1:64646", protocol.PeerBannedReasonPeerBan, now.Add(time.Hour), now)
	if !got.IsZero() {
		t.Fatalf("nil persistedMeta must return zero, got %v", got)
	}
}

// TestRecordRemoteBanLocked_OverwritesPreviousBan ensures a fresh notice
// from the same peer replaces the old expiration rather than being skipped
// or merged. Otherwise an extended ban from the responder would silently
// preserve the shorter window already on disk. reason=peer-ban is the only
// scope written through recordRemoteBanLocked (reason=blacklisted lands in
// the IP-wide table via recordRemoteIPBanLocked), so both writes here use
// the per-peer reason.
func TestRecordRemoteBanLocked_OverwritesPreviousBan(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	addr := domain.PeerAddress("10.0.0.1:64646")

	old := now.Add(15 * time.Minute)
	entry := &peerEntry{Address: addr, RemoteBannedUntil: &old, RemoteBanReason: string(protocol.PeerBannedReasonPeerBan)}
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{addr: entry},
	}

	fresh := now.Add(3 * time.Hour)
	got := svc.recordRemoteBanLocked(addr, protocol.PeerBannedReasonPeerBan, fresh, now)

	if !got.Equal(fresh) {
		t.Fatalf("returned effective expiry = %v, want %v", got, fresh)
	}
	if !entry.RemoteBannedUntil.Equal(fresh) {
		t.Errorf("persisted RemoteBannedUntil not refreshed: got %v, want %v", *entry.RemoteBannedUntil, fresh)
	}
	if entry.RemoteBanReason != string(protocol.PeerBannedReasonPeerBan) {
		t.Errorf("reason not refreshed: got %q, want %q",
			entry.RemoteBanReason, string(protocol.PeerBannedReasonPeerBan))
	}
}

// ---------------------------------------------------------------------------
// clearRemoteBanLocked — drop on successful handshake.
// ---------------------------------------------------------------------------

// TestClearRemoteBanLocked_DropsBothFields covers the contract that a
// successful handshake reconfirms the peer is accepting us, so both the
// expiration and the reason go away in one shot. Leaving Reason behind
// would leak stale evidence into peer diagnostics.
func TestClearRemoteBanLocked_DropsBothFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	addr := domain.PeerAddress("10.0.0.1:64646")

	until := now.Add(time.Hour)
	entry := &peerEntry{
		Address:           addr,
		RemoteBannedUntil: &until,
		RemoteBanReason:   string(protocol.PeerBannedReasonPeerBan),
	}
	svc := &Service{persistedMeta: map[domain.PeerAddress]*peerEntry{addr: entry}}

	svc.clearRemoteBanLocked(addr)

	if entry.RemoteBannedUntil != nil {
		t.Errorf("RemoteBannedUntil = %v, want nil", *entry.RemoteBannedUntil)
	}
	if entry.RemoteBanReason != "" {
		t.Errorf("RemoteBanReason = %q, want empty", entry.RemoteBanReason)
	}
}

// TestClearRemoteBanLocked_UnknownPeerIsNoop matches the read-side
// invariant: unknown addresses never produce side effects. The handshake
// success path may call clear without checking known-ness first.
func TestClearRemoteBanLocked_UnknownPeerIsNoop(t *testing.T) {
	t.Parallel()

	svc := &Service{persistedMeta: make(map[domain.PeerAddress]*peerEntry)}

	// Must not panic, must not create entries.
	svc.clearRemoteBanLocked("10.0.0.99:64646")

	if len(svc.persistedMeta) != 0 {
		t.Fatalf("clear on unknown peer must not insert entries, got %d", len(svc.persistedMeta))
	}
}

// TestClearRemoteBanLocked_NilPersistedMetaSafe — early-boot safety, same
// as the record-side counterpart.
func TestClearRemoteBanLocked_NilPersistedMetaSafe(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	// Must not panic.
	svc.clearRemoteBanLocked("10.0.0.1:64646")
}

// TestClearRemoteBanLocked_ReportsMutationSignal pins the return-value
// contract: the caller uses `true` to decide whether to flushPeerState.
// Returning `true` on no-op paths would trigger unnecessary disk writes;
// returning `false` on real clears would leave a stale RemoteBannedUntil
// on disk after a crash — exactly the non-durable-recovery bug the flush
// wiring closes. The three branches covered here mirror the three early
// returns inside clearRemoteBanLocked.
func TestClearRemoteBanLocked_ReportsMutationSignal(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	until := now.Add(time.Hour)

	cases := []struct {
		name string
		init func() *Service
		addr domain.PeerAddress
		want bool
	}{
		{
			name: "clears real ban",
			init: func() *Service {
				addr := domain.PeerAddress("10.0.0.1:64646")
				return &Service{persistedMeta: map[domain.PeerAddress]*peerEntry{
					addr: {
						Address:           addr,
						RemoteBannedUntil: &until,
						RemoteBanReason:   string(protocol.PeerBannedReasonPeerBan),
					},
				}}
			},
			addr: "10.0.0.1:64646",
			want: true,
		},
		{
			name: "unknown peer is no-op",
			init: func() *Service {
				return &Service{persistedMeta: map[domain.PeerAddress]*peerEntry{}}
			},
			addr: "10.0.0.99:64646",
			want: false,
		},
		{
			name: "already cleared entry is no-op",
			init: func() *Service {
				addr := domain.PeerAddress("10.0.0.1:64646")
				return &Service{persistedMeta: map[domain.PeerAddress]*peerEntry{
					addr: {Address: addr}, // RemoteBannedUntil == nil, Reason == ""
				}}
			},
			addr: "10.0.0.1:64646",
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.init().clearRemoteBanLocked(tc.addr)
			if got != tc.want {
				t.Fatalf("clearRemoteBanLocked returned %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isPeerRemoteBannedLocked — dial gate read.
// ---------------------------------------------------------------------------

// TestIsPeerRemoteBannedLocked_UnknownPeerFalse — the dial gate must treat
// unknown addresses as eligible. Otherwise a missing persistedMeta entry
// (e.g. a new bootstrap peer) would silently be excluded.
func TestIsPeerRemoteBannedLocked_UnknownPeerFalse(t *testing.T) {
	t.Parallel()

	svc := &Service{persistedMeta: make(map[domain.PeerAddress]*peerEntry)}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	if svc.isPeerRemoteBannedLocked("10.0.0.99:64646", now) {
		t.Fatal("unknown peer must be considered not banned")
	}
}

// TestIsPeerRemoteBannedLocked_NilUntilFalse covers the common case where
// the peer is known but never received a ban notice — RemoteBannedUntil is
// nil and the gate must let the dial through.
func TestIsPeerRemoteBannedLocked_NilUntilFalse(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:64646")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr}, // RemoteBannedUntil = nil
		},
	}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	if svc.isPeerRemoteBannedLocked(addr, now) {
		t.Fatal("known peer with nil RemoteBannedUntil must not be banned")
	}
}

// TestIsPeerRemoteBannedLocked_ElapsedFalse — the ban window is bounded;
// after expiration the gate must reopen so the peer is dialable again
// without a separate cleanup pass.
func TestIsPeerRemoteBannedLocked_ElapsedFalse(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:64646")
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Hour)

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr, RemoteBannedUntil: &expired},
		},
	}

	if svc.isPeerRemoteBannedLocked(addr, now) {
		t.Fatal("elapsed RemoteBannedUntil must not gate the dial")
	}
}

// TestIsPeerRemoteBannedLocked_ActiveTrue — the positive path that the
// PeerProvider gate 6c.2 ultimately depends on. Without this the entire
// honour-remote-ban feature is silently broken.
func TestIsPeerRemoteBannedLocked_ActiveTrue(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:64646")
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	active := now.Add(time.Hour)

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr, RemoteBannedUntil: &active},
		},
	}

	if !svc.isPeerRemoteBannedLocked(addr, now) {
		t.Fatal("active RemoteBannedUntil must gate the dial")
	}
}

// TestIsPeerRemoteBannedLocked_BoundaryAtUntilFalse — the comparison is
// `now.Before(until)`, so `now == until` is treated as "ban just lifted".
// This pins the boundary so a future change to `<=` would surface here
// rather than silently keep the peer banned for a tick longer than the
// remote asked for.
func TestIsPeerRemoteBannedLocked_BoundaryAtUntilFalse(t *testing.T) {
	t.Parallel()

	addr := domain.PeerAddress("10.0.0.1:64646")
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	atBoundary := now // *RemoteBannedUntil == now

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			addr: {Address: addr, RemoteBannedUntil: &atBoundary},
		},
	}

	if svc.isPeerRemoteBannedLocked(addr, now) {
		t.Fatal("boundary now == until must be treated as ban lifted (now.Before(until) is false)")
	}
}

// ---------------------------------------------------------------------------
// recordRemoteIPBanLocked — IP-wide state mutation. Unlike the per-peer
// helper this MUST accept unknown peers, because the ban key is the
// server IP and later sibling discovery (peer exchange, announce) needs
// the gate to already be set.
// ---------------------------------------------------------------------------

// TestRecordRemoteIPBanLocked_UnknownPeerStillRecords is the headline
// difference from recordRemoteBanLocked: when the responder sends
// reason=blacklisted, the ban must apply to every sibling peer behind
// the same IP — including peers we haven't discovered yet. Tying the
// record to persistedMeta the way per-peer bans do would let a sibling
// introduced tomorrow sail past the gate and restart the retry storm
// this notice was designed to end.
func TestRecordRemoteIPBanLocked_UnknownPeerStillRecords(t *testing.T) {
	t.Parallel()

	svc := &Service{
		persistedMeta:   make(map[domain.PeerAddress]*peerEntry),
		remoteBannedIPs: make(map[string]remoteIPBanEntry),
	}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	until := now.Add(2 * time.Hour)

	got := svc.recordRemoteIPBanLocked("10.0.0.1", protocol.PeerBannedReasonBlacklisted, until, now)

	if !got.Equal(until) {
		t.Fatalf("effective = %v, want %v", got, until)
	}
	entry, ok := svc.remoteBannedIPs["10.0.0.1"]
	if !ok {
		t.Fatal("remoteBannedIPs entry missing after record")
	}
	if !entry.Until.Equal(until) {
		t.Errorf("entry.Until = %v, want %v", entry.Until, until)
	}
	if entry.Reason != string(protocol.PeerBannedReasonBlacklisted) {
		t.Errorf("entry.Reason = %q, want %q", entry.Reason, string(protocol.PeerBannedReasonBlacklisted))
	}
}

// TestRecordRemoteIPBanLocked_EmptyIPNoOp pins the contract that the
// caller must extract the host before calling — an empty IP key would
// collide every blacklist record on the same bucket and break sibling
// lookup. Helper returns zero so the caller knows not to flush.
func TestRecordRemoteIPBanLocked_EmptyIPNoOp(t *testing.T) {
	t.Parallel()

	svc := &Service{remoteBannedIPs: make(map[string]remoteIPBanEntry)}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	got := svc.recordRemoteIPBanLocked("", protocol.PeerBannedReasonBlacklisted, now.Add(time.Hour), now)

	if !got.IsZero() {
		t.Fatalf("empty IP must return zero, got %v", got)
	}
	if len(svc.remoteBannedIPs) != 0 {
		t.Fatalf("empty IP must not insert into remoteBannedIPs, got %d entries", len(svc.remoteBannedIPs))
	}
}

// TestRecordRemoteIPBanLocked_NilMapAllocatesLazily covers the
// constructor-bypass path used by tests and early-boot Services: the
// map is allocated on demand so the helper is safe even before
// NewService has run.
func TestRecordRemoteIPBanLocked_NilMapAllocatesLazily(t *testing.T) {
	t.Parallel()

	svc := &Service{} // remoteBannedIPs is nil
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	until := now.Add(time.Hour)

	got := svc.recordRemoteIPBanLocked("10.0.0.1", protocol.PeerBannedReasonBlacklisted, until, now)

	if !got.Equal(until) {
		t.Fatalf("effective = %v, want %v", got, until)
	}
	if _, ok := svc.remoteBannedIPs["10.0.0.1"]; !ok {
		t.Fatal("remoteBannedIPs must be lazily allocated and populated")
	}
}

// TestRecordRemoteIPBanLocked_OverwritesPreviousEntry mirrors the
// per-peer overwrite contract: a fresh notice from the same IP replaces
// whatever was there rather than leaving the shorter window in place.
func TestRecordRemoteIPBanLocked_OverwritesPreviousEntry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	svc := &Service{
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: now.Add(15 * time.Minute), Reason: string(protocol.PeerBannedReasonPeerBan)},
		},
	}

	fresh := now.Add(3 * time.Hour)
	got := svc.recordRemoteIPBanLocked("10.0.0.1", protocol.PeerBannedReasonBlacklisted, fresh, now)

	if !got.Equal(fresh) {
		t.Fatalf("effective = %v, want %v", got, fresh)
	}
	entry := svc.remoteBannedIPs["10.0.0.1"]
	if !entry.Until.Equal(fresh) {
		t.Errorf("entry.Until = %v, want %v", entry.Until, fresh)
	}
	if entry.Reason != string(protocol.PeerBannedReasonBlacklisted) {
		t.Errorf("entry.Reason = %q, want %q", entry.Reason, string(protocol.PeerBannedReasonBlacklisted))
	}
}

// ---------------------------------------------------------------------------
// isRemoteIPBannedLocked — IP-wide dial gate read.
// ---------------------------------------------------------------------------

// TestIsRemoteIPBannedLocked_ActiveTrue — positive path that the sibling
// suppression feature depends on. Without this the entire reason=blacklisted
// blast-radius fix is silently broken.
func TestIsRemoteIPBannedLocked_ActiveTrue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	svc := &Service{
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: now.Add(time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}

	if !svc.isRemoteIPBannedLocked("10.0.0.1", now) {
		t.Fatal("active IP-wide remote ban must gate the dial")
	}
}

// TestIsRemoteIPBannedLocked_ElapsedFalse — expiration reopens the gate
// without a separate cleanup pass, matching the per-peer behaviour.
func TestIsRemoteIPBannedLocked_ElapsedFalse(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	svc := &Service{
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: now.Add(-time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}

	if svc.isRemoteIPBannedLocked("10.0.0.1", now) {
		t.Fatal("elapsed IP-wide remote ban must not gate the dial")
	}
}

// TestIsRemoteIPBannedLocked_UnknownIPFalse — unknown IPs must be
// dialable by default; otherwise a missing entry would silently exclude
// every sibling peer discovered for the first time.
func TestIsRemoteIPBannedLocked_UnknownIPFalse(t *testing.T) {
	t.Parallel()

	svc := &Service{remoteBannedIPs: make(map[string]remoteIPBanEntry)}
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	if svc.isRemoteIPBannedLocked("10.0.0.99", now) {
		t.Fatal("unknown IP must be considered not banned")
	}
}

// TestIsRemoteIPBannedLocked_EmptyIPFalse guards the degenerate call
// site: a PeerAddress that fails splitHostPort must not accidentally
// match a sentinel empty-string key.
func TestIsRemoteIPBannedLocked_EmptyIPFalse(t *testing.T) {
	t.Parallel()

	svc := &Service{
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"": {Until: time.Now().Add(time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}
	if svc.isRemoteIPBannedLocked("", time.Now()) {
		t.Fatal("empty IP key must not be treated as banned")
	}
}

// ---------------------------------------------------------------------------
// isPeerRemoteBannedLocked — sibling propagation via IP-wide record.
// ---------------------------------------------------------------------------

// TestIsPeerRemoteBannedLocked_IPBanSuppressesSibling is the core
// regression guard for the blast-radius fix: when the responder bans
// the IP, a sibling PeerAddress on the same IP that has NO per-peer
// record must still be skipped by the dial gate. Before the fix only
// the single PeerAddress that carried the notice was suppressed, which
// left the dialler hammering the remaining sibling peers on the
// blacklisted egress IP.
func TestIsPeerRemoteBannedLocked_IPBanSuppressesSibling(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	sibling := domain.PeerAddress("10.0.0.1:64646") // different port, same IP

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			sibling: {Address: sibling}, // no per-peer remote ban
		},
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: now.Add(time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}

	if !svc.isPeerRemoteBannedLocked(sibling, now) {
		t.Fatal("sibling peer on IP-banned egress must be suppressed even without a per-peer record")
	}
}

// TestIsPeerRemoteBannedLocked_UnknownSiblingAlsoSuppressed covers the
// peer-exchange path: a brand-new PeerAddress we have never seen before
// still shares an egress IP with the peer that was told reason=blacklisted,
// so the dialler must refuse it. Tying suppression to persistedMeta
// would allow fresh discoveries to sail past the gate.
func TestIsPeerRemoteBannedLocked_UnknownSiblingAlsoSuppressed(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	unknown := domain.PeerAddress("10.0.0.1:7070") // no persistedMeta entry

	svc := &Service{
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: now.Add(time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}

	if !svc.isPeerRemoteBannedLocked(unknown, now) {
		t.Fatal("unknown sibling must be suppressed by IP-wide remote ban")
	}
}

// TestIsPeerRemoteBannedLocked_PerPeerDoesNotSuppressSibling is the
// symmetric regression guard: reason=peer-ban is scoped to one
// PeerAddress, so a sibling on the same IP with no per-peer entry must
// remain dialable. Confusing the two scopes would over-ban and starve
// the dialler of legitimate candidates.
func TestIsPeerRemoteBannedLocked_PerPeerDoesNotSuppressSibling(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	banned := domain.PeerAddress("10.0.0.1:64646")
	sibling := domain.PeerAddress("10.0.0.1:7070") // same IP, no per-peer ban
	until := now.Add(time.Hour)

	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{
			banned:  {Address: banned, RemoteBannedUntil: &until, RemoteBanReason: string(protocol.PeerBannedReasonPeerBan)},
			sibling: {Address: sibling},
		},
		remoteBannedIPs: make(map[string]remoteIPBanEntry), // no IP-wide record
	}

	if !svc.isPeerRemoteBannedLocked(banned, now) {
		t.Fatal("peer with per-peer ban must be suppressed")
	}
	if svc.isPeerRemoteBannedLocked(sibling, now) {
		t.Fatal("sibling without per-peer ban must stay dialable when reason was peer-ban")
	}
}

// ---------------------------------------------------------------------------
// clearRemoteIPBanLocked — drop signal for flushPeerState.
// ---------------------------------------------------------------------------

// TestClearRemoteIPBanLocked_DropsEntry — positive path: the map entry
// is removed in-place. Used by operator tooling and by the future
// successful-handshake-on-sibling heuristic.
func TestClearRemoteIPBanLocked_DropsEntry(t *testing.T) {
	t.Parallel()

	svc := &Service{
		remoteBannedIPs: map[string]remoteIPBanEntry{
			"10.0.0.1": {Until: time.Now().Add(time.Hour), Reason: string(protocol.PeerBannedReasonBlacklisted)},
		},
	}

	if !svc.clearRemoteIPBanLocked("10.0.0.1") {
		t.Fatal("clear must return true for a real entry")
	}
	if _, ok := svc.remoteBannedIPs["10.0.0.1"]; ok {
		t.Fatal("remoteBannedIPs entry must be removed")
	}
}

// TestClearRemoteIPBanLocked_UnknownIPNoOp mirrors the per-peer
// contract: no mutation, return false so the caller does not trigger
// an unnecessary flush.
func TestClearRemoteIPBanLocked_UnknownIPNoOp(t *testing.T) {
	t.Parallel()

	svc := &Service{remoteBannedIPs: make(map[string]remoteIPBanEntry)}

	if svc.clearRemoteIPBanLocked("10.0.0.99") {
		t.Fatal("clear on unknown IP must return false")
	}
}

// TestClearRemoteIPBanLocked_EmptyIPNoOp guards the degenerate key: an
// empty string must never collide with a sentinel entry.
func TestClearRemoteIPBanLocked_EmptyIPNoOp(t *testing.T) {
	t.Parallel()

	svc := &Service{remoteBannedIPs: make(map[string]remoteIPBanEntry)}
	if svc.clearRemoteIPBanLocked("") {
		t.Fatal("clear on empty IP must return false")
	}
}
