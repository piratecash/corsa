package datagram

import (
	"context"
	"crypto/ed25519"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// local_senders_test.go pins the attribution of locally originated sends
// (docs/rpc/datagram.md, `local_senders`): every SendLocal call is charged to
// the subsystem that made it, with exactly one outcome, and the shared local
// request quota says WHOSE request it turned away.

// localSendFixture is an origin counting into real Metrics, one hop from a
// target that declares the fixture's dtypes.
type localSendFixture struct {
	net     *fakeNetwork
	private ed25519.PrivateKey
	metrics *Metrics
	origin  *pipelineNode
	target  *pipelineNode
}

func newLocalSendFixture(t *testing.T) *localSendFixture {
	t.Helper()
	private, signer := newSigner(t)
	net := newFakeNetwork()
	metrics := NewMetrics()
	origin := newPipelineNode(t, net, nodeOpts{id: signer, metrics: metrics})
	target := newPipelineNode(t, net, nodeOpts{name: "target"})
	link(origin, target, false, false)
	route(origin, target.id, target.id, 1)
	return &localSendFixture{net: net, private: private, metrics: metrics, origin: origin, target: target}
}

func (f *localSendFixture) send(t *testing.T, sender LocalSender, frame LocalSendOpts) SendOutcome {
	t.Helper()
	return f.origin.pipeline.SendLocal(context.Background(), sender, frame)
}

func (f *localSendFixture) request(t *testing.T, seed string, dtype domain.DType) LocalSendOpts {
	t.Helper()
	return LocalSendOpts{Frame: requestFrame(t, requestOpts{label: newLabel(t, seed), dst: f.target.id, dtype: dtype})}
}

func (f *localSendFixture) routed(t *testing.T, signedAt time.Time) LocalSendOpts {
	t.Helper()
	return LocalSendOpts{Frame: signedRouted(t, routedOpts{
		private: f.private, src: f.origin.id, dst: f.target.id, now: signedAt,
	})}
}

func requireSendKind(t *testing.T, outcome SendOutcome, want SendOutcomeKind) {
	t.Helper()
	if outcome.Kind() != want {
		t.Fatalf("SendLocal = %s, want %s", outcome, want)
	}
}

func requireLocalSendInvariants(t *testing.T, snapshot LocalSendersSnapshot) {
	t.Helper()
	for sender, counts := range snapshot.BySender {
		if counts.Attempts != counts.Queued+counts.NoRoute+counts.Failed+counts.Rejected {
			t.Fatalf("%s: attempts %d != queued + no_route + failed + rejected in %+v", sender, counts.Attempts, counts)
		}
		subs := counts.GateRefused + counts.QuotaCapped + counts.SlotBusy + counts.Expired + counts.OtherRejected
		if counts.Rejected != subs {
			t.Fatalf("%s: rejected %d != sum of its causes %d in %+v: a rejection was returned without a cause",
				sender, counts.Rejected, subs, counts)
		}
	}
}

// TestLocalQuotaRefusalIsChargedToTheSenderItTurnedAway is the isolation
// detector the attribution exists for: one subsystem fills the shared local
// request quota, another is refused, and the refusal lands on the one that was
// turned away — while the per-dtype tally cannot tell them apart, because both
// ask with the same dtype.
func TestLocalQuotaRefusalIsChargedToTheSenderItTurnedAway(t *testing.T) {
	fixture := newLocalSendFixture(t)

	for i := 0; i < defaultReversePerUpstreamCap; i++ {
		requireSendKind(t, fixture.send(t, LocalSenderPresenceProber,
			fixture.request(t, "probe-"+strconv.Itoa(i), dtypeQuery)), SendQueued)
	}
	requireSendKind(t, fixture.send(t, LocalSenderIdentityResolver, fixture.request(t, "resolve", dtypeQuery)), SendRejected)

	snapshot := fixture.metrics.LocalSendersSnapshot()
	prober := snapshot.BySender[LocalSenderPresenceProber.String()]
	resolver := snapshot.BySender[LocalSenderIdentityResolver.String()]
	if resolver.QuotaCapped != 1 || resolver.Attempts != 1 {
		t.Fatalf("resolver = %+v, want its one attempt capped", resolver)
	}
	if prober.QuotaCapped != 0 || prober.Queued != defaultReversePerUpstreamCap {
		t.Fatalf("prober = %+v, want %d queued and no refusal", prober, defaultReversePerUpstreamCap)
	}

	var capped, refusedByDType uint64
	for _, counts := range snapshot.BySender {
		capped += counts.QuotaCapped
	}
	for _, count := range fixture.origin.reverse.LocalRefusals() {
		refusedByDType += count
	}
	if capped != refusedByDType {
		t.Fatalf("Σ quota_capped = %d, Σ LocalRefusals = %d: two cuts of the same refusals disagree", capped, refusedByDType)
	}
	requireLocalSendInvariants(t, snapshot)
}

// TestLocalSendCountsOneOutcomePerReturn walks every return of the local send
// paths once and expects exactly one count in the matching field — and the
// invariants on the whole set, which is what turns red when a new return
// leaves without a cause.
func TestLocalSendCountsOneOutcomePerReturn(t *testing.T) {
	fixture := newLocalSendFixture(t)
	now := fixture.origin.clock()

	cases := []struct {
		name   string
		sender LocalSender
		send   func() SendOutcome
		kind   SendOutcomeKind
		want   LocalSendCounts
	}{
		{"queued routed", LocalSenderIdentityDiscovery,
			func() SendOutcome { return fixture.send(t, LocalSenderIdentityDiscovery, fixture.routed(t, now)) },
			SendQueued, LocalSendCounts{Attempts: 1, Queued: 1}},
		{"no route", LocalSenderDMControl,
			func() SendOutcome {
				return fixture.send(t, LocalSenderDMControl, LocalSendOpts{Frame: requestFrame(t, requestOpts{
					label: newLabel(t, "nowhere"), dst: domain.PeerIdentity{0x42},
				})})
			},
			SendNoRoute, LocalSendCounts{Attempts: 1, NoRoute: 1}},
		{"gate refused", LocalSenderIdentityResolver,
			func() SendOutcome {
				return fixture.send(t, LocalSenderIdentityResolver, fixture.request(t, "gate", domain.DType("never_declared")))
			},
			SendRejected, LocalSendCounts{Attempts: 1, Rejected: 1, GateRefused: 1}},
		{"expired", LocalSenderIdentityDiscovery,
			func() SendOutcome {
				return fixture.send(t, LocalSenderIdentityDiscovery, fixture.routed(t, now.Add(-domain.DatagramFreshnessWindow+time.Second)))
			},
			SendRejected, LocalSendCounts{Attempts: 1, Rejected: 1, Expired: 1}},
		{"response mode", LocalSenderDMControl,
			func() SendOutcome {
				return fixture.send(t, LocalSenderDMControl, LocalSendOpts{Frame: responseFrame(t, responseOpts{
					label: newLabel(t, "never-asked"), subject: fixture.origin.id,
				})})
			},
			SendRejected, LocalSendCounts{Attempts: 1, Rejected: 1, OtherRejected: 1}},
		{"walk failed", LocalSenderPresenceProber,
			func() SendOutcome {
				fixture.net.refuseQueue(fixture.target.id)
				defer fixture.net.acceptQueue(fixture.target.id)
				return fixture.send(t, LocalSenderPresenceProber, fixture.request(t, "refused-queue", dtypeQuery))
			},
			SendFailed, LocalSendCounts{Attempts: 1, Failed: 1}},
		{"unset sender", LocalSenderUnset,
			func() SendOutcome { return fixture.send(t, LocalSenderUnset, fixture.request(t, "nobody", dtypeQuery)) },
			SendFailed, LocalSendCounts{Attempts: 1, Failed: 1}},
	}

	for _, testCase := range cases {
		key := testCase.sender.statsName()
		before := fixture.metrics.LocalSendersSnapshot().BySender[key]
		requireSendKind(t, testCase.send(), testCase.kind)
		after := fixture.metrics.LocalSendersSnapshot().BySender[key]
		if delta := after.minus(before); delta != testCase.want {
			t.Fatalf("%s: %s moved by %+v, want %+v", testCase.name, key, delta, testCase.want)
		}
	}

	// The two branches that need a FIRST send to set them up.
	busy := fixture.request(t, "same-label", dtypeQuery)
	requireSendKind(t, fixture.send(t, LocalSenderIdentityResolver, busy), SendQueued)
	before := fixture.metrics.LocalSendersSnapshot().BySender[LocalSenderIdentityResolver.String()]
	requireSendKind(t, fixture.send(t, LocalSenderIdentityResolver, busy), SendRejected)
	after := fixture.metrics.LocalSendersSnapshot().BySender[LocalSenderIdentityResolver.String()]
	if delta := after.minus(before); delta != (LocalSendCounts{Attempts: 1, Rejected: 1, SlotBusy: 1}) {
		t.Fatalf("a busy label moved the resolver by %+v", delta)
	}

	replayed := fixture.routed(t, now)
	requireSendKind(t, fixture.send(t, LocalSenderIdentityDiscovery, replayed), SendQueued)
	before = fixture.metrics.LocalSendersSnapshot().BySender[LocalSenderIdentityDiscovery.String()]
	requireSendKind(t, fixture.send(t, LocalSenderIdentityDiscovery, replayed), SendRejected)
	after = fixture.metrics.LocalSendersSnapshot().BySender[LocalSenderIdentityDiscovery.String()]
	if delta := after.minus(before); delta != (LocalSendCounts{Attempts: 1, Rejected: 1, OtherRejected: 1}) {
		t.Fatalf("a replayed routed frame moved discovery by %+v", delta)
	}

	snapshot := fixture.metrics.LocalSendersSnapshot()
	requireLocalSendInvariants(t, snapshot)

	// gate_refused is the local share of SendRefusals; on a node that carried
	// no transit the two cuts are equal.
	var gateRefused, sendRefusals uint64
	for _, counts := range snapshot.BySender {
		gateRefused += counts.GateRefused
	}
	for _, count := range fixture.metrics.Snapshot().SendRefusals {
		sendRefusals += count
	}
	if gateRefused != sendRefusals {
		t.Fatalf("Σ gate_refused = %d, Σ SendRefusals = %d on a node without transit", gateRefused, sendRefusals)
	}
}

// minus is the growth of one row between two readings.
func (c LocalSendCounts) minus(before LocalSendCounts) LocalSendCounts {
	return LocalSendCounts{
		Attempts:      c.Attempts - before.Attempts,
		Queued:        c.Queued - before.Queued,
		NoRoute:       c.NoRoute - before.NoRoute,
		Failed:        c.Failed - before.Failed,
		Rejected:      c.Rejected - before.Rejected,
		GateRefused:   c.GateRefused - before.GateRefused,
		QuotaCapped:   c.QuotaCapped - before.QuotaCapped,
		SlotBusy:      c.SlotBusy - before.SlotBusy,
		Expired:       c.Expired - before.Expired,
		OtherRejected: c.OtherRejected - before.OtherRejected,
	}
}

// TestUnsetLocalSenderIsRefusedAndCountedApart pins the zero value: it is not
// a sender, the send is refused before any route is looked at, and the attempt
// lands on its own `unattributed` row rather than on a real subsystem.
func TestUnsetLocalSenderIsRefusedAndCountedApart(t *testing.T) {
	fixture := newLocalSendFixture(t)

	outcome := fixture.send(t, LocalSenderUnset, fixture.request(t, "anonymous", dtypeQuery))
	requireSendKind(t, outcome, SendFailed)
	if !errors.Is(outcome.Err(), ErrLocalSenderUnset) {
		t.Fatalf("Err() = %v, want ErrLocalSenderUnset", outcome.Err())
	}
	if journal := fixture.net.journal(); len(journal) != 0 {
		t.Fatalf("an unattributed send reached the wire: %d events", len(journal))
	}
	if fixture.origin.reverse.Len() != 0 {
		t.Fatal("an unattributed send took a reverse slot")
	}

	snapshot := fixture.metrics.LocalSendersSnapshot()
	if got := snapshot.BySender[localSendersUnattributed]; got != (LocalSendCounts{Attempts: 1, Failed: 1}) {
		t.Fatalf("unattributed = %+v, want one failed attempt", got)
	}
	for _, sender := range LocalSenders() {
		if got := snapshot.BySender[sender.String()]; got != (LocalSendCounts{}) {
			t.Fatalf("%s was charged for an unattributed send: %+v", sender, got)
		}
	}
}

// TestLocalSendersSnapshotListsEverySender pins the shape: every sender and the
// unattributed row are present from the start, so zero is a reading and not a
// missing key, and the period stamps are there.
func TestLocalSendersSnapshotListsEverySender(t *testing.T) {
	started := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	snapshot := NewMetricsStartedAt(started).LocalSendersSnapshot()

	if len(snapshot.BySender) != len(LocalSenders())+1 {
		t.Fatalf("rows %v, want every sender plus %q", snapshot.BySender, localSendersUnattributed)
	}
	if _, ok := snapshot.BySender[localSendersUnattributed]; !ok {
		t.Fatalf("no %q row", localSendersUnattributed)
	}
	if snapshot.StartedAt == nil || !snapshot.StartedAt.Equal(started) || snapshot.ReadAt == nil {
		t.Fatalf("period = %v..%v, want it to start at %s", snapshot.StartedAt, snapshot.ReadAt, started)
	}
}

// TestLocalSenderNamesAreStable pins the labels dashboards key on and the
// closed range every walk iterates.
func TestLocalSenderNamesAreStable(t *testing.T) {
	want := map[LocalSender]string{
		LocalSenderIdentityResolver:  "identity_resolver",
		LocalSenderPresenceProber:    "presence_prober",
		LocalSenderIdentityDiscovery: "identity_discovery",
		LocalSenderDMControl:         "dm_control",
	}
	if len(LocalSenders()) != len(want) {
		t.Fatalf("LocalSenders() = %v, want %d senders", LocalSenders(), len(want))
	}
	for _, sender := range LocalSenders() {
		if sender.String() != want[sender] || !sender.Valid() {
			t.Fatalf("sender %d = %q (valid %v), want %q", sender, sender.String(), sender.Valid(), want[sender])
		}
	}
	if LocalSenderUnset.Valid() || localSenderBound.Valid() {
		t.Fatal("the zero value and the bound are not senders")
	}
}

// TestEveryRejectionNamesItsCause is the static half of the invariant: every
// SendOutcome literal of kind SendRejected in this package sets `cause`. A
// rejection without one would still be counted as rejected and fall into no
// cause, so the counters would stop adding up — on a live node, where no test
// is watching.
func TestEveryRejectionNamesItsCause(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, source, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if typeName, ok := literal.Type.(*ast.Ident); !ok || typeName.Name != "SendOutcome" {
				return true
			}
			keys := literalKeys(literal)
			if keys["kind"] != "SendRejected" {
				return true
			}
			checked++
			if _, named := keys["cause"]; !named {
				t.Errorf("%s: a SendRejected outcome is built without a cause", name)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("found no SendRejected literal: the guard is looking at the wrong thing")
	}
}

// literalKeys maps a composite literal's keys to the identifier each is set
// to; a value that is not a bare identifier maps to "".
func literalKeys(literal *ast.CompositeLit) map[string]string {
	keys := make(map[string]string, len(literal.Elts))
	for _, element := range literal.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok {
			continue
		}
		value := ""
		if ident, ok := pair.Value.(*ast.Ident); ok {
			value = ident.Name
		}
		keys[key.Name] = value
	}
	return keys
}
