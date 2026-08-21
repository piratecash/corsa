package node

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// contact_admission_test.go pins the WORK budget of the `contacts` reply.
//
// The response plane already meters bytes and frames per neighbour
// (peer_session_admission.go), and `contacts` is the one reply type that buys
// the wide 8 MiB budget. What no budget covered was what the bytes BUY: one
// Ed25519 signature verification per array element, run in a bare loop over the
// whole array. An authenticated neighbour answering a legitimate fetch_contacts
// with a maximum-size reply therefore bought tens of thousands of signature
// checks with one frame — and the byte burst admitted two of them back to back.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// countingContactBudget records how many verifications were CHARGED, which is
// the only observable that distinguishes "the reply was refused" from "the reply
// was verified and then discarded". grant caps how many it allows.
type countingContactBudget struct {
	grant   int
	charged int
}

func (b *countingContactBudget) ChargeContactVerify() bool {
	if b.charged >= b.grant {
		return false
	}
	b.charged++
	return true
}

// validContactFrames builds n contact entries that all VERIFY, cycling a small
// pool of real identities: the cost under test is one verification per array
// element, and that is paid per element whether or not the elements repeat.
func validContactFrames(t *testing.T, n int) []protocol.ContactFrame {
	t.Helper()
	const pool = 32
	identities := make([]*identity.Identity, pool)
	for i := range identities {
		id, err := identity.Generate()
		if err != nil {
			t.Fatalf("identity.Generate: %v", err)
		}
		identities[i] = id
	}
	contacts := make([]protocol.ContactFrame, n)
	for i := range contacts {
		id := identities[i%pool]
		contacts[i] = protocol.ContactFrame{
			Address: id.Address,
			PubKey:  identity.PublicKeyBase64(id.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(id.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(id),
		}
	}
	return contacts
}

// junkContactFrames builds n structurally complete but cryptographically
// worthless entries. They are what an attacker actually sends: filling the
// required fields is free, and every one of them reaches the verification the
// budget exists to meter.
func junkContactFrames(n int) []protocol.ContactFrame {
	contacts := make([]protocol.ContactFrame, n)
	for i := range contacts {
		contacts[i] = protocol.ContactFrame{
			Address: strings.Repeat("a", 40),
			PubKey:  strings.Repeat("A", 44),
			BoxKey:  strings.Repeat("B", 44),
			BoxSig:  strings.Repeat("C", 86),
		}
	}
	return contacts
}

// ---------------------------------------------------------------------------
// The count cap is checked BEFORE the loop
// ---------------------------------------------------------------------------

// TestOversizeContactsReplyChargesNoVerification is the finding.
//
// A reply past maxContactsPerResponse must cost ZERO verifications: the cap is
// read from len(), before the first element is touched. Refusing element by
// element would still let a maximum-size reply buy a full budget's worth of
// signature checks, and the whole point is that the expensive work never starts.
//
// The mutation this kills: moving the cap check inside the loop (or dropping it
// and relying on the budget alone) — the charge counter then reaches the grant
// instead of staying at zero.
func TestOversizeContactsReplyChargesNoVerification(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	budget := &countingContactBudget{grant: maxContactsPerResponse}

	report := svc.importAdvertisedContacts(budget, junkContactFrames(maxContactsPerResponse+1))

	if report.Outcome != contactImportRefusedCountCap {
		t.Fatalf("outcome = %v, want the count cap refusal", report.Outcome)
	}
	if budget.charged != 0 {
		t.Fatalf("an over-cap reply charged %d verifications, want 0: the cap is read after the work starts", budget.charged)
	}
	if report.Imported != 0 || report.Verified != 0 {
		t.Fatalf("a refused reply imported %d / verified %d, want 0/0", report.Imported, report.Verified)
	}
}

// TestContactsReplyAtTheCapIsFullyImported is the other side: the cap must not
// cut a legitimate exchange.
//
// A reply exactly at maxContactsPerResponse is the largest thing an honest
// producer sends — the network-side contactsFrame trims to that number — and
// every one of its entries has to be verified and imported.
//
// The mutation this kills: an off-by-one that refuses at the cap rather than
// past it, which would make the largest honest reply unusable.
func TestContactsReplyAtTheCapIsFullyImported(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	budget := &countingContactBudget{grant: maxContactsPerResponse}
	contacts := validContactFrames(t, maxContactsPerResponse)

	report := svc.importAdvertisedContacts(budget, contacts)

	if report.Outcome != contactImportCompleted {
		t.Fatalf("outcome = %v, want a completed import at exactly the cap", report.Outcome)
	}
	if report.Imported != maxContactsPerResponse {
		t.Fatalf("imported = %d, want every one of the %d contacts at the cap", report.Imported, maxContactsPerResponse)
	}
	if budget.charged != maxContactsPerResponse {
		t.Fatalf("charged = %d, want one verification per contact", budget.charged)
	}
}

// TestContactVerificationIsChargedImmediatelyBeforeTheCheck pins WHERE the
// charge lands, which is what §5 makes the second stage for: a structurally
// incomplete entry never reaches a signature check, so it must not spend a
// token either. Charging per array element instead would let an attacker drain
// the budget with entries that cost nothing to refuse, and starve the
// verification of the entries that follow them.
func TestContactVerificationIsChargedImmediatelyBeforeTheCheck(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	budget := &countingContactBudget{grant: maxContactsPerResponse}

	contacts := []protocol.ContactFrame{
		{Address: "", PubKey: "x", BoxKey: "y", BoxSig: "z"},
		{Address: "a", PubKey: "", BoxKey: "y", BoxSig: "z"},
		{Address: "a", PubKey: "x", BoxKey: "", BoxSig: "z"},
		{Address: "a", PubKey: "x", BoxKey: "y", BoxSig: ""},
	}
	contacts = append(contacts, validContactFrames(t, 2)...)

	report := svc.importAdvertisedContacts(budget, contacts)

	if budget.charged != 2 {
		t.Fatalf("charged = %d, want 2 — one per entry that actually reaches VerifyBoxKeyBinding", budget.charged)
	}
	if report.Imported != 2 {
		t.Fatalf("imported = %d, want the two complete entries", report.Imported)
	}
}

// TestExhaustedContactBudgetStopsVerifying pins the second stage doing its job
// within one reply: once the neighbour's budget is empty the loop stops, and it
// stops WITHOUT verifying the remainder.
func TestExhaustedContactBudgetStopsVerifying(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	budget := &countingContactBudget{grant: 3}

	report := svc.importAdvertisedContacts(budget, validContactFrames(t, 16))

	if report.Outcome != contactImportBudgetExhausted {
		t.Fatalf("outcome = %v, want the budget-exhausted outcome", report.Outcome)
	}
	if budget.charged != 3 || report.Verified != 3 {
		t.Fatalf("charged = %d / verified = %d, want 3/3 — the loop kept going past an empty budget",
			budget.charged, report.Verified)
	}
}

// ---------------------------------------------------------------------------
// The per-remote bucket
// ---------------------------------------------------------------------------
//
// The sustained bucket itself is pinned in contact_verify_budget_test.go: it is
// node-scoped and keyed on the remote endpoint, not carried by the session, so
// a reconnect (or a fresh recovery dial) is no longer a reset.

// TestContactBudgetIsSubordinateToTheByteBurst is the sizing argument, pinned as
// an assertion so a future edit of either number has to re-read it.
//
// The finding was that the byte budget alone bounds the reply: the response
// plane admits up to protocol.MaxResponseLine per line and 16 MiB of burst, and
// at ~265 wire bytes per contact that is tens of thousands of signature checks.
// The count cap has to bind FIRST — a reply at the cap must be comfortably
// inside one line, so the byte budget is never what decides how much crypto a
// neighbour buys.
func TestContactBudgetIsSubordinateToTheByteBurst(t *testing.T) {
	t.Parallel()

	if wire := maxContactsPerResponse * approximateContactWireBytes; wire >= maxResponseLineBytes {
		t.Fatalf("a reply at the count cap is %d wire bytes, which the line budget (%d) refuses first: the cap bounds nothing",
			wire, maxResponseLineBytes)
	}
	if contactVerifyBurst != maxContactsPerResponse {
		t.Fatalf("burst %d != count cap %d: one honest reply at the cap would be half-verified",
			contactVerifyBurst, maxContactsPerResponse)
	}
}

// ---------------------------------------------------------------------------
// Punishment, and what a reconnect costs
// ---------------------------------------------------------------------------

// TestRepeatedOversizeContactsRepliesEndTheSession pins the punishment model:
// one over-cap reply is a dropped reply, a SERIES is a neighbour that found out
// violations are free.
//
// It reuses the ledger peer_session_admission.go already owns — the same one the
// wide-line refusals feed — so a peer cannot split its abuse across the two
// gates to stay under both.
//
// The mutation this kills: dropping the reply without scoring it, after which an
// attacker repeats it for the lifetime of the session.
func TestRepeatedOversizeContactsRepliesEndTheSession(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	session := &peerSession{
		address:      domain.PeerAddress("10.4.4.4:64646"),
		peerIdentity: domain.PeerIdentityFromWire(strings.Repeat("c", 40)),
		sendCh:       make(chan peerSendItem, 4),
	}

	for i := 0; i < peerSessionViolationBudget; i++ {
		if err := svc.refuseOversizeContactsReply(session, maxContactsPerResponse+1); err != nil {
			t.Fatalf("refusal %d ended the session; %d are tolerated first: %v", i, peerSessionViolationBudget, err)
		}
		if session.sendQueueClosedForTest() {
			t.Fatalf("refusal %d closed the session", i)
		}
	}

	err := svc.refuseOversizeContactsReply(session, maxContactsPerResponse+1)
	if err == nil {
		t.Fatalf("the reply past a budget of %d tolerated violations did not end the session", peerSessionViolationBudget)
	}
	if !errors.Is(err, protocol.ErrRateLimited) {
		t.Fatalf("teardown error = %v, want one wrapping protocol.ErrRateLimited so markPeerDisconnected records `rate-limited`", err)
	}
	if !session.sendQueueClosedForTest() {
		t.Fatal("the teardown was reported but the session was left standing: the punishment depends on what the caller does with the error")
	}
	if cause := sessionCloseCauseFromError(err); cause != sessionClosePeerInitiated {
		t.Fatalf("close cause = %v, want peer-initiated so the disconnect_storm quarantine prices the reconnect", cause)
	}
}

// ---------------------------------------------------------------------------
// The sending side
// ---------------------------------------------------------------------------

// TestNetworkContactsReplyIsCappedAndTheLocalOneIsNot is the other end of the
// same rule, and the reason the cap does not cut legitimate traffic.
//
// contactsFrame() has NO count cap of its own: it serialises every address in
// s.boxKeys, which is bounded only by maxKnownIdentities (50 000) plus the
// pinned trust store. A node that legitimately holds more than the cap would
// therefore have had its reply refused by every peer running this guard — so the
// NETWORK reply is trimmed to the same number the receiver accepts, and the two
// ends agree by construction.
//
// The LOCAL RPC answer is deliberately untrimmed: dm_crypto looks a recipient's
// box key up in that list, and trimming it would make key lookup fail on a node
// with many correspondents for no security gain — no wire and no verification
// loop is involved.
//
// The mutation this kills: capping contactsFrame() itself (the local lookup
// silently loses entries) or leaving the network reply uncapped (an honest
// large node's reply is refused by the receiver).
func TestNetworkContactsReplyIsCappedAndTheLocalOneIsNot(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	const held = maxContactsPerResponse + 64
	for i := 0; i < held; i++ {
		address := contactTestAddress(i)
		svc.addKnownBoxKey(address, "boxkey")
		svc.addKnownPubKey(address, "pubkey")
		svc.addKnownBoxSig(address, "boxsig")
	}

	local := svc.contactsFrame()
	if len(local.Contacts) < held {
		t.Fatalf("the local contacts answer holds %d of %d: the RPC lookup lost entries", len(local.Contacts), held)
	}

	network := svc.contactsFrameForNetwork()
	if len(network.Contacts) != maxContactsPerResponse {
		t.Fatalf("the network contacts reply carries %d entries, want the cap %d",
			len(network.Contacts), maxContactsPerResponse)
	}
	if network.Count != len(network.Contacts) {
		t.Fatalf("count = %d but %d contacts travel: the header and the body disagree",
			network.Count, len(network.Contacts))
	}
}

func TestLastOnlineIsLocalTrustedContactMetadata(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	peer := domaintest.ID("last-online-local-metadata")
	if stored, err := svc.trust.remember(trustedContact{
		Address: peer.String(),
		PubKey:  "pubkey",
		BoxKey:  "boxkey",
	}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}
	want := time.Date(2026, time.August, 21, 7, 6, 16, 123456789, time.UTC)
	if updated, err := svc.trust.recordLastOnlineAt([]domain.PeerIdentity{peer}, want); err != nil || updated != 1 {
		t.Fatalf("record last online: updated=%d err=%v", updated, err)
	}
	svc.addKnownPubKey(peer.String(), "pubkey")
	svc.addKnownBoxKey(peer.String(), "boxkey")

	var localLastOnline string
	for _, contact := range svc.trustedContactsFrame().Contacts {
		if contact.Address == peer.String() {
			localLastOnline = contact.LastOnlineAt
			break
		}
	}
	if localLastOnline != want.Format(time.RFC3339Nano) {
		t.Fatalf("trusted contact last_online_at = %q, want %q", localLastOnline, want.Format(time.RFC3339Nano))
	}

	foundOnNetwork := false
	for _, contact := range svc.contactsFrameForNetwork().Contacts {
		if contact.Address != peer.String() {
			continue
		}
		foundOnNetwork = true
		if contact.LastOnlineAt != "" {
			t.Fatalf("P2P contact leaked local last_online_at %q", contact.LastOnlineAt)
		}
	}
	if !foundOnNetwork {
		t.Fatal("test setup: contact was absent from P2P fetch_contacts response")
	}
}

// bytesAllocatedBy reports how many bytes the call allocated.
//
// TotalAlloc is cumulative and unaffected by GC, so the delta is the allocation
// the call performed and nothing else. It is the only way to observe "the reply
// was built from the whole map and then cut down" from outside: both shapes
// return the same trimmed array, and only one of them pays for the map.
func bytesAllocatedBy(call func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	call()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestNetworkContactsReplyBoundsTheWalkAndNotOnlyTheResult is the finding: the
// wire cap has to bound the WORK, not just the answer.
//
// A `fetch_contacts` frame is four bytes of intent. Answering it by walking all
// of s.boxKeys — up to maxKnownIdentities (50 000) plus the pinned trust store —
// under knowledgeMu.RLock and materialising the whole set before cutting it to
// 4096 is an amplification vector of the same class as the solicited-reply
// budgets: the requester pays nothing, the responder pays for its entire
// knowledge base, and it holds a domain read lock while doing it.
//
// Three things are asserted, and each kills a different way of getting this
// wrong:
//
//   - the array the reply travels in has room for the CAP and not for the map,
//     which is what a build-then-trim cannot fake — appending past a 4096-slot
//     preallocation leaves a capacity in the tens of thousands;
//   - the network build allocates a small fraction of what the unbounded local
//     build does on the SAME node, which is the amplification stated directly;
//   - the reply is still a correct, duplicate-free sample of the real
//     knowledge, because a cheap wrong answer is not the goal.
//
// The mutation this kills: reverting contactsFrameForNetwork to
// `frame := s.contactsFrame(); frame.Contacts = frame.Contacts[:cap]`.
func TestNetworkContactsReplyBoundsTheWalkAndNotOnlyTheResult(t *testing.T) {
	// Deliberately NOT parallel: it measures process-wide allocation.
	svc := newTestService(t, config.NodeTypeFull)

	// Comfortably past the wire cap and still inside maxKnownIdentities, so the
	// bounded set never evicts the entries this test just wrote.
	const held = 10 * maxContactsPerResponse
	for i := 0; i < held; i++ {
		address := contactTestAddress(i)
		svc.addKnownBoxKey(address, "boxkey-"+address)
		svc.addKnownPubKey(address, "pubkey-"+address)
		svc.addKnownBoxSig(address, "boxsig-"+address)
	}

	network := svc.contactsFrameForNetwork()
	if len(network.Contacts) != maxContactsPerResponse {
		t.Fatalf("the network reply carries %d contacts, want the cap %d", len(network.Contacts), maxContactsPerResponse)
	}
	if cap(network.Contacts) > maxContactsPerResponse {
		t.Fatalf("the reply array has room for %d contacts and carries %d: the whole of s.boxKeys was serialised before the cap was applied",
			cap(network.Contacts), len(network.Contacts))
	}

	networkBytes := bytesAllocatedBy(func() { _ = svc.contactsFrameForNetwork() })
	localBytes := bytesAllocatedBy(func() { _ = svc.contactsFrame() })
	if networkBytes*4 >= localBytes {
		t.Fatalf("the bounded network build allocated %d bytes against the unbounded local build's %d on a node holding %d contacts: a cheap remote request still costs the whole knowledge base",
			networkBytes, localBytes, held)
	}

	// Correct, and a real sample of the real map.
	svc.knowledgeMu.RLock()
	defer svc.knowledgeMu.RUnlock()
	seen := make(map[string]struct{}, len(network.Contacts))
	for _, contact := range network.Contacts {
		if _, duplicate := seen[contact.Address]; duplicate {
			t.Fatalf("contact %q travels twice in one reply", contact.Address)
		}
		seen[contact.Address] = struct{}{}
		if want := svc.boxKeys[contact.Address]; contact.BoxKey != want {
			t.Fatalf("contact %q carries box key %q, want %q", contact.Address, contact.BoxKey, want)
		}
		if want := svc.pubKeys[contact.Address]; contact.PubKey != want {
			t.Fatalf("contact %q carries pub key %q, want %q", contact.Address, contact.PubKey, want)
		}
		if want := svc.boxSigs[contact.Address]; contact.BoxSig != want {
			t.Fatalf("contact %q carries box sig %q, want %q", contact.Address, contact.BoxSig, want)
		}
	}
}

// TestBoundedNetworkContactsReplyStillSamplesTheWholeSet pins the property the
// trim had and the bounded walk must not lose.
//
// Which 4096 of the 40 960 contacts travel is decided by Go's randomised map
// iteration, and that is deliberate rather than incidental: successive fetches
// from the same peer return different subsets, so a requester converges on the
// whole set over several passes instead of being pinned to one prefix forever.
// Sorting the addresses and taking the first 4096 would be "deterministic" and
// strictly worse — a contact is not more relevant for sorting earlier, and the
// tail would become permanently unreachable through this reply.
//
// Eight passes are compared because one pair can coincide (the iteration picks a
// random start bucket, and two runs can pick the same one); eight identical
// samples cannot happen by chance.
//
// The mutation this kills: making the bounded walk deterministic by sorting the
// addresses, or by walking a stable index.
func TestBoundedNetworkContactsReplyStillSamplesTheWholeSet(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	const held = 10 * maxContactsPerResponse
	for i := 0; i < held; i++ {
		address := contactTestAddress(i)
		svc.addKnownBoxKey(address, "boxkey")
		svc.addKnownPubKey(address, "pubkey")
		svc.addKnownBoxSig(address, "boxsig")
	}

	first := svc.contactsFrameForNetwork().Contacts
	for pass := 0; pass < 8; pass++ {
		again := svc.contactsFrameForNetwork().Contacts
		if len(again) != maxContactsPerResponse {
			t.Fatalf("pass %d carries %d contacts, want the cap %d", pass, len(again), maxContactsPerResponse)
		}
		if !sameContactOrder(first, again) {
			return
		}
	}
	t.Fatal("eight successive network replies returned the identical sample: the bounded walk pins the requester to one subset, and the rest of this node's contacts became unreachable through fetch_contacts")
}

func sameContactOrder(left, right []protocol.ContactFrame) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Address != right[i].Address {
			return false
		}
	}
	return true
}

// TestNetworkFetchContactsServesTheTrimmedReply pins the call site, because the
// trim is only worth anything if the wire path uses it.
//
// The two builders differ by one word at one call site, and the wrong one is the
// one that was there before: a network reply built from contactsFrame is exactly
// the uncapped array the receiver now refuses. It is asserted structurally
// because the observable — a reply longer than the cap — needs a node holding
// more than maxContactsPerResponse box keys wired to a live socket, which
// proves the same thing at a hundred times the cost.
//
// The mutation this kills: reverting the fetch_contacts case of
// dispatchNetworkFrame to contactsFrame().
func TestNetworkFetchContactsServesTheTrimmedReply(t *testing.T) {
	t.Parallel()

	calls := callsInsideFunctionBody(t, "service.go", "dispatchNetworkFrame")
	for _, name := range calls {
		if name == "contactsFrameForNetwork" {
			return
		}
	}
	t.Fatal("the network fetch_contacts reply is not built by contactsFrameForNetwork: an honest large node's reply is refused by every peer running the count cap")
}

// TestSessionContactSyncPunishesAnOversizeReply pins that the SESSION path routes
// an over-cap reply into the violation ledger.
//
// Without it the reply is merely dropped, which costs the neighbour nothing and
// can be repeated for the lifetime of the session — the exact "violations are
// free" state peerSessionViolationBudget exists to end. The refusal itself is
// covered behaviourally above; what this pins is that syncContactsViaSession
// reaches it.
//
// The mutation this kills: replacing the refuseOversizeContactsReply call in
// syncContactsViaSession with a bare `return 0, nil`.
func TestSessionContactSyncPunishesAnOversizeReply(t *testing.T) {
	t.Parallel()

	calls := callsInsideFunctionBody(t, "peer_management.go", "syncContactsViaSession")
	imports, punishes := false, false
	for _, name := range calls {
		switch name {
		case "importAdvertisedContacts":
			imports = true
		case "refuseOversizeContactsReply":
			punishes = true
		}
	}
	if !imports {
		t.Fatal("the session contact sync no longer goes through the metered import: the verification loop is unbudgeted again")
	}
	if !punishes {
		t.Fatal("the session contact sync drops an over-cap reply without scoring it: a neighbour repeats it for free")
	}
}

// contactTestAddress builds a distinct 40-character address string. The content
// is irrelevant here — nothing in this test verifies a binding — but the LENGTH
// is what makes the wire-size arithmetic of the cap realistic.
func contactTestAddress(i int) string {
	const digits = "0123456789abcdef"
	suffix := []byte("00000000")
	for pos := 7; pos >= 0; pos-- {
		suffix[pos] = digits[i&0xf]
		i >>= 4
	}
	return strings.Repeat("f", 32) + string(suffix)
}

// callsInsideFunctionBody returns the called names inside one function of one
// file of this package, in source order.
func callsInsideFunctionBody(t *testing.T, file, function string) []string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(thisFile), file), nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	var names []string
	found := false
	for _, declaration := range parsed.Decls {
		fn, isFunc := declaration.(*ast.FuncDecl)
		if !isFunc || fn.Name.Name != function {
			continue
		}
		found = true
		ast.Inspect(fn, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				names = append(names, fun.Sel.Name)
			case *ast.Ident:
				names = append(names, fun.Name)
			}
			return true
		})
	}
	if !found {
		t.Fatalf("function %s not found in %s", function, file)
	}
	return names
}

// sendQueueClosedForTest reports whether the session's upper queue has been
// fenced off, which is what peerSession.Close does first and therefore the
// observable that says "this session was torn down".
func (ps *peerSession) sendQueueClosedForTest() bool {
	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()
	return ps.sendClosed
}
