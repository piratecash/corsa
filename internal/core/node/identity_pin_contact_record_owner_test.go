package node

import (
	"bytes"
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestContactRecordServiceOwnsCompletionAfterCallerReturns(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, files := stagedContactRecord(t, s, peer)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	stage.owed.mu.Lock()
	state := stage.owed.completion.state
	stage.owed.mu.Unlock()
	if state != contactRecordHistoricalProtectionCompleted {
		t.Fatalf("Service worker left earned record obligation in state %v", state)
	}
}

func TestContactRecordFreshCandidateDoesNotRestageEarnedAllocation(t *testing.T) {
	s, peer := localContactFixture(t)
	_, first, files := stagedContactRecord(t, s, peer)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	store := s.secureSessions.store
	store.mu.Lock()
	beforeEvidence, beforeProtection, beforeChanges := store.evidenceRevision, store.protectionRevision, store.changes
	store.mu.Unlock()
	_, verified := verifiedContactRecord(t, s, peer)
	second, err := s.stageContactRecord(t.Context(), verified)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if second.owed != first.owed || store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection || store.changes != beforeChanges {
		t.Fatal("fresh candidate created a second job or mutated earned protection")
	}
}

func TestContactRecordPendingMirrorNeedsActualBarrierAndCurrentACK(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, files := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	if _, err := store.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	if store.contactKStatus(id) != domain.LocalContactKStagedUnconfirmed {
		t.Fatal("generic ACK falsely completed owed barrier")
	}
	if _, err := s.completeOwedContactRecord(t.Context(), stage, files); err != nil {
		t.Fatal(err)
	}
	if store.contactKStatus(id) != domain.LocalContactKConfirmed {
		t.Fatal("actual barrier did not publish completion mirror")
	}
	next := metadataSnapshot(t, store, id).metadata
	next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: time.Now(), SourceGroup: domain.SourceGroupLoopback}
	stageMetadata(t, store, id, next)
	if store.contactKStatus(id) != domain.LocalContactKStagedUnconfirmed {
		t.Fatal("historical barrier ACKed newer metadata")
	}
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	if entry.metadata.recordOwner != stage.owed || entry.metadata.recordMirror != contactRecordOwnerCompleted {
		t.Fatal("COW lost completed history")
	}
	_, verified := verifiedContactRecord(t, s, peer)
	before := metadataSnapshot(t, store, id)
	again, err := s.stageContactRecord(t.Context(), verified)
	if err != nil || again.disposition != contactRecordAlreadyHistoricalCompleted || again.owed != stage.owed || metadataSnapshot(t, store, id) != before {
		t.Fatal("fresh record restaged completed history", err)
	}
}

func TestContactRecordAlreadyEligibleWithoutOwnerDoesNotInventJob(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		t.Run(map[bool]string{false: "K1", true: "loaded"}[loaded], func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			store := s.secureSessions.store
			id := domain.PeerIdentityFromWire(peer.Address)
			if err := store.noteProvenInbound(id); err != nil {
				t.Fatal(err)
			}
			next := metadataSnapshot(t, store, id).metadata
			next.Contact = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: time.Now(), Reference: "earlier-K1"}
			stageMetadata(t, store, id, next)
			if loaded {
				if err := store.persist(); err != nil {
					t.Fatal(err)
				}
				replacement := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
				s.secureSessions.store = replacement
				store = replacement
			}
			candidate, verified := verifiedContactRecord(t, s, peer)
			before := metadataSnapshot(t, store, id)
			store.mu.Lock()
			evidence, protection, changes := store.evidenceRevision, store.protectionRevision, store.changes
			store.mu.Unlock()
			stage, err := s.stageContactRecord(t.Context(), verified)
			if err != nil || stage.disposition != contactRecordAlreadyEligibleNoRecordObligation || stage.owed != nil {
				t.Fatal("eligible existing protection invented a record job", err)
			}
			if replay, ok := candidate.stagedOutcome(); !ok || replay != stage {
				t.Fatal("no-job outcome not retained")
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.evidenceRevision != evidence || store.protectionRevision != protection || store.changes != changes || store.identities[id].classMetadata() != before.metadata {
				t.Fatal("no-job changed protection")
			}
		})
	}
}

func TestContactRecordOwnedZeroAndContactMutationRefuseBeforeEffects(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, _ := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	before := metadataSnapshot(t, store, id)
	for _, next := range []domain.IdentityPinMetadata{{}, before.metadata} {
		next.Contact = domain.IdentityPinAllocation{}
		if _, err := store.stagePinMetadata(before, next); !errors.Is(err, errContactRecordInvariant) {
			t.Fatal("owned Contact removal accepted", err)
		}
		if metadataSnapshot(t, store, id) != before {
			t.Fatal("refused mutation changed revisions")
		}
	}
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	if entry.metadata.recordOwner != stage.owed || entry.metadata.recordMirror != contactRecordOwnerPending {
		t.Fatal("refusal dropped owner")
	}
}

func TestContactRecordWorkerBusyAdvancesRRWithoutTicketLock(t *testing.T) {
	s, peer := localContactFixture(t)
	_, first, _ := stagedContactRecord(t, s, peer)
	other, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	_, second, _ := stagedContactRecord(t, s, other)
	firstID := domain.PeerIdentityFromWire(peer.Address)
	secondID := domain.PeerIdentityFromWire(other.Address)
	// Force first to be the next RR choice irrespective of random fixture IDs.
	if bytes.Compare(firstID[:], secondID[:]) > 0 {
		s.contactPins.cursor = &secondID
	}
	first.owed.mu.Lock()
	err = s.contactPins.scanOwed(t.Context())
	first.owed.mu.Unlock()
	if err != nil || s.contactPins.cursor == nil || *s.contactPins.cursor != firstID {
		t.Fatal("Busy did not return/advance RR", err)
	}
	if err := s.contactPins.scanOwed(t.Context()); err != nil {
		t.Fatal(err)
	}
	second.owed.mu.Lock()
	state := second.owed.completion.state
	second.owed.mu.Unlock()
	if state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("Busy ticket starved next identity")
	}
}

func TestContactRecordUnknownRetryRetainsOwnedTicketWithoutRestage(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	before := metadataSnapshot(t, store, id)
	failed := NewMockidentityPinDurableFiles(t)
	failed.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	failed.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, errors.New("unknown namespace commit")).Once()
	result, err := s.tryCompleteOwedContactRecord(t.Context(), stage.owed, failed)
	if err == nil || result.state != contactRecordAttemptFinished || result.completion.attemptCovered {
		t.Fatal("Unknown became successful coverage")
	}
	s.deleteTrustedContactFrame(id)
	if err := s.contactPins.scanOwed(t.Context()); err != nil {
		t.Fatal(err)
	}
	if metadataSnapshot(t, store, id) != before {
		t.Fatal("worker retry restaged/revised earned allocation")
	}
	stage.owed.mu.Lock()
	state := stage.owed.completion.state
	stage.owed.mu.Unlock()
	if state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("worker lost ticket after contact deletion")
	}
	_ = real
}

func TestContactRecordOverflowDoesNotPublishOwner(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	candidate, verified := verifiedContactRecord(t, s, peer)
	store := s.secureSessions.store
	store.mu.Lock()
	store.evidenceRevision = math.MaxUint64
	store.mu.Unlock()
	if _, err := s.stageContactRecord(t.Context(), verified); !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
		t.Fatal(err)
	}
	if _, ok := candidate.stagedOutcome(); ok {
		t.Fatal("refused stage published an owner")
	}
	if selected := store.selectContactRecordOwedAfter(nil); selected != nil {
		t.Fatal("refused stage retained a pending owner")
	}
}

func TestContactRecordCooperativeDrainDeadlineAndRetiredSelection(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	_, verified := verifiedContactRecord(t, s, peer)
	id := domain.PeerIdentityFromWire(peer.Address)
	impostor := legacyOutboundSession(legacyAttackerAddress, id)
	impostor.capabilities = routingTwinsCaps
	s.peerMu.Lock()
	s.sessions[impostor.address] = impostor
	s.peerMu.Unlock()
	admitted, release := make(chan struct{}), make(chan struct{})
	handlerDone := make(chan struct{})
	s.routingInputAdmittedHook = func(_ routingSender, kind string) {
		if kind == "announce_routes" {
			close(admitted)
			<-release
		}
	}
	go func() {
		defer close(handlerDone)
		s.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(id, idTargetX))
	}()
	<-admitted
	defer func() { close(release); <-handlerDone }()
	stage, err := s.stageContactRecord(t.Context(), verified)
	if err != nil {
		t.Fatal(err)
	}
	other, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	_, otherStage, _ := stagedContactRecord(t, s, other)
	otherID := domain.PeerIdentityFromWire(other.Address)
	if bytes.Compare(id[:], otherID[:]) > 0 {
		s.contactPins.cursor = &otherID
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := s.contactPins.scanOwed(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("held drain did not honor attempt context", err)
	}
	s.unprovenRouting.mu.Lock()
	retained := len(s.unprovenRouting.drained[id])
	_, owed := s.unprovenRouting.owed[id]
	s.unprovenRouting.mu.Unlock()
	if retained != 0 || !owed {
		t.Fatal("deadline retained waiter or lost last-writer purge")
	}
	if stage.owed.completion.state != contactRecordCoveredOwed {
		t.Fatal("cancelled drain lost covering receipt")
	}
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	otherStage.owed.mu.Lock()
	state := otherStage.owed.completion.state
	otherStage.owed.mu.Unlock()
	if state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("timed-out first identity blocked next RR owner")
	}
	// One selected reference may retire, but never rebind a replacement.
	selected := s.secureSessions.store.selectContactRecordOwedAfter(nil)
	if selected == nil || selected.ticket != stage.owed {
		t.Fatal("pending ticket not retained")
	}
	removeContactPinForTest(t, s, id)
	if err := s.secureSessions.store.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	files, err := s.secureSessions.store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	result, err := s.tryCompleteOwedContactRecord(t.Context(), selected.ticket, files)
	if !errors.Is(err, errPinMetadataSnapshotStale) || result.completion.state != contactRecordStaleOwner {
		t.Fatal("retired selected ticket rebound replacement", err)
	}
}

func TestContactRecordHeldBackendIsNotDetachedOnDeadline(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	files := NewMockidentityPinDurableFiles(t)
	entered, release := make(chan struct{}), make(chan struct{})
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		close(entered)
		<-release
		// Deliberately noncooperative backend models an executing syscall. Its
		// eventual outcome is real, not a timeout fabricated by a detached waiter.
		return real.WriteMain(context.WithoutCancel(ctx), raw)
	}).Once()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan contactRecordAttempt, 1)
	go func() { result, _ := s.tryCompleteOwedContactRecord(ctx, stage.owed, files); done <- result }()
	<-entered
	cancel()
	select {
	case <-done:
		t.Fatal("cancel fabricated a joined backend")
	default:
	}
	if stage.owed.mu.TryLock() {
		stage.owed.mu.Unlock()
		t.Fatal("active IO lost its ticket owner")
	}
	close(release)
	result := <-done
	if result.completion.state != contactRecordCoveredOwed || !result.completion.attemptCovered {
		t.Fatal("late real commit lost its retained receipt")
	}
	if err := s.contactPins.scanOwed(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestContactRecordK1ErrorStillOffersOwnedCompletion(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, _ := stagedContactRecord(t, s, peer)
	publishContactProof(t, s, peer)
	id := domain.PeerIdentityFromWire(peer.Address)
	s.trust.mu.Lock()
	row := s.trust.contacts[id.String()]
	state := *row.LocalContact
	state.confirmed = 0
	row.LocalContact = &state
	s.trust.contacts[id.String()] = row
	s.trust.mu.Unlock()
	injected := errors.New("K1 fact write unavailable")
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().Write(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, injected).Once()
	files.EXPECT().Close().Return(nil).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	if err := s.contactPins.scan(t.Context()); !errors.Is(err, injected) {
		t.Fatal("K1 error lost", err)
	}
	stage.owed.mu.Lock()
	completion := stage.owed.completion.state
	stage.owed.mu.Unlock()
	if completion != contactRecordHistoricalProtectionCompleted {
		t.Fatal("K1 error suppressed K2 opportunity")
	}
}

func TestContactRecordWakeStormDoesNotBypassWholePassPacing(t *testing.T) {
	s, _ := localContactFixture(t)
	p := s.contactPins
	fixed := time.Now()
	var reads atomic.Int32
	paced := make(chan struct{})
	p.clock = func() time.Time {
		if reads.Add(1) == 4 {
			close(paced)
		}
		return fixed
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	<-paced
	for range 1000 {
		p.notify()
		runtime.Gosched()
	}
	cancel()
	<-done
	if got := reads.Load(); got != 4 {
		t.Fatalf("wake storm bypassed pending pacing timer: %d clock reads", got)
	}
}

func TestContactRecordWorkerUsesActualCooperativeCeiling(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	_, verified := verifiedContactRecord(t, s, peer)
	id := domain.PeerIdentityFromWire(peer.Address)
	// This is the bookkeeping of one admitted legacy routing writer. The
	// separate actual-handler test covers its real publication/purge path.
	s.unprovenRouting.enter(id)
	stage, err := s.stageContactRecord(t.Context(), verified)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.contactPins.scanOwed(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("worker did not impose its own attempt ceiling", err)
	}
	if !s.unprovenRouting.leave(id) {
		t.Fatal("deadline dropped owed last-writer purge")
	}
	s.unprovenRouting.leave(id)
	if stage.owed.completion.state != contactRecordCoveredOwed {
		t.Fatal("deadline falsely completed barrier")
	}
	if err := s.contactPins.scanOwed(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type contactRecordReferenceSite struct{ file, function string }

func contactRecordReferenceErrors(sources map[string][]byte) []string {
	expected := map[string]map[contactRecordReferenceSite]int{
		"observeContactRecord": {{"identity_record_admission.go", "prepareContactRecordPush"}: 1}, "verifyContactMinSessionRecord": {{"identity_record_admission.go", "prepareContactRecordPush"}: 1, {"identity_pin_contact_reconciliation.go", "scanReconciliation"}: 1}, "stageContactRecord": {}, "completeOwedContactRecord": {},
		"setUnpublishedMetadata":          {{"identity_pin_metadata_mutation.go", "stagePinMetadataLocked"}: 1, {"identity_pin_contact_record.go", "stageContactRecordLocked"}: 1, {"identity_pin_relation.go", "stageClassifiedRelationRecordLocked"}: 1, {"identity_pin_inbound.go", "stageInboundPin"}: 1, {"secure_session_store.go", "loadFile"}: 1, {"identity_pin_recovery.go", "recoverIdentityPinProtection"}: 1},
		"stageContactRecordLocked":        {{"identity_pin_contact_record.go", "stageContactRecordWithAuthority"}: 1},
		"tryCompleteOwedContactRecord":    {{"identity_pin_contact.go", "scanOwed"}: 1},
		"completeOwedContactRecordLocked": {{"identity_pin_contact_record.go", "completeOwedContactRecord"}: 1, {"identity_pin_contact_record.go", "tryCompleteOwedContactRecord"}: 1},
	}
	actual := make(map[string]map[contactRecordReferenceSite]int, len(expected))
	for name := range expected {
		actual[name] = map[contactRecordReferenceSite]int{}
	}
	var failures []string
	constructors := map[contactRecordReferenceSite]int{}
	for name, source := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, name, source, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range file.Decls {
			site := contactRecordReferenceSite{name, "<package>"}
			var body ast.Node = decl
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site.function = fn.Name.Name
				body = fn.Body
			}
			if body == nil {
				continue
			}
			checkedCOW := contactRecordCheckedCOWReferences(set, body, site)
			ast.Inspect(body, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok {
					if id.Name == "setUnpublishedMetadata" && !checkedCOW[id.Pos()] {
						failures = append(failures, "unreviewed or unchecked COW reference")
					}
					if sites, tracked := actual[id.Name]; tracked {
						sites[site]++
					}
				}
				if lit, ok := node.(*ast.CompositeLit); ok {
					if id, ok := lit.Type.(*ast.Ident); ok && id.Name == "contactRecordOwedState" {
						constructors[site]++
					}
				}

				return true
			})
		}
	}
	for name, want := range expected {
		if !reflect.DeepEqual(actual[name], want) {
			failures = append(failures, name+" reference set changed")
		}
	}
	if !reflect.DeepEqual(constructors, map[contactRecordReferenceSite]int{{"identity_pin_contact_record.go", "stageContactRecordLocked"}: 1}) {
		failures = append(failures, "owed owner constructor set changed")
	}
	return failures
}

// This is a finite syntactic inventory of the six reviewed error-check
// statements, not an arbitrary AST control/dataflow proof.
func contactRecordCheckedCOWReferences(set *token.FileSet, body ast.Node, site contactRecordReferenceSite) map[token.Pos]bool {
	shapes := map[contactRecordReferenceSite]string{
		{"identity_pin_inbound.go", "stageInboundPin"}:                      "if err := entry.setUnpublishedMetadata(next); err != nil { return inboundPinStage{}, err }",
		{"identity_pin_metadata_mutation.go", "stagePinMetadataLocked"}:     "if err := entry.setUnpublishedMetadata(next); err != nil { return identityPinMetadataMutation{}, err }",
		{"identity_pin_contact_record.go", "stageContactRecordLocked"}:      "if err := entry.setUnpublishedMetadata(next); err != nil { return contactRecordStage{}, err }",
		{"identity_pin_relation.go", "stageClassifiedRelationRecordLocked"}: "if err := entry.setUnpublishedMetadata(next); err != nil { return relationPinStage{}, err }",
		{"secure_session_store.go", "loadFile"}:                             "if err := entry.setUnpublishedMetadata(file.IdentityMetadata[id]); err != nil { s.mu.Unlock() return err }",
		{"identity_pin_recovery.go", "recoverIdentityPinProtection"}:        "if err := entry.setUnpublishedMetadata(record.Metadata); err != nil { s.mu.Unlock() return domain.IdentityPinReceipt{}, err }",
	}
	result := map[token.Pos]bool{}
	want, reviewed := shapes[site]
	if !reviewed {
		return result
	}
	ast.Inspect(body, func(node ast.Node) bool {
		statement, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		var printed bytes.Buffer
		if err := format.Node(&printed, set, statement); err != nil {
			return true
		}
		if strings.Join(strings.Fields(printed.String()), " ") != want {
			return true
		}
		ast.Inspect(statement.Init, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Name == "setUnpublishedMetadata" {
				result[id.Pos()] = true
			}
			return true
		})
		return true
	})
	return result
}

func TestContactRecordOwnerRootsRemainInactiveAndMetadataErrorsHandled(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = raw
	}
	if failures := contactRecordReferenceErrors(sources); len(failures) > 0 {
		t.Fatal(failures)
	}
	uncheckedSources := make(map[string][]byte, len(sources))
	for name, source := range sources {
		uncheckedSources[name] = append([]byte(nil), source...)
	}
	checkedRelationCOW := []byte("\tif err := entry.setUnpublishedMetadata(next); err != nil {\n\t\treturn relationPinStage{}, err\n\t}")
	if bytes.Count(uncheckedSources["identity_pin_relation.go"], checkedRelationCOW) != 1 {
		t.Fatal("expected one checked relation COW fixture")
	}
	uncheckedSources["identity_pin_relation.go"] = bytes.Replace(uncheckedSources["identity_pin_relation.go"], checkedRelationCOW, []byte("\t_ = entry.setUnpublishedMetadata(next)"), 1)
	if failures := contactRecordReferenceErrors(uncheckedSources); !reflect.DeepEqual(failures, []string{"unreviewed or unchecked COW reference"}) {
		t.Fatal("guard did not isolate unchecked relation COW", failures)
	}

	sources["unexpected.go"] = []byte("package node\nfunc unsafeIngress(s *Service) { _ = s.observeContactRecord }")
	if failures := contactRecordReferenceErrors(sources); len(failures) == 0 {
		t.Fatal("guard accepted a production function-value escape")
	}
}

// Exact owner shapes complement the existing map-key inventory: these fields
// may retain tickets without a map, and external historical handles add cost.
func contactRecordOwnerShapeErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]string{
		"identityPinMetadataState":             {"value": "domain.IdentityPinMetadata", "confirmedRevision": "identityPinProtectionRevision", "recordOwner": "*contactRecordOwedState", "recordMirror": "contactRecordOwnerMirror"},
		"contactPinProducer":                   {"service": "*Service", "wake": "chan struct{}", "cursor": "*domain.PeerIdentity", "reconcileCursor": "*domain.PeerIdentity", "clock": "func() time.Time"},
		"contactRecordSelection":               {"binding": "identityPinEntryBinding", "ticket": "*contactRecordOwedState"},
		"preparedContactRecordStage":           {"candidate": "*contactRecordCandidate", "store": "*secureSessionStore", "identity": "domain.PeerIdentity", "observed": "pinOwnerObservation", "original": "identityPinEntry", "present": "bool", "recovery": "*identityPinRecoveryState", "permit": "*domain.IdentityPinRemovalPermit", "permitValue": "domain.IdentityPinRemovalPermit", "entry": "identityPinEntry", "metadata": "domain.IdentityPinMetadata", "err": "error"},
		"contactRecordStage":                   {"disposition": "contactRecordStageDisposition", "owed": "*contactRecordOwedState"},
		"contactRecordOwedState":               {"mu": "sync.Mutex", "protection": "owedContactProtection", "completion": "contactRecordCompletion"},
		"contactRecordAttempt":                 {"state": "contactRecordAttemptState", "completion": "contactRecordCompletion"},
		"contactRecordCompletion":              {"state": "contactRecordCompletionState", "receipt": "contactRecordCoverReceipt", "attemptCovered": "bool", "barrierAttempted": "bool"},
		"owedContactProtection":                {"service": "*Service", "binding": "identityPinEntryBinding", "contact": "domain.IdentityPinAllocation", "fact": "localContactStamp", "recordDigest": "[sha256.Size]byte", "recordSequence": "domain.IdentityRecordSeq", "evidenceRevision": "identityPinEvidenceRevision", "protectionRevision": "identityPinProtectionRevision"},
		"contactRecordReconciliationAuthority": {"admission": "*relationRecordAdmission", "resolver": "*identityResolver", "service": "*Service", "network": "domain.NetworkID", "index": "uint16", "generation": "uint64", "ctx": "context.Context", "cancel": "context.CancelFunc", "candidate": "*contactRecordCandidate", "revoked": "bool", "finished": "bool", "staged": "bool"},
		"contactRecordCandidate":               {"mu": "sync.Mutex", "service": "*Service", "trust": "*trustStore", "store": "*secureSessionStore", "fact": "localContactStamp", "factAt": "time.Time", "observed": "pinOwnerObservation", "network": "domain.NetworkID", "record": "protocol.SignedIdentityRecord", "sequence": "domain.IdentityRecordSeq", "digest": "[sha256.Size]byte", "verified": "bool", "rejected": "bool", "staged": "contactRecordStage"},
	}
	actual := map[string]map[string]string{}
	for name, source := range sources {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, name, source, 0)
		if err != nil {
			return []string{err.Error()}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if _, tracked := expected[spec.Name.Name]; !tracked {
				return true
			}
			fields := map[string]string{}
			if body, ok := spec.Type.(*ast.StructType); ok {
				for _, field := range body.Fields.List {
					if len(field.Names) == 0 {
						fields["<anonymous>"] = "rejected"
						continue
					}
					var printed bytes.Buffer
					if err := format.Node(&printed, set, field.Type); err != nil {
						fields["<invalid>"] = err.Error()
						continue
					}
					for _, id := range field.Names {
						fields[id.Name] = printed.String()
					}
				}
			}
			actual[spec.Name.Name] = fields
			return false
		})
	}
	var failures []string
	for name, fields := range expected {
		if !reflect.DeepEqual(actual[name], fields) {
			failures = append(failures, name+" retained owner shape changed")
		}
	}
	return failures
}

func TestContactRecordOwnerShapesExcludeHiddenInventory(t *testing.T) {
	sources := map[string][]byte{}
	for _, name := range []string{"identity_pin_metadata_durable.go", "identity_pin_contact_record.go", "identity_pin_contact.go", "identity_pin_contact_reconciliation.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = raw
	}
	if failures := contactRecordOwnerShapeErrors(sources); len(failures) > 0 {
		t.Fatal(failures)
	}
	// A history slice is invisible to the existing map-key inventory; reject it
	// even if it is added to the single selected-reference wrapper.
	name := "identity_pin_contact.go"
	sources[name] = bytes.Replace(sources[name], []byte("type contactRecordSelection struct {"), []byte("type contactRecordSelection struct {\nhistory []*contactRecordOwedState"), 1)
	if failures := contactRecordOwnerShapeErrors(sources); len(failures) == 0 {
		t.Fatal("owner guard accepted retained history outside the pin owner")
	}
}

func TestContactRecordGuardsRejectEmbeddedOwnerAndBlankCOW(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	baseline := map[string][]byte{}
	for _, name := range paths {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		baseline[name] = raw
	}
	for _, variant := range []string{"embedded-history", "blank-assignment", "expression-drop", "method-value"} {
		t.Run(variant, func(t *testing.T) {
			sources := map[string][]byte{}
			for name, raw := range baseline {
				sources[name] = raw
			}
			snippet := "package fixture\ntype entry struct{}\nfunc (*entry) setUnpublishedMetadata(int) error{return nil}\n"
			switch variant {
			case "embedded-history":
				snippet += "type ticket struct{}\ntype hiddenRecordHistory struct{history []*ticket}\ntype owner struct{*hiddenRecordHistory}\n"
				name := "identity_pin_contact.go"
				sources[name] = bytes.Replace(sources[name], []byte("type contactRecordSelection struct {"), []byte("type contactRecordSelection struct {\n*hiddenRecordHistory"), 1)
				sources[name] = append(sources[name], []byte("\ntype hiddenRecordHistory struct {history []*contactRecordOwedState}\n")...)
			case "blank-assignment", "expression-drop":
				expression := "entry.setUnpublishedMetadata(next)"
				if variant == "blank-assignment" {
					expression = "_ = " + expression
				}
				snippet += "func discard(entry *entry,next int){" + expression + "}\n"
				name := "identity_pin_metadata_mutation.go"
				before := []byte("\tif err := entry.setUnpublishedMetadata(next); err != nil {\n\t\treturn identityPinMetadataMutation{}, err\n\t}")
				if bytes.Count(sources[name], before) != 1 {
					t.Fatal("reviewed helper call shape changed")
				}
				sources[name] = bytes.Replace(sources[name], before, []byte(expression), 1)
			case "method-value":
				snippet += "func discard(entry *entry){_ = entry.setUnpublishedMetadata}\n"
				sources["unexpected.go"] = []byte("package node\nfunc droppedCOW(entry *identityPinEntry){_ = entry.setUnpublishedMetadata}\n")
			}
			set := token.NewFileSet()
			parsed, err := parser.ParseFile(set, "valid_counterexample.go", snippet, 0)
			if err != nil {
				t.Fatal(err)
			}
			configuration := types.Config{}
			if _, err := configuration.Check("fixture", set, []*ast.File{parsed}, nil); err != nil {
				t.Fatal("counterexample must be valid Go", err)
			}
			var failures []string
			if variant == "embedded-history" {
				failures = contactRecordOwnerShapeErrors(sources)
			} else {
				failures = contactRecordReferenceErrors(sources)
			}
			if len(failures) == 0 {
				t.Fatal("closed guard accepted a valid Go counterexample")
			}
		})
	}
}
