package node

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// The node's own record states the minimum session version, and each side
// hands it to the other when a session is set up — over a secure session
// and over a legacy one alike, since a neighbour still on legacy is exactly
// the one the statement is for. The neighbour must end up holding the very
// bytes the owner signed: a record that only the owner knows states nothing.
//
// The record must arrive by the initial push of the session, not by a
// lookup: each side's push_identity handler accepted a push and neither
// side's post_identity handler — the only other way a record gets in — ran
// at all.
//
// Only the listener's statement is checked. The legacy dialler is a node of
// this build demoted by asOldNode AFTER NewService issued its record, so that
// record still states 2 although the node now speaks v1 only — an artefact
// of the fixture, not of any real build (a real legacy-only build issues it
// without the field: TestSelfRecordMinSessionFollowsTheSessionMode). For the
// dialler the test checks only that its own bytes arrived.
func TestSessionSetupDeliversTheSelfRecordWithMinSession(t *testing.T) {
	t.Parallel()

	kinds := map[string]struct {
		dialerSetup func(*Service)
		wantV2      bool
	}{
		"secure session": {dialerSetup: nil, wantV2: true},
		"legacy session": {dialerSetup: asLegacyOnlyDialler, wantV2: false},
	}
	for name, kind := range kinds {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			addrListener := freeAddress(t)
			listener, stopListener := startEmptyRegistryDatagramNode(t, addrListener)
			defer stopListener()
			dialer, stopDialer := startDatagramNodeWithRegistry(t, freeAddress(t), kind.dialerSetup, normalizeAddress(addrListener))
			defer stopDialer()

			listenerID := domain.PeerIdentityFromWire(listener.Address())
			dialerID := domain.PeerIdentityFromWire(dialer.Address())
			waitForConditionMsg(t, 25*time.Second, "the pair did not exchange the initial push_identity", func() bool {
				_, _, dialerHolds := dialer.trust.recordFor(testRecordStoreNetwork, listenerID)
				_, _, listenerHolds := listener.trust.recordFor(testRecordStoreNetwork, dialerID)
				return dialerHolds && listenerHolds
			})

			for side, svc := range map[string]*Service{"listener": listener, "dialler": dialer} {
				pushes, lookupAnswers := recordArrivalPaths(t, svc)
				if pushes == 0 || lookupAnswers != 0 {
					t.Fatalf("%s: %d accepted push_identity, %d post_identity calls; want the record by the initial push alone",
						side, pushes, lookupAnswers)
				}
			}

			v2, v1 := authenticatedInbound(listener)
			if kind.wantV2 && (v2 == 0 || v1 != 0) || !kind.wantV2 && (v1 == 0 || v2 != 0) {
				t.Fatalf("listener sessions v2=%d v1=%d, want only the %s kind", v2, v1, name)
			}

			ownRecord, ownBody := listener.SelfIdentityRecord()
			if version, declared := ownBody.MinSession.Version(); !declared || version != domain.SessionVersionSecure {
				t.Fatalf("the listener's own record states min_session (%d, %v), want (2, true)", version, declared)
			}
			held, heldBody, _ := dialer.trust.recordFor(testRecordStoreNetwork, listenerID)
			if !bytes.Equal(held.Body, ownRecord.Body) {
				t.Fatalf("the dialler holds %s, not the listener's own record %s", held.Body, ownRecord.Body)
			}
			if version, declared := heldBody.MinSession.Version(); !declared || version != domain.SessionVersionSecure {
				t.Fatalf("the dialler reads min_session (%d, %v) from the listener's record, want (2, true)", version, declared)
			}

			dialerRecord, _ := dialer.SelfIdentityRecord()
			heldByListener, _, _ := listener.trust.recordFor(testRecordStoreNetwork, dialerID)
			if !bytes.Equal(heldByListener.Body, dialerRecord.Body) {
				t.Fatalf("the listener holds %s, not the dialler's own record %s", heldByListener.Body, dialerRecord.Body)
			}
		})
	}
}

// recordArrivalPaths reads, from svc's datagram handler counters, how many
// push_identity frames its handler accepted and how many post_identity
// frames (lookup answers) reached its handler at all.
func recordArrivalPaths(t *testing.T, svc *Service) (pushesAccepted, lookupAnswers uint64) {
	t.Helper()
	layer := svc.datagramLayer()
	if layer == nil {
		t.Fatal("the datagram layer is down")
	}
	stats := layer.types.HandlerStats()
	for _, counts := range stats[domain.DTypePushIdentity.String()] {
		pushesAccepted += counts.Accepted
	}
	for _, counts := range stats[domain.DTypePostIdentity.String()] {
		lookupAnswers += counts.Calls
	}
	return pushesAccepted, lookupAnswers
}

// The record gate: min_session is stated from the session mode of the build,
// never before the build speaks the secure session — a legacy-only build
// that claimed it would tell every neighbour to refuse the only session it
// can make.
func TestSelfRecordMinSessionFollowsTheSessionMode(t *testing.T) {
	t.Parallel()
	cases := map[sessionv2.Mode]domain.IdentityRecordMinSession{
		sessionv2.ModeLegacyOnly: domain.AbsentMinSession(),
		sessionv2.ModeTransition: domain.DeclaredMinSession(domain.SessionVersionSecure),
		sessionv2.ModeV2Only:     domain.DeclaredMinSession(domain.SessionVersionSecure),
		sessionv2.Mode(0):        domain.AbsentMinSession(),
	}
	for mode, want := range cases {
		if got := selfRecordMinSession(mode); got != want {
			t.Errorf("%s: min_session %+v, want %+v", mode, got, want)
		}
	}
}

// The node NewService builds from this build's version constants states
// min_session 2 once the build speaks the secure session and nothing before
// — read from the constants themselves, so the gate is checked against the
// build rather than against itself.
func TestTheConfiguredBuildIssuesMinSession(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	_, body := svc.SelfIdentityRecord()
	version, declared := body.MinSession.Version()
	if config.ProtocolVersion < config.ProtocolVersionSecureSession {
		if declared {
			t.Fatalf("protocol %d below the activation %d states min_session %d",
				config.ProtocolVersion, config.ProtocolVersionSecureSession, version)
		}
		return
	}
	if !declared || version != 2 {
		t.Fatalf("protocol %d (activation %d): the issued record states (%d, %v), want (2, true)",
			config.ProtocolVersion, config.ProtocolVersionSecureSession, version, declared)
	}
}

// A change of the statement is a change of the record: a build that starts
// stating min_session (the activation upgrade) re-issues under the next seq,
// and a restart of the same build re-issues nothing.
func TestEnsureSelfIdentityRecordReissuesOnMinSessionChange(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	now := time.Unix(1780000000, 0)
	before := selfRecordSpec{
		network:    testRecordStoreNetwork,
		dm:         true,
		dtypes:     domain.ExplicitDTypes([]domain.DType{"get_identity"}),
		minSession: domain.AbsentMinSession(),
	}
	_, beforeBody, err := ensureSelfIdentityRecord(store, owner, before, now)
	if err != nil {
		t.Fatalf("ensure before the activation: %v", err)
	}

	activated := before
	activated.minSession = domain.DeclaredMinSession(domain.SessionVersionSecure)
	activatedRecord, activatedBody, err := ensureSelfIdentityRecord(store, owner, activated, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("ensure after the activation: %v", err)
	}
	if activatedBody.Seq != beforeBody.Seq.Next() {
		t.Fatalf("activation seq %d, want %d", activatedBody.Seq, beforeBody.Seq.Next())
	}
	if version, declared := activatedBody.MinSession.Version(); !declared || version != domain.SessionVersionSecure {
		t.Fatalf("activated record states (%d, %v), want (2, true)", version, declared)
	}

	restartedRecord, restartedBody, err := ensureSelfIdentityRecord(store, owner, activated, now.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("ensure on restart: %v", err)
	}
	if restartedBody.Seq != activatedBody.Seq || !bytes.Equal(restartedRecord.Body, activatedRecord.Body) {
		t.Fatalf("a restart of the same build re-issued: seq %d → %d", activatedBody.Seq, restartedBody.Seq)
	}
}

// selfRecordOnDisk reads the owner's own record back from the trust-store
// file, the way the next start finds it.
func selfRecordOnDisk(t *testing.T, path string, owner *identity.Identity) protocol.IdentityRecordBody {
	t.Helper()
	store, err := loadTrustStore(path, trustedContact{Address: owner.Address, PubKey: "pk", Source: "self"})
	if err != nil {
		t.Fatalf("reload the trust store: %v", err)
	}
	_, body, ok := store.recordFor(testRecordStoreNetwork, domain.PeerIdentityFromWire(owner.Address))
	if !ok {
		t.Fatal("no own record on disk")
	}
	return body
}

// A start that is refused leaves the own record as it found it, even when the
// refused build would state something else: a refusal that re-issued would
// change, under a new seq, the record of a node that never ran — and the
// next real start would bump it once more.
func TestARefusedStartDoesNotReissueTheSelfRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	trustPath := filepath.Join(dir, "trust.json")
	cfg := deriveTestAdvertisePort(config.Node{
		ListenAddress:     freeAddress(t),
		Type:              domain.NodeTypeFull,
		IdentityPath:      filepath.Join(dir, "identity-64646.json"),
		PeersStatePath:    filepath.Join(dir, "peers-64646.json"),
		TrustStorePath:    trustPath,
		ChatLogDir:        dir,
		AllowPrivatePeers: true,
	})
	first := NewService(cfg, owner, nil)
	first.WaitBackground()
	if err := first.StartupRefusal(); err != nil {
		t.Fatalf("the first start is refused: %v", err)
	}
	before := selfRecordOnDisk(t, trustPath, owner)

	// A marker that vouches for a store which is not there refuses the next
	// start; the flipped dm policy is what that start would re-issue for.
	marker := filepath.Join(dir, "identity-64646.json"+pinStoreMarkerSuffix)
	if err := os.WriteFile(marker, []byte(`{"format":1,"store":"/elsewhere/secure-sessions-64646.json"}`), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	refusedCfg := cfg
	refusedCfg.DisableDirectMessages = true
	refused := NewService(refusedCfg, owner, nil)
	refused.WaitBackground()
	if err := refused.StartupRefusal(); !errors.Is(err, domain.ErrPinStoreRefused) {
		t.Fatalf("StartupRefusal = %v, want ErrPinStoreRefused", err)
	}

	after := selfRecordOnDisk(t, trustPath, owner)
	if after.Seq != before.Seq || after.DM != before.DM || after.MinSession != before.MinSession {
		t.Fatalf("the refused start rewrote the own record: seq %d → %d, dm %v → %v, min_session %+v → %+v",
			before.Seq, after.Seq, before.DM, after.DM, before.MinSession, after.MinSession)
	}
}

// The stand-in state of a build whose v2 state could not be built runs as
// legacy-only, so the gate reads it as "state nothing". That reading must not
// reach the disk: the start is refused, and the record stated min_session 2
// before it and still does after it.
func TestAnUnbuildableSecureSessionKeepsTheStoredMinSession(t *testing.T) {
	t.Parallel()
	store, path := newRecordTestStore(t)
	owner := generatedIdentity(t)
	spec := selfRecordSpec{
		network:    testRecordStoreNetwork,
		dm:         true,
		dtypes:     domain.ExplicitDTypes([]domain.DType{"get_identity"}),
		minSession: domain.DeclaredMinSession(domain.SessionVersionSecure),
	}
	_, before, err := ensureSelfIdentityRecord(store, owner, spec, time.Unix(1780000000, 0))
	if err != nil {
		t.Fatalf("issue the record of a working start: %v", err)
	}

	builders := productionSecureSessionBuilders()
	builders.certificates = func(sessionv2.Clock) (sessionv2.CertificateSource, error) {
		return sessionv2.CertificateSource{}, errInjectedCertificates
	}
	sessions := buildSecureSessions(owner, sessionsOptionsIn(t.TempDir()), builders)
	refusal := (&Service{secureSessions: sessions}).StartupRefusal()
	if refusal == nil {
		t.Fatal("premise: an unusable certificate source must refuse the start")
	}
	refusedSpec := spec
	refusedSpec.minSession = selfRecordMinSession(sessions.mode)

	_, served, err := selfIdentityRecordForStart(store, owner, refusedSpec, refusal, time.Unix(1780003600, 0))
	if err != nil {
		t.Fatalf("self record for a refused start: %v", err)
	}
	if served.Seq != before.Seq || served.MinSession != before.MinSession {
		t.Fatalf("a refused start serves seq %d %+v, want the stored seq %d %+v", served.Seq, served.MinSession, before.Seq, before.MinSession)
	}
	ondisk := selfRecordOnDisk(t, path, owner)
	if ondisk.Seq != before.Seq || ondisk.MinSession != before.MinSession {
		t.Fatalf("on disk after the refusal: seq %d %+v, want seq %d %+v", ondisk.Seq, ondisk.MinSession, before.Seq, before.MinSession)
	}
}
