package node

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// self_record.go owns the node's OWN signed identity record
// (docs/protocol/identity-lookup.md §4.1): the only artifact this node is
// ever entitled to answer a get_identity with, and the payload of every
// push_identity it sends.
//
// Seq issuance is atomic and strictly ordered: reserve the next seq → build
// and PERSIST the record → only then publish it (return it to the Service,
// hand it to handlers, push it to peers). A crash between publish and
// persist would otherwise resurrect a different body under an
// already-published seq after restart — a false conflict at every receiver.
// The order here makes that impossible by construction: nothing observable
// exists before the persist has succeeded.

// SelfIdentityRecord returns the node's own signed record and its parsed
// body — the artifact get_identity answers with and push_identity carries.
func (s *Service) SelfIdentityRecord() (protocol.SignedIdentityRecord, protocol.IdentityRecordBody) {
	s.knowledgeMu.RLock()
	defer s.knowledgeMu.RUnlock()
	return s.selfRecord, s.selfRecordBody
}

// selfRecordSpec is everything the desired self-record depends on. A value
// type so the comparison "would re-issuing change anything?" is a plain
// field-by-field check.
type selfRecordSpec struct {
	dtypes     domain.DeclaredDTypeSet
	network    domain.NetworkID
	minSession domain.IdentityRecordMinSession
	dm         bool
}

// selfRecordMinSessionByMode is the record gate: the min_session the node's
// own record states in each session mode. A build that speaks the secure
// session states 2 — its listener always takes v2, whatever it still allows
// for peers that never proved v2. A legacy-only build states nothing: a
// minimum it cannot meet would tell every neighbour to refuse the only
// session it can make.
var selfRecordMinSessionByMode = map[sessionv2.Mode]domain.IdentityRecordMinSession{
	sessionv2.ModeLegacyOnly: domain.AbsentMinSession(),
	sessionv2.ModeTransition: domain.DeclaredMinSession(domain.SessionVersionSecure),
	sessionv2.ModeV2Only:     domain.DeclaredMinSession(domain.SessionVersionSecure),
}

// selfRecordMinSession is the min_session the own record states in mode. A
// mode the gate does not know states nothing: claiming a minimum the node
// may not meet cuts it off, while leaving one out only withholds a guard.
func selfRecordMinSession(mode sessionv2.Mode) domain.IdentityRecordMinSession {
	if minSession, known := selfRecordMinSessionByMode[mode]; known {
		return minSession
	}
	return domain.AbsentMinSession()
}

// selfIdentityRecordForStart is the self-record a starting node runs with.
// A start that is refused (refusal != nil) re-issues nothing and serves the
// stored record as it is, or none: a refusal must leave on disk what it
// found. Re-issuing there would put a new seq on the record of a node that
// never ran — and a refused v2 build runs a legacy-only stand-in state
// (unavailableSessions), whose gate would drop min_session from the record;
// the next real start would then bump the seq once more to put it back.
func selfIdentityRecordForStart(trust *trustStore, owner *identity.Identity, spec selfRecordSpec, refusal error, now time.Time) (protocol.SignedIdentityRecord, protocol.IdentityRecordBody, error) {
	if refusal == nil {
		return ensureSelfIdentityRecord(trust, owner, spec, now)
	}
	ownerID, err := domain.ParsePeerIdentity(owner.Address)
	if err != nil {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, fmt.Errorf("parse own address: %w", err)
	}
	record, body, exists := trust.recordFor(spec.network, ownerID)
	log.Info().
		Str("address", owner.Address).
		Bool("stored", exists).
		Uint64("seq", uint64(body.Seq)).
		Msg("self_identity_record_kept_for_refused_start")
	return record, body, nil
}

// ensureSelfIdentityRecord loads-or-issues the node's self-record so that
// after it returns the trust store holds a persisted record matching the
// running build: same dm policy, same key material, same declared dtypes,
// same min_session.
//
// A matching stored record is returned as-is — no re-issue, no seq churn: a
// clean restart must not make every peer re-merge an identical body under a
// new seq. Any difference (first start, binary upgrade OR rollback that
// changed the dtypes set or the session mode, key rotation, dm policy flip)
// issues seq+1.
func ensureSelfIdentityRecord(trust *trustStore, owner *identity.Identity, spec selfRecordSpec, now time.Time) (protocol.SignedIdentityRecord, protocol.IdentityRecordBody, error) {
	ownerID, err := domain.ParsePeerIdentity(owner.Address)
	if err != nil {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, fmt.Errorf("parse own address: %w", err)
	}

	stored, storedBody, exists := trust.recordFor(spec.network, ownerID)
	floor := domain.IdentityRecordSeq(owner.RecordSeqFloor)
	if exists && storedBody.Seq > floor && selfRecordMatchesSpec(storedBody, owner, spec) {
		return stored, storedBody, nil
	}

	// The issue counter starts from the highest fact available: the stored
	// record's seq, or the backup floor — a restore without the old trust
	// store must NOT re-issue seq 1, which every peer holding the
	// pre-backup record would reject as stale.
	seq := floor
	if exists && storedBody.Seq > seq {
		seq = storedBody.Seq
	}
	record, err := protocol.BuildSignedIdentityRecord(owner, protocol.IdentityRecordSpec{
		Network:    spec.network,
		DM:         spec.dm,
		DTypes:     spec.dtypes,
		MinSession: spec.minSession,
		IssuedAt:   uint64(now.Unix()),
		Seq:        seq.Next(),
	})
	if err != nil {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, fmt.Errorf("issue self record: %w", err)
	}
	body, err := protocol.VerifyIdentityRecord(record, spec.network, ownerID)
	if err != nil {
		// Own issue failing own verification is a bug, not an input problem;
		// surface it before anything is persisted or published.
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, fmt.Errorf("self record failed self-verification: %w", err)
	}

	outcome, err := trust.rememberOwnRecord(spec.network, record, body)
	if err != nil {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, err
	}
	if !outcome.Accepted() {
		return protocol.SignedIdentityRecord{}, protocol.IdentityRecordBody{}, fmt.Errorf(
			"self record seq %d not accepted by own store: %s", body.Seq, outcome)
	}

	event := log.Info().
		Str("address", owner.Address).
		Uint64("seq", uint64(body.Seq)).
		Bool("dm", body.DM).
		Str("dtypes_declaration", body.DTypes.Declaration().String()).
		Int("dtypes", body.DTypes.Len())
	// An absent min_session is left out of the line rather than written as
	// 0: a zero reads as a version, and there is no session version 0.
	if minSession, declared := body.MinSession.Version(); declared {
		event = event.Uint8("min_session", uint8(minSession))
	}
	event.Msg("self_identity_record_issued")
	return record, body, nil
}

// selfRecordMatchesSpec reports whether the stored self-record already
// states exactly what the running build would issue: key material, dm
// policy, min_session and the declared dtypes set. issued_at and seq are
// excluded — they describe the issue event, not the content.
func selfRecordMatchesSpec(stored protocol.IdentityRecordBody, owner *identity.Identity, spec selfRecordSpec) bool {
	if stored.DM != spec.dm || stored.MinSession != spec.minSession {
		return false
	}
	if string(stored.PubKey) != identity.PublicKeyBase64(owner.PublicKey) {
		return false
	}
	if spec.dm {
		if string(stored.BoxKey) != identity.BoxPublicKeyBase64(owner.BoxPublicKey) {
			return false
		}
		if string(stored.BoxSig) != identity.SignBoxKeyBinding(owner) {
			return false
		}
	}
	return declaredDTypeSetsEqual(stored.DTypes, spec.dtypes)
}

// declaredDTypeSetsEqual compares two declarations as SETS plus the
// declaration kind: absent and explicitly-empty name the same types but are
// different statements (docs/protocol/datagram.md §6.1), and flipping between
// them is a content change worth a new seq.
func declaredDTypeSetsEqual(a, b domain.DeclaredDTypeSet) bool {
	if a.Declaration() != b.Declaration() {
		return false
	}
	if a.Len() != b.Len() {
		return false
	}
	names := make(map[domain.DType]struct{}, a.Len())
	for _, dtype := range a.Types() {
		names[dtype] = struct{}{}
	}
	for _, dtype := range b.Types() {
		if _, ok := names[dtype]; !ok {
			return false
		}
	}
	return true
}
