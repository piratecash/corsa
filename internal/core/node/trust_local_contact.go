package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

type localContactFactID [16]byte

func (id localContactFactID) String() string               { return hex.EncodeToString(id[:]) }
func (id localContactFactID) MarshalJSON() ([]byte, error) { return json.Marshal(id.String()) }
func (id *localContactFactID) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) != len(id) || text != hex.EncodeToString(decoded) {
		return domain.ErrLocalContactIntent
	}
	copy(id[:], decoded)
	return nil
}

type localContactFactRevision uint64

// The immutable, nonzero-sized token exists only for a genuinely local row
// application. It is not serialized and carries no durable fact authority.
type localContactRowIncarnation struct{ marker byte }
type contactApplyOrigin uint8

const (
	contactApplyGeneric contactApplyOrigin = iota
	contactApplyExplicitLocal
)

type localContactRowStamp struct {
	owner       *trustStore
	identity    domain.PeerIdentity
	incarnation *localContactRowIncarnation
}

func (stamp localContactRowStamp) matches(owner *trustStore, row trustedContact) bool {
	return stamp.owner == owner && !stamp.identity.IsZero() && row.Address == stamp.identity.String() && stamp.incarnation != nil && row.localRow == stamp.incarnation
}

// bindLocalContactRowLocked requires s.mu for writing. Generic refreshes keep
// the existing immutable marker; only the typed local apply mints one.
func (s *trustStore) bindLocalContactRowLocked(row *trustedContact, origin contactApplyOrigin) localContactRowStamp {
	if origin != contactApplyExplicitLocal {
		return localContactRowStamp{}
	}
	if row.localRow == nil {
		row.localRow = &localContactRowIncarnation{marker: 1}
	}
	return localContactRowStamp{owner: s, identity: domain.PeerIdentityFromWire(row.Address), incarnation: row.localRow}
}

// This state is immutable/COW inside the existing contact row. Only fact is
// serialized; revisions, acknowledgments and one bounded proof are process-local.
type localContactFact struct {
	Version  uint8               `json:"version"`
	ID       localContactFactID  `json:"id"`
	Network  domain.NetworkID    `json:"network"`
	Identity domain.PeerIdentity `json:"identity"`
	At       time.Time           `json:"at"`
}
type localContactState struct {
	fact      localContactFact
	revision  localContactFactRevision
	confirmed localContactFactRevision
	proof     *localContactProof
}

func (s localContactState) MarshalJSON() ([]byte, error) { return json.Marshal(s.fact) }
func (s *localContactState) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&s.fact); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return domain.ErrLocalContactIntent
	}
	if s.fact.Version != 1 || s.fact.ID == (localContactFactID{}) || s.fact.Identity.IsZero() || s.fact.At.IsZero() || s.fact.Network != domain.NetworkID(networkName) {
		return domain.ErrLocalContactIntent
	}
	return nil
}

type localContactStamp struct {
	owner    *trustStore
	identity domain.PeerIdentity
	id       localContactFactID
	revision localContactFactRevision
}

func (s *localContactState) stamp(owner *trustStore) localContactStamp {
	return localContactStamp{owner: owner, identity: s.fact.Identity, id: s.fact.ID, revision: s.revision}
}
func (stamp localContactStamp) matches(state *localContactState, owner *trustStore) bool {
	return state != nil && stamp.owner == owner && stamp.revision != 0 && state.revision == stamp.revision && state.fact.ID == stamp.id && state.fact.Identity == stamp.identity
}

type localContactProof struct {
	proof   sessionv2.ProvenIdentity
	binding identityPinEntryBinding
	fact    localContactStamp
}

// All implementations preserve commit classification; test doubles are generated.
type localContactDurableFiles interface {
	Preflight(context.Context) error
	Write(context.Context, []byte) (secretfile.DurableCommit, error)
	Close() error
}
type pinnedLocalContactFiles struct {
	dir  *secretfile.Dir
	name string
}

func openLocalContactFiles(path string) (localContactDurableFiles, error) {
	if path == "" {
		return nil, secretfile.ErrDurabilityUnsupported
	}
	if err := secretfile.AuditLocation(path); err != nil {
		return nil, err
	}
	dir, err := secretfile.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	return &pinnedLocalContactFiles{dir: dir, name: filepath.Base(path)}, nil
}
func (f *pinnedLocalContactFiles) Preflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.dir.DurablePreflight()
}
func (f *pinnedLocalContactFiles) Write(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
	if err := ctx.Err(); err != nil {
		return secretfile.DurableNotCommitted, err
	}
	return f.dir.DurableWrite(f.name, raw)
}
func (f *pinnedLocalContactFiles) Close() error { return f.dir.Close() }
func (s *trustStore) preflightLocalContact(ctx context.Context) error {
	files, err := s.localFiles()
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	return files.Preflight(ctx)
}
func (s *trustStore) stageBoundLocalContact(ctx context.Context, applied localContactRowStamp) (localContactStamp, error) {
	id := applied.identity
	var factID localContactFactID
	if _, err := rand.Read(factID[:]); err != nil {
		return localContactStamp{}, err
	}
	// Serialize the authority latch with writer-strength selection; an already
	// running legacy write completes before the first authority stage.
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.pinEvidenceMu.Lock()
	defer s.pinEvidenceMu.Unlock()
	if err := ctx.Err(); err != nil {
		return localContactStamp{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return localContactStamp{}, err
	}
	row, ok := s.contacts[id.String()]
	if !ok || !applied.matches(s, row) {
		return localContactStamp{}, domain.ErrLocalContactFactStale
	}
	if row.LocalContact != nil {
		return row.LocalContact.stamp(s), nil
	}
	if s.factRevision == math.MaxUint64 {
		return localContactStamp{}, domain.ErrIdentityPinRevisionExhausted
	}
	wallAt := s.clock()
	state := &localContactState{fact: localContactFact{Version: 1, ID: factID, Network: domain.NetworkID(networkName), Identity: id, At: wallAt}}
	address := id.String()
	publication := s.beginTrustPublication()
	s.factRevision++
	state.revision = s.factRevision
	row.LocalContact = state
	s.contacts[address] = row
	s.authorityObserved = true
	if publication != nil {
		publication.Complete()
	}
	return state.stamp(s), nil
}
func (s *trustStore) confirmLocalContacts(ctx context.Context, targets ...localContactStamp) error {
	if s.authorityReadErr != nil {
		return s.authorityReadErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := s.localFiles()
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	if err := files.Preflight(ctx); err != nil {
		return err
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	raw, err := encodeTrustSnapshot(snapshot)
	if err != nil {
		return err
	}
	outcome, err := files.Write(ctx, raw)
	if err != nil || outcome != secretfile.DurableCommitted {
		return errors.Join(domain.ErrLocalContactFactUnconfirmed, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stamp := range targets {
		address := stamp.identity.String()
		captured := snapshot.contacts[address].LocalContact
		current, ok := s.contacts[address]
		if !ok || !stamp.matches(captured, s) || !stamp.matches(current.LocalContact, s) {
			continue
		}
		state := *current.LocalContact
		state.confirmed = state.revision
		current.LocalContact = &state
		publication := s.beginTrustPublication()
		s.contacts[address] = current
		if publication != nil {
			publication.Complete()
		}
	}

	s.savedGen = snapshot.gen
	return nil
}
func (s *trustStore) localContactStatus(stamp localContactStamp) domain.LocalContactFactState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.contacts[stamp.identity.String()].LocalContact
	if !stamp.matches(state, s) {
		return domain.LocalContactFactAbsent
	}
	if state.confirmed == state.revision {
		return domain.LocalContactFactDurableConfirmed
	}
	return domain.LocalContactFactUnconfirmed
}
func (s *trustStore) captureLocalContactProof(proof sessionv2.ProvenIdentity, binding identityPinEntryBinding) bool {
	id, ok := proof.Identity()
	if !ok || id != binding.identity || binding.incarnation == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.contacts[id.String()]
	if !ok || row.LocalContact == nil {
		return false
	}
	state := *row.LocalContact
	state.proof = &localContactProof{proof: proof, binding: binding, fact: state.stamp(s)}
	row.LocalContact = &state
	address := id.String()
	publication := s.beginTrustPublication()
	s.contacts[address] = row
	if publication != nil {
		publication.Complete()
	}
	return true
}
func validateLocalContactRow(row trustedContact) error {
	if row.LocalContact == nil {
		return nil
	}
	state := row.LocalContact
	verdict, keyErr := classifyContactKeys(row.Address, row)
	if row.Address != state.fact.Identity.String() || verdict == contactKeysRefused {
		return errors.Join(fmt.Errorf("%w: local fact identity/key mismatch", domain.ErrLocalContactIntent), keyErr)
	}
	return nil
}

// A writer audit cannot retroactively authenticate bytes read from a writable
// or substituted old trust file. Re-read through a pinned, audited handle.
func readLocalContactAuthority(path string, initial []byte) error {
	if err := secretfile.AuditLocation(path); err != nil {
		return err
	}
	dir, err := secretfile.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	name := filepath.Base(path)
	named, err := dir.Lstat(name)
	if err != nil {
		return err
	}
	if !named.Mode().IsRegular() {
		return domain.ErrLocalContactIntent
	}
	file, err := dir.OpenFileForRead(name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(named, opened) {
		return domain.ErrLocalContactIntent
	}
	return readLocalContactAuthorityContent(file, initial)
}

func readLocalContactAuthorityContent(file *os.File, initial []byte) error {
	limit := int64(len(initial))
	if limit == math.MaxInt64 {
		return domain.ErrLocalContactIntent
	}
	protected, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(initial, protected) {
		return domain.ErrLocalContactIntent
	}
	return nil
}
func decodeTrustAuthority(raw []byte, payload *trustFile) error {
	if payload.Version == trustFileVersion {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(payload); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return domain.ErrLocalContactIntent
		}
		return nil
	}
	var fields struct {
		Contacts map[string]map[string]json.RawMessage `json:"contacts"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, row := range fields.Contacts {
		if _, present := row["local_contact_intent"]; present {
			return domain.ErrLocalContactIntent
		}
	}
	return nil
}

func cloneLocalContactState(state *localContactState) *localContactState {
	if state == nil {
		return nil
	}
	copy := *state
	if state.proof != nil {
		proof := *state.proof
		copy.proof = &proof
	}
	return &copy
}
