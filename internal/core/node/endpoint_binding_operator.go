package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_binding_operator.go is the operator's hand on the endpoint bindings
// (docs/protocol/session_v2.md, "Operator actions on endpoint bindings"). It
// is the only way a binding leaves the store other than the expiry of an
// ordinary one, and it never acts on more than the operator named:
//
//   - the list shows the live bindings a page at a time;
//   - an ordinary binding is removed by its address; a protected one is
//     refused — removal is not how a protection ends;
//   - selected protected bindings move to the ordinary tier in two steps: a
//     dry run that changes nothing and shows the endpoints, the term they get
//     and the consequence, and a confirmation that must present the dry run's
//     token, a digest of exactly that selection with the protection each
//     holds. There is no "all" and no "all of an identity".
//
// A change is written before it is made: the store as it would be after the
// change is written as one version of the file, and only once that write
// landed is the change made in memory. A failed write leaves both the disk and
// memory as they were — after a demotion that could not be written, no binding
// is lowered. Every change is journalled per endpoint under an operation id of
// its own, which no connection has, so the lines of one action are found
// together. The journal pairs endpoints with identities and stays in the local
// log; the answers name endpoints only.

// storedBindingKey names a binding the way the store holds it: under its
// canonical endpoint, or — for a record an earlier build stored under an
// address with no canonical form — under that exact address.
type storedBindingKey struct {
	// address is the key the file holds the binding under.
	address domain.PeerAddress
	// canonical is nil for a record kept under an address with no canonical
	// form (secureSessionStore.nonCanonical).
	canonical *domain.CanonicalEndpoint
}

// storedKeyOf is the key address names: its canonical endpoint — so every
// spelling of an endpoint names one binding — or, for an address without one,
// that exact address.
func storedKeyOf(address domain.PeerAddress) storedBindingKey {
	endpoint, err := canonicalEndpoint(address)
	if err != nil {
		return storedBindingKey{address: address}
	}
	return storedBindingKey{address: endpoint.Address(), canonical: &endpoint}
}

func (k storedBindingKey) form() domain.EndpointBindingKeyForm {
	if k.canonical == nil {
		return domain.EndpointBindingKeyNonCanonical
	}
	return domain.EndpointBindingKeyCanonical
}

// record is binding as the file holds it under k.
func (k storedBindingKey) record(binding endpointBinding) endpointBindingRecord {
	if k.canonical == nil {
		return recordWithoutTransport(binding)
	}
	return recordOf(*k.canonical, binding)
}

// bindingAtLocked is the binding held under k. Caller holds mu.
func (s *secureSessionStore) bindingAtLocked(k storedBindingKey) (endpointBinding, bool) {
	if k.canonical == nil {
		binding, present := s.nonCanonical[k.address]
		return binding, present
	}
	binding, present := s.endpoints[*k.canonical]
	return binding, present
}

// replaceBindingLocked puts binding under k in place of the one there, moving
// it between the counts. Caller holds mu.
func (s *secureSessionStore) replaceBindingLocked(k storedBindingKey, binding endpointBinding) {
	if k.canonical != nil {
		s.insertBindingLocked(*k.canonical, binding)
		return
	}
	if previous, present := s.nonCanonical[k.address]; present {
		s.uncountBindingLocked(previous)
	}
	s.nonCanonical[k.address] = binding
	s.countBindingLocked(binding)
}

// deleteBindingLocked removes the binding held under k and uncounts it.
// Caller holds mu.
func (s *secureSessionStore) deleteBindingLocked(k storedBindingKey) {
	if k.canonical != nil {
		s.removeBindingLocked(*k.canonical)
		return
	}
	if binding, present := s.nonCanonical[k.address]; present {
		delete(s.nonCanonical, k.address)
		s.uncountBindingLocked(binding)
	}
}

// --- the list ---

// listBindings is one page of the live bindings the query keeps, in key order.
// The views are made under mu — one pass over at most 49 152 bindings, nothing
// called out — and sorted and cut after it is released.
func (s *secureSessionStore) listBindings(query domain.EndpointBindingQuery) (domain.EndpointBindingPage, error) {
	if query.Limit < 1 || query.Limit > domain.MaxEndpointBindingPage {
		return domain.EndpointBindingPage{}, fmt.Errorf("%w: %d is not within 1..%d", domain.ErrEndpointBindingPageLimit, query.Limit, domain.MaxEndpointBindingPage)
	}
	s.mu.Lock()
	if s.unreadable {
		s.mu.Unlock()
		return domain.EndpointBindingPage{}, domain.ErrEndpointBindingStoreUnavailable
	}
	views := s.matchingViewsLocked(query, s.now())
	s.mu.Unlock()
	slices.SortFunc(views, func(a, b domain.EndpointBindingView) int {
		return strings.Compare(string(a.Endpoint), string(b.Endpoint))
	})
	return pageOf(views, query), nil
}

// matchingViewsLocked are the views of the live bindings the query's filters
// keep, unordered. Caller holds mu.
func (s *secureSessionStore) matchingViewsLocked(query domain.EndpointBindingQuery, now time.Time) []domain.EndpointBindingView {
	keep := func(binding endpointBinding) bool {
		return binding.live(now) && namesIdentity(binding, query.Identity)
	}
	if query.Endpoint != nil {
		key := storedKeyOf(*query.Endpoint)
		binding, present := s.bindingAtLocked(key)
		if !present || !keep(binding) {
			return nil
		}
		return []domain.EndpointBindingView{viewOf(key, binding)}
	}
	var views []domain.EndpointBindingView
	for endpoint, binding := range s.endpoints {
		if keep(binding) {
			views = append(views, viewOf(storedBindingKey{address: endpoint.Address(), canonical: &endpoint}, binding))
		}
	}
	for stored, binding := range s.nonCanonical {
		if keep(binding) {
			views = append(views, viewOf(storedBindingKey{address: stored}, binding))
		}
	}
	return views
}

// namesIdentity reports whether binding is one of id's: proven there last, or
// protected by it. A nil id keeps every binding.
func namesIdentity(binding endpointBinding, id *domain.PeerIdentity) bool {
	if id == nil || binding.identity == *id {
		return true
	}
	return binding.protection != nil && binding.protection.by == *id
}

// pageOf cuts the page the query asks for out of views, sorted by key.
func pageOf(views []domain.EndpointBindingView, query domain.EndpointBindingQuery) domain.EndpointBindingPage {
	page := domain.EndpointBindingPage{Total: len(views)}
	start := 0
	if query.After != nil {
		after := string(*query.After)
		start, _ = slices.BinarySearchFunc(views, after, func(view domain.EndpointBindingView, target string) int {
			if string(view.Endpoint) <= target {
				return -1
			}
			return 1
		})
	}
	end := min(start+query.Limit, len(views))
	page.Bindings = views[start:end]
	if end < len(views) {
		next := views[end-1].Endpoint
		page.NextAfter = &next
	}
	return page
}

// viewOf is the operator's view of binding held under k. Every pointer is a
// copy: the view leaves the store's mutex.
func viewOf(k storedBindingKey, binding endpointBinding) domain.EndpointBindingView {
	view := domain.EndpointBindingView{
		Endpoint:    k.address,
		KeyForm:     k.form(),
		SourceGroup: binding.group,
		Tier:        domain.EndpointBindingTierGeneral,
		Identity:    binding.identity,
		LastV2:      binding.lastV2,
		FirstAt:     copiedTime(binding.firstAt),
		DemotedAt:   copiedTime(binding.demotedAt),
	}
	if k.canonical != nil {
		transport := k.canonical.Transport()
		view.Transport = &transport
	}
	if binding.derivedFrom != nil {
		name := binding.derivedFrom.Address()
		view.DerivedFrom = &name
	}
	if binding.protection == nil {
		expires := binding.termEnd()
		view.ExpiresAt = &expires
		return view
	}
	pool, by := binding.protection.pool, binding.protection.by
	view.Tier, view.Pool, view.ProtectedBy = domain.EndpointBindingTierProtected, &pool, &by
	return view
}

func copiedTime(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	copied := *at
	return &copied
}

// --- changes made by the operator ---

// bindingChange is what an operator action does to one binding.
type bindingChange interface {
	// stillApplies reports whether current is still a binding the change was
	// checked for: the binding may have moved on between the check and the
	// moment the written change is made in memory.
	stillApplies(current endpointBinding) bool
	// after is the binding once changed; false removes it.
	after(current endpointBinding) (endpointBinding, bool)
}

// demoteToGeneral moves a protected binding to the ordinary tier, its term
// counted from at. owner is the identity last proven on the endpoint when the
// demotion was checked: a binding taken over since is not the one confirmed.
type demoteToGeneral struct {
	from  bindingProtection
	owner domain.PeerIdentity
	at    time.Time
}

func (d demoteToGeneral) stillApplies(current endpointBinding) bool {
	return current.identity == d.owner && current.protection != nil && *current.protection == d.from
}

func (d demoteToGeneral) after(current endpointBinding) (endpointBinding, bool) {
	at := d.at
	current.protection, current.demotedAt, current.limitRefusalCounted = nil, &at, false
	return current, true
}

// removeOrdinary removes an ordinary binding last proven by owner when the
// removal was checked.
type removeOrdinary struct {
	owner domain.PeerIdentity
}

func (r removeOrdinary) stillApplies(current endpointBinding) bool {
	return current.identity == r.owner && current.protection == nil
}

func (removeOrdinary) after(endpointBinding) (endpointBinding, bool) {
	return endpointBinding{}, false
}

// bindingEdit is one binding an operator action changes, as it was when the
// action was checked.
type bindingEdit struct {
	key     storedBindingKey
	checked endpointBinding
	change  bindingChange
}

// writeInto puts the changed binding into the snapshot of the file.
func (e bindingEdit) writeInto(file secureSessionFile) {
	next, keep := e.change.after(e.checked)
	if !keep {
		delete(file.Endpoints, e.key.address)
		return
	}
	file.Endpoints[e.key.address] = e.key.record(next)
}

// commitOperatorEdits checks an operator action against the store and writes
// it before making it: check runs under mu and returns the edits; the store as
// it would be after them is written as one version of the file; only then are
// they made in memory. A failed write returns its error and changes nothing.
// writeMu is held from the check to the change in memory, so no other write
// can put the old state back on disk in between.
//
// A binding that moved on between the check and the change in memory — a
// session changed its owner or raised it while the file was written
// (stillApplies) — fails the whole action: none of it is made in memory, and
// the file is written again from memory in the same operation, still under
// writeMu, so the disk never stays without a protection memory holds. The
// caller gets domain.ErrEndpointBindingSelectionChanged and repeats. Takes
// writeMu, then mu; no domain mutex is held by the caller.
func (s *secureSessionStore) commitOperatorEdits(check func(now time.Time) ([]bindingEdit, error)) ([]bindingEdit, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.unreadable {
		s.mu.Unlock()
		return nil, domain.ErrEndpointBindingStoreUnavailable
	}
	edits, err := check(s.now())
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.memoryOnlyLocked() {
		// Nothing runs between the check and this change: one hold of mu.
		s.applyEditsLocked(edits)
		s.mu.Unlock()
		return edits, nil
	}
	covers := s.changes
	file := s.snapshotLocked()
	for _, edit := range edits {
		edit.writeInto(file)
	}
	s.mu.Unlock()

	if err := s.writeSnapshot(file); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if !s.editsStillApplyLocked(edits) {
		// The file now holds the action, memory does not: the written
		// version is no longer the store's, whatever it covered.
		s.changes++
		s.mu.Unlock()
		return nil, s.rewriteFromMemoryHoldingWriteMu()
	}
	s.applyEditsLocked(edits)
	s.markWrittenLocked(covers)
	s.mu.Unlock()
	return edits, nil
}

// editsStillApplyLocked reports whether every binding of edits is still the
// one the action was checked against. Caller holds mu.
func (s *secureSessionStore) editsStillApplyLocked(edits []bindingEdit) bool {
	for _, edit := range edits {
		current, present := s.bindingAtLocked(edit.key)
		if !present || !edit.change.stillApplies(current) {
			return false
		}
	}
	return true
}

// applyEditsLocked makes checked edits in memory. Caller holds mu and has
// seen editsStillApplyLocked hold, or checked the edits under this same hold.
func (s *secureSessionStore) applyEditsLocked(edits []bindingEdit) {
	for _, edit := range edits {
		current, _ := s.bindingAtLocked(edit.key)
		next, keep := edit.change.after(current)
		if keep {
			s.replaceBindingLocked(edit.key, next)
			continue
		}
		s.deleteBindingLocked(edit.key)
	}
}

// rewriteFromMemoryHoldingWriteMu writes the store as memory holds it after an
// operator action was written but could not be made, and reports the action as
// changed. Should the rewrite fail too, the store stays marked unwritten and
// the periodic flush retries it; the write error is reported. Caller holds
// writeMu and not mu.
func (s *secureSessionStore) rewriteFromMemoryHoldingWriteMu() error {
	s.mu.Lock()
	covers := s.changes
	file := s.snapshotLocked()
	s.mu.Unlock()
	if err := s.writeSnapshot(file); err != nil {
		return fmt.Errorf("%w; restoring the store file: %w", domain.ErrEndpointBindingSelectionChanged, err)
	}
	s.mu.Lock()
	s.markWrittenLocked(covers)
	s.mu.Unlock()
	return fmt.Errorf("%w: a binding changed while the action was written; nothing was changed, repeat it", domain.ErrEndpointBindingSelectionChanged)
}

// --- the addressed removal ---

// removeGeneralBinding removes the ordinary binding address names, written to
// disk before it leaves memory, and journals it under op. A protected binding
// is refused (domain.ErrEndpointBindingProtected): it leaves its tier only by
// a confirmed demotion.
func (s *secureSessionStore) removeGeneralBinding(address domain.PeerAddress, op domain.EndpointBindingOperationID) (domain.EndpointBindingRemoval, error) {
	key := storedKeyOf(address)
	edits, err := s.commitOperatorEdits(func(now time.Time) ([]bindingEdit, error) {
		binding, present := s.bindingAtLocked(key)
		switch {
		case !present || !binding.live(now):
			return nil, fmt.Errorf("%w: %s", domain.ErrEndpointBindingNotFound, address)
		case binding.protection != nil:
			return nil, fmt.Errorf("%w: %s", domain.ErrEndpointBindingProtected, key.address)
		}
		return []bindingEdit{{key: key, checked: binding, change: removeOrdinary{owner: binding.identity}}}, nil
	})
	if err != nil {
		s.logOperatorRefusal("remove", op, err)
		return domain.EndpointBindingRemoval{}, err
	}
	removed := edits[0]
	s.logger.Info().
		Str("peer", string(removed.key.address)).
		Str("key_form", string(removed.key.form())).
		Str("identity", removed.checked.identity.String()).
		Time("last_v2", removed.checked.lastV2).
		Str("operation_id", string(op)).
		Msg("endpoint_binding_removed_by_operator")
	return domain.EndpointBindingRemoval{OperationID: op, Endpoint: removed.key.address, KeyForm: removed.key.form()}, nil
}

// --- the demotion ---

// planDemotion is the dry run of a demotion of the selected endpoints: what
// each would become, the consequence and the token that confirms exactly this
// selection. It changes nothing.
func (s *secureSessionStore) planDemotion(addresses []domain.PeerAddress) (domain.EndpointBindingDemotionPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return domain.EndpointBindingDemotionPlan{}, domain.ErrEndpointBindingStoreUnavailable
	}
	edits, err := s.demotionEditsLocked(addresses, s.now())
	if err != nil {
		return domain.EndpointBindingDemotionPlan{}, err
	}
	return domain.EndpointBindingDemotionPlan{
		Items:       s.demotionItemsLocked(edits),
		Consequence: domain.EndpointBindingDemotionConsequence,
		Token:       selectionToken(edits),
	}, nil
}

// demote moves the selected protected bindings to the ordinary tier, their
// term counted from now, if token is the dry run's token of this selection as
// it is now — all of them in one version of the file, or none. Each is
// journalled under op with the protection it lost.
func (s *secureSessionStore) demote(addresses []domain.PeerAddress, token domain.EndpointBindingSelectionToken, op domain.EndpointBindingOperationID) (domain.EndpointBindingDemotionResult, error) {
	var items []domain.EndpointBindingDemotionItem
	edits, err := s.commitOperatorEdits(func(now time.Time) ([]bindingEdit, error) {
		edits, err := s.demotionEditsLocked(addresses, now)
		if err != nil {
			return nil, err
		}
		if selectionToken(edits) != token {
			return nil, domain.ErrEndpointBindingSelectionChanged
		}
		items = s.demotionItemsLocked(edits)
		return edits, nil
	})
	if err != nil {
		s.logOperatorRefusal("demote", op, err)
		return domain.EndpointBindingDemotionResult{}, err
	}
	s.logDemotions(op, edits)
	return domain.EndpointBindingDemotionResult{OperationID: op, Items: items, Consequence: domain.EndpointBindingDemotionConsequence}, nil
}

// demotionEditsLocked checks a selection — named, at most
// MaxEndpointBindingSelection endpoints, each once, each a live protected
// binding — and returns its demotions at now, in key order. Caller holds mu.
func (s *secureSessionStore) demotionEditsLocked(addresses []domain.PeerAddress, now time.Time) ([]bindingEdit, error) {
	switch {
	case len(addresses) == 0:
		return nil, domain.ErrEndpointBindingSelectionEmpty
	case len(addresses) > domain.MaxEndpointBindingSelection:
		return nil, fmt.Errorf("%w: %d endpoints, at most %d", domain.ErrEndpointBindingSelectionTooLarge, len(addresses), domain.MaxEndpointBindingSelection)
	}
	edits := make([]bindingEdit, 0, len(addresses))
	selected := make(map[domain.PeerAddress]bool, len(addresses))
	for _, address := range addresses {
		key := storedKeyOf(address)
		if selected[key.address] {
			return nil, fmt.Errorf("%w: %s", domain.ErrEndpointBindingSelectionDuplicate, key.address)
		}
		selected[key.address] = true
		binding, present := s.bindingAtLocked(key)
		switch {
		case !present || !binding.live(now):
			return nil, fmt.Errorf("%w: %s", domain.ErrEndpointBindingNotFound, address)
		case binding.protection == nil:
			return nil, fmt.Errorf("%w: %s", domain.ErrEndpointBindingNotProtected, key.address)
		}
		edits = append(edits, bindingEdit{key: key, checked: binding, change: demoteToGeneral{from: *binding.protection, owner: binding.identity, at: now}})
	}
	slices.SortFunc(edits, func(a, b bindingEdit) int {
		return strings.Compare(string(a.key.address), string(b.key.address))
	})
	return edits, nil
}

// demotionItemsLocked is what each demotion of edits makes of its endpoint.
// Caller holds mu, for the pins.
func (s *secureSessionStore) demotionItemsLocked(edits []bindingEdit) []domain.EndpointBindingDemotionItem {
	items := make([]domain.EndpointBindingDemotionItem, 0, len(edits))
	for _, edit := range edits {
		demoted, _ := edit.change.after(edit.checked)
		_, pinned := s.identities[edit.checked.identity]
		items = append(items, domain.EndpointBindingDemotionItem{
			Endpoint:           edit.key.address,
			KeyForm:            edit.key.form(),
			Pool:               edit.checked.protection.pool,
			ExpiresAt:          demoted.termEnd(),
			IdentityPinRemains: pinned,
		})
	}
	return items
}

// selectionTokenDomain keeps a digest of a demotion selection from being
// mistaken for a digest of anything else.
const selectionTokenDomain = "corsa endpoint binding demotion v1\n"

// selectionToken is the digest of a selection in key order with the
// protection each binding holds and the identity last proven on it: another
// selection, or the same one after a protection or an owner changed — which
// would make the dry run's identity_pin_remains stale — gives another token.
// It is deterministic and guards against the operator's mistake of confirming
// what was not shown, not against an adversary: who may confirm is decided by
// the RPC's authentication.
func selectionToken(edits []bindingEdit) domain.EndpointBindingSelectionToken {
	// A hash.Hash never fails a write, so its errors are not there to check.
	digest := sha256.New()
	_, _ = digest.Write([]byte(selectionTokenDomain))
	for _, edit := range edits {
		protection := edit.checked.protection
		_, _ = fmt.Fprintf(digest, "%s\t%s\t%s\t%s\n", edit.key.address, protection.pool, protection.by, edit.checked.identity)
	}
	return domain.EndpointBindingSelectionToken(hex.EncodeToString(digest.Sum(nil)))
}

// logDemotions journals each demoted endpoint with the protection it lost.
// The lines pair endpoints with identities, so like logOwnerChange they stay
// in the local log. Called outside mu.
func (s *secureSessionStore) logDemotions(op domain.EndpointBindingOperationID, edits []bindingEdit) {
	for _, edit := range edits {
		demoted, _ := edit.change.after(edit.checked)
		s.logger.Info().
			Str("peer", string(edit.key.address)).
			Str("key_form", string(edit.key.form())).
			Str("identity", edit.checked.identity.String()).
			Str("previous_protected_by", edit.checked.protection.by.String()).
			Str("pool", string(edit.checked.protection.pool)).
			Time("expires_at", demoted.termEnd()).
			Str("operation_id", string(op)).
			Int("selected", len(edits)).
			Msg("endpoint_binding_demoted_by_operator")
	}
}

// logOperatorRefusal journals an operator action that changed nothing: a
// refusal of what it named, or a write that failed.
func (s *secureSessionStore) logOperatorRefusal(action string, op domain.EndpointBindingOperationID, err error) {
	s.logger.Warn().Err(err).
		Str("action", action).
		Str("operation_id", string(op)).
		Msg("endpoint_binding_operator_action_refused")
}

// --- the node's entry points ---

// newEndpointBindingOperationID mints the id of one operator action.
func newEndpointBindingOperationID() (domain.EndpointBindingOperationID, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("endpoint binding operation id: %w", err)
	}
	return domain.EndpointBindingOperationID(hex.EncodeToString(raw)), nil
}

// operatorBindingStore is the store the operator's actions act on; a node
// without v2 state has none.
func (s *Service) operatorBindingStore() (*secureSessionStore, error) {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return nil, domain.ErrEndpointBindingStoreUnavailable
	}
	return s.secureSessions.store, nil
}

// ListEndpointBindings is one page of the live endpoint bindings the query
// keeps. Takes only the store's leaf mutex.
func (s *Service) ListEndpointBindings(ctx context.Context, query domain.EndpointBindingQuery) (domain.EndpointBindingPage, error) {
	if err := ctx.Err(); err != nil {
		return domain.EndpointBindingPage{}, err
	}
	store, err := s.operatorBindingStore()
	if err != nil {
		return domain.EndpointBindingPage{}, err
	}
	return store.listBindings(query)
}

// RemoveEndpointBinding removes the ordinary binding endpoint names, written to
// disk first and journalled under a fresh operation id; a protected binding is
// refused. Disk I/O: the caller holds no domain mutex.
func (s *Service) RemoveEndpointBinding(ctx context.Context, endpoint domain.PeerAddress) (domain.EndpointBindingRemoval, error) {
	if err := ctx.Err(); err != nil {
		return domain.EndpointBindingRemoval{}, err
	}
	store, err := s.operatorBindingStore()
	if err != nil {
		return domain.EndpointBindingRemoval{}, err
	}
	op, err := newEndpointBindingOperationID()
	if err != nil {
		return domain.EndpointBindingRemoval{}, err
	}
	return store.removeGeneralBinding(endpoint, op)
}

// PlanEndpointBindingDemotion is the dry run of a demotion of the selected
// protected bindings. It changes nothing.
func (s *Service) PlanEndpointBindingDemotion(ctx context.Context, endpoints []domain.PeerAddress) (domain.EndpointBindingDemotionPlan, error) {
	if err := ctx.Err(); err != nil {
		return domain.EndpointBindingDemotionPlan{}, err
	}
	store, err := s.operatorBindingStore()
	if err != nil {
		return domain.EndpointBindingDemotionPlan{}, err
	}
	return store.planDemotion(endpoints)
}

// DemoteEndpointBindings moves the selected protected bindings to the ordinary
// tier if token confirms this selection as the dry run showed it — all in one
// version of the file, or none — journalled under a fresh operation id. Disk
// I/O: the caller holds no domain mutex.
func (s *Service) DemoteEndpointBindings(ctx context.Context, endpoints []domain.PeerAddress, token domain.EndpointBindingSelectionToken) (domain.EndpointBindingDemotionResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.EndpointBindingDemotionResult{}, err
	}
	store, err := s.operatorBindingStore()
	if err != nil {
		return domain.EndpointBindingDemotionResult{}, err
	}
	op, err := newEndpointBindingOperationID()
	if err != nil {
		return domain.EndpointBindingDemotionResult{}, err
	}
	return store.demote(endpoints, token, op)
}
