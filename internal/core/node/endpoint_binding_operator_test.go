package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// endpoint_binding_operator_test.go pins the operator's actions on endpoint
// bindings (docs/protocol/session_v2.md, "Operator actions on endpoint
// bindings"): the list, the addressed removal of an ordinary binding — a
// protected one is refused — and the two-step demotion of selected protected
// bindings to the ordinary tier: a dry run that shows the endpoints and the
// consequence and changes nothing, then a confirmation by the token of exactly
// that selection, written as one version of the file before anything changes
// in memory, journalled per endpoint under one operation id.

const operatorTestOp = domain.EndpointBindingOperationID("op-test-0001")

func protectEndpoint(t *testing.T, store *secureSessionStore, address domain.PeerAddress, id domain.PeerIdentity, pool domain.ProtectedBindingPool) {
	t.Helper()
	result, err := store.noteProvenOutbound(dialledAt(address), id, endpointProtectionWanted(pool))
	if err != nil || result.tier != bindingTierProtected {
		t.Fatalf("protect %s: tier %v, refused %v, err %v", address, result.tier, result.refused, err)
	}
}

func bindOrdinary(t *testing.T, store *secureSessionStore, address domain.PeerAddress, id domain.PeerIdentity) {
	t.Helper()
	result, err := store.noteProvenOutbound(dialledAt(address), id, endpointProtectionNone())
	if err != nil || result.tier != bindingTierGeneral {
		t.Fatalf("bind %s: tier %v, err %v", address, result.tier, err)
	}
}

// countWrites makes every write of the store go to disk through a counter.
func countWrites(store *secureSessionStore) *int {
	writes := 0
	store.writeFile = func(raw []byte) error {
		writes++
		return store.writeToDisk(raw)
	}
	return &writes
}

// An ordinary binding is removed by its address — any spelling of the
// endpoint names it — and the removal is on disk and journalled with the
// operation id; the other bindings stay.
func TestAnOperatorRemovesAnOrdinaryBindingByAddressAndJournalsIt(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	journal := journalledStore(store)
	removed, kept := domain.PeerAddress("192.0.2.10:64646"), domain.PeerAddress("192.0.2.11:64646")
	owner := domain.PeerIdentity{0x10}
	bindOrdinary(t, store, removed, owner)
	bindOrdinary(t, store, kept, owner)

	removal, err := store.removeGeneralBinding("[::ffff:192.0.2.10]:64646", operatorTestOp)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if removal.Endpoint != removed || removal.KeyForm != domain.EndpointBindingKeyCanonical || removal.OperationID != operatorTestOp {
		t.Fatalf("removal = %+v; want endpoint %s, canonical, operation %s", removal, removed, operatorTestOp)
	}
	if store.endpointRequiresV2(removed) || !store.endpointRequiresV2(kept) {
		t.Fatal("the removal did not take exactly the addressed binding")
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	if restarted.endpointRequiresV2(removed) || !restarted.endpointRequiresV2(kept) {
		t.Fatal("the removal is not on disk")
	}
	lines := journal.entries(t, "endpoint_binding_removed_by_operator")
	if len(lines) != 1 {
		t.Fatalf("endpoint_binding_removed_by_operator logged %d times, want once", len(lines))
	}
	if line := lines[0]; line["peer"] != string(removed) || line["identity"] != owner.String() || line["operation_id"] != string(operatorTestOp) {
		t.Fatalf("removal journal = %v; want peer %s, identity %s, operation_id %s", line, removed, owner, operatorTestOp)
	}
}

// The addressed removal is for ordinary bindings only. A protected one is
// refused with the reason, stays protected and nothing is written; an address
// without a binding is refused as not found.
func TestTheAddressedRemovalRefusesAProtectedBinding(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	journal := journalledStore(store)
	protected := domain.PeerAddress("192.0.2.20:64646")
	protectEndpoint(t, store, protected, domain.PeerIdentity{0x20}, domain.ProtectedBindingPoolContact)
	before := protectedRecords(store)
	writes := countWrites(store)

	if _, err := store.removeGeneralBinding(protected, operatorTestOp); !errors.Is(err, domain.ErrEndpointBindingProtected) {
		t.Fatalf("remove a protected binding = %v, want ErrEndpointBindingProtected", err)
	}
	if _, err := store.removeGeneralBinding("192.0.2.99:64646", operatorTestOp); !errors.Is(err, domain.ErrEndpointBindingNotFound) {
		t.Fatalf("remove an unbound address = %v, want ErrEndpointBindingNotFound", err)
	}
	if after := protectedRecords(store); !reflect.DeepEqual(before, after) {
		t.Fatalf("a refused removal changed the protected bindings: %v → %v", before, after)
	}
	if *writes != 0 {
		t.Fatalf("a refused removal wrote the store %d times", *writes)
	}
	if lines := journal.entries(t, "endpoint_binding_removed_by_operator"); len(lines) != 0 {
		t.Fatalf("a refused removal was journalled as done: %v", lines)
	}
}

// The dry run lists the selected endpoints with the term they would get
// and the consequence, hands back a token, and changes nothing.
func TestADemotionDryRunShowsTheEndpointsAndTheConsequenceAndChangesNothing(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	contact, relation := domain.PeerAddress("192.0.2.31:64646"), domain.PeerAddress("192.0.2.30:64646")
	protectEndpoint(t, store, contact, domain.PeerIdentity{0x31}, domain.ProtectedBindingPoolContact)
	protectEndpoint(t, store, relation, domain.PeerIdentity{0x30}, domain.ProtectedBindingPoolRelation)
	before := protectedRecords(store)
	writes := countWrites(store)

	plan, err := store.planDemotion([]domain.PeerAddress{contact, relation})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	term := clock.read().UTC().Add(endpointBindingTTL)
	want := []domain.EndpointBindingDemotionItem{
		{Endpoint: relation, KeyForm: domain.EndpointBindingKeyCanonical, Pool: domain.ProtectedBindingPoolRelation, ExpiresAt: term, IdentityPinRemains: true},
		{Endpoint: contact, KeyForm: domain.EndpointBindingKeyCanonical, Pool: domain.ProtectedBindingPoolContact, ExpiresAt: term, IdentityPinRemains: true},
	}
	if !reflect.DeepEqual(plan.Items, want) {
		t.Fatalf("dry run items = %+v, want %+v", plan.Items, want)
	}
	if plan.Consequence != domain.EndpointBindingDemotionConsequence || plan.Token == "" {
		t.Fatalf("dry run consequence %q, token %q", plan.Consequence, plan.Token)
	}
	if after := protectedRecords(store); !reflect.DeepEqual(before, after) || *writes != 0 {
		t.Fatalf("the dry run changed the store: %d writes, protected %v → %v", *writes, before, after)
	}
}

// The confirmation by the dry run's token moves every selected binding
// to the ordinary tier with the term counted from the confirmation, in one
// write of the file, and journals each endpoint with its former giver under
// one operation id. An unselected protected binding is untouched; there is
// no "all".
func TestAConfirmedDemotionMovesTheSelectionInOneWriteAndJournalsEachEndpoint(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	journal := journalledStore(store)
	giver := domain.PeerIdentity{0x40}
	first, second, unselected := domain.PeerAddress("192.0.2.40:64646"), domain.PeerAddress("192.0.2.41:64646"), domain.PeerAddress("192.0.2.42:64646")
	for _, endpoint := range []domain.PeerAddress{first, second, unselected} {
		protectEndpoint(t, store, endpoint, giver, domain.ProtectedBindingPoolContact)
	}
	plan, err := store.planDemotion([]domain.PeerAddress{first, second})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	writes := countWrites(store)
	clock.now = clock.now.Add(time.Hour)
	confirmedAt := clock.read().UTC()

	result, err := store.demote([]domain.PeerAddress{second, first}, plan.Token, operatorTestOp)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if *writes != 1 {
		t.Fatalf("the demotion wrote the store %d times, want one version of the file", *writes)
	}
	if result.OperationID != operatorTestOp || len(result.Items) != 2 || result.Consequence != domain.EndpointBindingDemotionConsequence {
		t.Fatalf("result = %+v", result)
	}
	for _, item := range result.Items {
		if !item.ExpiresAt.Equal(confirmedAt.Add(endpointBindingTTL)) {
			t.Fatalf("%s expires at %v, want the confirmation plus 30 days %v", item.Endpoint, item.ExpiresAt, confirmedAt.Add(endpointBindingTTL))
		}
	}
	for _, endpoint := range []domain.PeerAddress{first, second} {
		binding, _ := bindingOf(store, endpoint)
		if binding.tier() != bindingTierGeneral || binding.demotedAt == nil || !binding.demotedAt.Equal(confirmedAt) {
			t.Fatalf("%s after the demotion: tier %v, demoted at %v", endpoint, binding.tier(), binding.demotedAt)
		}
		if !store.endpointRequiresV2(endpoint) {
			t.Fatalf("%s stopped requiring v2 at once: the demotion only starts a term", endpoint)
		}
	}
	if binding, _ := bindingOf(store, unselected); binding.tier() != bindingTierProtected {
		t.Fatal("an unselected protected binding was lowered")
	}
	assertTallies(t, store)

	lines := journal.entries(t, "endpoint_binding_demoted_by_operator")
	if len(lines) != 2 {
		t.Fatalf("endpoint_binding_demoted_by_operator logged %d times, want once per endpoint", len(lines))
	}
	journalled := map[string]bool{}
	for _, line := range lines {
		if line["operation_id"] != string(operatorTestOp) || line["previous_protected_by"] != giver.String() || line["pool"] != string(domain.ProtectedBindingPoolContact) {
			t.Fatalf("demotion journal = %v; want operation_id %s, previous_protected_by %s, pool K", line, operatorTestOp, giver)
		}
		journalled[line["peer"].(string)] = true
	}
	if !journalled[string(first)] || !journalled[string(second)] {
		t.Fatalf("journalled endpoints %v, want %s and %s", journalled, first, second)
	}

	restarted := loadSecureSessionStore(path, clock.trusted())
	for endpoint, tier := range map[domain.PeerAddress]endpointBindingTier{first: bindingTierGeneral, second: bindingTierGeneral, unselected: bindingTierProtected} {
		if binding, _ := bindingOf(restarted, endpoint); binding.tier() != tier {
			t.Fatalf("after a restart %s is %v, want %v", endpoint, binding.tier(), tier)
		}
	}
	assertTallies(t, restarted)
}

// The token confirms exactly the selection it was given for. Another
// selection — a different endpoint, or more of them — is refused and nothing
// changes.
func TestAConfirmationOfAnotherSelectionDemotesNothing(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	shown, other := domain.PeerAddress("192.0.2.50:64646"), domain.PeerAddress("192.0.2.51:64646")
	protectEndpoint(t, store, shown, domain.PeerIdentity{0x50}, domain.ProtectedBindingPoolContact)
	protectEndpoint(t, store, other, domain.PeerIdentity{0x51}, domain.ProtectedBindingPoolContact)
	before := protectedRecords(store)
	plan, err := store.planDemotion([]domain.PeerAddress{shown})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	for name, selection := range map[string][]domain.PeerAddress{
		"another endpoint": {other},
		"a larger set":     {shown, other},
	} {
		if _, err := store.demote(selection, plan.Token, operatorTestOp); !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) {
			t.Fatalf("%s confirmed with the token of %s = %v, want ErrEndpointBindingSelectionChanged", name, shown, err)
		}
	}
	if _, err := store.demote([]domain.PeerAddress{shown}, "not-a-token", operatorTestOp); !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) {
		t.Fatalf("a made-up token = %v, want ErrEndpointBindingSelectionChanged", err)
	}
	if after := protectedRecords(store); !reflect.DeepEqual(before, after) {
		t.Fatalf("a refused confirmation changed the protected bindings: %v → %v", before, after)
	}
}

// A demotion names every endpoint, each once, and only protected ones.
// There is no wildcard and no empty selection.
func TestADemotionNamesEveryEndpointAndOnlyProtectedOnes(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	protected, ordinary := domain.PeerAddress("192.0.2.60:64646"), domain.PeerAddress("192.0.2.61:64646")
	protectEndpoint(t, store, protected, domain.PeerIdentity{0x60}, domain.ProtectedBindingPoolContact)
	bindOrdinary(t, store, ordinary, domain.PeerIdentity{0x61})
	tooMany := make([]domain.PeerAddress, domain.MaxEndpointBindingSelection+1)
	for i := range tooMany {
		tooMany[i] = domain.PeerAddress(fmt.Sprintf("10.9.%d.%d:64646", byte(i>>8), byte(i)))
	}
	for name, tc := range map[string]struct {
		selection []domain.PeerAddress
		want      error
	}{
		"empty":       {selection: nil, want: domain.ErrEndpointBindingSelectionEmpty},
		"wildcard":    {selection: []domain.PeerAddress{"*"}, want: domain.ErrEndpointBindingNotFound},
		"ordinary":    {selection: []domain.PeerAddress{protected, ordinary}, want: domain.ErrEndpointBindingNotProtected},
		"duplicate":   {selection: []domain.PeerAddress{protected, "[::ffff:192.0.2.60]:64646"}, want: domain.ErrEndpointBindingSelectionDuplicate},
		"not bound":   {selection: []domain.PeerAddress{"192.0.2.69:64646"}, want: domain.ErrEndpointBindingNotFound},
		"over bound":  {selection: tooMany, want: domain.ErrEndpointBindingSelectionTooLarge},
		"no selector": {selection: []domain.PeerAddress{""}, want: domain.ErrEndpointBindingNotFound},
	} {
		if _, err := store.planDemotion(tc.selection); !errors.Is(err, tc.want) {
			t.Fatalf("%s: dry run = %v, want %v", name, err, tc.want)
		}
	}
	if binding, _ := bindingOf(store, protected); binding.tier() != bindingTierProtected {
		t.Fatal("a refused dry run lowered a binding")
	}
}

// A demotion whose write fails lowers nothing: not in memory, not on disk,
// and nothing is journalled as done. The same confirmation lands once the
// disk works.
func TestAFailedWriteOfADemotionLowersNoBinding(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	journal := journalledStore(store)
	selection := []domain.PeerAddress{"192.0.2.70:64646", "192.0.2.71:64646"}
	for _, endpoint := range selection {
		protectEndpoint(t, store, endpoint, domain.PeerIdentity{0x70}, domain.ProtectedBindingPoolContact)
	}
	before := protectedRecords(store)
	onDisk, err := os.ReadFile(path) //nolint:gosec // the file this test's store wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	plan, err := store.planDemotion(selection)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	diskFull := errors.New("disk full")
	writes := 0
	store.writeFile = func([]byte) error { writes++; return diskFull }

	if _, err := store.demote(selection, plan.Token, operatorTestOp); !errors.Is(err, diskFull) {
		t.Fatalf("demotion on a failing disk = %v, want the write error", err)
	} else {
		var action *domain.EndpointBindingActionError
		if !errors.As(err, &action) || action.Outcome != domain.EndpointBindingActionIndeterminate || action.Restoration != domain.EndpointBindingRestorationUnconfirmed || action.OperationID != operatorTestOp || !reflect.DeepEqual(action.Endpoints, selection) || writes != 2 {
			t.Fatalf("an untyped writer error was called previsible or a failed protective rewrite acknowledged: %+v writes=%d", action, writes)
		}
	}
	if after := protectedRecords(store); !reflect.DeepEqual(before, after) {
		t.Fatalf("a failed write lowered bindings in memory: %v → %v", before, after)
	}
	if after, _ := os.ReadFile(path); string(after) != string(onDisk) { //nolint:gosec // the file this test's store wrote
		t.Fatal("a failed demotion changed the file")
	}
	if lines := journal.entries(t, "endpoint_binding_demoted_by_operator"); len(lines) != 0 {
		t.Fatalf("a failed demotion was journalled as done: %v", lines)
	}

	store.writeFile = store.writeToDisk
	if _, err := store.demote(selection, plan.Token, operatorTestOp); err != nil {
		t.Fatalf("the same confirmation on a working disk: %v", err)
	}
	if len(protectedRecords(loadSecureSessionStore(path, clock.trusted()))) != 0 {
		t.Fatal("the demotion that landed is not on disk")
	}
}

// A demoted binding keeps its term — the demotion plus 30 days, however
// old its last v2 session — across a restart; at its end the endpoint admits
// v1 again, while the pin of the identity last proven there still refuses a
// v1 session naming that identity.
func TestADemotedBindingKeepsItsTermAcrossARestartAndThenExpires(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	endpoint, owner := domain.PeerAddress("192.0.2.80:64646"), domain.PeerIdentity{0x80}
	protectEndpoint(t, store, endpoint, owner, domain.ProtectedBindingPoolRelation)
	// A protected binding's last v2 is old by the time an operator acts.
	clock.now = clock.now.Add(100 * 24 * time.Hour)
	plan, err := store.planDemotion([]domain.PeerAddress{endpoint})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	demotedAt := clock.read().UTC()
	if _, err := store.demote([]domain.PeerAddress{endpoint}, plan.Token, operatorTestOp); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	clock.now = clock.now.Add(24 * time.Hour)
	restarted := loadSecureSessionStore(path, clock.trusted())
	page, err := restarted.listBindings(domain.EndpointBindingQuery{Endpoint: &endpoint, Limit: 1})
	if err != nil || len(page.Bindings) != 1 {
		t.Fatalf("list after the restart: %+v, %v", page, err)
	}
	view := page.Bindings[0]
	if view.Tier != domain.EndpointBindingTierGeneral || view.ExpiresAt == nil || !view.ExpiresAt.Equal(demotedAt.Add(endpointBindingTTL)) {
		t.Fatalf("after the restart: tier %s, expires %v; want general until %v", view.Tier, view.ExpiresAt, demotedAt.Add(endpointBindingTTL))
	}
	if view.DemotedAt == nil || !view.DemotedAt.Equal(demotedAt) {
		t.Fatalf("after the restart demoted_at = %v, want %v", view.DemotedAt, demotedAt)
	}

	clock.now = demotedAt.Add(endpointBindingTTL - time.Second)
	if !restarted.endpointRequiresV2(endpoint) {
		t.Fatal("the demoted binding expired before its term")
	}
	clock.now = demotedAt.Add(endpointBindingTTL)
	if restarted.endpointRequiresV2(endpoint) {
		t.Fatal("the demoted binding outlived its term")
	}
	if !restarted.identityPinned(owner) {
		t.Fatal("the expiry of the binding dropped the pin of the identity proven there")
	}
}

// The list shows each live binding once, in key order, a page at a time;
// filters by identity — last proven or giver — and by endpoint in any
// spelling; and tells the tiers and the key forms apart. An expired ordinary
// binding is not listed.
func TestTheBindingListFiltersAndPages(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	giver, ordinaryOwner, newOwner := domain.PeerIdentity{0x90}, domain.PeerIdentity{0x91}, domain.PeerIdentity{0x92}
	expired := domain.PeerAddress("192.0.2.99:64646")
	bindOrdinary(t, store, expired, ordinaryOwner)
	clock.now = clock.now.Add(endpointBindingTTL + time.Hour)
	protectedA, protectedB, ordinary := domain.PeerAddress("192.0.2.90:64646"), domain.PeerAddress("192.0.2.91:64646"), domain.PeerAddress("192.0.2.92:64646")
	protectEndpoint(t, store, protectedA, giver, domain.ProtectedBindingPoolContact)
	protectEndpoint(t, store, protectedB, giver, domain.ProtectedBindingPoolContact)
	// A new owner on B: B stays protected by the giver, and lists under both.
	if _, err := store.noteProvenOutbound(dialledAt(protectedB), newOwner, endpointProtectionNone()); err != nil {
		t.Fatalf("owner change: %v", err)
	}
	bindOrdinary(t, store, ordinary, ordinaryOwner)
	nonCanonical := domain.PeerAddress("127.1:64646")
	store.mu.Lock()
	kept := endpointBinding{identity: giver, lastV2: store.now(), protection: &bindingProtection{by: giver, pool: domain.ProtectedBindingPoolContact}, group: domain.SourceGroupUnresolvedName}
	store.nonCanonical[nonCanonical] = kept
	store.countBindingLocked(kept)
	store.mu.Unlock()

	all, err := store.listBindings(domain.EndpointBindingQuery{Limit: domain.MaxEndpointBindingPage})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var keys []domain.PeerAddress
	for _, view := range all.Bindings {
		keys = append(keys, view.Endpoint)
	}
	wantKeys := []domain.PeerAddress{nonCanonical, protectedA, protectedB, ordinary}
	if !reflect.DeepEqual(keys, wantKeys) || all.Total != 4 || all.NextAfter != nil {
		t.Fatalf("list = %v (total %d, next %v), want %v on one page", keys, all.Total, all.NextAfter, wantKeys)
	}

	ordinaryView := all.Bindings[3]
	lastV2 := clock.read().UTC()
	tcp := domain.EndpointTransportTCP
	group := endpointSourceGroup(mustCanonical(t, ordinary), nil)
	wantOrdinary := domain.EndpointBindingView{
		Endpoint: ordinary, KeyForm: domain.EndpointBindingKeyCanonical, Transport: &tcp, SourceGroup: group,
		Tier: domain.EndpointBindingTierGeneral, Identity: ordinaryOwner, LastV2: lastV2,
		ExpiresAt: ptrTo(lastV2.Add(endpointBindingTTL)), FirstAt: ptrTo(lastV2),
	}
	if !reflect.DeepEqual(ordinaryView, wantOrdinary) {
		t.Fatalf("ordinary view = %+v, want %+v", ordinaryView, wantOrdinary)
	}
	protectedView := all.Bindings[2]
	if protectedView.Tier != domain.EndpointBindingTierProtected || protectedView.Pool == nil || *protectedView.Pool != domain.ProtectedBindingPoolContact ||
		protectedView.ProtectedBy == nil || *protectedView.ProtectedBy != giver || protectedView.Identity != newOwner || protectedView.ExpiresAt != nil {
		t.Fatalf("protected view = %+v", protectedView)
	}
	if view := all.Bindings[0]; view.KeyForm != domain.EndpointBindingKeyNonCanonical || view.Transport != nil || view.Tier != domain.EndpointBindingTierProtected {
		t.Fatalf("non-canonical view = %+v", view)
	}

	first, err := store.listBindings(domain.EndpointBindingQuery{Limit: 3})
	if err != nil || len(first.Bindings) != 3 || first.Total != 4 || first.NextAfter == nil || *first.NextAfter != protectedB {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	second, err := store.listBindings(domain.EndpointBindingQuery{After: first.NextAfter, Limit: 3})
	if err != nil || len(second.Bindings) != 1 || second.Bindings[0].Endpoint != ordinary || second.NextAfter != nil {
		t.Fatalf("second page = %+v, %v", second, err)
	}

	for name, tc := range map[string]struct {
		query domain.EndpointBindingQuery
		want  []domain.PeerAddress
	}{
		"giver":         {query: domain.EndpointBindingQuery{Identity: &giver}, want: []domain.PeerAddress{nonCanonical, protectedA, protectedB}},
		"last identity": {query: domain.EndpointBindingQuery{Identity: &newOwner}, want: []domain.PeerAddress{protectedB}},
		"endpoint":      {query: domain.EndpointBindingQuery{Endpoint: ptrTo(domain.PeerAddress("[::ffff:192.0.2.92]:64646"))}, want: []domain.PeerAddress{ordinary}},
		"stored key":    {query: domain.EndpointBindingQuery{Endpoint: &nonCanonical}, want: []domain.PeerAddress{nonCanonical}},
		"expired":       {query: domain.EndpointBindingQuery{Endpoint: &expired}, want: nil},
	} {
		tc.query.Limit = domain.MaxEndpointBindingPage
		page, err := store.listBindings(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var got []domain.PeerAddress
		for _, view := range page.Bindings {
			got = append(got, view.Endpoint)
		}
		if !reflect.DeepEqual(got, tc.want) || page.Total != len(tc.want) {
			t.Fatalf("%s: listed %v (total %d), want %v", name, got, page.Total, tc.want)
		}
	}
	for _, limit := range []int{0, -1, domain.MaxEndpointBindingPage + 1} {
		if _, err := store.listBindings(domain.EndpointBindingQuery{Limit: limit}); !errors.Is(err, domain.ErrEndpointBindingPageLimit) {
			t.Fatalf("limit %d = %v, want ErrEndpointBindingPageLimit", limit, err)
		}
	}
}

func ptrTo[T any](value T) *T { return &value }

// What the operator's actions answer carries no history of who owned an
// endpoint. After an owner change the list names the current owner only, and
// the removal, the dry run, the confirmation and the refusals name no
// identity at all; the pairs stay in the local journal.
func TestOperatorAnswersCarryNoEndpointOwnerHistory(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	previous, current, giver := domain.PeerIdentity{0xa0}, domain.PeerIdentity{0xa1}, domain.PeerIdentity{0xa2}
	changed, protected := domain.PeerAddress("192.0.2.100:64646"), domain.PeerAddress("192.0.2.101:64646")
	bindOrdinary(t, store, changed, previous)
	if result, err := store.noteProvenOutbound(dialledAt(changed), current, endpointProtectionNone()); err != nil || result.ownerChange == nil {
		t.Fatalf("owner change: %+v, %v", result, err)
	}
	protectEndpoint(t, store, protected, giver, domain.ProtectedBindingPoolContact)
	if result, err := store.noteProvenOutbound(dialledAt(protected), previous, endpointProtectionNone()); err != nil || result.ownerChange == nil {
		t.Fatalf("owner change of the protected endpoint: %+v, %v", result, err)
	}

	page, err := store.listBindings(domain.EndpointBindingQuery{Endpoint: &changed, Limit: 1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	assertNamesNoIdentity(t, "the list after an owner change", page, previous)
	_, refusal := store.removeGeneralBinding(protected, operatorTestOp)
	removal, err := store.removeGeneralBinding(changed, operatorTestOp)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	plan, err := store.planDemotion([]domain.PeerAddress{protected})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	result, err := store.demote([]domain.PeerAddress{protected}, plan.Token, operatorTestOp)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	_, again := store.demote([]domain.PeerAddress{protected}, plan.Token, operatorTestOp)
	for name, answer := range map[string]any{
		"removal": removal, "dry run": plan, "confirmation": result,
		"refused removal": refusal.Error(), "refused confirmation": again.Error(),
	} {
		assertNamesNoIdentity(t, name, answer, previous, current, giver)
	}
}

// assertNamesNoIdentity fails when answer, as JSON, names any of ids.
func assertNamesNoIdentity(t *testing.T, name string, answer any, ids ...domain.PeerIdentity) {
	t.Helper()
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("%s: marshal: %v", name, err)
	}
	for _, id := range ids {
		if strings.Contains(string(raw), id.String()) {
			t.Fatalf("%s names identity %s: %s", name, id, raw)
		}
	}
}

// Public mutations remain unavailable until the owner-approved outcome and
// competing-writer mechanism is qualified. A preview token is not activation.
// The private store-engine tests above do not assert public availability.
func TestServiceEndpointMutationsStayUnavailableWhileReadsWork(t *testing.T) {
	t.Parallel()
	paths := newPinStorePaths(t)
	store, _ := openPinStore(t, paths, newStoreClock())
	requireStarts(t, store)
	endpoint := domain.PeerAddress("192.0.2.110:64646")
	protected := domain.PeerAddress("192.0.2.111:64646")
	bindOrdinary(t, store, endpoint, domain.PeerIdentity{0xb1})
	protectEndpoint(t, store, protected, domain.PeerIdentity{0xb2}, domain.ProtectedBindingPoolContact)
	svc := &Service{secureSessions: &secureSessions{store: store}}
	ctx := context.Background()
	store.mu.Lock()
	before := store.snapshotLocked()
	changes, written := store.changes, store.written
	store.mu.Unlock()
	onDisk, err := os.ReadFile(paths.store)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	store.writeFile = func([]byte) error { writes++; return errors.New("unavailable mutation reached store writer") }
	store.writeMarker = func([]byte) error { writes++; return errors.New("unavailable mutation reached marker writer") }

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.RemoveEndpointBinding(cancelled, endpoint); !errors.Is(err, context.Canceled) || !store.endpointRequiresV2(endpoint) {
		t.Fatalf("a cancelled removal = %v; the binding must stay", err)
	}
	if _, err := svc.DemoteEndpointBindings(cancelled, []domain.PeerAddress{protected}, "unused"); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled demotion = %v", err)
	}
	page, err := svc.ListEndpointBindings(ctx, domain.EndpointBindingQuery{Limit: 10})
	if err != nil || page.Total != 2 {
		t.Fatalf("list = %+v, %v", page, err)
	}
	plan, err := svc.PlanEndpointBindingDemotion(ctx, []domain.PeerAddress{protected})
	if err != nil || len(plan.Items) != 1 || plan.Token == "" {
		t.Fatalf("read-only preview = %+v, %v", plan, err)
	}
	removal, err := svc.RemoveEndpointBinding(ctx, endpoint)
	if !errors.Is(err, domain.ErrEndpointBindingMutationUnavailable) || !reflect.DeepEqual(removal, domain.EndpointBindingRemoval{}) {
		t.Fatalf("default removal = %+v, %v", removal, err)
	}
	demotion, err := svc.DemoteEndpointBindings(ctx, []domain.PeerAddress{protected}, plan.Token)
	if !errors.Is(err, domain.ErrEndpointBindingMutationUnavailable) || !reflect.DeepEqual(demotion, domain.EndpointBindingDemotionResult{}) {
		t.Fatalf("preview token activated default demotion = %+v, %v", demotion, err)
	}
	store.mu.Lock()
	after := store.snapshotLocked()
	unchanged := changes == store.changes && written == store.written
	store.mu.Unlock()
	if writes != 0 || !unchanged || !reflect.DeepEqual(before, after) || !store.endpointRequiresV2(endpoint) {
		t.Fatal("unavailable public mutation changed protection, counters or reached filesystem writers")
	}
	if raw, err := os.ReadFile(paths.store); err != nil || string(raw) != string(onDisk) {
		t.Fatal("unavailable public mutation changed visible store")
	}

	unreadablePath := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := os.WriteFile(unreadablePath, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	for name, node := range map[string]*Service{
		"no v2 state": {},
		"no store":    {secureSessions: &secureSessions{}},
		"unreadable":  {secureSessions: &secureSessions{store: loadSecureSessionStore(unreadablePath, newStoreClock().trusted())}},
	} {
		if _, err := node.ListEndpointBindings(ctx, domain.EndpointBindingQuery{Limit: 10}); !errors.Is(err, domain.ErrEndpointBindingStoreUnavailable) {
			t.Fatalf("%s: list = %v", name, err)
		}
		if _, err := node.RemoveEndpointBinding(ctx, endpoint); !errors.Is(err, domain.ErrEndpointBindingMutationUnavailable) {
			t.Fatalf("%s: remove = %v", name, err)
		}
		if _, err := node.PlanEndpointBindingDemotion(ctx, []domain.PeerAddress{endpoint}); !errors.Is(err, domain.ErrEndpointBindingStoreUnavailable) {
			t.Fatalf("%s: dry run = %v", name, err)
		}
		if _, err := node.DemoteEndpointBindings(ctx, []domain.PeerAddress{endpoint}, "token"); !errors.Is(err, domain.ErrEndpointBindingMutationUnavailable) {
			t.Fatalf("%s: confirm = %v", name, err)
		}
	}
}

// A record that is protected and also carries the moment of a demotion
// contradicts itself: the file is not trusted, and the store fails closed —
// v1 refused for every peer — rather than guessing which half is true.
func TestAProtectedRecordMarkedDemotedFailsClosed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	raw := `{"format":1,"identities":{},"endpoints":{"192.0.2.120:64646":{"identity":"c000000000000000000000000000000000000000","last_v2":"2026-05-28T21:46:40Z","protected":true,"protected_by":"c000000000000000000000000000000000000000","pool":"K","demoted_at":"2026-05-28T21:46:40Z"}}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	store := loadSecureSessionStore(path, newStoreClock().trusted())
	if !store.unreadable || !store.endpointRequiresV2("192.0.2.200:64646") {
		t.Fatal("a protected record marked demoted was accepted")
	}
}

// A binding whose owner changed between the dry run and the confirmation is
// not the selection the operator was shown: the token covers the identity
// last proven on each endpoint, so the confirmation is refused and nothing is
// lowered.
func TestAnOwnerChangeBetweenTheDryRunAndTheConfirmationRefusesIt(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	endpoint := domain.PeerAddress("192.0.2.130:64646")
	protectEndpoint(t, store, endpoint, domain.PeerIdentity{0xd0}, domain.ProtectedBindingPoolContact)
	plan, err := store.planDemotion([]domain.PeerAddress{endpoint})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if result, err := store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{0xd1}, endpointProtectionNone()); err != nil || result.ownerChange == nil {
		t.Fatalf("owner change: %+v, %v", result, err)
	}
	if _, err := store.demote([]domain.PeerAddress{endpoint}, plan.Token, operatorTestOp); !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) {
		t.Fatalf("confirmation after an owner change = %v, want ErrEndpointBindingSelectionChanged", err)
	}
	if binding, _ := bindingOf(store, endpoint); binding.tier() != bindingTierProtected {
		t.Fatal("a confirmation of a changed selection lowered the binding")
	}
}

// movedOnDuringWrite injects a defensive stale-selection state directly under
// mu. It deliberately bypasses pinMutationGate, unlike every supported proof
// or expiry producer. The separate concurrent producer test covers that gate.
func movedOnDuringWrite(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress, move func(*endpointBinding), uncertain bool) *int {
	t.Helper()
	writes := 0
	store.writeFile = func(raw []byte) error {
		writes++
		if err := store.writeToDisk(raw); err != nil {
			return err
		}
		if writes == 1 {
			key := mustCanonical(t, endpoint)
			store.mu.Lock()
			binding := store.endpoints[key]
			move(&binding)
			store.insertBindingLocked(key, binding)
			store.changes++
			store.mu.Unlock()
			if uncertain {
				return &pinFileWriteError{commit: secretfile.DurableIndeterminate, cause: errors.New("test postvisible stale write uncertain")}
			}
		}
		return nil
	}
	return &writes
}

// A binding that moved on between the operator's write and the change in
// memory — its owner changed, or it was raised to protected — fails the
// action as changed, applies none of it, and rewrites the file from memory in
// the same operation: the disk holds the protection memory holds, not the
// action's version, with no window until the next flush.
func TestABindingThatMovedOnDuringTheWriteRewritesTheFileFromMemory(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		setUp func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress)
		act   func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress) error
		move  func(*endpointBinding)
	}{
		"demotion, owner changed": {
			setUp: func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress) {
				protectEndpoint(t, store, endpoint, domain.PeerIdentity{0xd2}, domain.ProtectedBindingPoolContact)
			},
			act: func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress) error {
				plan, err := store.planDemotion([]domain.PeerAddress{endpoint})
				if err != nil {
					t.Fatalf("dry run: %v", err)
				}
				_, err = store.demote([]domain.PeerAddress{endpoint}, plan.Token, operatorTestOp)
				return err
			},
			move: func(binding *endpointBinding) { binding.identity = domain.PeerIdentity{0xd3} },
		},
		"removal, raised to protected": {
			setUp: func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress) {
				bindOrdinary(t, store, endpoint, domain.PeerIdentity{0xd4})
			},
			act: func(t *testing.T, store *secureSessionStore, endpoint domain.PeerAddress) error {
				_, err := store.removeGeneralBinding(endpoint, operatorTestOp)
				return err
			},
			move: func(binding *endpointBinding) {
				binding.protection = &bindingProtection{by: binding.identity, pool: domain.ProtectedBindingPoolContact}
			},
		},
	} {
		for _, uncertain := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uncertain-%v", name, uncertain), func(t *testing.T) {
				t.Parallel()
				clock := newStoreClock()
				path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
				store := loadSecureSessionStore(path, clock.trusted())
				endpoint := domain.PeerAddress("192.0.2.131:64646")
				tc.setUp(t, store, endpoint)
				writes := movedOnDuringWrite(t, store, endpoint, tc.move, uncertain)

				if err := tc.act(t, store, endpoint); !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) {
					t.Fatalf("action on a binding that moved on = %v, want ErrEndpointBindingSelectionChanged", err)
				} else {
					var action *domain.EndpointBindingActionError
					if !errors.As(err, &action) || action.Outcome != domain.EndpointBindingActionIndeterminate || action.Restoration != domain.EndpointBindingRestorationConfirmed || action.CauseCode != domain.EndpointBindingCauseSelectionChanged || action.OperationID != operatorTestOp || !reflect.DeepEqual(action.Endpoints, []domain.PeerAddress{endpoint}) {
						t.Fatalf("postvisible stale state lost its indeterminate receipt: %+v", action)
					}
				}
				if *writes != 2 {
					t.Fatalf("the store was written %d times, want the action and the rewrite from memory", *writes)
				}
				inMemory, bound := bindingOf(store, endpoint)
				if !bound || inMemory.protection == nil {
					t.Fatalf("memory lost the protection: bound %v, %+v", bound, inMemory)
				}
				onDisk, bound := bindingOf(loadSecureSessionStore(path, clock.trusted()), endpoint)
				if !bound || onDisk.identity != inMemory.identity || onDisk.protection == nil || *onDisk.protection != *inMemory.protection {
					t.Fatalf("the disk does not hold memory's binding: %+v, memory %+v", onDisk, inMemory)
				}
				store.mu.Lock()
				pending := store.changes != store.written
				store.mu.Unlock()
				if pending {
					t.Fatal("the rewrite from memory left the store marked unwritten")
				}
				assertTallies(t, store)
			})
		}
	}
}

// The moment of a demotion survives the file both ways: written, read back,
// and written again byte for byte.
func TestADemotionRoundTripsThroughTheFile(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	store := loadSecureSessionStore(path, clock.trusted())
	endpoint := domain.PeerAddress("192.0.2.140:64646")
	protectEndpoint(t, store, endpoint, domain.PeerIdentity{0xe0}, domain.ProtectedBindingPoolContact)
	plan, err := store.planDemotion([]domain.PeerAddress{endpoint})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if _, err := store.demote([]domain.PeerAddress{endpoint}, plan.Token, operatorTestOp); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	written, err := os.ReadFile(path) //nolint:gosec // the file this test's store wrote
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(written), `"demoted_at"`) {
		t.Fatalf("the file does not carry the demotion: %s", written)
	}
	restarted := loadSecureSessionStore(path, clock.trusted())
	restarted.mu.Lock()
	restarted.changes++
	restarted.mu.Unlock()
	if err := restarted.persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if rewritten, _ := os.ReadFile(path); string(rewritten) != string(written) { //nolint:gosec // the file this test's store wrote
		t.Fatalf("read back and written again, the file changed:\n%s\n%s", written, rewritten)
	}
}

// A demotion is never refused for room: the binding already held its place in
// the store. It may leave the ordinary tier above its bound, and a new
// ordinary binding is then refused until the tier falls back under it.
func TestADemotionIntoAFullOrdinaryTierIsKeptAndHoldsBackNewOnes(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	protected := domain.PeerAddress("198.51.100.150:64646")
	protectEndpoint(t, store, protected, domain.PeerIdentity{0xf0}, domain.ProtectedBindingPoolContact)
	store.mu.Lock()
	now := store.now()
	for i := range maxGeneralBindings {
		insertSeededLocked(t, store, boundEndpoint(i), endpointBinding{identity: boundIdentity(i), lastV2: now})
	}
	store.mu.Unlock()
	plan, err := store.planDemotion([]domain.PeerAddress{protected})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if _, err := store.demote([]domain.PeerAddress{protected}, plan.Token, operatorTestOp); err != nil {
		t.Fatalf("a demotion into a full ordinary tier = %v, want it written", err)
	}
	assertCounts(t, store, false)
	if store.stats().GeneralBindings != maxGeneralBindings+1 {
		t.Fatalf("ordinary bindings %d, want the bound plus the demoted one", store.stats().GeneralBindings)
	}
	fresh := domain.PeerAddress("203.0.113.150:64646")
	if result, _ := store.noteProvenOutbound(dialledAt(fresh), domain.PeerIdentity{0xf1}, endpointProtectionNone()); result.tier != bindingTierNone {
		t.Fatalf("a new ordinary binding above the bound: tier %v, want none", result.tier)
	}
}
