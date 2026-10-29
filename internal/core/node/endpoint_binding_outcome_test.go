//go:build !windows

package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// This fixture calls only the closed private engine. It never enables or
// substitutes for Service.RemoveEndpointBinding/DemoteEndpointBindings.
type endpointOutcomeFixture struct {
	store     *secureSessionStore
	journal   *storeJournal
	paths     pinStorePaths
	clock     *storeClock
	kind      string
	selection []domain.PeerAddress
	owner     domain.PeerIdentity
	act       func() (domain.EndpointBindingActionOutcome, error)
}

func newEndpointOutcomeFixture(t *testing.T, kind string) *endpointOutcomeFixture {
	t.Helper()
	f := &endpointOutcomeFixture{paths: newPinStorePaths(t), clock: newStoreClock(), kind: kind,
		selection: []domain.PeerAddress{"192.0.2.210:64646"}, owner: domain.PeerIdentity{0xe1}}
	f.store, f.journal = openPinStore(t, f.paths, f.clock)
	requireStarts(t, f.store)
	switch kind {
	case "remove":
		bindOrdinary(t, f.store, f.selection[0], f.owner)
		f.act = func() (domain.EndpointBindingActionOutcome, error) {
			result, err := f.store.removeGeneralBinding("[::ffff:192.0.2.210]:64646", operatorTestOp)
			return result.Outcome, err
		}
	case "demote":
		f.selection = append(f.selection, "192.0.2.211:64646")
		for _, endpoint := range f.selection {
			protectEndpoint(t, f.store, endpoint, f.owner, domain.ProtectedBindingPoolContact)
		}
		input := []domain.PeerAddress{f.selection[1], f.selection[0]}
		plan, err := f.store.planDemotion(input)
		if err != nil {
			t.Fatal(err)
		}
		f.act = func() (domain.EndpointBindingActionOutcome, error) {
			result, err := f.store.demote(input, plan.Token, operatorTestOp)
			return result.Outcome, err
		}
	default:
		t.Fatalf("unsupported test action %q", kind)
	}
	return f
}

func (f *endpointOutcomeFixture) assertState(t *testing.T, store *secureSessionStore, applied bool) {
	t.Helper()
	for _, endpoint := range f.selection {
		binding, present := bindingOf(store, endpoint)
		if present && binding.identity != f.owner {
			t.Fatalf("selected endpoint %s changed owner: %v", endpoint, binding.identity)
		}
		if f.kind == "remove" {
			if present == applied {
				t.Fatalf("remove applied=%v but binding present=%v", applied, present)
			}
		} else {
			want := bindingTierProtected
			if applied {
				want = bindingTierGeneral
			}
			if !present || binding.tier() != want {
				t.Fatalf("demote applied=%v: present=%v tier=%v", applied, present, binding.tier())
			}
			if !applied && (binding.protection == nil || binding.protection.by != f.owner || binding.protection.pool != domain.ProtectedBindingPoolContact) {
				t.Fatal("protective state restored a different protection")
			}
		}
	}
	if !store.identityPinned(f.owner) {
		t.Fatal("endpoint action removed independent identity protection")
	}
	assertTallies(t, store)
}

func requireEndpointActionOutcome(t *testing.T, f *endpointOutcomeFixture, err error, outcome domain.EndpointBindingActionOutcome, restoration domain.EndpointBindingRestoration, cause error) *domain.EndpointBindingActionError {
	t.Helper()
	var action *domain.EndpointBindingActionError
	if !errors.As(err, &action) || action == nil {
		t.Fatalf("missing typed disposition: %v", err)
	}
	if action.OperationID != operatorTestOp || !reflect.DeepEqual(action.Endpoints, f.selection) || action.Outcome != outcome || action.Restoration != restoration {
		t.Fatalf("wrong exact action receipt: %+v", action)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("underlying cause lost: %v", err)
	}
	// Check only the dedicated safe operator event. Existing low-level
	// persistence diagnostics may log their local cause; no global logger
	// is replaced and no whole-stream privacy claim is made here.
	var matched []map[string]any
	for _, line := range f.journal.entries(t, "endpoint_binding_operator_action_outcome") {
		if line["operation_id"] == string(operatorTestOp) {
			matched = append(matched, line)
		}
	}
	if len(matched) != 1 {
		t.Fatalf("operator outcome event count=%d", len(matched))
	}
	line := matched[0]
	var endpoints []string
	for _, endpoint := range f.selection {
		endpoints = append(endpoints, string(endpoint))
	}
	rawEndpoints, marshalErr := json.Marshal(line["endpoints"])
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var recorded []string
	if err := json.Unmarshal(rawEndpoints, &recorded); err != nil || !reflect.DeepEqual(recorded, endpoints) || line["outcome"] != string(outcome) || line["restoration"] != string(restoration) || line["cause_code"] != string(action.CauseCode) || line["restoration_cause_code"] != string(action.RestorationCauseCode) {
		t.Fatalf("incomplete operator receipt journal: %+v", line)
	}
	rawEvent, marshalErr := json.Marshal(line)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(rawEvent), "/private/") || line["error"] != nil {
		t.Fatalf("operator receipt leaked a local cause: %s", rawEvent)
	}
	for _, message := range []string{"endpoint_binding_removed_by_operator", "endpoint_binding_demoted_by_operator"} {
		for _, success := range f.journal.entries(t, message) {
			if success["operation_id"] == string(operatorTestOp) {
				t.Fatalf("nonnil outcome fabricated success event: %+v", success)
			}
		}
	}
	return action
}

// Real owned-file writes establish visibility. Fault receipts below model the
// existing writer's confirmation/cleanup boundary; they do not simulate a
// power cut or prove a kernel fsync failure from an injected return value.
func TestEndpointOperatorWriteOutcomesKeepExactSelection(t *testing.T) {
	for _, kind := range []string{"remove", "demote"} {
		for _, outcome := range []domain.EndpointBindingActionOutcome{domain.EndpointBindingActionNotApplied, domain.EndpointBindingActionIndeterminate, domain.EndpointBindingActionAppliedCleanupError, domain.EndpointBindingActionCommitted} {
			t.Run(kind+"/"+string(outcome), func(t *testing.T) {
				f := newEndpointOutcomeFixture(t, kind)
				beforeMain, readErr := os.ReadFile(f.paths.store)
				if readErr != nil {
					t.Fatal(readErr)
				}
				beforeMarker, readErr := os.ReadFile(f.paths.marker)
				if readErr != nil {
					t.Fatal(readErr)
				}
				actual := f.store.writeFile
				injected := errors.New("injected writer failure /private/local/pin-cause")
				writes := 0
				f.store.writeFile = func(raw []byte) error {
					writes++
					if writes > 1 {
						return actual(raw)
					}
					if outcome == domain.EndpointBindingActionNotApplied {
						return &pinFileWriteError{commit: secretfile.DurableNotCommitted, cause: injected}
					}
					if err := actual(raw); err != nil {
						return err
					}
					switch outcome {
					case domain.EndpointBindingActionIndeterminate:
						return &pinFileWriteError{commit: secretfile.DurableIndeterminate, cause: injected}
					case domain.EndpointBindingActionAppliedCleanupError:
						return &pinFileWriteError{commit: secretfile.DurableCommitted, cause: injected}
					}
					return nil
				}
				returned, err := f.act()
				applied := outcome == domain.EndpointBindingActionCommitted || outcome == domain.EndpointBindingActionAppliedCleanupError
				if outcome == domain.EndpointBindingActionCommitted {
					if err != nil || returned != outcome {
						t.Fatalf("normal commit: %q %v", returned, err)
					}
				} else {
					restoration := domain.EndpointBindingRestorationNotAttempted
					if outcome == domain.EndpointBindingActionIndeterminate {
						restoration = domain.EndpointBindingRestorationConfirmed
					}
					action := requireEndpointActionOutcome(t, f, err, outcome, restoration, injected)
					wantCause := domain.EndpointBindingCauseWriteFailed
					if outcome == domain.EndpointBindingActionAppliedCleanupError {
						wantCause = domain.EndpointBindingCauseCleanupFailed
					}
					if returned != "" || action.CauseCode != wantCause || action.RestorationCauseCode != domain.EndpointBindingCauseNone {
						t.Fatalf("error returned success or wrong cause: %q %+v", returned, action)
					}
				}
				wantWrites := 1
				if outcome == domain.EndpointBindingActionIndeterminate {
					wantWrites = 2
				}
				if writes != wantWrites {
					t.Fatalf("writes=%d want=%d", writes, wantWrites)
				}
				f.assertState(t, f.store, applied)
				if outcome == domain.EndpointBindingActionNotApplied {
					afterMain, err := os.ReadFile(f.paths.store)
					if err != nil {
						t.Fatal(err)
					}
					afterMarker, err := os.ReadFile(f.paths.marker)
					if err != nil {
						t.Fatal(err)
					}
					if string(beforeMain) != string(afterMain) || string(beforeMarker) != string(afterMarker) {
						t.Fatal("known previsible refusal changed main/marker bytes")
					}
				}
				restarted, _ := openPinStore(t, f.paths, f.clock)
				requireStarts(t, restarted)
				f.assertState(t, restarted, applied)
			})
		}
	}
}

func TestEndpointOperatorWholeSnapshotIncludesRequiredMarker(t *testing.T) {
	for _, stage := range []string{"main-cleanup-marker-unconfirmed", "marker-not-committed", "marker-committed-cleanup"} {
		t.Run(stage, func(t *testing.T) {
			f := newEndpointOutcomeFixture(t, "demote")
			actual, actualMarker := f.store.writeFile, f.store.writeMarker
			// A visible previous marker is deliberately NOT treated as a
			// confirmed marker for this modeled transaction.
			f.store.markerWritten = false
			injected := errors.New("whole snapshot marker boundary")
			var order []string
			writes, markers := 0, 0
			f.store.writeFile = func(raw []byte) error {
				writes++
				order = append(order, "main")
				if err := actual(raw); err != nil {
					return err
				}
				if stage == "main-cleanup-marker-unconfirmed" && writes == 1 {
					return &pinFileWriteError{commit: secretfile.DurableCommitted, cause: injected}
				}
				return nil
			}
			f.store.writeMarker = func(raw []byte) error {
				markers++
				order = append(order, "marker")
				if markers == 1 && stage == "marker-not-committed" {
					return &pinFileWriteError{commit: secretfile.DurableNotCommitted, cause: injected}
				}
				if err := actualMarker(raw); err != nil {
					return err
				}
				if markers == 1 && stage == "marker-committed-cleanup" {
					return &pinFileWriteError{commit: secretfile.DurableCommitted, cause: injected}
				}
				return nil
			}
			returned, err := f.act()
			outcome, restoration := domain.EndpointBindingActionIndeterminate, domain.EndpointBindingRestorationConfirmed
			wantOrder := []string{"main", "main", "marker"}
			applied := false
			if stage == "marker-not-committed" {
				wantOrder = []string{"main", "marker", "main", "marker"}
			}
			if stage == "marker-committed-cleanup" {
				outcome, restoration = domain.EndpointBindingActionAppliedCleanupError, domain.EndpointBindingRestorationNotAttempted
				wantOrder = []string{"main", "marker"}
				applied = true
			}
			action := requireEndpointActionOutcome(t, f, err, outcome, restoration, injected)
			if returned != "" || !f.store.markerWritten || !reflect.DeepEqual(order, wantOrder) {
				t.Fatalf("whole main/marker result: %q %+v order=%v", returned, action, order)
			}
			f.assertState(t, f.store, applied)
			restarted, _ := openPinStore(t, f.paths, f.clock)
			requireStarts(t, restarted)
			f.assertState(t, restarted, applied)
		})
	}
}

func TestEndpointOperatorFailedRestoreIsNotRollbackProof(t *testing.T) {
	for _, kind := range []string{"remove", "demote"} {
		for _, name := range []string{"restore-before-visible", "restore-after-visible", "restore-committed-cleanup"} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				f := newEndpointOutcomeFixture(t, kind)
				actual := f.store.writeFile
				initial, restoration := errors.New("initial namespace uncertain"), errors.New("protective restore uncertain")
				writes := 0
				f.store.writeFile = func(raw []byte) error {
					writes++
					if writes == 1 {
						if err := actual(raw); err != nil {
							return err
						}
						return &pinFileWriteError{commit: secretfile.DurableIndeterminate, cause: initial}
					}
					if writes != 2 {
						return errors.New("unexpected automatic restore retry")
					}
					if name != "restore-before-visible" {
						if err := actual(raw); err != nil {
							return err
						}
						if name == "restore-committed-cleanup" {
							return &pinFileWriteError{commit: secretfile.DurableCommitted, cause: restoration}
						}
						return &pinFileWriteError{commit: secretfile.DurableIndeterminate, cause: restoration}
					}
					return &pinFileWriteError{commit: secretfile.DurableNotCommitted, cause: restoration}
				}
				_, err := f.act()
				wantRestoration, wantCause := domain.EndpointBindingRestorationUnconfirmed, domain.EndpointBindingCauseWriteFailed
				if name == "restore-committed-cleanup" {
					wantRestoration, wantCause = domain.EndpointBindingRestorationConfirmed, domain.EndpointBindingCauseCleanupFailed
				}
				action := requireEndpointActionOutcome(t, f, err, domain.EndpointBindingActionIndeterminate, wantRestoration, initial)
				if !errors.Is(err, restoration) || action.RestorationCauseCode != wantCause || writes != 2 {
					t.Fatalf("restore failure lost: %+v writes=%d", action, writes)
				}
				f.assertState(t, f.store, false)
				f.store.mu.Lock()
				pending := f.store.changes != f.store.written
				f.store.mu.Unlock()
				if pending != (name != "restore-committed-cleanup") {
					t.Fatal("protective rewrite did not preserve confirmation versus cleanup distinction")
				}
				// Startup may load the exact confirmed, still-valid operator
				// action after an indeterminate return. A visible read/reopen
				// is not evidence that the earlier rename was durable.
				restarted, _ := openPinStore(t, f.paths, f.clock)
				requireStarts(t, restarted)
				f.assertState(t, restarted, name == "restore-before-visible")
			})
		}
	}
}

func TestEndpointOperatorGateSerializesProofAndExpiryProducers(t *testing.T) {
	for _, producer := range []string{"outbound-proof", "inbound-proof", "flush-expiry"} {
		t.Run(producer, func(t *testing.T) {
			f := newEndpointOutcomeFixture(t, "demote")
			expiring := domain.PeerAddress("198.51.100.212:64646")
			bindOrdinary(t, f.store, expiring, f.owner)
			actual := f.store.writeFile
			entered, release := make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var writes atomic.Int32
			f.store.writeFile = func(raw []byte) error {
				if writes.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return actual(raw)
			}
			opDone, producerDone := make(chan error, 1), make(chan error, 1)
			opExited, producerExited := make(chan struct{}), make(chan struct{})
			producerStarted := false
			t.Cleanup(func() {
				cancel()
				finish()
				for _, done := range []struct {
					active bool
					ch     chan struct{}
				}{{true, opExited}, {producerStarted, producerExited}} {
					if done.active {
						select {
						case <-done.ch:
						case <-time.After(2 * time.Second):
							t.Error("owned producer did not join")
						}
					}
				}
			})
			go func() { defer close(opExited); _, err := f.act(); opDone <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("operator writer did not enter")
			}
			if f.store.pinMutationGate.TryRLock() {
				f.store.pinMutationGate.RUnlock()
				t.Fatal("operator did not hold exclusive mutation gate")
			}
			if !f.store.mu.TryLock() {
				t.Fatal("operator held leaf mu during filesystem I/O")
			}
			f.store.mu.Unlock()
			// Both original clock sources advance together before the flush
			// starts; no goroutine writes the test clock while a reader runs.
			if producer == "flush-expiry" {
				f.clock.now = f.clock.now.Add(endpointBindingTTL + time.Hour)
			}
			newOwner := domain.PeerIdentity{0xe2}
			started := make(chan struct{})
			producerStarted = true
			go func() {
				defer close(producerExited)
				close(started)
				var err error
				switch producer {
				case "outbound-proof":
					_, err = f.store.noteProvenOutbound(dialledAt(f.selection[0]), newOwner, endpointProtectionNone())
				case "inbound-proof":
					err = f.store.noteProvenInbound(newOwner)
				case "flush-expiry":
					err = f.store.flushPending(flushRenewalsAtStop)
				}
				producerDone <- err
			}()
			<-started
			// This is a bounded no-early-publication observation followed by
			// actual progress/join, not a scheduler fairness/hard-time proof.
			select {
			case err := <-producerDone:
				t.Fatalf("producer completed inside operator transaction: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			if writes.Load() != 1 {
				t.Fatal("producer entered writer before operator completed")
			}
			binding, present := bindingOf(f.store, f.selection[0])
			if !present || binding.identity != f.owner || binding.protection == nil {
				t.Fatal("proof mutated selected binding during operator transaction")
			}
			if f.store.identityPinned(newOwner) {
				t.Fatal("proof published identity before gate release")
			}
			if _, present := bindingOf(f.store, expiring); !present {
				t.Fatal("flush pruned expiry before exclusive transaction completed")
			}
			finish()
			for _, done := range []<-chan error{opDone, producerDone} {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("gate release failed to permit real producer progress")
				}
			}
			if producer == "flush-expiry" {
				if _, present := bindingOf(f.store, expiring); present {
					t.Fatal("released flush did not prune expired binding")
				}
			} else if !f.store.identityPinned(newOwner) {
				t.Fatal("released proof did not publish")
			}
		})
	}
}

func TestEndpointOperatorStalePrecheckNeverWritesAnAction(t *testing.T) {
	f := newEndpointOutcomeFixture(t, "demote")
	if _, err := f.store.noteProvenOutbound(dialledAt(f.selection[0]), domain.PeerIdentity{0xe3}, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.paths.store)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	f.store.writeFile = func([]byte) error { writes++; return errors.New("stale selection reached writer") }
	_, err = f.act()
	var action *domain.EndpointBindingActionError
	if !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) || errors.As(err, &action) || writes != 0 {
		t.Fatalf("stale precheck fabricated applied receipt: %v writes=%d", err, writes)
	}
	after, err := os.ReadFile(f.paths.store)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("stale refusal changed disk")
	}
	for i, endpoint := range f.selection {
		binding, present := bindingOf(f.store, endpoint)
		wantOwner := f.owner
		if i == 0 {
			wantOwner = domain.PeerIdentity{0xe3}
		}
		if !present || binding.identity != wantOwner || binding.protection == nil {
			t.Fatal("stale precheck changed the currently selected protected state")
		}
	}
	assertTallies(t, f.store)
}
