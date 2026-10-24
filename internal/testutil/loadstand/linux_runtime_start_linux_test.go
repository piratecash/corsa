//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"golang.org/x/sys/unix"
)

func linuxRuntimeTestTuple(t *testing.T) (domain.NodeStatus, protocol.ContactFrame) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	status := domain.NodeStatus{Identity: id.Address, Address: id.Address, PublicKey: identity.PublicKeyBase64(id.PublicKey), BoxPublicKey: identity.BoxPublicKeyBase64(id.BoxPublicKey), StartedAt: time.Unix(1700000000, 0).UTC()}
	contact := protocol.ContactFrame{Address: id.Address, PubKey: status.PublicKey, BoxKey: status.BoxPublicKey, BoxSig: identity.SignBoxKeyBinding(id)}
	return status, contact
}
func linuxRuntimeTestFacts(t *testing.T) linuxRuntimeFacts {
	t.Helper()
	status, contact := linuxRuntimeTestTuple(t)
	public, err := linuxRuntimePublic(status, status, []protocol.ContactFrame{contact})
	if err != nil {
		t.Fatal(err)
	}
	return linuxRuntimeFacts{Provenance: LinuxProcessProvenance{Incarnation: ProcessIncarnation{PID: 42, StartTicks: 73}, UID: 65532, RunningExecutableSHA256: ExecutableSHA256(strings.Repeat("a", 64)), RunningExecutableVerified: true, KernelStartTimeAvailable: true}, Namespace: linuxRuntimeNamespace{Device: 3, Inode: 4}, Public: public, StartedAt: status.StartedAt, BeganAt: status.StartedAt.Add(time.Second), EndedAt: status.StartedAt.Add(2 * time.Second)}
}
func TestLinuxRuntimePublicTupleValidation(t *testing.T) {
	status, contact := linuxRuntimeTestTuple(t)
	other, foreign := linuxRuntimeTestTuple(t)
	if _, err := linuxRuntimePublic(status, status, []protocol.ContactFrame{contact}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*domain.NodeStatus, *domain.NodeStatus, *[]protocol.ContactFrame){
		"fingerprint": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) {
			a.PublicKey = other.PublicKey
			b.PublicKey = other.PublicKey
			(*c)[0].PubKey = other.PublicKey
		},
		"invalid-proof": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { (*c)[0].BoxSig = foreign.BoxSig },
		"box-canonical": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) {
			a.BoxPublicKey += "\n"
			b.BoxPublicKey = a.BoxPublicKey
			(*c)[0].BoxKey = a.BoxPublicKey
		},
		"identity-canonical": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) {
			a.Identity = strings.ToUpper(a.Identity)
			a.Address = a.Identity
			b.Identity = a.Identity
			b.Address = a.Address
			(*c)[0].Address = a.Address
		},
		"no-start": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) {
			a.StartedAt = time.Time{}
			b.StartedAt = time.Time{}
		},
		"changed-start":    func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { b.StartedAt = b.StartedAt.Add(time.Second) },
		"changed-box":      func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { b.BoxPublicKey = other.BoxPublicKey },
		"changed-identity": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { *b = other },
		"no-self":          func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { *c = nil },
		"duplicate-self":   func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) { *c = append(*c, contact) },
		"conflicting-self": func(a, b *domain.NodeStatus, c *[]protocol.ContactFrame) {
			wrong := contact
			wrong.PubKey = other.PublicKey
			*c = append(*c, wrong)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			a, b, rows := status, status, []protocol.ContactFrame{contact}
			change(&a, &b, &rows)
			if _, err := linuxRuntimePublic(a, b, rows); err == nil {
				t.Fatal("invalid testimony accepted")
			}
		})
	}
}
func TestLinuxRuntimeJointFactsRequired(t *testing.T) {
	valid := linuxRuntimeTestFacts(t)
	if !validLinuxRuntimeFacts(valid) {
		t.Fatal("valid fixture refused")
	}
	for name, mutate := range map[string]func(*linuxRuntimeFacts){
		"unavailable-exe": func(f *linuxRuntimeFacts) { f.Provenance.RunningExecutableVerified = false },
		"no-kernel-start": func(f *linuxRuntimeFacts) { f.Provenance.KernelStartTimeAvailable = false },
		"no-pid":          func(f *linuxRuntimeFacts) { f.Provenance.Incarnation.PID = 0 },
		"no-ticks":        func(f *linuxRuntimeFacts) { f.Provenance.Incarnation.StartTicks = 0 },
		"no-uid":          func(f *linuxRuntimeFacts) { f.Provenance.UID = 0 },
		"no-netns":        func(f *linuxRuntimeFacts) { f.Namespace.Inode = 0 },
		"no-public":       func(f *linuxRuntimeFacts) { f.Public = PublicIdentity{} },
		"backward-wall":   func(f *linuxRuntimeFacts) { f.EndedAt = f.BeganAt.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			f := valid
			mutate(&f)
			if validLinuxRuntimeFacts(f) {
				t.Fatal("partial observation accepted")
			}
		})
	}
}

// Real packet/work-owner composition only; these rows are NOT OS observations.
func linuxRuntimeTestWire(t *testing.T) (*LinuxFixture, *linuxPacketConn, linuxRuntimeFacts) {
	t.Helper()
	owner, _, work := linuxTestRootWire(t)
	facts := linuxRuntimeTestFacts(t)
	owner.mu.Lock()
	owner.namespace = &OwnedNetNS{fd: 1, claimed: true, identity: linuxNamespaceIdentity{device: 3, inode: 4}}
	owner.readyProvenance = facts.Provenance
	owner.mu.Unlock()
	return owner, work, facts
}
func linuxRuntimeTestReply(t *testing.T, work *linuxPacketConn, request linuxPacket, reply linuxRuntimeReply) {
	t.Helper()
	raw, err := encodeLinuxPacket(linuxRuntimeResult, request.id, request.ordinal, reply)
	if err != nil {
		t.Fatal(err)
	}
	if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
func TestLinuxRuntimeSharedWorkAndCopiedHistory(t *testing.T) {
	owner, work, facts := linuxRuntimeTestWire(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	outcome := make(chan linuxRuntimeOutcome, 1)
	go func() { value, err := owner.ObserveOwnedRuntime(ctx); outcome <- linuxRuntimeOutcome{value, err} }()
	request, err := work.read(time.Now().Add(time.Second))
	if err != nil || request.kind != linuxObserveRuntime {
		t.Fatal("observation did not use work channel", err)
	}
	if _, err = owner.Sample(ctx); !errors.Is(err, ErrLinuxOwnerBusy) {
		t.Fatal("Sample bypassed shared work admission", err)
	}
	if _, err = owner.ObserveOwnedRuntime(ctx); !errors.Is(err, ErrLinuxOwnerBusy) {
		t.Fatal("Observe bypassed shared work admission", err)
	}
	linuxRuntimeTestReply(t, work, request, linuxRuntimeReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Facts: facts})
	result := <-outcome
	if result.err != nil || !result.observation.known {
		t.Fatal(result.err)
	}
	public, known := result.observation.Public()
	if !known || public != facts.Public {
		t.Fatal("wrong public copy")
	}
	public.PublicKey = "changed"
	again, _ := result.observation.Public()
	if again == public || again != facts.Public || owner.runtimeObservation.facts.Public != facts.Public {
		t.Fatal("getter mutated retained observation")
	}
	raw, err := json.Marshal(result.observation) //nolint:staticcheck // SA9005: intentionally test opaque JSON boundary.
	if err != nil {
		t.Fatal(err)
	}
	var forged LinuxOwnedRuntimeObservation
	if err = json.Unmarshal(raw, &forged); err != nil || forged.known { //nolint:staticcheck // SA9005: intentionally test opaque JSON boundary.
		t.Fatal("JSON created private observation", err)
	}
	if err = owner.requestStop(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, known = result.observation.Public(); !known {
		t.Fatal("historical copy disappeared")
	}
	if _, err = owner.ObserveOwnedRuntime(ctx); !errors.Is(err, ErrLinuxOwnerPoisoned) {
		t.Fatal("stopped owner replayed cached success", err)
	}
	if owner.Snapshot().HelperWaitKnown || owner.Snapshot().Finality != FinalUnavailable {
		t.Fatal("wire result invented Wait/finality")
	}
}
func TestLinuxRuntimePublicationRejectsTerminalRaces(t *testing.T) {
	for _, reason := range []string{"stop", "cancel", "target-exit", "namespace-closed", "namespace-poison"} {
		t.Run(reason, func(t *testing.T) {
			owner, work, facts := linuxRuntimeTestWire(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			owner.namespace.mu.Lock() // prevent mint, without holding the work reader
			outcome := make(chan linuxRuntimeOutcome, 1)
			go func() { v, e := owner.ObserveOwnedRuntime(ctx); outcome <- linuxRuntimeOutcome{v, e} }()
			request, err := work.read(time.Now().Add(time.Second))
			if err != nil {
				owner.namespace.mu.Unlock()
				t.Fatal(err)
			}
			linuxRuntimeTestReply(t, work, request, linuxRuntimeReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Facts: facts})
			switch reason {
			case "stop":
				err = owner.requestStop(time.Second)
			case "cancel":
				cancel()
			case "target-exit":
				owner.mu.Lock()
				owner.targetKnown = true
				owner.mu.Unlock()
			case "namespace-closed":
				owner.namespace.closed = true
			case "namespace-poison":
				owner.namespace.poisoned = true
			}
			owner.namespace.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			result := <-outcome
			if reason == "stop" || reason == "cancel" {
				select {
				case <-owner.workDone:
				case <-time.After(time.Second):
					t.Fatal("terminal work owner did not join")
				}
			}
			if result.err == nil || result.observation.known {
				t.Fatal("terminal/cancel result minted historical authority")
			}
			owner.mu.Lock()
			minted := owner.runtimeObservation.known
			owner.mu.Unlock()
			if minted {
				t.Fatal("terminal state still populated retained success")
			}
		})
	}
}
func TestLinuxRuntimeReplyCorrelationAndOriginalNamespace(t *testing.T) {
	for _, reason := range []string{"nonce", "request", "ordinal", "kind", "namespace", "start", "unavailable"} {
		t.Run(reason, func(t *testing.T) {
			owner, work, facts := linuxRuntimeTestWire(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			outcome := make(chan linuxRuntimeOutcome, 1)
			go func() { v, e := owner.ObserveOwnedRuntime(ctx); outcome <- linuxRuntimeOutcome{v, e} }()
			request, err := work.read(time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			reply := linuxRuntimeReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Facts: facts}
			kind := linuxRuntimeResult
			switch reason {
			case "nonce":
				reply.Nonce = LaunchNonce{9}
			case "request":
				request.id++
			case "ordinal":
				request.ordinal++
			case "kind":
				kind = linuxSampleResult
			case "namespace":
				reply.Facts.Namespace.Inode++
			case "start":
				reply.Facts.Provenance.Incarnation.StartTicks++
			case "unavailable":
				reply.Code = linuxResultUnavailable
				reply.Facts = linuxRuntimeFacts{}
			}
			raw, err := encodeLinuxPacket(kind, request.id, request.ordinal, reply)
			if err != nil {
				t.Fatal(err)
			}
			if err = work.write(raw, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			result := <-outcome
			if result.err == nil || result.observation.known {
				t.Fatal("foreign/partial result published")
			}
		})
	}
}
func TestLinuxRuntimeTimeoutNeverReplaysLateSuccess(t *testing.T) {
	owner, work, facts := linuxRuntimeTestWire(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	outcome := make(chan linuxRuntimeOutcome, 1)
	go func() { v, e := owner.ObserveOwnedRuntime(ctx); outcome <- linuxRuntimeOutcome{v, e} }()
	request, err := work.read(time.Now().Add(time.Second))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel() // actual admitted request; work reader may still consume the late packet
	result := <-outcome
	if result.err == nil || result.observation.known {
		t.Fatal("canceled request succeeded")
	}
	raw, err := encodeLinuxPacket(linuxRuntimeResult, request.id, request.ordinal, linuxRuntimeReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Facts: facts})
	if err != nil {
		t.Fatal(err)
	}
	// Peer close or an accepted kernel write are both compatible with poisoning.
	_ = work.write(raw, time.Now().Add(time.Second))
	fresh, done := context.WithTimeout(t.Context(), time.Second)
	defer done()
	if _, err = owner.ObserveOwnedRuntime(fresh); !errors.Is(err, ErrLinuxOwnerPoisoned) {
		t.Fatal("late success made owner reusable", err)
	}
}

// This checker freezes named syntactic capability references, not arbitrary Go
// dataflow. Sources are immutable inputs; no production hook/authority is added.
type linuxRuntimeAuthoritySite struct{ file, scope, mode, expression string }

func linuxRuntimeAuthorityExpected() map[linuxRuntimeAuthoritySite]int {
	return map[linuxRuntimeAuthoritySite]int{
		{"linux_member_linux.go", "*linuxMember.Start", "call", "CreateLinuxFixture(readiness, m.spec, m.parent, row.namespace)"}:                                                                                  1,
		{"linux_member_linux.go", "*linuxMember.Start", "call", "fixture.Start(readiness)"}:                                                                                                                        1,
		{"linux_member_start_linux.go", "*linuxHelperRuntime.writeMemberBind", "call", "r.process.CompletedStart(receipt)"}:                                                                                        1,
		{"linux_member_start_linux.go", "*linuxHelperRuntime.reduceMemberFinal", "call", "r.process.FinalAccounting(ctx, binding, prior)"}:                                                                         1,
		{"linux_member_rpc_linux.go", "*linuxMember.PeerHealth", "call", "m.fixture.RPC()"}:                                                                                                                        1,
		{"linux_member_rpc_linux.go", "*linuxMember.ImportContacts", "call", "m.fixture.RPC()"}:                                                                                                                    1,
		{"linux_member_rpc_linux.go", "*linuxMember.Sample", "call", "m.fixture.RPC()"}:                                                                                                                            1,
		{"linux_member_rpc_linux.go", "*linuxMember.Sample", "call", "NewBoundProcessSource(m.fixture.RPC(), clock, RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})"}: 1,
		{"linux_member_rpc_linux.go", "*linuxSDKMember.PlannedRoute", "call", "m.fixture.RPC()"}:                                                                                                                   1,
		{"linux_member_rpc_linux.go", "*linuxSDKMember.PlannedRoute", "call", "NewProcessSource(m.fixture.RPC())"}:                                                                                                 1,
		{"linux_member_rpc_linux.go", "*linuxSDKMember.SendDM", "call", "m.fixture.RPC()"}:                                                                                                                         1,
		{"linux_member_rpc_linux.go", "*linuxSDKMember.PublishGlobal", "call", "m.fixture.RPC()"}:                                                                                                                  1,
		{"linux_member_rpc_linux.go", "*linuxSDKMember.PublishGlobal", "call", "NewProcessSource(m.fixture.RPC())"}:                                                                                                1,
		{"linux_member_rpc_linux.go", "*linuxFullMember.GlobalReplica", "call", "m.fixture.RPC()"}:                                                                                                                 1,
		{"linux_member_rpc_linux.go", "*linuxFullMember.GlobalReplica", "call", "NewProcessSource(m.fixture.RPC())"}:                                                                                               1,
		{"planned_child_config.go", "copyPlannedChild", "reference", "c.RPC"}:                                                                                                                                      2,
		{"fullnode/main.go", "run", "call", "fullwitness.RegisterPlannedFullPublicIdentity(table, public)"}:                                                                                                        1,
		{"fullnode/main.go", "run", "reference", "child.RPC"}:                                                                                                                                                      1,
		{"fullnode/main.go", "run", "reference", "config.RPC"}:                                                                                                                                                     1,
		{"planned_network_nft_linux.go", "linuxPlannedNFT.runTracked", "call", "command.Start()"}:                                                                                                                  1,
		{"planned_network_nft_linux.go", "linuxPlannedNFT.runTracked", "call", "command.Wait()"}:                                                                                                                   1,
		{"planned_network_backend_linux.go", "*linuxPlannedNetworkBackend.apply", "call", "CreateOwnedNetNS(ctx)"}:                                                                                                 1,
		{"planned_network_namespace_linux.go", "*OwnedNetNS.plannedOperation", "reference", "n.claimed"}:                                                                                                           1,
		{"planned_network_namespace_linux.go", "*OwnedNetNS.plannedFD", "reference", "n.claimed"}:                                                                                                                  1,
		{"linux_runtime_start_linux.go", "*LinuxFixture.observeOnWorkOwner", "call", "o.readRuntimeObservation(exchange)"}:                                                                                         1,
		{"linux_runtime_start_linux.go", "*LinuxFixture.readRuntimeObservation", "call", "o.readRuntimeReply(exchange)"}:                                                                                           1,
		{"linux_runtime_start_linux.go", "*LinuxFixture.readRuntimeObservation", "call", "o.publishRuntimeObservation(exchange.ctx, exchange.id, reply)"}:                                                          1,
		{"linux_process_linux.go", "CreateLinuxFixture", "call", "newLinuxFixtureResources(ctx, spec, parent, namespace)"}:                                                                                         1,
		{"linux_process_linux.go", "<package>", "reference", "RPCClient"}:                                                                                                                                          1,
		{"process.go", "<package>", "reference", "RPCClient"}:                                                                                                                                                      1,
		{"source.go", "<package>", "reference", "RPCClient"}:                                                                                                                                                       1,
		{"rpc.go", "NewRPCClient", "reference", "RPCClient"}:                                                                                                                                                       1,
		{"linux_process_linux.go", "newLinuxFixtureResources", "call", "NewRPCClient(RPCOptions{Address: spec.Config.RPC, Credentials: spec.Credentials, DialContext: boundedLinuxRPCDial(o.dialContext)})"}:       1,
		{"linux_process_linux.go", "newLinuxFixtureResources", "reference", "spec.Config.RPC"}:                                                                                                                     2,
		{"linux_process_linux.go", "newLinuxFixtureResources", "reference", "namespace.claimed"}:                                                                                                                   2,
		{"linux_process_linux.go", "*LinuxFixture.startLinuxHelper", "call", "command.Start()"}:                                                                                                                    1,
		{"linux_process_linux.go", "*LinuxFixture.startLinuxHelper", "call", "command.Wait()"}:                                                                                                                     1,
		{"linux_helper_linux.go", "validateLinuxHelperInit", "reference", "config.RPC"}:                                                                                                                            1,
		{"linux_helper_linux.go", "validateLinuxHelperInit", "reference", "init.Config.RPC"}:                                                                                                                       1,
		{"linux_helper_linux.go", "launchLinuxTarget", "call", "NewRPCClient(RPCOptions{Address: init.Config.RPC, Credentials: credentials})"}:                                                                     1,
		{"linux_helper_linux.go", "launchLinuxTarget", "reference", "init.Config.RPC"}:                                                                                                                             1,
		{"linux_helper_linux.go", "launchLinuxTarget", "call", "command.Start()"}:                                                                                                                                  1,
		{"process.go", "LaunchLoopbackProcess", "call", "NewRPCClient(RPCOptions{Address: options.Config.RPC, Credentials: options.Credentials})"}:                                                                 1,
		{"process.go", "LaunchLoopbackProcess", "reference", "options.Config.RPC"}:                                                                                                                                 1,
		{"process.go", "LaunchLoopbackProcess", "call", "command.Start()"}:                                                                                                                                         1,
		{"process.go", "*Process.awaitReady", "call", "NewProcessSource(p.rpc)"}:                                                                                                                                   2,
		{"process.go", "sandboxProfile", "reference", "config.RPC"}:                                                                                                                                                1,
		{"process_exit.go", "startOwnedProcess", "call", "command.Wait()"}:                                                                                                                                         1,
		{"child_config.go", "ChildConfig.ValidateLoopback", "reference", "c.RPC"}:                                                                                                                                  1,
		{"child_config.go", "ChildConfig.environmentUnchecked", "reference", "c.RPC"}:                                                                                                                              1,
		{"datagram_runtime_source.go", "NewBoundProcessSource", "call", "NewProcessSourceWithClock(client, clock)"}:                                                                                                1,
		{"sdkedge/main.go", "run", "reference", "config.RPC"}:                                                                                                                                                      1,
		{"sdkedge/main.go", "run", "reference", "cfg.RPC"}:                                                                                                                                                         1,
		{"sdkedge/main.go", "run", "call", "stack.Start(ctx)"}:                                                                                                                                                     1,
		{"sdkedge/main.go", "run", "call", "stack.Wait()"}:                                                                                                                                                         1,
		{"linuxhelper/main.go", "main", "call", "loadstand.RunLinuxHelper(context.Background())"}:                                                                                                                  1,
		{"linux_runtime_start_linux.go", "*LinuxFixture.publishRuntimeObservation", "reference", "o.namespace.claimed"}:                                                                                            1,
		{"linux_runtime_start_linux.go", "*LinuxFixture.ObserveOwnedRuntime", "call", "ctx.Done()"}:                                                                                                                2,
		{"linux_runtime_start_linux.go", "*LinuxFixture.observeOnWorkOwner", "call", "exchange.ctx.Done()"}:                                                                                                        1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "r.process.Done()"}:                                                                                                 2,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "reference", "r.process"}:                                                                                                   5,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "NewProcessSource(r.process.RPC())"}:                                                                                1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "r.process.RPC()"}:                                                                                                  3,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "source.Status(ctx)"}:                                                                                               2,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", `r.process.RPC().Call(ctx, "fetchContacts", nil, &contacts)`}:                                                       1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", `r.process.RPC().Call(ctx, "loadstandFullPublicIdentity", nil, &witness)`}:                                          1,
	}
}
func linuxRuntimeAuthorityName(name string) bool {
	switch name {
	case "RegisterPlannedFullPublicIdentity", "newLinuxFixtureResources", "CreateLinuxFixture", "CreateOwnedNetNS", "RunLinuxHelper", "NewRPCClient", "RPCClient", "NewProcessSource", "NewProcessSourceWithClock", "NewBoundProcessSource", "ObserveOwnedRuntime", "readRuntimeReply", "readRuntimeObservation", "publishRuntimeObservation", "Wait", "Start", "CompletedStart", "FinalAccounting", "RPC", "claimed":
		return true
	}
	return false
}
func linuxRuntimeAuthorityText(fset *token.FileSet, n ast.Node) (string, error) {
	var out bytes.Buffer
	if err := format.Node(&out, fset, n); err != nil {
		return "", err
	}
	return strings.Join(strings.Fields(out.String()), " "), nil
}
func linuxRuntimeAuthorityReferences(sources map[string]string) (map[linuxRuntimeAuthoritySite]int, error) {
	references := map[linuxRuntimeAuthoritySite]int{}
	for file, source := range sources {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, source, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range parsed.Decls {
			var root ast.Node
			scope := "<package>"
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				root = d.Body
				scope = d.Name.Name
				if d.Recv != nil {
					receiver, err := linuxRuntimeAuthorityText(fset, d.Recv.List[0].Type)
					if err != nil {
						return nil, err
					}
					scope = receiver + "." + scope
				}
			case *ast.GenDecl:
				root = d
			default:
				continue
			}
			var stack []ast.Node
			var verdict error
			ast.Inspect(root, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return false
				}
				var parent ast.Node
				if len(stack) > 0 {
					parent = stack[len(stack)-1]
				}
				stack = append(stack, n)
				if verdict != nil {
					return true
				}
				// A claim write is allowed only at the original immutable constructor site.
				if assign, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assign.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || sel.Sel.Name != "claimed" {
							continue
						}
						text, err := linuxRuntimeAuthorityText(fset, assign)
						if err != nil {
							verdict = err
							return true
						}
						if file != "linux_process_linux.go" || scope != "newLinuxFixtureResources" || text != "namespace.claimed = true" {
							verdict = fmt.Errorf("unexpected claim write in %s/%s", file, scope)
							return true
						}
					}
				}
				var expression ast.Expr
				switch node := n.(type) {
				case *ast.SelectorExpr:
					name := node.Sel.Name
					observerEdge := file == "linux_runtime_start_linux.go" && (name == "Call" || name == "Status" || name == "rpc" || name == "client" || name == "http" || name == "transport" || name == "process" || name == "Done")
					if linuxRuntimeAuthorityName(name) || observerEdge {
						expression = node
					}
				case *ast.Ident:
					if sel, ok := parent.(*ast.SelectorExpr); ok && sel.Sel == node {
						return true
					}
					if spec, ok := parent.(*ast.TypeSpec); ok && spec.Name == node {
						return true
					} // declaration, not capability reference
					if field, ok := parent.(*ast.Field); ok {
						for _, declared := range field.Names {
							if declared == node {
								return true
							}
						}
					}
					if value, ok := parent.(*ast.ValueSpec); ok {
						for _, declared := range value.Names {
							if declared == node {
								return true
							}
						}
					}
					if linuxRuntimeAuthorityName(node.Name) && node.Name != "RPC" && node.Name != "claimed" {
						expression = node
					}
					if file == "linux_runtime_start_linux.go" && (node.Name == "RPCOptions" || node.Name == "Process") {
						expression = node
					}
				}
				if expression == nil {
					return true
				}
				mode := "reference"
				var rendered ast.Node = expression
				if call, ok := parent.(*ast.CallExpr); ok && call.Fun == expression {
					mode = "call"
					rendered = call
				}
				text, err := linuxRuntimeAuthorityText(fset, rendered)
				if err != nil {
					verdict = err
					return true
				}
				references[linuxRuntimeAuthoritySite{file, scope, mode, text}]++
				return true
			})
			if verdict != nil {
				return nil, verdict
			}
		}
	}
	return references, nil
}
func linuxRuntimeCheckAuthoritySources(sources map[string]string, expected map[linuxRuntimeAuthoritySite]int) error {
	references, err := linuxRuntimeAuthorityReferences(sources)
	if err != nil {
		return err
	}
	for site, count := range references {
		if count != expected[site] {
			return fmt.Errorf("unexpected authority references in %s/%s (%s)", site.file, site.scope, site.mode)
		}
	}
	for site, count := range expected {
		if count != references[site] {
			return fmt.Errorf("missing original authority references in %s/%s", site.file, site.scope)
		}
	}
	return nil
}
func TestLinuxRuntimeClosedAuthorityInventory(t *testing.T) {
	sources := map[string]string{}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(path)] = string(raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = linuxRuntimeCheckAuthoritySources(sources, linuxRuntimeAuthorityExpected()); err != nil {
		t.Fatal(err)
	}
}

func TestLinuxRuntimeClosedAuthorityNegativeReferences(t *testing.T) {
	// go/types is executed by this FUTURE test, not by static author preparation.
	// These are minimal self-contained language/type-valid capability patterns.
	const declarations = `package loadstand
 type LinuxFixture struct{}
 func (*LinuxFixture) ObserveOwnedRuntime(){}
 func (*LinuxFixture) readRuntimeReply(){}
 func (*LinuxFixture) publishRuntimeObservation(){}
 func newLinuxFixtureResources(){}
 type RPCClient struct{}
 func NewRPCClient()*RPCClient{return nil}
 type Process struct{}
 func (*Process) Wait(){}
 func (*Process) CompletedStart(){}
 func (*Process) FinalAccounting(){}
 func (*Process) RPC()*RPCClient{return nil}
 func NewProcessSource(*RPCClient){}
 type linuxHelperRuntime struct{process,alternate *Process}
 `
	cases := map[string]string{
		"observe-method-expression":       `var observeAlias = (*LinuxFixture).ObserveOwnedRuntime`,
		"constructor-function-value":      `var constructorAlias = newLinuxFixtureResources`,
		"alternate-rpc-constructor-value": `var rpcAlias = NewRPCClient`,
		"rpc-method-expression":           `var rpcMethod = (*Process).RPC`,
		"wait-method-expression":          `var waitAlias = (*Process).Wait`,
		"start-method-expression":         `var startAlias = (*Process).CompletedStart`,
		"final-method-expression":         `var finalAlias = (*Process).FinalAccounting`,
		"private-prefix-alias":            `var readAlias = (*LinuxFixture).readRuntimeReply`,
		"private-publication-alias":       `var publishAlias = (*LinuxFixture).publishRuntimeObservation`,
		"rpc-composite":                   `var alternate = &RPCClient{}`,
		"method-value":                    `func escape(p *Process){ getter := p.RPC; _ = getter }`,
	}
	for name, snippet := range cases {
		t.Run(name, func(t *testing.T) {
			source := declarations + snippet
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "escape.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (&types.Config{}).Check("loadstand", fset, []*ast.File{file}, nil); err != nil {
				t.Fatal("negative snippet is not language/type valid", err)
			}
			if err = linuxRuntimeCheckAuthoritySources(map[string]string{"escape.go": source}, nil); err == nil {
				t.Fatal("aliased capability escaped reference guard")
			}
		})
	}
	// Same reviewed function/file, different original client: this must reject
	// even if the number of RPC calls and constructor calls remains identical.
	const original = `func(r *linuxHelperRuntime)observeOriginalRuntime(){ NewProcessSource(r.process.RPC()) }`
	expected := map[linuxRuntimeAuthoritySite]int{
		{"linux_runtime_start_linux.go", "<package>", "reference", "Process"}:                                                       1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "NewProcessSource(r.process.RPC())"}: 1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "call", "r.process.RPC()"}:                   1,
		{"linux_runtime_start_linux.go", "*linuxHelperRuntime.observeOriginalRuntime", "reference", "r.process"}:                    1,
	}
	source := declarations + original
	if err := linuxRuntimeCheckAuthoritySources(map[string]string{"linux_runtime_start_linux.go": source}, expected); err != nil {
		t.Fatal("reviewed exact-site control refused", err)
	}
	alternate := strings.Replace(source, "NewProcessSource(r.process.RPC())", "NewProcessSource(r.alternate.RPC())", 1)
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "linux_runtime_start_linux.go", alternate, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (&types.Config{}).Check("loadstand", fset, []*ast.File{parsed}, nil); err != nil {
		t.Fatal("alternate-client snippet is not language/type valid", err)
	}
	if err = linuxRuntimeCheckAuthoritySources(map[string]string{"linux_runtime_start_linux.go": alternate}, expected); err == nil {
		t.Fatal("alternate original-RPC edge accepted in same file/function")
	}
}

func TestLinuxRuntimePublicationContextIsolatedFromOuterPoison(t *testing.T) {
	// Actual shared packet prefix returns a VALIDATED reply before cancellation.
	// Then invoke the SAME publication body, without outer Observe/poison/Stop.
	// Synthetic wire facts test the predicate, never OS launch ownership.
	owner, peer, facts := linuxRuntimeTestWire(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	owner.mu.Lock()
	owner.workBusy = true
	id, err := owner.correlation.allocate()
	owner.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertLive := func() {
		t.Helper()
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if !owner.running || owner.stopping || owner.correlation.poisoned || owner.targetKnown || owner.helperWaitKnown || !owner.workBusy || owner.correlation.lastID != id {
			t.Fatal("outer poison/Stop/currentID masked isolated predicate")
		}
	}
	type prefixResult struct {
		reply linuxRuntimeReply
		err   error
	}
	parsed := make(chan prefixResult, 1)
	prefixDone := make(chan struct{})
	t.Cleanup(func() {
		if err := owner.work.close(); err != nil {
			t.Error(err)
		}
		select {
		case <-prefixDone:
		case <-time.After(time.Second):
			t.Error("prefix goroutine cleanup not joined")
		}
	})
	go func() {
		defer close(prefixDone)
		reply, err := owner.readRuntimeReply(linuxSampleExchange{ctx: ctx, id: id})
		parsed <- prefixResult{reply, err}
	}()
	request, err := peer.read(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if request.kind != linuxObserveRuntime || request.id != id {
		t.Fatal("wrong original work request")
	}
	linuxRuntimeTestReply(t, peer, request, linuxRuntimeReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Facts: facts})
	var validated prefixResult
	select {
	case validated = <-parsed:
	case <-time.After(time.Second):
		t.Fatal("actual shared read/validation prefix did not join")
	}
	select {
	case <-prefixDone:
	case <-time.After(time.Second):
		t.Fatal("validated prefix goroutine did not return")
	}
	sameFacts := validated.reply.Facts.Provenance == facts.Provenance && validated.reply.Facts.Namespace == facts.Namespace && validated.reply.Facts.Public == facts.Public && validated.reply.Facts.StartedAt.Equal(facts.StartedAt) && validated.reply.Facts.BeganAt.Equal(facts.BeganAt) && validated.reply.Facts.EndedAt.Equal(facts.EndedAt)
	if validated.err != nil || !sameFacts || validated.reply.Code != linuxResultReady || ctx.Err() != nil {
		t.Fatal("reply not validated before isolated cancel", validated.err)
	}
	assertLive()
	owner.namespace.mu.Lock() // publication cannot commit before this gate releases
	locked := true
	defer func() {
		if locked {
			owner.namespace.mu.Unlock()
		}
	}()
	entering := make(chan struct{})
	outcome := make(chan linuxRuntimeOutcome, 1)
	publicationDone := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-publicationDone:
		case <-time.After(time.Second):
			t.Error("publication goroutine cleanup not joined")
		}
	})
	go func() {
		defer close(publicationDone)
		close(entering)
		value, err := owner.publishRuntimeObservation(ctx, id, validated.reply)
		outcome <- linuxRuntimeOutcome{value, err}
	}()
	<-entering // phase initiated AFTER validated-prefix completion; no timing guess
	assertLive()
	cancel() // before gate release; only final ctx predicate may now refuse
	assertLive()
	owner.namespace.mu.Unlock()
	locked = false
	var result linuxRuntimeOutcome
	select {
	case result = <-outcome:
	case <-time.After(time.Second):
		t.Fatal("isolated shared publication did not join")
	}
	select {
	case <-publicationDone:
	case <-time.After(time.Second):
		t.Fatal("publication goroutine did not return")
	}
	assertLive() // Stop, poisoned state, target/provenance and current ID are unchanged
	if !errors.Is(result.err, context.Canceled) || result.observation.known {
		t.Fatal("canceled live publication minted instead of ctx rejection", result.err)
	}
	owner.mu.Lock()
	retained := owner.runtimeObservation.known
	owner.workBusy = false
	owner.mu.Unlock()
	if retained {
		t.Fatal("ignore-final-cancel minted retained success")
	}
}

func TestLinuxRuntimeOwnedObservationNative(t *testing.T) {
	if os.Getenv("LOADSTAND_LINUX_NATIVE") != "1" {
		t.Skip("requires separately authorized exact own-container runtime-start profile")
	}
	for _, base := range []string{"/profiles", "/unsupported"} {
		if !t.Run(base, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			var nonce LaunchNonce
			if _, err := rand.Read(nonce[:]); err != nil {
				t.Fatal(err)
			}
			parent, err := CreateAuditedProfileParent(ctx, base, nonce)
			if err != nil {
				t.Fatal(err)
			}
			defer linuxTestClose(t, parent)
			ns, err := CreateOwnedNetNS(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
				defer stop()
				if err := ns.Close(cleanup); err != nil {
					t.Error(err)
				}
			}()
			credentials, err := NewCredentials()
			if err != nil {
				t.Fatal(err)
			}
			spec := LinuxFixtureSpec{HelperBinary: os.Getenv("LOADSTAND_NATIVE_HELPER"), HelperSHA256: ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_HELPER_SHA")), TargetBinary: os.Getenv("LOADSTAND_NATIVE_TARGET"), TargetSHA256: ExecutableSHA256(os.Getenv("LOADSTAND_NATIVE_TARGET_SHA")), UID: 65532, GID: 65532, Credentials: credentials, Config: ChildConfig{DataDir: parent.MemberDirectory(), Listen: domain.ListenAddress("127.0.0.1:42050"), RPC: domain.ListenAddress("127.0.0.1:42051"), Bootstrap: []domain.PeerAddress{"127.0.0.1:1"}, Listener: false, Procs: 1}}
			owner, err := CreateLinuxFixture(ctx, spec, parent, ns)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
				defer stop()
				if err := owner.Close(cleanup); err != nil && !errors.Is(err, ErrLinuxOwnerPoisoned) {
					t.Error(err)
				}
			}()
			assertSecondClaim := func() {
				t.Helper()
				second, err := CreateLinuxFixture(ctx, spec, parent, ns)
				if second != nil || !errors.Is(err, ErrLinuxOwnerUnsupported) {
					if second != nil {
						cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
						defer stop()
						_ = second.Close(cleanup)
					}
					t.Fatal("used namespace admitted second fixture", err)
				}
			}
			assertSecondClaim() // first claim consumed, but no target Start yet
			start, startCancel := context.WithTimeout(ctx, 5*time.Second)
			err = owner.Start(start)
			startCancel()
			if base == "/unsupported" {
				if !errors.Is(err, ErrLinuxOwnerUnsupported) {
					t.Fatal("unsupported mount was not refused", err)
				}
				assertSecondClaim() // failed Start does not release the namespace claim
			} else {
				if err != nil {
					t.Fatal(err)
				}
				ready, readyCancel := context.WithTimeout(ctx, 5*time.Second)
				for {
					_, err = NewProcessSource(owner.RPC()).Status(ready)
					if err == nil {
						break
					}
					select {
					case <-ready.Done():
						readyCancel()
						t.Fatal("normal public status unavailable", err)
					case <-time.After(10 * time.Millisecond):
					}
				}
				readyCancel()
				sample, sampleCancel := context.WithTimeout(ctx, 2*time.Second)
				observed, err := owner.ObserveOwnedRuntime(sample)
				sampleCancel()
				if err != nil || !observed.known {
					t.Fatal("joint observation unavailable", err)
				}
				facts := observed.facts
				if facts.Provenance != owner.readyProvenance || facts.Namespace != (linuxRuntimeNamespace{Device: ns.identity.device, Inode: ns.identity.inode}) || !validLinuxRuntimeFacts(facts) {
					t.Fatal("original OS/public facts diverged")
				}
				current, err := NewProcessSource(owner.RPC()).Status(ctx)
				if err != nil || current.Identity != facts.Public.Identity.String() || !current.StartedAt.Equal(facts.StartedAt) {
					t.Fatal("normal public testimony diverged", err)
				}
			}
			cleanup, stop := context.WithTimeout(ctx, 8*time.Second)
			defer stop()
			if err := owner.Close(cleanup); err != nil && !errors.Is(err, ErrLinuxOwnerPoisoned) {
				t.Fatal("original owner cleanup unknown", err)
			}
			assertSecondClaim() // completed/failed, closed namespace is never reset
			if base == "/profiles" {
				result := owner.Snapshot()
				if !result.TargetKnown || !result.HelperJoined || !result.HelperWaitKnown || !result.TargetExit.Clean() || result.Finality != FinalUnavailable {
					t.Fatal("missing actual Wait or fabricated finality", result)
				}
			}
		}) {
			return
		}
	}
}

func TestLinuxRuntimePinnedNamespaceDescriptors(t *testing.T) {
	// Actual self proc/namespace objects test the shared FD helper only. This
	// does NOT construct a Process owner or claim a launch/public identity.
	root, dir, err := openProcessDirectory(ProcessID(os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := errors.Join(unix.Close(dir), unix.Close(root)); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stat, _, err := readProcessIdentity(ctx, dir, ProcessID(os.Getpid()), ProcessUID(os.Getuid()))
	if err != nil {
		t.Fatal(err)
	}
	fd, ns, err := openThreadNetNS()
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Close(fd); err != nil {
		t.Fatal(err)
	}
	sampler := &LinuxProcessSampler{root: root, directory: dir, state: processSamplerOpen, expected: LinuxProcessExpectation{Incarnation: stat.ID, UID: ProcessUID(os.Getuid())}}
	helper := &linuxHelperRuntime{sampler: sampler, init: linuxInit{NSDevice: ns.device, NSInode: ns.inode}}
	count := linuxTestFDCount(t)
	for range 3 {
		observed, err := helper.readTargetRuntimeNS(ctx)
		if err != nil || observed != (linuxRuntimeNamespace{Device: ns.device, Inode: ns.inode}) {
			t.Fatal("pinned namespace unavailable", err)
		}
	}
	helper.init.NSInode++
	if _, err = helper.readTargetRuntimeNS(ctx); !errors.Is(err, ErrProcessObservation) {
		t.Fatal("target namespace mismatch accepted", err)
	}
	helper.init.NSInode--
	sampler.expected.Incarnation.StartTicks++
	if _, err = helper.readTargetRuntimeNS(ctx); !errors.Is(err, ErrProcessObservation) {
		t.Fatal("different incarnation accepted", err)
	}
	sampler.expected.Incarnation.StartTicks--
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err = helper.readTargetRuntimeNS(canceled); !errors.Is(err, ErrProcessObservation) {
		t.Fatal("canceled observation accepted", err)
	}
	if after := linuxTestFDCount(t); after != count {
		t.Fatalf("temporary namespace/proc FDs leaked: before=%d after=%d", count, after)
	}
}
