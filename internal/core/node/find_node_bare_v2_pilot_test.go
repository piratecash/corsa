package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// This is an explicit TEST-ONLY opt-in, not stock/default overlay admission.
// All operational R objects come from its original Service.Run handlers.
func newOwnedBareV2PilotRelay(t *testing.T, maxMembers int) (*Service, string) {
	t.Helper()
	if maxMembers != 1 && maxMembers != 2 {
		t.Fatal("pilot member profile outside closed small2 cases")
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true,
		Type: domain.NodeTypeFull, BootstrapPeers: []string{},
	})
	profile := ownedCutProfile()
	profile.Capture = findNodeOwnedCaptureProfile{
		Degree:   degreeProfile{MaxActive: 2},
		Priority: findNodePriorityProfile{MaxMembers: maxMembers, MaxRegistryVisits: 4},
	}
	// Copy the existing explicit P0 timing tuple; no production/default change.
	profile.Intake = findnode.DormantIntakeProfile{
		Timing: findnode.TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second},
		Round:  100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4,
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil {
		t.Fatal(err)
	}
	if !r.finalized || !r.checked || !r.bindings(s) || !s.bridgeCapture.consumer.IsRuntime() {
		t.Fatal("pilot did not retain original finalized Ready tuple")
	}
	s.ownedFindNodeAdvertiseTest = true
	running := runServiceForTest(t.Context(), s)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.Stop(ctx); err != nil {
			t.Error("original pilot Run did not stop", err)
		}
		if err := running.DrainBackground(ctx); err != nil {
			t.Error("original pilot background did not join", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := running.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	s.peerMu.RLock()
	address := s.listener.Addr().String()
	s.peerMu.RUnlock()
	if s.disableRateLimiting || s.loopbackInbound.bounds.maxActive > 64 {
		t.Fatal("pilot bypassed original listener budgets")
	}
	return s, address
}

// Protocol clients own only their real dialled sockets and proof identities.
// They never construct/install a Service, peerSession, registry entry or Session.
type ownedBareV2PilotClient struct {
	id       domain.PeerIdentity
	wire     net.Conn
	writeMu  sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	requests chan protocol.FindNodeRequest
	answers  chan protocol.FindNodeAnswer
	pongs    chan struct{}
	errors   chan error
}

func (c *ownedBareV2PilotClient) write(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.wire.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	n, err := c.wire.Write(raw)
	if err == nil && n != len(raw) {
		return fmt.Errorf("pilot short write %d/%d", n, len(raw))
	}
	return err
}

func (c *ownedBareV2PilotClient) stop(t *testing.T) {
	t.Helper()
	c.cancel()
	_ = c.wire.Close()
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Error("bare client reader did not join after own socket close")
	}
}

func dialOwnedBareV2PilotClient(t *testing.T, s *Service, address string, capability bool) *ownedBareV2PilotClient {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	local, err := sessionv2.NewLocal(id, domain.NetworkID(networkName))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = raw.Close()
		}
	}()
	hello := protocol.Frame{Version: config.ProtocolVersion, Client: "node", Listener: "0", NodeType: string(domain.NodeTypeFull)}
	if capability {
		hello.Capabilities = []string{string(domain.CapOverlayFindNodeV1)}
	}
	proof, err := sessionv2.Dial(ctx, raw, local, hello, sessionv2.ExpectPeer(domain.PeerIdentityFromWire(s.identity.Address)), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal("genuine original listener TLS/proof refused", err)
	}
	peer, err := proof.Peer()
	if err != nil || peer.Identity != domain.PeerIdentityFromWire(s.identity.Address) {
		t.Fatal("client did not prove exact original R", err)
	}
	seen := 0
	for _, name := range peer.Intro.Capabilities {
		if name == string(domain.CapOverlayFindNodeV1) {
			seen++
		}
	}
	if seen != 1 {
		t.Fatal("original authenticated welcome did not carry exact test selection", peer.Intro.Capabilities)
	}
	wire, err := proof.Conn()
	if err != nil {
		t.Fatal(err)
	}
	clientCtx, clientCancel := context.WithCancel(t.Context())
	c := &ownedBareV2PilotClient{id: domain.PeerIdentityFromWire(id.Address), wire: wire, cancel: clientCancel, done: make(chan struct{}), requests: make(chan protocol.FindNodeRequest, 1), answers: make(chan protocol.FindNodeAnswer, 1), pongs: make(chan struct{}, 1), errors: make(chan error, 1)}
	t.Cleanup(func() { c.stop(t) })
	transferred = true
	go func() {
		defer close(c.done)
		report := func(err error) {
			if clientCtx.Err() == nil {
				select {
				case c.errors <- err:
				default:
				}
			}
		}
		reader := bufio.NewReaderSize(wire, 64*1024)
		for i := 0; i < 128; i++ {
			line, err := reader.ReadSlice('\n')
			if err != nil {
				report(err)
				return
			}
			if request, err := protocol.ParseFindNodeRequest(line); err == nil {
				select {
				case c.requests <- request:
				default:
					report(fmt.Errorf("unexpected second forwarded request"))
					return
				}
				continue
			}
			if answer, err := protocol.ParseFindNodeAnswer(line); err == nil {
				select {
				case c.answers <- answer:
				default:
					report(fmt.Errorf("unexpected second original answer"))
					return
				}
				continue
			}
			var frame protocol.Frame
			if err := json.Unmarshal(line, &frame); err != nil {
				report(err)
				return
			}
			switch frame.Type {
			case "ping":
				pong, err := protocol.MarshalFrameLine(protocol.Frame{Type: "pong"})
				if err != nil {
					report(err)
					return
				}
				if err := c.write([]byte(pong)); err != nil {
					report(err)
					return
				}
			case "pong":
				select {
				case c.pongs <- struct{}{}:
				default:
					report(fmt.Errorf("unexpected second barrier pong"))
					return
				}
			}
		}
		report(fmt.Errorf("bare pilot bounded frame inventory exhausted"))
	}()
	c.barrier(t) // actual authenticated handleConn reached its original read loop
	return c
}

// A pong comes from the original sequential handler after the preceding request
// dispatch returned. It is not proof of a scheduler commit or target admission.
func (c *ownedBareV2PilotClient) barrier(t *testing.T) {
	t.Helper()
	raw, err := protocol.MarshalFrameLine(protocol.Frame{Type: "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.pongs:
	case err := <-c.errors:
		t.Fatal("bare client before handler barrier", err)
	case <-time.After(2 * time.Second):
		t.Fatal("original handler barrier not observed")
	}
}

func bareV2PilotOriginalMembers(t *testing.T, s *Service, want ...domain.PeerIdentity) []findnode.Session {
	t.Helper()
	var result []findnode.Session
	waitFor(t, 2*time.Second, "original listener did not publish exact genuine F1 tuple", func() bool {
		snapshot, err := s.findNodePriorityMembers(t.Context(), findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4})
		if err != nil || len(snapshot.sessions) != len(want) {
			return false
		}
		seen := map[domain.PeerIdentity]bool{}
		ids := map[domain.ConnID]bool{}
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		for _, original := range snapshot.sessions {
			entry := s.connEntryByIDLocked(original.ConnID())
			if original.ConnID() == 0 || ids[original.ConnID()] || seen[original.Identity()] || entry == nil || entry.common == nil || entry.core == nil {
				return false
			}
			life := entry.common
			life.mu.Lock()
			valid := !life.ended && life.token == s.proofToken && life.entry == entry && life.id == original.ConnID() && life.findNode != nil && life.findNode.published && life.findNode.session == original && life.role == sessionv2.RoleListener
			life.mu.Unlock()
			auth := entry.core.Auth()
			if !valid || auth == nil || !auth.Verified {
				return false
			}
			ids[original.ConnID()], seen[original.Identity()] = true, true
		}
		for _, id := range want {
			if !seen[id] {
				return false
			}
		}
		result = snapshot.sessions
		return true
	})
	return result
}

func TestOwnedBareV2PilotStockAdvertisementUnchanged(t *testing.T) {
	cfg, id := readyTestInputs(t)
	ordinary := NewService(cfg, id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	readyCfg, readyID := readyTestInputs(t)
	ready, err := newFindNodeReadyCandidateService(readyCfg, readyID, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ready.runCancel(); ready.closeBackgroundAdmission(); ready.WaitBackground() })
	for _, s := range []*Service{ordinary, ready} {
		want := domain.CapabilityStrings(localCapabilities(s.cfg.EnableMeshRoutingV3, s.localDatagramAdvertise()))
		for _, frame := range []protocol.Frame{s.nodeHelloFrame(), s.welcomeFrame("", "")} {
			if !reflect.DeepEqual(frame.Capabilities, want) {
				t.Fatal("ordinary/existing Ready wire advertisement changed", frame.Capabilities, want)
			}
			assertAdvertisedNames(t, "stock pilot exclusion", frame.Capabilities)
		}
	}
}

func TestOwnedBareV2PilotOriginalMembershipAndForward(t *testing.T) {
	s, address := newOwnedBareV2PilotRelay(t, 2)
	from := dialOwnedBareV2PilotClient(t, s, address, true)
	target := dialOwnedBareV2PilotClient(t, s, address, true)
	if from.id == target.id || from.id == domain.PeerIdentityFromWire(s.identity.Address) || target.id == domain.PeerIdentityFromWire(s.identity.Address) {
		t.Fatal("pilot identities not distinct")
	}
	bareV2PilotOriginalMembers(t, s, from.id, target.id)
	if _, err := s.captureOwnedFindNodeCut(t.Context()); err != nil {
		t.Fatal("original Runtime complete cut refused", err)
	}
	before := s.bridgeCapture.intake.ResourceSnapshot()
	raw, request := ownedReceiveWire(t, target.id)
	if err := from.write([]byte(raw + "\n")); err != nil {
		t.Fatal(err)
	}
	var forwarded protocol.FindNodeRequest
	select {
	case forwarded = <-target.requests:
	case err := <-target.errors:
		t.Fatal("original target wire", err)
	case <-time.After(2 * time.Second):
		t.Fatal("original round/writer did not forward the one pilot request")
	}
	if forwarded.ID != 0 || forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
		t.Fatal("original writer changed target/challenge", forwarded, request)
	}
	waitFor(t, 2*time.Second, "original committed/taken publication absent", func() bool {
		after := s.bridgeCapture.intake.ResourceSnapshot()
		return after.Intake.Committed == before.Intake.Committed+1 && after.Intake.Taken == before.Intake.Taken+1
	})
	// The unique request, exact target wire and original commit/take are separate
	// observations. No T Service, signed answer, delivery or timing SLA is asserted.
	from.barrier(t)
}

func TestOwnedBareV2PilotRefusalsDoNotAdmit(t *testing.T) {
	for _, kind := range []string{"unnegotiated-source", "ended-target", "partial-member-coverage"} {
		t.Run(kind, func(t *testing.T) {
			maxMembers := 2
			if kind == "partial-member-coverage" {
				maxMembers = 1
			}
			s, address := newOwnedBareV2PilotRelay(t, maxMembers)
			from := dialOwnedBareV2PilotClient(t, s, address, kind != "unnegotiated-source")
			target := dialOwnedBareV2PilotClient(t, s, address, true)
			if kind == "unnegotiated-source" {
				bareV2PilotOriginalMembers(t, s, target.id)
			} else {
				bareV2PilotOriginalMembers(t, s, from.id, target.id)
			}
			if kind == "ended-target" {
				target.stop(t)
				bareV2PilotOriginalMembers(t, s, from.id)
			}
			if kind == "partial-member-coverage" {
				observation, err := s.captureOwnedFindNodeObservation(t.Context())
				ownedCaptureAssertEmpty(t, observation, err)
			}
			before := s.bridgeCapture.intake.ResourceSnapshot()
			raw, _ := ownedReceiveWire(t, target.id)
			if err := from.write([]byte(raw + "\n")); err != nil {
				t.Fatal(err)
			}
			from.barrier(t)
			after := s.bridgeCapture.intake.ResourceSnapshot()
			if after.Intake.Offered != before.Intake.Offered || after.Intake.Committed != before.Intake.Committed || after.Intake.Taken != before.Intake.Taken || after.Pending != before.Pending || s.bridgeCapture.intake.OwnedRoundPending() {
				t.Fatal("refused original source/target/coverage admitted partial work", before.Intake, after.Intake)
			}
		})
	}
}

// Finite Source closure, not a proof about arbitrary unsafe/dynamic dataflow.
func bareV2PilotSources(t *testing.T) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			raw, err := os.ReadFile(entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			sources[entry.Name()] = raw
		}
	}
	return sources
}

func bareV2PilotSourceErrors(sources map[string][]byte) []string {
	const field = "ownedFindNodeAdvertiseTest"
	const factory = "newOwnedBareV2PilotRelay"
	var failures []string
	declared, read, written, called, factories := 0, 0, 0, 0, 0
	for path, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{"pilot Source parse"}
		}
		for _, declaration := range file.Decls {
			site := path + ":<package>"
			allowed := map[*ast.Ident]bool{}
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				if site == "find_node_automatic_endpoint_trace_test.go:newOwnedAutomaticTraceService" && fmt.Sprintf("%x", sha256.Sum256(raw[int(fn.Pos())-1:int(fn.End())-1])) != "6f6d1dc32de5e34cdadff8ee53ff87f546f16366688ab5170488df7ed8669426" {
					failures = append(failures, "automatic trace fixed factory changed")
				}
				if fn.Name.Name == factory {
					factories++
					allowed[fn.Name] = true
					if site != "find_node_bare_v2_pilot_test.go:"+factory || fmt.Sprintf("%x", sha256.Sum256(raw[int(fn.Pos())-1:int(fn.End())-1])) != "60e61733b29eaca468c001b5056c2cf8d674184cbbe71d70f3364c3b13b20853" {
						failures = append(failures, "pilot fixed factory changed")
					}
				}
				if site == "datagram_integration.go:localHandshakeCapabilityNames" && fmt.Sprintf("%x", sha256.Sum256(raw[int(fn.Pos())-1:int(fn.End())-1])) != "af61e48321ba810c4904c8cd4f9f0dd32c84af30cfb0d37828734a6ccb06df54" {
					failures = append(failures, "pilot advertisement projection changed")
				}
				if site == "capabilities.go:localHandshakeCapabilities" && fmt.Sprintf("%x", sha256.Sum256(raw[int(fn.Pos())-1:int(fn.End())-1])) != "250f5a5f371b5c3db05e0c1f488c56e3c0308533bca5b194fe0c09d7378815b6" {
					failures = append(failures, "pilot typed selection changed")
				}
				if site == "find_node_capability_selection_test.go:TestOwnedFindNodeHandshakeCapabilitySelection" && fmt.Sprintf("%x", sha256.Sum256(raw[int(fn.Pos())-1:int(fn.End())-1])) != "b176d545f234f948050560c9ed689e83a8671c7410d8d25b52f43a8900683e11" {
					failures = append(failures, "pilot fixed capability regression changed")
				}
			}
			ast.Inspect(declaration, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Field:
					for _, name := range n.Names {
						if name.Name == field {
							declared++
							allowed[name] = true
							kind, ok := n.Type.(*ast.Ident)
							if site != "service.go:<package>" || !ok || kind.Name != "bool" {
								failures = append(failures, "pilot field declaration escaped")
							}
						}
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if selector, ok := lhs.(*ast.SelectorExpr); ok && selector.Sel.Name == field {
							written++
							allowed[selector.Sel] = true
							var value *ast.Ident
							if len(n.Rhs) == 1 {
								value, _ = n.Rhs[0].(*ast.Ident)
							}
							if (site != "find_node_bare_v2_pilot_test.go:"+factory && site != "find_node_automatic_endpoint_trace_test.go:newOwnedAutomaticTraceService" && site != "find_node_capability_selection_test.go:TestOwnedFindNodeHandshakeCapabilitySelection") || len(n.Lhs) != 1 || len(n.Rhs) != 1 || value == nil || value.Name != "true" {
								failures = append(failures, "pilot unclosed setter")
							}
						}
					}
				case *ast.SelectorExpr:
					if n.Sel.Name == field && !allowed[n.Sel] {
						allowed[n.Sel] = true
						read++
						if site != "capabilities.go:localHandshakeCapabilities" {
							failures = append(failures, "pilot unclosed field read")
						}
					}
				case *ast.UnaryExpr:
					if selector, ok := n.X.(*ast.SelectorExpr); ok && selector.Sel.Name == field && n.Op == token.AND {
						failures = append(failures, "pilot field address escape")
					}
				case *ast.CallExpr:
					if name, ok := n.Fun.(*ast.Ident); ok && name.Name == factory {
						allowed[name] = true
						called++
						if site != "find_node_bare_v2_pilot_test.go:TestOwnedBareV2PilotOriginalMembershipAndForward" && site != "find_node_bare_v2_pilot_test.go:TestOwnedBareV2PilotRefusalsDoNotAdmit" {
							failures = append(failures, "pilot factory call escaped")
						}
					}
				case *ast.BasicLit:
					if !strings.HasSuffix(path, "_test.go") && n.Kind == token.STRING {
						value, _ := strconv.Unquote(n.Value)
						if strings.Contains(value, field) {
							failures = append(failures, "pilot reflection/string escape")
						}
					}
				case *ast.Ident:
					if (n.Name == field || n.Name == factory) && !allowed[n] {
						failures = append(failures, "pilot unclosed field/factory reference")
					}
				}
				return true
			})
		}
	}
	if declared != 1 || read != 1 || written != 3 || called != 2 || factories != 1 {
		failures = append(failures, "pilot exact declaration/read/write/call inventory")
	}
	return failures
}

func TestOwnedBareV2PilotClosedTestOnlyAdvertisement(t *testing.T) {
	if failures := bareV2PilotSourceErrors(bareV2PilotSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

func TestOwnedBareV2PilotAdvertisementSourceControls(t *testing.T) {
	baseline := bareV2PilotSources(t)
	if failures := bareV2PilotSourceErrors(baseline); len(failures) != 0 {
		t.Fatal("pilot Source controls baseline", failures)
	}
	for _, tc := range []struct{ name, file, source, cause string }{
		{"production-initializer", "pilot_escape.go", "package node\nvar pilotEscape=&Service{ownedFindNodeAdvertiseTest:true}\n", "unclosed field/factory reference"},
		{"production-setter", "pilot_escape.go", "package node\nfunc pilotEscape(s *Service){s.ownedFindNodeAdvertiseTest=true}\n", "unclosed setter"},
		{"late-test-setter", "pilot_escape_test.go", "package node\nfunc pilotEscape(s *Service){s.ownedFindNodeAdvertiseTest=true}\n", "unclosed setter"},
		{"field-alias", "pilot_escape.go", "package node\nfunc pilotEscape(s *Service)*bool{return &s.ownedFindNodeAdvertiseTest}\n", "field address escape"},
		{"reflection-name", "pilot_escape.go", "package node\nfunc pilotEscape(){_=reflect.ValueOf(Service{}).FieldByName(\"ownedFindNodeAdvertiseTest\")}\n", "reflection/string escape"},
		{"factory-value-escape", "pilot_escape_test.go", "package node\nvar pilotEscape=newOwnedBareV2PilotRelay\n", "unclosed field/factory reference"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for path, raw := range baseline {
				sources[path] = bytes.Clone(raw)
			}
			if _, exists := sources[tc.file]; exists {
				t.Fatal("pilot Source control path already exists")
			}
			sources[tc.file] = []byte(tc.source)
			found := false
			for _, failure := range bareV2PilotSourceErrors(sources) {
				found = found || strings.Contains(failure, tc.cause)
			}
			if !found {
				t.Fatal("test-only capability escape lacked exact Source refusal", tc.name)
			}
		})
	}
	for _, tc := range []struct{ name, before, after string }{
		{"ungated-advertisement", "if s.ownedFindNodeAdvertiseTest {", "if true {"},
		{"extra-responder", "caps = append(caps, domain.CapOverlayFindNodeV1)", "caps = append(caps, domain.CapOverlayResponderV1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for path, raw := range baseline {
				sources[path] = bytes.Clone(raw)
			}
			raw := string(sources["capabilities.go"])
			if strings.Count(raw, tc.before) != 1 {
				t.Fatal("pilot exact projection control anchor")
			}
			sources["capabilities.go"] = []byte(strings.Replace(raw, tc.before, tc.after, 1))
			found := false
			for _, failure := range bareV2PilotSourceErrors(sources) {
				found = found || failure == "pilot typed selection changed"
			}
			if !found {
				t.Fatal("pilot wire projection escaped Source closure", tc.name)
			}
		})
	}
}
