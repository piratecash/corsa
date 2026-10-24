//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLinuxMemberBindPassiveDiagnostic(t *testing.T) {
	t.Run("ready-lying-diagnostic-cannot-fix-nonce", func(t *testing.T) {
		facts := linuxRuntimeTestFacts(t)
		request := linuxMemberBindRequest{Nonce: LaunchNonce{1}, Ordinal: 1}
		reply := linuxMemberBindReply{Nonce: LaunchNonce{2}, Code: linuxResultReady, Receipt: StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}, Provenance: facts.Provenance, Diagnostic: &linuxBindHelperDiagnostic{Stage: "ready", PreconditionsKnown: true, Planned: true, Membership: true, Unbound: true, OrdinalKnown: true, OrdinalEqual: true, BindingKnown: true, BindingEqual: true}}
		mask, checked := linuxBindReplyMismatch(linuxPacket{kind: linuxMemberBindResult, id: 7, ordinal: 1}, reply, request, 7, facts.Provenance)
		if mask != linuxBindMismatchNonce || checked {
			t.Fatal("helper diagnostic weakened original nonce guard or short circuit", mask, checked)
		}
	})
	t.Run("unavailable-success-diagnostic-is-still-refusal", func(t *testing.T) {
		facts := linuxRuntimeTestFacts(t)
		request := linuxMemberBindRequest{Nonce: LaunchNonce{1}, Ordinal: 1}
		reply := linuxMemberBindReply{Nonce: request.Nonce, Code: linuxResultUnavailable, Receipt: StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}, Provenance: facts.Provenance, Diagnostic: &linuxBindHelperDiagnostic{Stage: "ready", BindingKnown: true, BindingEqual: true}}
		mask, checked := linuxBindReplyMismatch(linuxPacket{kind: linuxMemberBindResult, id: 7, ordinal: 1}, reply, request, 7, facts.Provenance)
		if mask != linuxBindMismatchCode || checked {
			t.Fatal("success-looking diagnostic authorized unavailable reply", mask, checked)
		}
	})
	t.Run("public-check-remains-required", func(t *testing.T) {
		facts := linuxRuntimeTestFacts(t)
		request := linuxMemberBindRequest{Nonce: LaunchNonce{1}, Ordinal: 1}
		reply := linuxMemberBindReply{Nonce: request.Nonce, Code: linuxResultReady, Receipt: StartReceipt{Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}, Provenance: facts.Provenance}
		mask, checked := linuxBindReplyMismatch(linuxPacket{kind: linuxMemberBindResult, id: 7, ordinal: 1}, reply, request, 7, facts.Provenance)
		if mask != linuxBindMismatchPublic || !checked {
			t.Fatal("public validation bypassed by diagnostic split", mask, checked)
		}
	})
	t.Run("successful-reply-wire-unchanged", func(t *testing.T) {
		facts := linuxRuntimeTestFacts(t)
		reply := linuxMemberBindReply{Nonce: LaunchNonce{1}, Code: linuxResultReady, Receipt: StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}, Provenance: facts.Provenance}
		legacy := struct {
			Nonce      LaunchNonce
			Code       linuxResultCode
			Receipt    StartReceipt
			Provenance LinuxProcessProvenance
		}{reply.Nonce, reply.Code, reply.Receipt, reply.Provenance}
		raw, err := encodeLinuxPacket(linuxMemberBindResult, 7, 1, reply)
		if err != nil {
			t.Fatal(err)
		}
		old, err := encodeLinuxPacket(linuxMemberBindResult, 7, 1, legacy)
		if err != nil || !bytes.Equal(raw, old) {
			t.Fatal("nil diagnostic changed successful wire", err)
		}
		packet, err := decodeLinuxPacket(raw)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeLinuxBody[linuxMemberBindReply](packet)
		if err != nil || decoded.Diagnostic != nil {
			t.Fatal("absent diagnostic was not UNKNOWN", err)
		}
		var object map[string]any
		if json.Unmarshal(packet.body, &object) != nil {
			t.Fatal("fixture JSON")
		}
		object["diagnostic"] = map[string]any{}
		packet.body, err = json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeLinuxBody[linuxMemberBindReply](packet); !errors.Is(err, ErrLinuxOwnerProtocol) {
			t.Fatal("wrong-case diagnostic bypassed closed schema", err)
		}
	})
	t.Run("helper-precondition-is-closed-testimony", func(t *testing.T) {
		first, second := linuxTestPacketPair(t)
		r := &linuxHelperRuntime{work: first, init: linuxInit{Nonce: LaunchNonce{1}}}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		if err := r.writeMemberBind(ctx, linuxPacket{id: 7}, linuxMemberBindRequest{Ordinal: 1}); err != nil {
			t.Fatal(err)
		}
		deadline, _ := ctx.Deadline()
		packet, err := second.read(deadline)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := decodeLinuxBody[linuxMemberBindReply](packet)
		if err != nil || reply.Code != linuxResultUnavailable || reply.Diagnostic == nil {
			t.Fatal("original precondition refusal missing diagnostic", err)
		}
		d := reply.Diagnostic
		if d.Stage != "precondition" || !d.PreconditionsKnown || d.Planned || d.MembershipKnown || d.UnboundKnown || d.OrdinalKnown || d.BindingKnown || d.StatusKnown {
			t.Fatal("unevaluated precondition/readiness became known", d)
		}
	})
	t.Run("helper-readiness-retains-last-existing-ban-failure", func(t *testing.T) {
		status, _ := linuxRuntimeTestTuple(t)
		var statusCalls, banCalls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var body struct {
				Command string `json:"command"`
			}
			if json.NewDecoder(req.Body).Decode(&body) != nil {
				t.Error("request JSON")
				w.WriteHeader(500)
				return
			}
			if body.Command == "getNodeStatus" {
				statusCalls.Add(1)
				_ = json.NewEncoder(w).Encode(status)
				return
			}
			if body.Command == "listBanned" {
				banCalls.Add(1)
				w.WriteHeader(503)
				return
			}
			t.Error("unexpected extra RPC", body.Command)
			w.WriteHeader(500)
		}))
		t.Cleanup(server.Close)
		credentials, err := NewCredentials()
		if err != nil {
			t.Fatal(err)
		}
		client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		done := make(chan struct{})
		close(done)
		process := &Process{rpc: client, done: done, exit: ProcessExitResult{Kind: WaitNormal, Code: 1, WaitFailed: true}}
		first, second := linuxTestPacketPair(t)
		r := &linuxHelperRuntime{work: first, process: process, membership: &linuxMemberMembershipView{}, init: linuxInit{Nonce: LaunchNonce{1}, Planned: &linuxPlannedInit{Ordinal: 1}, Config: ChildConfig{Listener: true}}}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		if err := r.writeMemberBind(ctx, linuxPacket{id: 7}, linuxMemberBindRequest{Ordinal: 1}); err != nil {
			t.Fatal(err)
		}
		deadline, _ := ctx.Deadline()
		packet, err := second.read(deadline)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := decodeLinuxBody[linuxMemberBindReply](packet)
		if err != nil || reply.Code != linuxResultUnavailable || reply.Diagnostic == nil {
			t.Fatal("readiness refusal changed", err)
		}
		d := reply.Diagnostic
		if d.Stage != "await-ready" || d.ReadinessStage != "ban" || d.ReadinessTerminal != "child-exit" || !d.StatusKnown || !d.StatusOK || !d.BanKnown || d.BanOK || d.ListenerKnown || d.BindingKnown || statusCalls.Load() != 1 || banCalls.Load() != 1 || !strings.Contains(d.Error, ErrFamilyUnavailable.Error()) {
			t.Fatal("last existing readiness failure or call count lost", d, statusCalls.Load(), banCalls.Load())
		}
	})
	t.Run("readiness-preserves-source-failures-across-terminal-status", func(t *testing.T) {
		status, _ := linuxRuntimeTestTuple(t)
		var statusCalls, banCalls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var body struct {
				Command string `json:"command"`
			}
			if json.NewDecoder(req.Body).Decode(&body) != nil {
				t.Error("request JSON")
				w.WriteHeader(500)
				return
			}
			switch body.Command {
			case "getNodeStatus":
				switch statusCalls.Add(1) {
				case 1:
					w.WriteHeader(503)
				case 2:
					_ = json.NewEncoder(w).Encode(status)
				case 3:
					<-req.Context().Done()
				default:
					t.Error("extra Status poll")
					w.WriteHeader(500)
				}
			case "listBanned":
				banCalls.Add(1)
				w.WriteHeader(403)
			default:
				t.Error("unexpected extra RPC", body.Command)
				w.WriteHeader(500)
			}
		}))
		t.Cleanup(server.Close)
		credentials, err := NewCredentials()
		if err != nil {
			t.Fatal(err)
		}
		client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		process := &Process{rpc: client, done: make(chan struct{})}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var d processReadinessDiagnostic
		err = process.awaitReady(ctx, true, "", nil, &d)
		if !errors.Is(err, context.DeadlineExceeded) || d.terminal != "context" || d.stage != "status" || d.poll < 3 || statusCalls.Load() != 3 || banCalls.Load() != 1 {
			t.Fatal("scripted existing readiness calls/terminal changed", err, d, statusCalls.Load(), banCalls.Load())
		}
		if !d.statusKnown || d.statusOK || d.statusPoll != d.poll || !errors.Is(d.statusErr, context.DeadlineExceeded) || !d.banKnown || d.banOK || d.banPoll != 2 || !errors.Is(d.banErr, ErrRPCAccess) || d.listenerKnown {
			t.Fatal("latest per-source observations lost or made coherent", d)
		}
		if !d.statusFailure.known || d.statusFailure.poll != 1 || !errors.Is(d.statusFailure.err, ErrFamilyUnavailable) || !d.banFailure.known || d.banFailure.poll != 2 || !errors.Is(d.banFailure.err, ErrRPCAccess) || d.listenerFailure.known {
			t.Fatal("terminal context or later success erased historical source failure", d)
		}
		wire := linuxBindHelperDiagnostic{Stage: "await-ready", ReadinessPoll: d.poll, StatusPoll: d.statusPoll, BanPoll: d.banPoll, StatusLastNonContextFailure: linuxBindReadinessFailureValue(d.statusFailure), BanLastNonContextFailure: linuxBindReadinessFailureValue(d.banFailure)}
		text := linuxBindHelperContext(wire)
		if !strings.Contains(text, "observations_noncoherent=true") || !strings.Contains(text, ErrFamilyUnavailable.Error()) || !strings.Contains(text, ErrRPCAccess.Error()) {
			t.Fatal("historical source testimony lost from bounded context", text)
		}
	})
	t.Run("namespace-and-public-collapse-retain-stage", func(t *testing.T) {
		r := &linuxHelperRuntime{sampler: &LinuxProcessSampler{state: processSamplerClosed}}
		d := linuxBindRuntimeDiagnostic{stage: "namespace-before"}
		if _, err := r.readTargetRuntimeNS(t.Context(), &d); !errors.Is(err, ErrProcessObservation) || d.stage != "namespace-before" || d.detail != "ns-sampler-state" || !errors.Is(d.err, ErrProcessObservation) {
			t.Fatal("namespace collapse lost original stage", d, err)
		}
		status, contact := linuxRuntimeTestTuple(t)
		after := status
		after.StartedAt = after.StartedAt.Add(time.Second)
		d = linuxBindRuntimeDiagnostic{stage: "public"}
		if _, err := linuxRuntimePublic(status, after, []protocol.ContactFrame{contact}, &d); !errors.Is(err, ErrProcessObservation) || d.detail != "public-status-pair" {
			t.Fatal("joint public collapse lost exact stage", d, err)
		}
	})
	t.Run("bounded-context-keeps-known-fields-first", func(t *testing.T) {
		d := linuxBindHelperDiagnostic{Stage: "await-ready", Error: strings.Repeat("x", 4000), PreconditionsKnown: true, ReadinessStage: "status", ReadinessTerminal: "context", StatusKnown: true}
		text := linuxBindHelperContext(d)
		if len(text) > 2048 || !strings.Contains(text, "status_known=true") || !strings.Contains(text, "readiness_terminal=\"context\"") {
			t.Fatal("bounded error hid diagnostic stage/known bits", text)
		}
		escaped := plannedMemberBindContext(linuxMemberBindDiagnostic{known: true, phase: "reply-validate", replyKnown: true, replyCode: linuxResultUnavailable, mismatch: linuxBindMismatchCode, helperKnown: true, helper: linuxBindHelperDiagnostic{Stage: "await-ready", Error: strings.Repeat("\x00\n\"\\", 2048)}})
		if len(escaped) > 2048 || !strings.Contains(escaped, "phase=reply-validate reply_known=true") || !strings.Contains(escaped, "mismatch_bits=0x10") || !strings.Contains(escaped, "helper_known=true") {
			t.Fatal("final quoted owner context exceeded bound or hid owner facts", len(escaped))
		}
		if plannedMemberBindContext(linuxMemberBindDiagnostic{}) != "UNKNOWN" {
			t.Fatal("absent owner result became known")
		}
		if text := plannedMemberBindContext(linuxMemberBindDiagnostic{known: true, phase: "read"}); !strings.Contains(text, "reply_code=\"UNKNOWN\"") || !strings.Contains(text, "helper=\"UNKNOWN\"") {
			t.Fatal("missing reply/helper implied zero/Ready", text)
		}
	})
}
