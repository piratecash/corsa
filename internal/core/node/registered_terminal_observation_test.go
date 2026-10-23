package node

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func k10TestService(observed bool) *Service {
	s := newRegistryTestService()
	if observed {
		s.terminalObservation = newK10TerminalSource([16]byte{1})
	}
	return s
}
func k10Attach(t *testing.T, s *Service, id uint64, dir netcore.Direction) (net.Conn, *connEntry) {
	t.Helper()
	conn := newNoIOConn(t, &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 64646})
	core := newTestNetCore(t, domain.ConnID(id), conn, dir, netcore.Options{Address: "same-peer:64646"})
	s.peerMu.Lock()
	if dir == netcore.Inbound {
		s.registerInboundConnLocked(conn, core, nil, nil)
	} else {
		s.attachOutboundCoreLocked(conn, core)
	}
	entry := s.conns[domain.ConnID(id)]
	s.peerMu.Unlock()
	return conn, entry
}
func k10Remove(s *Service, conn net.Conn, entry *connEntry) {
	entry.core.Close() // same mandatory writer join before original registry remove
	s.peerMu.Lock()
	s.unregisterConnLocked(conn)
	s.peerMu.Unlock()
}
func k10Session(s *Service, conn net.Conn, entry *connEntry) *peerSession {
	return &peerSession{address: "same-peer:64646", connID: entry.core.ConnID(), conn: conn, netCore: entry.core, terminal: entry.terminal, onClose: func() { s.peerMu.Lock(); s.unregisterConnLocked(conn); s.peerMu.Unlock() }}
}
func k10Invariant(t *testing.T, s k10TerminalSnapshot) {
	t.Helper()
	var buckets uint64
	for _, direction := range s.buckets {
		for _, kind := range direction {
			for _, count := range kind {
				buckets += count
			}
		}
	}
	if s.registered != s.active+s.removed || s.removed != s.classified+s.pending+s.missing || buckets != s.classified {
		t.Fatal("terminal cut is not coherent", s)
	}
}
func k10Wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("K10 controlled owner missed boundary")
	}
}
func k10Owned(t *testing.T, releases ...func()) func(<-chan struct{}) {
	t.Helper()
	var owners []<-chan struct{}
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
		deadline := time.Now().Add(3 * time.Second)
		for i, done := range owners {
			select {
			case <-done:
				continue
			default:
			}
			left := time.Until(deadline)
			if left <= 0 {
				t.Errorf("K10 owned worker %d failed cleanup join", i)
				continue
			}
			timer := time.NewTimer(left)
			select {
			case <-done:
			case <-timer.C:
				t.Errorf("K10 owned worker %d failed cleanup join", i)
			}
			timer.Stop()
		}
	})
	return func(done <-chan struct{}) { owners = append(owners, done) }
}

func TestK10RegisteredPopulation(t *testing.T) {
	t.Run("two-directions-same-address-not-last-ref", func(t *testing.T) {
		s := k10TestService(true)
		a, ea := k10Attach(t, s, 9, netcore.Inbound)
		b, eb := k10Attach(t, s, 3, netcore.Outbound)
		if ea.terminal == eb.terminal || ea.terminal.serial == eb.terminal.serial {
			t.Fatal("registered instances collapsed by address or decreasing ConnID")
		}
		ea.terminal.finish(k10ReportedOutcome(io.EOF), false)
		k10Remove(s, a, ea)
		v := s.terminalObservation.snapshot()
		k10Invariant(t, v)
		if v.status != "COMPLETE" || v.removed != 1 || v.active != 1 || v.classified != 1 {
			t.Fatal("first registered close lost behind another address reference", v)
		}
		eb.terminal.finish(k10ReportedOutcome(protocol.ErrRead), false)
		k10Remove(s, b, eb)
		v = s.terminalObservation.snapshot()
		k10Invariant(t, v)
		if v.removed != 2 || v.buckets[0][k10OwnerEOF][0] != 1 || v.buckets[1][k10OwnerError][18] != 1 {
			t.Fatal("direction/original outcome histogram lost", v)
		}
	})
	t.Run("unregistered-attempts-excluded", func(t *testing.T) {
		s := k10TestService(true)
		conn := newNoIOConn(t, &net.TCPAddr{})
		_ = conn.Close()
		s.peerMu.Lock()
		s.unregisterConnLocked(conn)
		s.peerMu.Unlock()
		if v := s.terminalObservation.snapshot(); v.registered != 0 || v.removed != 0 || v.status != "COMPLETE" {
			t.Fatal("raw attempt counted as registered terminal", v)
		}
	})
	t.Run("registered-pre-serve-failure", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		_ = ps.Close()
		if v := s.terminalObservation.snapshot(); v.status != "IN_FLIGHT" {
			t.Fatal("registered setup close fabricated a finalized cause", v)
		}
		ps.k10FinishReported(protocol.ErrAuthRequired)
		if v := s.terminalObservation.snapshot(); v.removed != 1 || v.classified != 1 || v.status != "COMPLETE" {
			t.Fatal("pre-serve owner outcome lost", v)
		}
	})
}

func TestK10TerminalOwnerJoin(t *testing.T) {
	t.Run("both-join-orders-and-idempotence", func(t *testing.T) {
		for _, before := range []bool{true, false} {
			s := k10TestService(true)
			conn, entry := k10Attach(t, s, 1, netcore.Inbound)
			if before {
				entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
			}
			k10Remove(s, conn, entry)
			if !before {
				if v := s.terminalObservation.snapshot(); v.pending != 1 || v.status != "IN_FLIGHT" {
					t.Fatal("remove without outcome became complete", v)
				}
				entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
			}
			entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
			k10Remove(s, conn, entry)
			v := s.terminalObservation.snapshot()
			k10Invariant(t, v)
			if v.removed != 1 || v.classified != 1 || v.pending != 0 || v.status != "COMPLETE" {
				t.Fatal("terminal join published other than exactly once", before, v)
			}
		}
	})
	t.Run("late-old-owner-never-steals-replacement", func(t *testing.T) {
		s := k10TestService(true)
		a, ea := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, a, ea)
		removed, gate, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		track := k10Owned(t, release)
		track(done)
		go func() { defer close(done); _ = ps.Close(); close(removed); <-gate; ps.k10FinishReported(io.EOF) }()
		k10Wait(t, removed)
		b, eb := k10Attach(t, s, 2, netcore.Inbound)
		eb.terminal.finish(k10ReportedOutcome(protocol.ErrRead), false)
		k10Remove(s, b, eb)
		v := s.terminalObservation.snapshot()
		if v.pending != 1 || v.removed != 2 || v.classified != 1 || v.status != "IN_FLIGHT" {
			t.Fatal("newer completion hid original pending owner", v)
		}
		release()
		k10Wait(t, done)
		v = s.terminalObservation.snapshot()
		k10Invariant(t, v)
		if ea.terminal.terminalSerial != 1 || eb.terminal.terminalSerial != 2 || v.classified != 2 || v.status != "COMPLETE" {
			t.Fatal("reversed outcome completion changed removal sequence", v)
		}
		c, ec := k10Attach(t, s, 3, netcore.Inbound)
		ea.terminal.finish(k10ReportedOutcome(io.EOF), false)
		if got := s.k10ForRegistered(c); got != ec.terminal || s.terminalObservation.snapshot().active != 1 {
			t.Fatal("old cleanup touched current replacement")
		}
		ec.terminal.finish(k10ReportedOutcome(io.EOF), false)
		k10Remove(s, c, ec)
	})
	t.Run("missing-defer-and-coarse-code-are-not-clean", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		func() { defer ps.k10MissingIfUnfinished(); _ = ps.Close() }()
		v := s.terminalObservation.snapshot()
		k10Invariant(t, v)
		if v.status != "UNKNOWN" || v.missing != 1 || v.classified != 0 || v.reason != "missing-original-outcome" {
			t.Fatal("missing owner deferred exit fabricated clean cause", v)
		}
		s2 := k10TestService(true)
		conn2, e2 := k10Attach(t, s2, 1, netcore.Inbound)
		e2.terminal.finish(k10Outcome{kind: k10CommandStopped}, false)
		k10Remove(s2, conn2, e2)
		v = s2.terminalObservation.snapshot()
		if v.status != "COMPLETE" || v.codeStatus != "UNKNOWN" || v.coarse != 1 {
			t.Fatal("command false claimed complete protocol cause", v)
		}
	})
}

func TestK10OriginalCloseWinner(t *testing.T) {
	t.Run("local-before-returned-error", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		_ = ps.closeAs(peerSessionClosedByLocalEviction)
		ps.k10FinishReported(protocol.ErrRead)
		_ = ps.Close()
		v := s.terminalObservation.snapshot()
		k10Invariant(t, v)
		if v.buckets[1][k10LocalEviction][0] != 1 || v.classified != 1 || v.status != "COMPLETE" || !entry.terminal.reported {
			t.Fatal("induced owner error overwrote original local closer", v)
		}
	})
	t.Run("owner-before-later-local", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		_ = ps.Close()
		ps.k10FinishReported(protocol.ErrRead)
		_ = ps.closeAs(peerSessionClosedByLocalEviction)
		v := s.terminalObservation.snapshot()
		if v.buckets[1][k10OwnerError][18] != 1 || v.buckets[1][k10LocalEviction][0] != 0 || v.status != "COMPLETE" {
			t.Fatal("later local request rewrote original owner winner", v)
		}
	})
	t.Run("discarded-CM-result-no-serve-owner", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		cm := &ConnectionManager{}
		cm.settleUndeliveredSlotEvent(DialSucceeded{Session: ps})
		v := s.terminalObservation.snapshot()
		if v.status != "COMPLETE" || v.pending != 0 || v.buckets[1][k10LocalEviction][0] != 1 {
			t.Fatal("discarded registered dial result awaited nonexistent serve owner", v)
		}
	})
	t.Run("generic-network-request-not-a-cause", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Inbound)
		b := &networkBridge{svc: s}
		if err := b.Close(context.Background(), entry.core.ConnID()); err != nil {
			t.Fatal(err)
		}
		k10Remove(s, conn, entry)
		v := s.terminalObservation.snapshot()
		if v.status != "IN_FLIGHT" || entry.terminal.requests&k10GenericClose == 0 {
			t.Fatal("generic Network.Close fabricated terminal cause", v)
		}
		entry.terminal.finish(k10ReportedOutcome(protocol.ErrRead), false)
		v = s.terminalObservation.snapshot()
		if v.buckets[0][k10LocalEviction][0] != 0 || v.classified != 1 {
			t.Fatal("read after local request claimed induced cause", v)
		}
	})
	t.Run("legacy-and-joined-error-schema", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		_ = ps.closeAs(peerSessionClosedByLegacyRefusal)
		ps.k10FinishReported(protocol.ErrRead)
		v := s.terminalObservation.snapshot()
		if v.buckets[1][k10LegacyRefusal][0] != 1 {
			t.Fatal("legacy refusal lost original winner", v)
		}
		outcome := k10ReportedOutcome(errors.Join(protocol.ErrFrameTooLarge, protocol.ErrRead))
		if outcome.code != 18 || outcome.kind != k10OwnerError {
			t.Fatal("existing ErrorCode join precedence changed", outcome)
		}
	})
}

func TestK10FrozenPrefix(t *testing.T) {
	for _, which := range []string{"register", "remove", "publication-version", "version", "zero-id", "issuer-wrap", "collision", "foreign-owner", "contradictory-final", "invalid-instance"} {
		t.Run(which, func(t *testing.T) {
			s := k10TestService(true)
			o := s.terminalObservation
			if which == "invalid-instance" {
				o = newK10TerminalSource([16]byte{})
				if o.snapshot().status != "UNKNOWN" {
					t.Fatal("zero instance certified")
				}
				return
			}
			conn, entry := k10Attach(t, s, 1, netcore.Inbound)
			switch which {
			case "register":
				o.mu.Lock()
				o.registered = k10Max
				o.active = k10Max
				o.mu.Unlock()
				_, _ = k10Attach(t, s, 2, netcore.Inbound)
			case "remove":
				o.mu.Lock()
				o.terminalSerial = k10Max
				o.mu.Unlock()
				k10Remove(s, conn, entry)
			case "publication-version":
				entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
				o.mu.Lock()
				o.version = k10Max - 1
				o.mu.Unlock()
				k10Remove(s, conn, entry)
			case "version":
				o.mu.Lock()
				o.version = k10Max
				o.mu.Unlock()
				entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
				k10Remove(s, conn, entry)
			case "zero-id":
				_, _ = k10Attach(t, s, 0, netcore.Inbound)
			case "issuer-wrap":
				s.peerMu.Lock()
				s.connIDCounter = k10Max
				s.nextConnIDLocked()
				s.peerMu.Unlock()
			case "collision":
				_, _ = k10Attach(t, s, 1, netcore.Inbound)
			case "foreign-owner":
				entry.terminal.remove(&connEntry{core: entry.core})
			case "contradictory-final":
				entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
				entry.terminal.finish(k10ReportedOutcome(protocol.ErrRead), false)
			}
			before := o.snapshot()
			if before.status != "UNKNOWN" {
				t.Fatal("counter/identity fault certified continuity", which, before)
			}
			entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
			k10Remove(s, conn, entry)
			_, _ = k10Attach(t, s, 100, netcore.Outbound)
			if after := o.snapshot(); after != before {
				t.Fatal("frozen prefix mutated after source failure", which, before, after)
			}
			k10Invariant(t, before)
		})
	}
}

type k10ReentrantError struct {
	s      *Service
	called *int
	t      *testing.T
}

func (e k10ReentrantError) Error() string { return "synthetic uncoded owner error" }
func (e k10ReentrantError) Is(error) bool {
	*e.called++
	if !e.s.peerMu.TryLock() {
		e.t.Error("normalization held application lock")
	} else {
		e.s.peerMu.Unlock()
	}
	if o := e.s.terminalObservation; o != nil {
		if !o.mu.TryLock() {
			e.t.Error("normalization held observer leaf")
		} else {
			o.mu.Unlock()
		}
	}
	return false
}
func TestK10OrdinaryAndNormalizer(t *testing.T) {
	t.Run("nil-binding-does-not-normalize", func(t *testing.T) {
		s := k10TestService(false)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		calls := 0
		ps.k10FinishReported(k10ReentrantError{s: s, called: &calls, t: t})
		_ = ps.Close()
		if calls != 0 || entry.terminal != nil || s.terminalObservation.snapshot().status != "INACTIVE" {
			t.Fatal("ordinary nil path activated observation", calls)
		}
	})
	t.Run("normalizer-outside-locks", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Outbound)
		ps := k10Session(s, conn, entry)
		calls := 0
		_ = ps.Close()
		ps.k10FinishReported(k10ReentrantError{s: s, called: &calls, t: t})
		v := s.terminalObservation.snapshot()
		if calls == 0 || v.classified != 1 || v.codeStatus != "UNKNOWN" {
			t.Fatal("uncoded error became complete code evidence", calls, v)
		}
	})
	t.Run("request-flags-and-generation-not-cause", func(t *testing.T) {
		s := k10TestService(true)
		conn, entry := k10Attach(t, s, 1, netcore.Inbound)
		entry.terminal.bindGeneration(9)
		s.k10Request(entry.core.ConnID(), k10ShutdownRequest)
		s.k10Request(entry.core.ConnID(), k10HandlerCleanup)
		entry.terminal.finish(k10ReportedOutcome(io.EOF), false)
		k10Remove(s, conn, entry)
		v := s.terminalObservation.snapshot()
		if entry.terminal.generation != 9 || v.buckets[0][k10OwnerEOF][0] != 1 {
			t.Fatal("request metadata changed observed owner outcome", v)
		}
	})
}

func TestK10ConcurrentCuts(t *testing.T) {
	s := k10TestService(true)
	conn, entry := k10Attach(t, s, 1, netcore.Outbound)
	ps := k10Session(s, conn, entry)
	start, stop := make(chan struct{}), make(chan struct{})
	releaseStart := sync.OnceFunc(func() { close(start) })
	releaseStop := sync.OnceFunc(func() { close(stop) })
	track := k10Owned(t, releaseStart, releaseStop)
	var owners []<-chan struct{}
	for i := 0; i < 4; i++ {
		done := make(chan struct{})
		track(done)
		owners = append(owners, done)
		go func() {
			defer close(done)
			<-start
			for j := 0; j < 32; j++ {
				select {
				case <-stop:
					return
				default:
				}
				v := s.terminalObservation.snapshot()
				var buckets uint64
				for _, direction := range v.buckets {
					for _, kind := range direction {
						for _, count := range kind {
							buckets += count
						}
					}
				}
				if v.registered != v.active+v.removed || v.removed != v.classified+v.pending+v.missing || buckets != v.classified {
					t.Error("concurrent cut split original count tuple", v)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	track(done)
	owners = append(owners, done)
	go func() {
		defer close(done)
		<-start
		_ = ps.Close()
		ps.k10FinishReported(protocol.ErrRead)
		ps.k10FinishReported(protocol.ErrRead)
	}()
	releaseStart()
	for _, done := range owners {
		k10Wait(t, done)
	}
	v := s.terminalObservation.snapshot()
	k10Invariant(t, v)
	if v.status != "COMPLETE" || v.removed != 1 || v.classified != 1 {
		t.Fatal("concurrent original owner/cuts lost terminal", v)
	}
}
