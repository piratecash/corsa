package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func newDialWelcomeResponder(t *testing.T) *identity.Identity {
	t.Helper()
	responder, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return responder
}

func listenForDialWelcome(t *testing.T) (net.Listener, domain.PeerAddress) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln, domain.PeerAddress(ln.Addr().String())
}

type recordedDial struct {
	received []string
	err      error
}

// serveV2Intro accepts one dial on ln as a v2 listener proving responder and
// announcing intro, then records the type of every frame the dialler sends
// inside the proven session until it closes. A v2 dial writes nothing outside
// the handshake before its first command, so an empty record means the dialler
// stopped at the intro. push_notice gets the answer a node gives it, so a
// dialler that does go on is not kept waiting for its deadline.
func serveV2Intro(ln net.Listener, responder *identity.Identity, intro protocol.Frame) <-chan recordedDial {
	done := make(chan recordedDial, 1)
	go func() {
		received, err := recordFramesAfterV2Intro(ln, responder, intro)
		done <- recordedDial{received: received, err: err}
	}()
	return done
}

func recordFramesAfterV2Intro(ln net.Listener, responder *identity.Identity, intro protocol.Frame) ([]string, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	wire, err := acceptSecureTestWire(conn, responder, intro)
	if err != nil {
		return nil, err
	}
	defer func() { _ = wire.Close() }()
	reader := bufio.NewReader(wire)
	received := []string{}
	for {
		frame, err := readTestFrameLine(reader)
		if err != nil {
			return received, nil
		}
		received = append(received, frame.Type)
		if err := writeTestFrameLine(wire, protocol.Frame{Type: "ok"}); err != nil {
			return received, nil
		}
	}
}

// acceptSecureTestWire completes a real handshake before reading test traffic.
func acceptSecureTestWire(conn net.Conn, responder *identity.Identity, intro protocol.Frame) (net.Conn, error) {
	local, err := sessionv2.NewLocal(responder, domain.NetworkID(networkName))
	if err != nil {
		return nil, err
	}
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		return nil, err
	}
	session, err := sessionv2.Accept(context.Background(), conn, local, intro, certificates, sessionv2.DefaultTimeouts)
	if err != nil {
		return nil, err
	}
	return session.Conn()
}

func currentSecureIntro() protocol.Frame {
	return protocol.Frame{Version: config.ProtocolVersion, MinimumProtocolVersion: config.MinimumProtocolVersion}
}

// v2IntroBelowFloor is the v2 listener intro of a node below this node's
// MinimumProtocolVersion; the identity fields are stamped by the handshake.
func v2IntroBelowFloor() protocol.Frame {
	return protocol.Frame{
		Version:                config.MinimumProtocolVersion - 1,
		MinimumProtocolVersion: config.MinimumProtocolVersion - 1,
		ClientVersion:          "0.0.1-below-floor",
	}
}

func awaitNothingAfterV2Intro(t *testing.T, done <-chan recordedDial) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("v2 listener: %v", got.err)
		}
		if len(got.received) != 0 {
			t.Fatalf("the listener received %v inside the proven session, want nothing", got.received)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dial did not end")
	}
}

// The secure listener sees EOF after the dialler recognizes its own proven
// identity, because the dialler ends the exchange before sending its proof.
func awaitSelfDialRefusal(t *testing.T, done <-chan recordedDial) {
	t.Helper()
	select {
	case got := <-done:
		if !errors.Is(got.err, sessionv2.ErrIntro) || len(got.received) != 0 {
			t.Fatalf("self dial: %+v, want EOF before application frames", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("self dial did not stop")
	}
}

func assertNotRecordedAsOutboundSuccess(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if pm := svc.persistedMeta[address]; pm != nil && pm.TrustedAdvertiseSource == trustedAdvertiseSourceOutbound {
		t.Fatal("a refused dial was recorded as an outbound auth success")
	}
}

func TestSyncPeer_V2IntroBelowFloor_SendsNothingAndPaces(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), v2IntroBelowFloor())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if imported := svc.syncPeer(ctx, address, true); imported != 0 {
		t.Fatalf("syncPeer imported %d contacts from a listener below the floor", imported)
	}
	awaitNothingAfterV2Intro(t, done)
	assertOnlyPaced(t, svc, address)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

func TestSendNoticeToPeer_V2IntroBelowFloor_WritesNothingAndPaces(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), v2IntroBelowFloor())

	svc.sendNoticeToPeer(address, time.Minute, "gated-notice-ciphertext")
	awaitNothingAfterV2Intro(t, done)
	assertOnlyPaced(t, svc, address)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

// A notice to an address under a running ban is not worth a socket: the
// connection manager would not dial it either, and the dial could only end
// at the welcome again.
func TestSendNoticeToPeer_AddressUnderBan_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), currentSecureIntro())
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	svc.peerMu.Lock()
	svc.ensurePeerHealthLocked(address).BannedUntil = time.Now().Add(time.Hour)
	svc.peerMu.Unlock()
	svc.sendNoticeToPeer(address, time.Minute, "banned-notice-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a banned address was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}

// banSiblingPortForIncompatibleVersion arms the incompatible-version ban on
// another port of address's host, the way repeated old-version welcomes do:
// the ban lands on that port and is widened to the whole IP.
func banSiblingPortForIncompatibleVersion(t *testing.T, svc *Service, address domain.PeerAddress) domain.PeerAddress {
	t.Helper()
	host, _, ok := splitHostPort(string(address))
	if !ok {
		t.Fatalf("splitHostPort(%q) failed", address)
	}
	sibling := domain.PeerAddress(net.JoinHostPort(host, "1"))
	oldVersion := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	for range peerBanThresholdIncompatible / peerBanIncrementIncompatible {
		svc.penalizeOldProtocolPeer(sibling, oldVersion, oldVersion, "")
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[sibling]; health == nil || !time.Now().Before(health.BannedUntil) {
		t.Fatalf("fixture: the sibling port %s is not under an incompatible-version ban", sibling)
	}
	return sibling
}

// A notice to another port of a host banned for an incompatible version is
// not dialled either: the connection manager's candidate filter drops every
// port of an IP-wide ban, and the old client answers on all of them.
func TestSendNoticeToPeer_HostBannedForIncompatibleVersion_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), currentSecureIntro())
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	banSiblingPortForIncompatibleVersion(t, svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "ip-banned-notice-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a port of an IP-wide banned host was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}

// The self-identity cooldown is address-scoped and never widens to the IP
// (buildBannedIPsSet carve-out): a sibling port of a self-alias is a
// different node behind the same host, and its notice is still dialled.
func TestSendNoticeToPeer_SiblingOfSelfIdentityAlias_IsStillDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), currentSecureIntro())
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	host, _, _ := splitHostPort(string(address))
	alias := domain.PeerAddress(net.JoinHostPort(host, "1"))
	svc.peerProvider.Add(alias, domain.PeerSourceAnnounce)
	svc.applySelfIdentityCooldown(alias, svc.newSelfIdentityError(alias))
	svc.sendNoticeToPeer(address, time.Minute, "self-alias-sibling-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent == 0 {
		t.Fatal("the sibling port of a self-identity alias was not dialled: the address-scoped cooldown widened to the IP")
	}
}

// Source (b) of the IP-wide ban: a running address ban of another KNOWN port
// of the host covers the host even when no bannedIPSet entry was written.
func TestSendNoticeToPeer_HostWithBannedKnownSiblingPort_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), currentSecureIntro())
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	host, _, _ := splitHostPort(string(address))
	sibling := domain.PeerAddress(net.JoinHostPort(host, "1"))
	svc.peerProvider.Add(sibling, domain.PeerSourceAnnounce)
	svc.peerMu.Lock()
	health := svc.ensurePeerHealthLocked(sibling)
	health.BannedUntil = time.Now().Add(time.Hour)
	health.LastErrorCode = protocol.ErrCodeIncompatibleProtocol
	svc.peerMu.Unlock()
	svc.ipStateMu.RLock()
	_, ipEntry := svc.bannedIPSet[host]
	svc.ipStateMu.RUnlock()
	if ipEntry {
		t.Fatal("fixture: the host must have no bannedIPSet entry")
	}

	svc.sendNoticeToPeer(address, time.Minute, "sibling-banned-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a host with a banned known sibling port was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}
