package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// listener_refusal_legacy_test.go: an old v1 dialler (an emulated release of
// version 28, 29 or 30 — asOldNode behind legacyVersionProxy) refused by a
// listener of this build. The refusal alert is seven bytes without a newline,
// so its line reader sees EOF, exactly as it saw the silent close the alert
// replaces: the test runs both and requires the old dialler to end the same
// way, with nothing charged on either side.
//
// THE OLD PEER IS AN EMULATION, NOT A RELEASED BINARY (see
// legacy_peer_emulation_test.go): its reader is this build's v1 reader, which
// is the same line reader the releases use. The run against the released
// binaries (release_interop_test.go, build tag interop) does not cover this
// refusal.

// refusedLegacyDial is how one refused dial of an old node ended: its error,
// the outcome class the old node recorded, and what both sides charged.
type refusedLegacyDial struct {
	err              error
	outcomes         domain.SessionOutcomeStats
	oldRemoteBans    int
	listenerBan      banEntry
	listenerBanned   bool
	refusals         domain.ListenerRefusalStats
	listenerSessions int
}

// refuseAnOldDialler makes an old node of version dial a listener of this
// build that refuses it by the loopback class's bound on connections held at
// once — the old node dials from loopback; alertSenders 0 is the silent close
// of the releases before the alert.
func refuseAnOldDialler(t *testing.T, version int, alertSenders int) refusedLegacyDial {
	t.Helper()
	listener := refusingNode(t, config.Node{}, func(svc *Service) {
		svc.listenerRefusals = newListenerRefusals(alertPoolBounds{total: alertSenders, perSource: max(alertSenders, 1)}, svc.startedAt)
		svc.loopbackInbound = newLoopbackInboundClass(loopbackInboundBounds{maxActive: 0, maxNewPerWindow: 1, window: time.Hour}, productionIdentityPenaltyPolicy(), nil)
	})
	proxy := startLegacyVersionProxy(t, listener.cfg.ListenAddress, oldPeerDials, version)
	old, stopOld := startQuietNode(t, asOldNode)
	t.Cleanup(stopOld)

	established, err := dialPastTransport(t, old, proxy.peerAddress())
	if established {
		t.Fatalf("v%d: the refused old dialler got a session", version)
	}
	proxy.drain(t)
	// The listener's alert sender is done once the proxy saw both ends close.
	waitForConditionMsg(t, 5*time.Second, "the listener never counted the refusal", func() bool {
		return listener.ListenerRefusalStats().LoopbackConnections >= 1
	})

	old.ipStateMu.RLock()
	oldRemoteBans := len(old.remoteBannedIPs)
	old.ipStateMu.RUnlock()
	listener.ipStateMu.RLock()
	ban, banned := listener.bans["127.0.0.1"]
	listener.ipStateMu.RUnlock()
	v2, v1 := authenticatedInbound(listener)
	return refusedLegacyDial{
		err:              err,
		outcomes:         old.SessionOutcomeStats(),
		oldRemoteBans:    oldRemoteBans,
		listenerBan:      ban,
		listenerBanned:   banned,
		refusals:         listener.ListenerRefusalStats(),
		listenerSessions: v2 + v1,
	}
}

func TestAnOldDiallerReadsTheRefusalAlertAsTheEOFOfASilentClose(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			alerted := refuseAnOldDialler(t, version, productionAlertPoolBounds().total)
			silent := refuseAnOldDialler(t, version, 0)

			if alerted.refusals.ClosedWithoutAlert != 0 {
				t.Fatalf("v%d: the refusal went out without the alert: %+v", version, alerted.refusals)
			}
			if silent.refusals.ClosedWithoutAlert == 0 {
				t.Fatalf("v%d: fixture: the silent run sent the alert: %+v", version, silent.refusals)
			}
			for name, run := range map[string]refusedLegacyDial{"alert": alerted, "silent close": silent} {
				// Our side: a refusal is not a violation.
				if run.listenerBanned && (run.listenerBan.Score > 0 || !run.listenerBan.Blacklisted.IsZero()) {
					t.Fatalf("v%d %s: the listener charged the old dialler's IP: %+v", version, name, run.listenerBan)
				}
				if run.listenerSessions != 0 {
					t.Fatalf("v%d %s: the refused dialler got %d sessions", version, name, run.listenerSessions)
				}
				// Their side: an ordinary failed dial — no ban, no
				// incompatibility, no self-connection, nothing a retry is
				// held back by.
				if errors.Is(run.err, protocol.ErrPeerBanned) || errors.Is(run.err, errIncompatibleProtocol) ||
					errors.Is(run.err, protocol.ErrSelfIdentity) || isUnchargedDialRefusal(run.err) {
					t.Fatalf("v%d %s: the old dialler read the refusal as %v", version, name, run.err)
				}
				if run.oldRemoteBans != 0 {
					t.Fatalf("v%d %s: the old dialler recorded a remote ban", version, name)
				}
				if run.outcomes.ErrorsCompat != 0 || run.outcomes.Succeeded != 0 {
					t.Fatalf("v%d %s: the old dialler's outcome = %+v, want an ordinary failure", version, name, run.outcomes)
				}
			}
			// The same ending as the silent close: the alert changes nothing
			// for an old reader.
			if outcomeClasses(alerted.outcomes) != outcomeClasses(silent.outcomes) {
				t.Fatalf("v%d: the old dialler ended the alerted dial as %+v (%v), the silent one as %+v (%v)",
					version, alerted.outcomes, alerted.err, silent.outcomes, silent.err)
			}
		})
	}
}

// outcomeClasses is a session outcome reading without its timestamps.
func outcomeClasses(stats domain.SessionOutcomeStats) [4]uint64 {
	return [4]uint64{stats.Succeeded, stats.ErrorsConnect, stats.ErrorsCompat, stats.ErrorsOther}
}
