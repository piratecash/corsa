//go:build linux

package loadstand

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLinuxRuntimeRelayFullWitness(t *testing.T) {
	status, signed := linuxRuntimeTestTuple(t)
	other, foreign := linuxRuntimeTestTuple(t)
	witness := PublicIdentity{Identity: domain.PeerIdentityFromWire(status.Address), PublicKey: status.PublicKey, BoxPublicKey: status.BoxPublicKey, BoxSignature: signed.BoxSig}
	relay := protocol.ContactFrame{Address: status.Address, PubKey: status.PublicKey}
	t.Run("relay-policy-with-genuine-witness", func(t *testing.T) {
		if _, err := linuxRuntimePublic(status, status, []protocol.ContactFrame{relay}); !errors.Is(err, ErrProcessObservation) {
			t.Fatal("original SDK contact proof unexpectedly accepted signing-only self", err)
		}
		got, err := linuxRuntimeFullPublic(MemberFull, status, status, []protocol.ContactFrame{relay}, witness)
		if err != nil || got != witness {
			t.Fatal("genuine relay Full public witness refused", err)
		}
	})
	cases := []struct {
		name   string
		change func(*MemberRole, *domain.NodeStatus, *domain.NodeStatus, *[]protocol.ContactFrame, *PublicIdentity)
	}{
		{"wrong-role", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			*role = MemberSDK
		}},
		{"missing-witness", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			*w = PublicIdentity{}
		}},
		{"foreign-signed-witness", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			w.Identity = domain.PeerIdentityFromWire(other.Address)
			w.PublicKey = other.PublicKey
			w.BoxPublicKey = other.BoxPublicKey
			w.BoxSignature = foreign.BoxSig
		}},
		{"status-incarnation-drift", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			b.StartedAt = b.StartedAt.Add(time.Second)
		}},
		{"status-box-mismatch", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			a.BoxPublicKey = other.BoxPublicKey
			b.BoxPublicKey = other.BoxPublicKey
		}},
		{"self-ed-mismatch", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			(*c)[0].PubKey = other.PublicKey
		}},
		{"missing-self", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			*c = nil
		}},
		{"duplicate-self", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			*c = append(*c, relay)
		}},
		{"contact-box-present", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			(*c)[0].BoxKey = status.BoxPublicKey
		}},
		{"contact-signature-present", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			(*c)[0].BoxSig = signed.BoxSig
		}},
		{"noncanonical-box", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			w.BoxPublicKey += "\n"
			a.BoxPublicKey = w.BoxPublicKey
			b.BoxPublicKey = w.BoxPublicKey
		}},
		{"bad-signature", func(role *MemberRole, a, b *domain.NodeStatus, c *[]protocol.ContactFrame, w *PublicIdentity) {
			w.BoxSignature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			role, a, b, rows, public := MemberFull, status, status, []protocol.ContactFrame{relay}, witness
			tc.change(&role, &a, &b, &rows, &public)
			var d linuxBindRuntimeDiagnostic
			if got, err := linuxRuntimeFullPublic(role, a, b, rows, public, &d); !errors.Is(err, ErrProcessObservation) || got != (PublicIdentity{}) || d.detail == "" {
				t.Fatal("invalid Full testimony accepted or unlabelled", tc.name, got, err, d)
			}
		})
	}
	t.Run("sdk-original-proof-remains-required", func(t *testing.T) {
		if _, err := linuxRuntimePublic(status, status, []protocol.ContactFrame{signed}); err != nil {
			t.Fatal("original SDK signed-contact success changed", err)
		}
		if _, err := linuxRuntimeFullPublic(MemberSDK, status, status, []protocol.ContactFrame{signed}, witness); !errors.Is(err, ErrProcessObservation) {
			t.Fatal("SDK switched to Full witness", err)
		}
	})
}
