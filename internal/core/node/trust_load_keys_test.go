package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
)

// TestTrustedContactWithBrokenBoxBindingKeepsItsSigningKey: a stored contact
// whose signing key is acceptable and certifies its address, but whose box
// binding does not verify, keeps the signing key in the knowledge maps — DMs
// FROM it still verify — while the box pair is dropped, so nothing is ever
// encrypted to, attached or offered with a key the contact never bound.
func TestTrustedContactWithBrokenBoxBindingKeepsItsSigningKey(t *testing.T) {
	t.Parallel()
	contact, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	stranger, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	now := time.Now().UTC()
	file := trustFile{
		Version: trustFileVersion,
		Contacts: map[string]trustedContact{
			contact.Address: {
				Address: contact.Address,
				PubKey:  identity.PublicKeyBase64(contact.PublicKey),
				// A box key the contact never signed: the binding fails.
				BoxKey:       identity.BoxPublicKeyBase64(stranger.BoxPublicKey),
				BoxSignature: identity.SignBoxKeyBinding(contact),
				FirstSeenAt:  now,
				LastSeenAt:   now,
				Source:       "pre-check",
			},
		},
	}
	payload, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("marshal trust file: %v", err)
	}
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write trust file: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: path,
	}, testIdentityForNetworkConsumerTest(t), nil)
	t.Cleanup(svc.WaitBackground)

	svc.knowledgeMu.RLock()
	pubKey := svc.pubKeys[contact.Address]
	_, hasBox := svc.boxKeys[contact.Address]
	_, hasBoxSig := svc.boxSigs[contact.Address]
	svc.knowledgeMu.RUnlock()
	if pubKey != identity.PublicKeyBase64(contact.PublicKey) {
		t.Fatalf("signing key in the knowledge maps = %q, want the contact's own", pubKey)
	}
	if hasBox || hasBoxSig {
		t.Fatal("an unbound box pair entered the knowledge maps")
	}
	stats := svc.keyHygieneStats()
	if stats.RefusedTrustedContacts != 0 {
		t.Fatalf("refused trusted contacts = %d: only the box pair was bad", stats.RefusedTrustedContacts)
	}
	if stats.DroppedTrustedBoxPairs != 1 {
		t.Fatalf("dropped trusted box pairs = %d, want 1", stats.DroppedTrustedBoxPairs)
	}

	// The operator sees it without reading logs: fetch_network_stats.
	reply := svc.networkStatsFrame()
	if reply.NetworkStats == nil || reply.NetworkStats.KeyHygiene == nil {
		t.Fatal("fetch_network_stats carries no key_hygiene block")
	}
	if got := reply.NetworkStats.KeyHygiene.DroppedTrustedBoxPairs; got != 1 {
		t.Fatalf("key_hygiene.dropped_trusted_box_pairs = %d, want 1", got)
	}

	// The local trusted-contact list keeps the contact and its signing key
	// but withholds the unbound box pair.
	for _, listed := range svc.trustedContactsFrame().Contacts {
		if listed.Address != contact.Address {
			continue
		}
		if listed.PubKey == "" {
			t.Fatal("the contact's signing key was withheld")
		}
		if listed.BoxKey != "" || listed.BoxSig != "" {
			t.Fatal("fetch_trusted_contacts handed out the unbound box pair")
		}
		return
	}
	t.Fatal("the contact vanished from fetch_trusted_contacts")
}
