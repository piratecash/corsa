package node

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// secret_leak_test.go answers the question the type-level block in
// identity/secret.go cannot: the identity refuses to serialise ITSELF, but
// every outgoing artifact is assembled field by field from it, and a hand-
// assembled frame can carry the signing key without ever calling
// json.Marshal on an Identity.
//
// So this test renders the artifacts the node actually emits and scans them
// for the private key material in every encoding a leak could take. The
// negative half — proving the scanner is not inert — lives in
// TestExportBackupIsTheOneDoor (identity package): the same scan run against
// a real backup must find the key.

// secretRenderings expands one secret blob into every encoding worth scanning
// for: the raw bytes, all four Base64 alphabets, hex, and the decimal byte
// list fmt produces for a []byte under %d.
//
// Checking one encoding proves only that one encoding is clean. The decimal
// entry earns its place specifically because %d is the verb that bypasses
// Stringer — so it is the verb a leak survives, and "[85 86 162 …]" is a
// perfectly usable rendering of a private key that none of the others match.
func secretRenderings(label string, blob []byte) map[string]string {
	return map[string]string{
		label + "/raw":            string(blob),
		label + "/base64-std":     base64.StdEncoding.EncodeToString(blob),
		label + "/base64-rawstd":  base64.RawStdEncoding.EncodeToString(blob),
		label + "/base64-url":     base64.URLEncoding.EncodeToString(blob),
		label + "/base64-raw-url": base64.RawURLEncoding.EncodeToString(blob),
		label + "/hex":            hex.EncodeToString(blob),
		label + "/decimal":        fmt.Sprintf("%d", blob),
	}
}

// nodeSecrets is every blob whose appearance in an outgoing artifact is a
// compromise: the Ed25519 private key, the seed (which reconstructs both the
// signing key and, through deriveBoxKeyPair, an SDK box key) and the X25519
// box private key. The PUBLIC keys are deliberately absent — they belong on
// the wire and are self-certifying against the address.
func nodeSecrets(id *identity.Identity) map[string]string {
	out := map[string]string{}
	for label, blob := range map[string][]byte{
		"ed25519-private": id.PrivateKey,
		"ed25519-seed":    id.PrivateKey.Seed(),
		"x25519-box":      id.BoxPrivateKey.Bytes(),
	} {
		for encoding, value := range secretRenderings(label, blob) {
			out[encoding] = value
		}
	}
	return out
}

// Compile-time proof that the service answers for every verb. Stringer alone
// would leave %d and %x resolving through fmt's reflective walk, which is the
// path that reaches an unexported field's contents.
var (
	_ fmt.Formatter  = &Service{}
	_ fmt.Stringer   = &Service{}
	_ fmt.GoStringer = &Service{}
)

// TestServiceNeverRendersPrivateKey: Service keeps the node's identity — both
// private keys — in an UNEXPORTED field, and fmt calls no method on the way
// down through one of those. Identity's own fail-closed methods are therefore
// worth nothing here; the container has to answer for itself.
func TestServiceNeverRendersPrivateKey(t *testing.T) {
	t.Parallel()
	svc, id, _ := newBackupTestService(t)
	secrets := nodeSecrets(id)

	for _, verb := range []string{"%v", "%+v", "%s", "%#v", "%d", "%x", "%q"} {
		rendered := fmt.Sprintf(verb, svc)
		for encoding, value := range secrets {
			if strings.Contains(rendered, value) {
				t.Fatalf("fmt %s of the service leaked the identity secret (%s)", verb, encoding)
			}
		}
		if !strings.Contains(rendered, "node.Service{") {
			t.Fatalf("fmt %s of the service did not go through Format: %s", verb, rendered)
		}
		// The address must survive, or a log line about "which node is this"
		// answers nothing and someone reaches for %+v on the guts instead.
		if !strings.Contains(rendered, id.Address) {
			t.Fatalf("fmt %s of the service dropped the address: %s", verb, rendered)
		}
	}

	// A struct merely HOLDING the service is the realistic accident.
	holder := struct {
		Service *Service
		Note    string
	}{Service: svc, Note: "support bundle"}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		rendered := fmt.Sprintf(verb, holder)
		for encoding, value := range secrets {
			if strings.Contains(rendered, value) {
				t.Fatalf("fmt %s of a struct holding the service leaked the secret (%s)", verb, encoding)
			}
		}
	}

	var missing *Service
	if rendered := fmt.Sprintf("%+v", missing); !strings.Contains(rendered, "node.Service(nil)") {
		t.Fatalf("nil service rendered as %s", rendered)
	}
}

// TestOutgoingArtifactsCarryNoPrivateKey: everything the node publishes —
// the node-hello line, the welcome frame, the signed self record, the node
// status and every read-only local RPC reply — serialises successfully and
// contains no rendering of either private key.
func TestOutgoingArtifactsCarryNoPrivateKey(t *testing.T) {
	t.Parallel()
	svc, id, _ := newBackupTestService(t)
	secrets := nodeSecrets(id)

	rendered := map[string]string{
		"node hello line": svc.nodeHelloJSONLine(),
	}

	record, body := svc.SelfIdentityRecord()
	structured := map[string]any{
		"welcome frame":        svc.welcomeFrame("challenge", "203.0.113.7:64646"),
		"self record":          record,
		"self record body":     body,
		"node status":          svc.NodeStatus(),
		"aggregate status":     svc.HandleLocalFrame(protocol.Frame{Type: "fetch_aggregate_status"}),
		"contacts reply":       svc.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"}),
		"identities reply":     svc.HandleLocalFrame(protocol.Frame{Type: "fetch_identities"}),
		"peers reply":          svc.HandleLocalFrame(protocol.Frame{Type: "get_peers"}),
		"dm headers reply":     svc.HandleLocalFrame(protocol.Frame{Type: "fetch_dm_headers"}),
		"relay status reply":   svc.HandleLocalFrame(protocol.Frame{Type: "fetch_relay_status"}),
		"presence reply":       svc.HandleLocalFrame(protocol.Frame{Type: "fetch_presence"}),
		"reachable ids reply":  svc.HandleLocalFrame(protocol.Frame{Type: "fetch_reachable_ids"}),
		"first hop guards":     svc.HandleLocalFrame(protocol.Frame{Type: "fetch_first_hop_guards"}),
		"resource usage reply": svc.HandleLocalFrame(protocol.Frame{Type: "fetch_resource_usage"}),
	}
	for what, subject := range structured {
		payload, err := json.Marshal(subject)
		if err != nil {
			t.Fatalf("%s: marshal failed: %v", what, err)
		}
		rendered[what] = string(payload)
	}

	for what, artifact := range rendered {
		if artifact == "" {
			t.Fatalf("%s rendered empty — the scan below would pass vacuously", what)
		}
		for encoding, value := range secrets {
			if strings.Contains(artifact, value) {
				t.Fatalf("%s leaked the identity secret (%s)", what, encoding)
			}
		}
	}

	// The artifacts must still be about THIS identity, or a scan of unrelated
	// output would clear everything for the wrong reason.
	if !strings.Contains(rendered["node hello line"], id.Address) {
		t.Fatalf("node hello line does not name the identity: %s", rendered["node hello line"])
	}
}
