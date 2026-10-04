package datagram

import (
	"net/netip"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// admission_legacy_keys_test.go pins the two namespaces of an ACCEPTED legacy
// connection (docs/refactoring/n1-legacy-residual.md §1): its auth_session can
// be relayed, so it is billed and owned by what this node observed of the
// socket, it never shares a bucket with the identity it names, and it never
// reads as proven.

func TestLegacyKeysAreTheirOwnNamespaces(t *testing.T) {
	t.Parallel()
	id := domaintest.ID("x")
	host := netip.MustParseAddr("10.0.0.66")
	keys := map[string]AdmissionKey{
		"proven":     provenIdentityKey(id),
		"dialled":    DialedAddressKey(domain.PeerAddress("10.0.0.66:64646")),
		"host":       AcceptedHostKey(host),
		"connection": AcceptedConnectionKey(domain.ConnID(66)),
	}
	seen := map[AdmissionKey]string{}
	for name, key := range keys {
		if key.IsZero() {
			t.Fatalf("%s key is zero", name)
		}
		if other, dup := seen[key]; dup {
			t.Fatalf("%s and %s are one key", name, other)
		}
		seen[key] = name
	}
	if AcceptedHostKey(netip.MustParseAddr("::ffff:10.0.0.66")) != AcceptedHostKey(host) {
		t.Fatal("an IPv4-mapped source must key the same host as the plain IPv4 one")
	}
	if !AcceptedHostKey(netip.Addr{}).IsZero() || !AcceptedConnectionKey(0).IsZero() {
		t.Fatal("a missing host or connection must name nobody")
	}
	if !ProvenIdentityKey(sessionv2.ProvenIdentity{}).IsZero() {
		t.Fatal("the zero proof must name nobody")
	}
}

// A legacy arrival naming X reads as claimed, whatever it names: the proven
// derivation is equality with the key a v2 proof would have produced.
func TestLegacyKeysNeverReadAsProven(t *testing.T) {
	t.Parallel()
	id := domaintest.ID("x")
	for name, key := range map[string]AdmissionKey{
		"host":       AcceptedHostKey(netip.MustParseAddr("10.0.0.66")),
		"connection": AcceptedConnectionKey(domain.ConnID(66)),
	} {
		frame := inboundFrame{peer: id, budgetKey: key}
		if frame.authority().Proven() {
			t.Fatalf("an arrival billed to the %s key read as proven", name)
		}
		if _, ok := frame.ingress().Identity(); ok {
			t.Fatalf("the %s ingress answered Identity(): only a proof may", name)
		}
	}
	if !(inboundFrame{peer: id, budgetKey: provenIdentityKey(id)}).authority().Proven() {
		t.Fatal("positive control: the proven key of the same identity must read as proven")
	}
}

// DG-3: a legacy connection naming X fills ITS per-upstream reverse quota;
// X's v2 session keeps its own.
func TestLegacyUpstreamDoesNotSpendTheProvenIdentitysReverseQuota(t *testing.T) {
	t.Parallel()
	fixture := newQuotaFixture(t, 64, 2)
	id := domaintest.ID("x")
	impostor := ChannelUpstream(testChannel("impostor"), AcceptedHostKey(netip.MustParseAddr("10.0.0.66")), id)
	genuine := ChannelUpstream(testChannel("genuine"), provenIdentityKey(id), id)

	for i, seed := range []string{"i1", "i2"} {
		if got := fixture.reserve(seed, impostor, fixture.now.Add(time.Duration(i)*time.Second)); got != ReverseSlotReserved {
			t.Fatalf("%s: %s", seed, got)
		}
	}
	if got := fixture.reserve("i3", impostor, fixture.now.Add(3*time.Second)); got != ReverseSlotCapped {
		t.Fatalf("precondition: the impostor's own quota is full, got %s", got)
	}
	if got := fixture.reserve("g1", genuine, fixture.now.Add(4*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("X's v2 session was refused a reverse record: the legacy connection that named X spent X's quota (%s)", got)
	}
}
