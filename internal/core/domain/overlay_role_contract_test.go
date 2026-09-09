package domain_test

// overlay_role_contract_test.go is the executable form of
// docs/protocol/overlay_role.md: it holds the reference implementation of the
// role classification Q and checks the published vectors against it.
//
// # Why the implementation lives in a test
//
// The mechanism that would use Q — role separation on the anonymous search
// path — is direction 2 of blocker O5, taken by the owner FOR VERIFICATION
// ONLY. O5 is open and G2 is not closed. A production function with no
// consumer is the speculative infrastructure this project already had to cut
// once, so the contract gets an implementation exactly where it is needed:
// next to the checks that pin it. The production one arrives in 21b, together
// with its caller, and this file is what it will have to agree with.
//
// # What these checks do NOT establish
//
// That the function is well defined and agreed upon. Nothing else. Whether the
// mechanism built on it holds — disjointness on real paths, refusal frequency,
// connectivity of the structural half — is verified elsewhere, and until then
// no anonymity is promised.
//
// ⚠️ Computing Q is CLASSIFICATION, never AUTHENTICATION. A NodeID can be
// copied and asserted by anybody; admission to a role additionally requires
// proof of key possession, the key matching the NodeID
// (identity.VerifyPublicKeyFingerprint), and binding to the connection the
// proof happened on. None of that is in this file, and none of it follows from
// Q.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// roleDomainSeparator is the ASCII domain separator of the contract: 21 bytes,
// no terminating NUL, no length prefix. It exists to keep this hash distinct
// from every other place the same NodeID is hashed.
const roleDomainSeparator = "corsa/overlay/role/v1"

// vectorDerivationPrefix derives the reproducible vectors V3..V8 of the
// contract. It is deliberately a DIFFERENT string from roleDomainSeparator:
// the vectors are test data, not another use of the role domain.
const vectorDerivationPrefix = "corsa/overlay/role/v1/vector/"

// nodeIDLen is the canonical input length of the contract: 20 raw bytes. Taken
// from the type rather than written as 20, so a change to PeerIdentity breaks
// the compile instead of the classification.
const nodeIDLen = len(domain.PeerIdentity{})

// roleDigest is the whole digest, not the bit. The contract pins the digest
// because a one-bit comparison lets two diverged implementations agree with
// probability about one half — which is the same as not checking.
func roleDigest(id domain.PeerIdentity) [sha256.Size]byte {
	return sha256.Sum256(append([]byte(roleDomainSeparator), id[:]...))
}

// overlayRoleQ is the reference implementation of docs/protocol/overlay_role.md §3.
//
// It takes domain.PeerIdentity — 20 raw bytes — and not a string, because the
// canonical input IS the raw bytes: the 40-character hex address is a
// representation of the same value that hashes to something else entirely.
func overlayRoleQ(id domain.PeerIdentity) int {
	digest := roleDigest(id)
	return int(digest[sha256.Size-1] & 1)
}

// contractVector is one published row of docs/protocol/overlay_role.md §6.
type contractVector struct {
	name      string
	nodeIDHex string
	digestHex string
	q         int
}

// publishedVectors is the table as it appears in the document. It is written
// out by hand on purpose: a table generated from the same code it verifies
// would agree with itself no matter what the document says.
var publishedVectors = []contractVector{
	{"V1", "0000000000000000000000000000000000000000", "301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6", 0},
	{"V2", "ffffffffffffffffffffffffffffffffffffffff", "2c76d911998fd56f963f2e7f3514e88c42c1d7d969da9348b0ad232cbbfaabef", 1},
	{"V3", "48312d0240502678af2c59455fa48fc04cfd751e", "753afef6fc6bdb450254436bb2dea652eafeaa4867f52c89435be26428ce44b1", 1},
	{"V4", "0d05f2139163b02febd289b3c492baeb4a58c8f2", "b483e93337be551d1b4828dde005d3a37aa342c827b824bf7d6762a3bb117447", 1},
	{"V5", "67f31ef9992f5bd32de9e7d09f262c462778e5ee", "e1cdeca809a3f93a5a77850d0c9ae8a9cb6b31389c72e93c17c1ee404eb326f1", 1},
	{"V6", "2cab5346ac6fa5dda132ecde6febf62e08d69e63", "ed82818e79f88f005627d1ec039c1c109d47f02d4c0445d67d6031a944ac7604", 0},
	{"V7", "ee792b613f849eb71c06179c85ab086efdbdc991", "ba576a6d7d9c3b4e8193590e07e0f22069d72694b0968f62a2c17aa12abb7062", 0},
	{"V8", "dc1e3462e586bbd00bfabbe15f10cc970b690cf7", "6542a4a9f84f85a7d49b8bb6d36d0e4042f43dcb6c98625a0cdea0eeca25c343", 1},
}

// negativeVectorDigest is V9: what the answer must NOT be for V1's NodeID. It
// is the digest of the 40-character hex TEXT instead of the 20 raw bytes.
const negativeVectorDigest = "7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9"

func mustIdentityFromHex(t *testing.T, s string) domain.PeerIdentity {
	t.Helper()
	raw, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode NodeID %q: %v", s, err)
	}
	id, err := domain.PeerIdentityFromBytes(raw)
	if err != nil {
		t.Fatalf("NodeID %q is not a PeerIdentity: %v", s, err)
	}
	return id
}

// TestPublishedVectorsReproduceDigestAndBit is the conformance check: every row
// of the document is recomputed from its own input.
func TestPublishedVectorsReproduceDigestAndBit(t *testing.T) {
	t.Parallel()

	for _, vector := range publishedVectors {
		id := mustIdentityFromHex(t, vector.nodeIDHex)

		gotDigest := hex.EncodeToString(func() []byte { d := roleDigest(id); return d[:] }())
		if gotDigest != vector.digestHex {
			t.Errorf("%s digest: got %s, document says %s", vector.name, gotDigest, vector.digestHex)
		}
		if got := overlayRoleQ(id); got != vector.q {
			t.Errorf("%s Q: got %d, document says %d", vector.name, got, vector.q)
		}
	}
}

// TestPublishedVectorsCoverBothRoles guards the table rather than the function:
// a table with one role in it would be passed by an implementation that always
// answers the same thing.
func TestPublishedVectorsCoverBothRoles(t *testing.T) {
	t.Parallel()

	seen := map[int]int{}
	for _, vector := range publishedVectors {
		seen[vector.q]++
	}
	if seen[0] == 0 || seen[1] == 0 {
		t.Fatalf("vectors must contain both roles, got Q=0 in %d rows and Q=1 in %d rows", seen[0], seen[1])
	}
}

// TestDerivedVectorsFollowTheDocumentedRule checks that V3..V8 were DERIVED by
// the published rule rather than pasted. A vector nobody can regenerate is a
// number, not a test vector.
func TestDerivedVectorsFollowTheDocumentedRule(t *testing.T) {
	t.Parallel()

	for index, vector := range publishedVectors[2:] {
		n := index + 1
		seed := sha256.Sum256([]byte(vectorDerivationPrefix + strconv.Itoa(n)))
		want := hex.EncodeToString(seed[:nodeIDLen])
		if vector.nodeIDHex != want {
			t.Errorf("%s NodeID: document says %s, rule with n=%d gives %s", vector.name, vector.nodeIDHex, n, want)
		}
	}
}

// TestRawBytesAndHexTextAreDifferentInputs is the canonicalisation check, and
// it is not academic: for the all-zero NodeID the wrong input also FLIPS the
// role, so an implementation that decodes its input wrongly does not announce
// itself — it just classifies the node into the other half.
func TestRawBytesAndHexTextAreDifferentInputs(t *testing.T) {
	t.Parallel()

	canonical := publishedVectors[0]
	id := mustIdentityFromHex(t, canonical.nodeIDHex)

	rawDigest := roleDigest(id)
	textDigest := sha256.Sum256([]byte(roleDomainSeparator + canonical.nodeIDHex))

	if hex.EncodeToString(rawDigest[:]) != canonical.digestHex {
		t.Fatalf("raw-byte digest drifted from the document: %s", hex.EncodeToString(rawDigest[:]))
	}
	if got := hex.EncodeToString(textDigest[:]); got != negativeVectorDigest {
		t.Fatalf("hex-text digest: got %s, document says %s", got, negativeVectorDigest)
	}
	if rawDigest == textDigest {
		t.Fatal("hashing the hex text must not equal hashing the raw bytes")
	}
	if rawByte, textByte := rawDigest[sha256.Size-1]&1, textDigest[sha256.Size-1]&1; rawByte == textByte {
		t.Errorf("the document claims this pair also flips the role; it no longer does (both %d)", rawByte)
	}
}

// TestS26RoleIsStableAcrossRepeatedCalls is scenario S26 in the form available
// without an overlay: the classification of one identity does not drift.
//
// Reconnects, restarts and chain changes are not simulated here and do not need
// to be: the function reads nothing but its argument, so there is no state for
// them to disturb. That is the property being pinned — anything that made Q
// depend on a clock, a config or stored state would have to break this test.
func TestS26RoleIsStableAcrossRepeatedCalls(t *testing.T) {
	t.Parallel()

	for _, vector := range publishedVectors {
		id := mustIdentityFromHex(t, vector.nodeIDHex)
		first := overlayRoleQ(id)
		for range 64 {
			if got := overlayRoleQ(id); got != first {
				t.Fatalf("%s: Q changed between calls: %d then %d", vector.name, first, got)
			}
		}
		// A copy of the value must classify identically: the role belongs to
		// the identifier, not to the variable holding it.
		clone := id
		if got := overlayRoleQ(clone); got != first {
			t.Errorf("%s: a copy classified differently: %d vs %d", vector.name, got, first)
		}
	}
}

// TestClassifierPartitionsTheIdentifiers checks the CLASSIFIER, not S22: the
// two role sets partition the identifier space, so one identity never carries
// both roles.
//
// ⚠️ This was labelled S22 and that was wrong — the review caught it. Sorting
// identifiers into two maps and observing that the maps do not overlap tests
// arithmetic that cannot fail: the sets are defined by a boolean, so they are
// disjoint by construction. It exercises no selection rule, and above all it
// would not notice an implementation that used a node's ANNOUNCED role instead
// of the computed one — which is the hole S22 exists to catch. S22 proper is
// TestS22SelectionUsesComputedRoleNotAnnouncedRole below.
func TestClassifierPartitionsTheIdentifiers(t *testing.T) {
	t.Parallel()

	const sample = 4096

	structural := make(map[domain.PeerIdentity]struct{}, sample)
	firstHop := make(map[domain.PeerIdentity]struct{}, sample)

	for i := range sample {
		id := randomIdentity(t, i)

		switch overlayRoleQ(id) {
		case roleStructural:
			structural[id] = struct{}{}
		case roleFirstHop:
			firstHop[id] = struct{}{}
		default:
			t.Fatalf("Q returned neither 0 nor 1 for %s", id)
		}
	}

	for id := range structural {
		if _, both := firstHop[id]; both {
			t.Fatalf("identity %s landed in both halves", id)
		}
	}
	if total := len(structural) + len(firstHop); total != sample {
		t.Fatalf("halves do not partition the sample: %d + %d = %d, want %d",
			len(structural), len(firstHop), total, sample)
	}

	// Neither half empty is a sanity check on the bit — NOT evidence for the
	// 50/50 assumption of §0″.4, which is a statement about the network and is
	// measured on the network (M3-a/M3-b), not asserted from a generator we
	// wrote ourselves.
	if len(structural) == 0 || len(firstHop) == 0 {
		t.Fatalf("one half is empty over %d random identities: structural=%d, first-hop=%d",
			sample, len(structural), len(firstHop))
	}
}

// --- S22: selection must use the computed role, never the announced one -----

const (
	roleFirstHop   = 0
	roleStructural = 1
)

// roleCandidate is a peer as a selector sees it: an identity, and whatever role
// that peer CLAIMS. The claim is deliberately a separate field — that is the
// whole point of the scenario. Nothing on the wire forces the two to agree.
type roleCandidate struct {
	identity domain.PeerIdentity
	// announced is what the peer says about itself. A hostile peer says
	// whatever gets it selected.
	announced int
}

// selectByComputedRole is the CORRECT selector: it derives the role from the
// identity and ignores the claim entirely.
func selectByComputedRole(candidates []roleCandidate, want int) []domain.PeerIdentity {
	out := make([]domain.PeerIdentity, 0, len(candidates))
	for _, candidate := range candidates {
		if overlayRoleQ(candidate.identity) == want {
			out = append(out, candidate.identity)
		}
	}
	return out
}

// selectByAnnouncedRole is the WRONG selector, kept as a negative control. It
// is what an implementation looks like when it trusts a peer's own statement —
// the rejected "role separation by announcement" candidate of §4.3.4. Without
// it the scenario could not show that the fixture distinguishes the two.
func selectByAnnouncedRole(candidates []roleCandidate, want int) []domain.PeerIdentity {
	out := make([]domain.PeerIdentity, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.announced == want {
			out = append(out, candidate.identity)
		}
	}
	return out
}

// lyingCandidates builds a population in which EVERY peer announces the
// opposite of its computed role. That is the strongest form of the scenario: a
// selector that trusts announcements gets exactly the inverse of the correct
// answer, so the two cannot be confused for one another by accident.
func lyingCandidates(t *testing.T, n int) []roleCandidate {
	t.Helper()

	out := make([]roleCandidate, 0, n+len(publishedVectors))

	// The published vectors go in FIRST, so both roles are present by
	// construction rather than by luck: the table is checked to contain both
	// (TestPublishedVectorsCoverBothRoles), and a run must not depend on the
	// random tail to be non-degenerate.
	for _, vector := range publishedVectors {
		id := mustIdentityFromHex(t, vector.nodeIDHex)
		out = append(out, roleCandidate{identity: id, announced: 1 - overlayRoleQ(id)})
	}
	for i := range n {
		id := randomIdentity(t, i)
		out = append(out, roleCandidate{identity: id, announced: 1 - overlayRoleQ(id)})
	}
	return out
}

// TestS22SelectionUsesComputedRoleNotAnnouncedRole is scenario S22 in the form
// available without an overlay: a model of choosing the first hop and the
// structural hop of ONE search, against peers that lie about their role.
//
// The scenario's claim is that a node picked as first hop cannot also serve on
// the structural leg of the same search. Two things have to hold for that, and
// only the second is about arithmetic:
//
//  1. the role must come from the identity, not from what the peer says;
//  2. the halves must not intersect.
//
// A model is enough here because both first-hop and structural selection are
// decisions the initiator makes locally from identities it already holds. What
// a model canNOT show is that the path an implementation actually builds obeys
// this — 16a/19/06 build it, and that is S25 on actual paths, in stage 2.
func TestS22SelectionUsesComputedRoleNotAnnouncedRole(t *testing.T) {
	t.Parallel()

	const population = 512
	candidates := lyingCandidates(t, population)

	firstHops := selectByComputedRole(candidates, roleFirstHop)
	structural := selectByComputedRole(candidates, roleStructural)

	if len(firstHops) == 0 || len(structural) == 0 {
		t.Fatalf("degenerate population: %d first-hop, %d structural", len(firstHops), len(structural))
	}

	// 1. Every selected peer really holds the role it was selected for, even
	//    though every one of them announced the opposite.
	for _, id := range firstHops {
		if got := overlayRoleQ(id); got != roleFirstHop {
			t.Errorf("first-hop selection returned %s whose computed role is %d", id, got)
		}
	}
	for _, id := range structural {
		if got := overlayRoleQ(id); got != roleStructural {
			t.Errorf("structural selection returned %s whose computed role is %d", id, got)
		}
	}

	// 2. No identity is eligible for both roles of the same search.
	chosen := make(map[domain.PeerIdentity]struct{}, len(firstHops))
	for _, id := range firstHops {
		chosen[id] = struct{}{}
	}
	for _, id := range structural {
		if _, both := chosen[id]; both {
			t.Fatalf("identity %s is eligible as first hop AND as structural hop on one search", id)
		}
	}

	// 3. The negative control: the announcement-trusting selector produces a
	//    DIFFERENT answer on this population. Without this the first two
	//    assertions would also pass an implementation that never consulted the
	//    identity at all — every peer here lies, so the wrong selector returns
	//    precisely the wrong set.
	byAnnouncement := selectByAnnouncedRole(candidates, roleFirstHop)
	if identitySetsEqual(firstHops, byAnnouncement) {
		t.Fatal("the announcement-trusting selector agreed with the computed one: " +
			"this population no longer distinguishes them, so the scenario proves nothing")
	}
	for _, id := range byAnnouncement {
		if overlayRoleQ(id) == roleFirstHop {
			t.Errorf("announcement-trusting selector returned %s, which really IS a first hop; "+
				"the population is not lying uniformly and the control is weakened", id)
		}
	}
}

// TestS22RejectsHostilePeerClaimingTheOtherHalf is the single-peer form of the
// same scenario, written separately because it is the case an implementation
// gets wrong in practice: not a whole lying population, but one peer that wants
// to appear on both legs.
func TestS22RejectsHostilePeerClaimingTheOtherHalf(t *testing.T) {
	t.Parallel()

	// The peer is V1 from the contract — a FIXED input whose published role is
	// first-hop — announcing that it is also good for the structural leg.
	//
	// ⚠️ Earlier revisions searched for such an identity at random. That was
	// wrong twice over, and both were found by mutating the classifier: an
	// unbounded search HANGS against a classifier stuck on "structural" instead
	// of failing, and even the bounded version replaced a fact with a
	// probability argument. A published vector needs neither: if V1 no longer
	// classifies as first-hop, the contract itself has moved, and that is what
	// the assertion below says.
	firstHopVector := publishedVectors[0]
	if firstHopVector.q != roleFirstHop {
		t.Fatalf("this test needs a first-hop vector; %s is published as Q=%d",
			firstHopVector.name, firstHopVector.q)
	}

	hostileID := mustIdentityFromHex(t, firstHopVector.nodeIDHex)
	if got := overlayRoleQ(hostileID); got != roleFirstHop {
		t.Fatalf("%s computes as Q=%d, the contract publishes Q=%d: the classifier disagrees "+
			"with the document, so this scenario cannot say anything about selection",
			firstHopVector.name, got, firstHopVector.q)
	}
	hostile := roleCandidate{identity: hostileID, announced: roleStructural}

	structural := selectByComputedRole([]roleCandidate{hostile}, roleStructural)
	if len(structural) != 0 {
		t.Fatalf("a peer announcing the structural role was admitted to it: %v", structural)
	}

	firstHops := selectByComputedRole([]roleCandidate{hostile}, roleFirstHop)
	if len(firstHops) != 1 {
		t.Fatalf("the peer's real role is first-hop, selection returned %d entries", len(firstHops))
	}

	// The negative control again, at the smallest scale: trusting the claim
	// admits it.
	if admitted := selectByAnnouncedRole([]roleCandidate{hostile}, roleStructural); len(admitted) != 1 {
		t.Fatal("the announcement-trusting selector did not admit the liar; the control is inert")
	}
}

func identitySetsEqual(left, right []domain.PeerIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[domain.PeerIdentity]struct{}, len(left))
	for _, id := range left {
		seen[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

func randomIdentity(t *testing.T, index int) domain.PeerIdentity {
	t.Helper()

	var raw [nodeIDLen]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("random identity %d: %v", index, err)
	}
	id, err := domain.PeerIdentityFromBytes(raw[:])
	if err != nil {
		t.Fatalf("random identity %d is not a PeerIdentity: %v", index, err)
	}
	return id
}

// TestRoleDependsOnEveryInputByte guards the concatenation: a NodeID differing
// in a single bit must be able to reach a different digest. Without it an
// implementation that hashed, say, only the separator would pass every vector
// whose role happened to match.
func TestRoleDependsOnEveryInputByte(t *testing.T) {
	t.Parallel()

	base := mustIdentityFromHex(t, publishedVectors[0].nodeIDHex)
	baseDigest := roleDigest(base)

	for position := range nodeIDLen {
		flipped := base
		flipped[position] ^= 0x01
		if roleDigest(flipped) == baseDigest {
			t.Fatalf("flipping byte %d did not change the digest", position)
		}
	}
}

// TestSeparatorIsPartOfTheInput pins the domain separation itself: hashing the
// bare NodeID must not give the contract's digest. Dropping the separator is
// the easiest way to reimplement this function wrongly, and every published
// vector would still have a one-in-two chance of agreeing on the bit.
func TestSeparatorIsPartOfTheInput(t *testing.T) {
	t.Parallel()

	id := mustIdentityFromHex(t, publishedVectors[0].nodeIDHex)
	bare := sha256.Sum256(id[:])
	if got := roleDigest(id); got == bare {
		t.Fatal("digest of the bare NodeID equals the contract digest: the separator is not being hashed")
	}
	if got, want := fmt.Sprintf("%d", len(roleDomainSeparator)), "21"; got != want {
		t.Errorf("domain separator length: got %s bytes, contract says %s", got, want)
	}
}
