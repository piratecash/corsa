package routing

import (
	"testing"
)

// score_signed_bonus_test.go pins the Phase 4 13.2-C trust-score
// signed-bonus: a verified attested-links signature edges a claim
// above an otherwise-identical unsigned claim, but the bonus is
// strictly within-tier — it never rescues a Bad / Questionable pair
// against a Good alternative, nor does it overpower a one-hop
// preference (scoreHopPenalty=10 > scoreSignedBonus=5).

func TestCompositeScore_VerifiedRanksAboveUnsignedSameTier(t *testing.T) {
	unsigned := CompositeScore(2, RouteSourceAnnouncement, nil, false)
	signed := CompositeScore(2, RouteSourceAnnouncement, nil, true)
	if signed <= unsigned {
		t.Fatalf("verified-signed must rank strictly above unsigned at the same tier; got signed=%f unsigned=%f", signed, unsigned)
	}
	if signed-unsigned != scoreSignedBonus {
		t.Fatalf("bonus magnitude wrong: got %f want %f", signed-unsigned, scoreSignedBonus)
	}
}

func TestCompositeScore_SignedBonusDoesNotOverpowerExtraHop(t *testing.T) {
	// Verified signed at hops=2 MUST stay below unsigned at hops=1 —
	// signing is a tie-break, not a hop saver. The contract is
	// scoreSignedBonus < scoreHopPenalty.
	signedFar := CompositeScore(2, RouteSourceAnnouncement, nil, true)
	unsignedNear := CompositeScore(1, RouteSourceAnnouncement, nil, false)
	if signedFar >= unsignedNear {
		t.Fatalf("signed-bonus must not overpower one extra hop: signedFar=%f unsignedNear=%f", signedFar, unsignedNear)
	}
}

func TestCompositeScore_SignedBonusDoesNotRescueQuestionable(t *testing.T) {
	// Strict-tier invariant: a verified-signed Questionable pair must
	// still rank below an unsigned Good pair. The signed bonus is
	// scoreSignedBonus=5; the Questionable penalty is
	// scoreHealthQPenalty=250.
	good := &RouteHealthState{Health: HealthGood}
	questionable := &RouteHealthState{Health: HealthQuestionable}
	signedQ := CompositeScore(1, RouteSourceAnnouncement, questionable, true)
	unsignedGood := CompositeScore(3, RouteSourceAnnouncement, good, false)
	if signedQ >= unsignedGood {
		t.Fatalf("signed-bonus must not rescue Questionable above Good: signedQ=%f unsignedGood=%f", signedQ, unsignedGood)
	}
}

func TestCompositeScore_DeadStillReturnsExcludedRegardlessOfSig(t *testing.T) {
	// Dead is a hard exclusion — the signed bonus must not unfilter it.
	dead := &RouteHealthState{Health: HealthDead}
	if got := CompositeScore(1, RouteSourceDirect, dead, true); got != scoreExcluded {
		t.Fatalf("Dead with verified sig must still return scoreExcluded; got %f", got)
	}
}
