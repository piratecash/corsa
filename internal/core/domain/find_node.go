package domain

// FindNodeRequestID is local to one proven session and one direction.
type FindNodeRequestID uint64

// FindNodeChallenge is public, fresh lookup material, not an identity proof.
type FindNodeChallenge [32]byte

// FindNodeSignature is the target's signature over the answer transcript.
type FindNodeSignature [64]byte
