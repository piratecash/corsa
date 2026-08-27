package domain

import (
	"encoding/json"
	"errors"
)

// conversation_delete.go carries the wire contract of "clear this chat for
// both of us": what the request says, what the answer says, and what the
// sender is allowed to conclude from it.
//
// The request is deliberately almost empty. It names the conversation only by
// being addressed to the peer, and it names no messages at all — the ids of a
// thread are the one thing a wipe must not put on the wire, both because some
// of them may never have reached the peer and because the request has to
// outlive the rows it came from.

// ConversationDeletePayload is the body of a DMCommandConversationDelete: the
// id of the gesture, echoed by the ack, and nothing else.
//
// There is no moment in it, and that is a decision rather than an omission.
// The request means "erase this conversation", full stop: whatever the
// receiver holds when it arrives, goes. It carried a boundary once — the
// moment of the click — so that a repeat could not reach messages written
// since; the boundary was a timestamp from one machine's clock compared
// against rows stamped by another's, and every way of reconciling those two
// clocks was wrong in one direction or the other. Removing it removes the
// problem, and the cost is stated in the same breath: a request that arrives
// again takes whatever is there again.
//
// Authorization is the envelope, and only the envelope. The body is sealed to
// the recipient's box key and the sender's identity is signed with the
// sender's key, so a request that decrypts and verifies came from the one peer
// this conversation is with. Nothing else is consulted — not authorship of the
// rows, not a clock, not a memory of previous requests.
type ConversationDeletePayload struct {
	RequestID ConversationDeleteRequestID `json:"request_id"`
}

// Valid reports whether the payload can be answered. A request with no id could
// be acked, but the ack would settle nothing in particular.
func (p ConversationDeletePayload) Valid() bool {
	return p.RequestID != ""
}

// MarshalConversationDeletePayload renders the request for CommandData.
func MarshalConversationDeletePayload(p ConversationDeletePayload) (string, error) {
	if !p.Valid() {
		return "", errors.New("conversation_delete payload: a request id is required")
	}
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ConversationDeleteStatus is the outcome the peer reports for a whole-thread
// wipe.
//
// There is no "denied" here, and that absence is the design. A per-message
// request can be refused because the message's own flag reserves it to its
// author; a thread wipe is not about any one message, and refusing the ones
// the requester did not write is exactly the behaviour that used to leave each
// side holding half a conversation the user believed was gone.
type ConversationDeleteStatus string

const (
	// ConversationDeleteStatusApplied reports that the peer erased what they
	// held of the conversation. Terminal. A count of zero is still applied:
	// a peer who had nothing left is consistent with us, which is all the
	// request asks for.
	ConversationDeleteStatusApplied ConversationDeleteStatus = "applied"

	// ConversationDeleteStatusError reports that the peer could not decide:
	// its chatlog was unavailable or the transaction failed. NOT terminal —
	// the thread may well still be there, so the sender keeps the request
	// and asks again on the normal backoff.
	ConversationDeleteStatusError ConversationDeleteStatus = "error"
)

// Valid reports whether the status is one of the recognised outcomes. An
// empty status is rejected.
func (s ConversationDeleteStatus) Valid() bool {
	switch s {
	case ConversationDeleteStatusApplied, ConversationDeleteStatusError:
		return true
	default:
		return false
	}
}

// IsTransient reports whether the peer failed to decide rather than decided.
// Such an answer settles nothing, so the sender keeps its request.
func (s ConversationDeleteStatus) IsTransient() bool {
	return s == ConversationDeleteStatusError
}

// ConversationDeleteAckPayload is the body of a
// DMCommandConversationDeleteAck: which request is being answered, and how it
// went. Nothing else.
//
// It carried a COUNT of the rows erased, and the count is gone. The whole
// design of the request is that it names no messages, because naming them is
// how a peer learns of messages that never reached them — and the answer gave
// that away from the other end: "I removed three" tells a requester who held
// two that the other side had one they never saw. The status settles the
// request; the number settled nothing and the interface never showed it.
type ConversationDeleteAckPayload struct {
	RequestID ConversationDeleteRequestID `json:"request_id"`
	Status    ConversationDeleteStatus    `json:"status"`
}

// Valid reports whether the ack can settle anything: it must name the request
// it answers and carry a recognised status.
func (p ConversationDeleteAckPayload) Valid() bool {
	return p.RequestID != "" && p.Status.Valid()
}

// MarshalConversationDeleteAckPayload renders the ack for CommandData.
func MarshalConversationDeleteAckPayload(p ConversationDeleteAckPayload) (string, error) {
	if !p.Valid() {
		return "", errors.New("conversation_delete_ack payload: request id and a recognised status are required")
	}
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
