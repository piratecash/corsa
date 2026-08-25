package domain

import "fmt"

// dm_control.go names the conversation-control datagram type and the commands
// that travel sealed inside it.
//
// One dtype carries every command that changes conversation state — reactions
// today, deletions and edits later — because the dtype is the ONE part of a
// datagram a relay reads. A separate `reaction` type would name the feature out
// loud to every hop it passes; a shared name says only "these two are talking".
//
// The cost of sharing a name is that "the peer declared dm_control" stops
// proving "the peer understands this command", and that is what
// DMControlUnsupported exists to repair. See
// docs/refactoring/reactions-protocol.md §6.

// DTypeDMControl is the datagram type of a sealed conversation-control command.
const DTypeDMControl DType = "dm_control"

// DMControlSchemaVersion is the version of the sealed payload layout. It is
// checked before anything else in the payload is trusted: a future layout is
// refused rather than half-read.
const DMControlSchemaVersion uint32 = 1

// DMControlCommand is what one sealed payload asks the receiver to do.
type DMControlCommand string

const (
	// DMControlReactions carries a batch of reaction facts.
	DMControlReactions DMControlCommand = "reactions"
	// DMControlUnsupported reports a command the receiver did not understand,
	// naming it so the sender can stop offering that feature to this peer.
	//
	// It is a command of dm_control rather than a dtype of its own, and it is
	// padded and delayed exactly like an ordinary command, because a refusal is
	// the loudest thing on the wire: a rare frame of unusual shape right after
	// a known one lets a relay map who supports what without decrypting a byte.
	DMControlUnsupported DMControlCommand = "unsupported"
)

func (c DMControlCommand) String() string { return string(c) }

// ConversationKind says how the payload names the conversation it belongs to.
//
// The conversation id is NOT carried for a one-to-one chat, and that is not an
// omission: each side calls the conversation by the OTHER side's identity, so a
// carried id would be the sender's name for it and wrong on arrival. The
// receiver derives it from the signed sender instead. A group id is the same on
// every node and will be carried.
type ConversationKind string

const (
	// ConversationDirect is a one-to-one chat: the conversation is the pair,
	// and each side resolves it from its counterpart's identity.
	ConversationDirect ConversationKind = "direct"
	// ConversationGroup is a named group. Reserved: no group exists yet, and a
	// payload claiming one is refused rather than guessed at.
	ConversationGroup ConversationKind = "group"
)

// ResolveConversation turns the conversation kind on the wire into the local
// scope facts are stored under.
//
// Sender is the signed source of the frame — the only identity a receiver may
// attribute anything to, and for a direct chat the whole of the answer.
func ResolveConversation(kind ConversationKind, sender PeerIdentity) (ReactionScope, error) {
	switch kind {
	case ConversationDirect:
		if sender.IsZero() {
			return "", fmt.Errorf("domain: a direct conversation needs a signed sender")
		}
		return ReactionScopeForPeer(sender), nil
	case ConversationGroup:
		return "", fmt.Errorf("domain: group conversations are not supported yet")
	default:
		return "", fmt.Errorf("domain: unknown conversation kind %q", kind)
	}
}
