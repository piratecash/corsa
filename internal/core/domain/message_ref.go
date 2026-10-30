package domain

import (
	"errors"
	"strconv"
	"strings"
)

// MessageRef identifies a local record without assuming that its unsigned,
// sender-selected wire ID is globally unique. Topic is an explicit namespace;
// Sender and Recipient are the exact strings covered by envelope verification.
// This does not authenticate the wire ID or provide replay freshness.
type MessageRef struct {
	Topic     string
	ID        string
	Sender    string
	Recipient string
}

// Key is an unambiguous encoding for in-memory indexes and Bloom inputs.
// Length framing keeps attacker-chosen delimiters from merging references.
func (r MessageRef) Key() string {
	var key strings.Builder
	key.Grow(len(r.Topic) + len(r.ID) + len(r.Sender) + len(r.Recipient) + 16)
	for _, value := range []string{r.Topic, r.ID, r.Sender, r.Recipient} {
		key.WriteString(strconv.Itoa(len(value)))
		key.WriteByte(':')
		key.WriteString(value)
	}
	return key.String()
}

// ErrAmbiguousMessage is shared by legacy ID-only local APIs.
var ErrAmbiguousMessage = errors.New("ambiguous message identity")
