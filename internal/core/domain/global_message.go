package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

type MessageTopic string

const GlobalMessageTopic MessageTopic = "global"

type GlobalMessageBody string

var ErrInvalidGlobalMessage = errors.New("invalid global message")
var ErrGlobalPublisherUnavailable = errors.New("global publisher unavailable")

func CanonicalGlobalMessageBody(body string) (GlobalMessageBody, error) {
	if !utf8.ValidString(body) {
		return "", ErrInvalidGlobalMessage
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return "", ErrInvalidGlobalMessage
	}
	return GlobalMessageBody(body), nil
}

type OutgoingGlobalMessage struct{ Body GlobalMessageBody }
type GlobalMessage struct {
	ID            MessageID         `json:"id"`
	Topic         MessageTopic      `json:"topic"`
	ClaimedSender PeerIdentity      `json:"claimed_sender"`
	Body          GlobalMessageBody `json:"body"`
	CreatedAt     time.Time         `json:"created_at"`
}
type GlobalPublicationOutcome string

const GlobalStoredLocally GlobalPublicationOutcome = "stored_locally"

type GlobalPublication struct {
	Message GlobalMessage            `json:"message"`
	Outcome GlobalPublicationOutcome `json:"outcome"`
}
