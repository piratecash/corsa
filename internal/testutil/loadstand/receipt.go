package loadstand

import "github.com/piratecash/corsa/internal/core/domain"

// Receipt proves a normal SDK decrypted delivery without exporting its body or
// any identity private material. It is functional smoke evidence, not a rate.
type Receipt struct {
	ID         domain.MessageID    `json:"id"`
	Sender     domain.PeerIdentity `json:"sender"`
	Recipient  domain.PeerIdentity `json:"recipient"`
	BodySHA256 string              `json:"body_sha256"`
}
