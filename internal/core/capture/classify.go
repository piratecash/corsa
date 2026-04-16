package capture

import (
	"encoding/json"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ClassifyPayload determines the PayloadKind of a raw wire line.
//
// Classification rules:
//   - starts with '{' or '[' and valid JSON → PayloadKindJSON
//   - starts with '{' or '[' but invalid    → PayloadKindInvalidJSON
//   - empty string                          → PayloadKindMalformed
//   - anything else                         → PayloadKindNonJSON
//
// PayloadKindFrameTooLarge is never returned here — it is assigned by
// the caller when the frame was truncated before reaching capture.
func ClassifyPayload(raw string) domain.PayloadKind {
	if len(raw) == 0 {
		return domain.PayloadKindMalformed
	}
	if raw[0] == '{' || raw[0] == '[' {
		if json.Valid([]byte(raw)) {
			return domain.PayloadKindJSON
		}
		return domain.PayloadKindInvalidJSON
	}
	return domain.PayloadKindNonJSON
}
