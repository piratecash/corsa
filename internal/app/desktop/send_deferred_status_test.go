package desktop

import (
	"errors"
	"fmt"
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestSendDeferredStatusIsLocalisedEverywhere: this is the one sentence
// of the deletion feature a user reads when the store refuses a SEND, and
// it has to arrive in their language like every other status of it. The
// key must exist in every catalogue, and none of them may fall through to
// the English text by accident.
func TestSendDeferredStatusIsLocalisedEverywhere(t *testing.T) {
	english, ok := messages["en"]["status.send_deferred"]
	if !ok || english == "" {
		t.Fatal("status.send_deferred is missing from the English catalogue")
	}
	for lang, catalogue := range messages {
		text, ok := catalogue["status.send_deferred"]
		if !ok || text == "" {
			t.Errorf("%s has no status.send_deferred", lang)
			continue
		}
		if lang != "en" && text == english {
			t.Errorf("%s carries the English text verbatim: %q", lang, text)
		}
	}
}

// TestDeferredSendIsRecognisedThroughTheWrappedError: the UI decides the
// wording from the error alone, and the error reaches it wrapped — the
// service layer names the node's detail and wraps the sentinel. A
// comparison that only worked on a bare sentinel would silently stop
// recognising the case.
func TestDeferredSendIsRecognisedThroughTheWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("send refused: %s: %w", "the refusals of deleted ids are unreadable", protocol.ErrStoreDeferred)
	if !errors.Is(wrapped, protocol.ErrStoreDeferred) {
		t.Fatal("a wrapped deferral is no longer recognised")
	}
	if errors.Is(errors.New("some other failure"), protocol.ErrStoreDeferred) {
		t.Fatal("an unrelated failure reads as a deferral")
	}
}
