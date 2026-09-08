package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestImageAnnounce(t *testing.T) {
	t.Parallel()

	imagePayload := `{"file_name":"cat.png","file_size":123,"content_type":"image/png","file_hash":"abc"}`
	webpPayload := `{"file_name":"cat.webp","file_size":123,"content_type":"image/webp","file_hash":"def"}`
	pdfPayload := `{"file_name":"doc.pdf","file_size":123,"content_type":"application/pdf","file_hash":"abc"}`

	cases := []struct {
		name        string
		command     domain.DMCommand
		commandData string
		wantHash    string
		want        bool
	}{
		{"png announce", domain.DMCommandFileAnnounce, imagePayload, "abc", true},
		{"webp announce", domain.DMCommandFileAnnounce, webpPayload, "def", true},
		{"non-image announce", domain.DMCommandFileAnnounce, pdfPayload, "", false},
		{"plain message", "", imagePayload, "", false},
		{"empty payload", domain.DMCommandFileAnnounce, "", "", false},
		{"broken payload", domain.DMCommandFileAnnounce, "{not json", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hash, got := imageAnnounce(tc.command, tc.commandData)
			if got != tc.want {
				t.Errorf("imageAnnounce(%q, %q) = %v, want %v",
					tc.command, tc.commandData, got, tc.want)
			}
			// The hash is what tells this picture from the next one to
			// occupy its file name, so a verdict without it is useless.
			if hash != tc.wantHash {
				t.Errorf("imageAnnounce(%q, %q) hash = %q, want %q",
					tc.command, tc.commandData, hash, tc.wantHash)
			}
		})
	}
}

func TestRebuildMsgCacheSetsIsImageFile(t *testing.T) {
	t.Parallel()

	imagePayload := `{"file_name":"cat.png","file_size":123,"content_type":"image/png","file_hash":"abc"}`
	pdfPayload := `{"file_name":"doc.pdf","file_size":123,"content_type":"application/pdf","file_hash":"abc"}`

	w := &Window{
		snap: service.RouterSnapshot{
			ActiveMessages: []service.DirectMessage{
				{ID: "text", Body: "hello"},
				{
					ID: "img", Body: domain.FileDMBodySentinel,
					Command: domain.DMCommandFileAnnounce, CommandData: imagePayload,
				},
				{
					ID: "pdf", Body: domain.FileDMBodySentinel,
					Command: domain.DMCommandFileAnnounce, CommandData: pdfPayload,
				},
			},
		},
	}
	w.rebuildMsgCache()

	cases := []struct {
		id       string
		wantHash string
		want     bool
	}{
		{"text", "", false},
		{"img", "abc", true},
		{"pdf", "", false},
	}
	for _, tc := range cases {
		cm, ok := w.findCachedMsg(tc.id)
		if !ok {
			t.Fatalf("findCachedMsg(%q) not found", tc.id)
		}
		if cm.IsImageFile != tc.want {
			t.Errorf("IsImageFile(%q) = %v, want %v", tc.id, cm.IsImageFile, tc.want)
		}
		// The reply quote hands this to the thumbnail cache; without it
		// the quote would draw whatever last answered to the file name.
		if cm.FileHash != tc.wantHash {
			t.Errorf("FileHash(%q) = %q, want %q", tc.id, cm.FileHash, tc.wantHash)
		}
	}
}

func TestReplyBodyForDisplay(t *testing.T) {
	t.Parallel()

	translate := func(key string, _ ...any) string { return "<" + key + ">" }

	cases := []struct {
		name        string
		body        string
		isImageFile bool
		want        string
	}{
		{"image without caption", domain.FileDMBodySentinel, true, "<chat.photo_label>"},
		{"image with caption", "look at this", true, "look at this"},
		{"non-image file keeps sentinel", domain.FileDMBodySentinel, false, domain.FileDMBodySentinel},
		{"plain text", "hello", false, "hello"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := replyBodyForDisplay(tc.body, tc.isImageFile, translate); got != tc.want {
				t.Errorf("replyBodyForDisplay(%q, %v) = %q, want %q",
					tc.body, tc.isImageFile, got, tc.want)
			}
		})
	}
}
