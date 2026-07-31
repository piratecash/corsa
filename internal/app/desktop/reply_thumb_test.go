package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

func TestIsImageFileAnnounce(t *testing.T) {
	t.Parallel()

	imagePayload := `{"file_name":"cat.png","file_size":123,"content_type":"image/png","file_hash":"abc"}`
	webpPayload := `{"file_name":"cat.webp","file_size":123,"content_type":"image/webp","file_hash":"abc"}`
	pdfPayload := `{"file_name":"doc.pdf","file_size":123,"content_type":"application/pdf","file_hash":"abc"}`

	cases := []struct {
		name        string
		command     domain.DMCommand
		commandData string
		want        bool
	}{
		{"png announce", domain.DMCommandFileAnnounce, imagePayload, true},
		{"webp announce", domain.DMCommandFileAnnounce, webpPayload, true},
		{"non-image announce", domain.DMCommandFileAnnounce, pdfPayload, false},
		{"plain message", "", imagePayload, false},
		{"empty payload", domain.DMCommandFileAnnounce, "", false},
		{"broken payload", domain.DMCommandFileAnnounce, "{not json", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isImageFileAnnounce(tc.command, tc.commandData); got != tc.want {
				t.Errorf("isImageFileAnnounce(%q, %q) = %v, want %v",
					tc.command, tc.commandData, got, tc.want)
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
		id   string
		want bool
	}{
		{"text", false},
		{"img", true},
		{"pdf", false},
	}
	for _, tc := range cases {
		cm, ok := w.findCachedMsg(tc.id)
		if !ok {
			t.Fatalf("findCachedMsg(%q) not found", tc.id)
		}
		if cm.IsImageFile != tc.want {
			t.Errorf("IsImageFile(%q) = %v, want %v", tc.id, cm.IsImageFile, tc.want)
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
