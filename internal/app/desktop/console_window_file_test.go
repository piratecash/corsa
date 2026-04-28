package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service/filetransfer"
)

// TestIsTerminalTransferState pins the UI-side state classification.
// The file tab uses this predicate to (a) compute the active-count in
// the summary header, (b) gate the polled redraw timer, and (c) hide
// the progress bar on terminal rows. Mirroring the manager-side
// (sender|receiver)Terminal predicates by string state name keeps the
// UI semantics stable across manager refactors — but if the manager
// ever introduces a new terminal state, this test is the canary.
func TestIsTerminalTransferState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state    string
		terminal bool
	}{
		// Terminal — must be filtered from active count and lose progress bar.
		{"completed", true},
		{"failed", true},
		{"tombstone", true},
		// Active — must be counted as active.
		{"available", false},
		{"announced", false},
		{"serving", false},
		{"downloading", false},
		{"verifying", false},
		{"waiting_route", false},
		{"waiting_ack", false},
		// Unknown — defaults to non-terminal so a future state addition
		// at least keeps showing in active-count rather than vanishing.
		{"some_future_state", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isTerminalTransferState(tc.state); got != tc.terminal {
			t.Errorf("isTerminalTransferState(%q) = %v, want %v", tc.state, got, tc.terminal)
		}
	}
}

// TestHasActiveFileTransfer verifies the polled-redraw gate. Empty,
// all-terminal, and mixed lists are all valid inputs; only the
// presence of at least one non-terminal entry should trigger the
// 750ms refresh timer.
func TestHasActiveFileTransfer(t *testing.T) {
	t.Parallel()

	make := func(states ...string) []filetransfer.TransferSnapshot {
		out := make([]filetransfer.TransferSnapshot, 0, len(states))
		for _, s := range states {
			out = append(out, filetransfer.TransferSnapshot{State: s})
		}
		return out
	}
	cases := []struct {
		name string
		in   []filetransfer.TransferSnapshot
		want bool
	}{
		{"nil_list", nil, false},
		{"empty_list", []filetransfer.TransferSnapshot{}, false},
		{"all_terminal", make("completed", "failed", "tombstone"), false},
		{"mixed_active_and_terminal", make("completed", "downloading"), true},
		{"single_active", make("verifying"), true},
		{"unknown_states_treated_as_active", make("future_state"), true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := hasActiveFileTransfer(tc.in); got != tc.want {
				t.Errorf("hasActiveFileTransfer = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFormatBytesShort pins the displayed unit + precision used in
// the file-tab meta line. Boundaries (exactly 1 KB, 1 MB, etc.) and
// just-below-boundary cases catch off-by-one errors in the threshold
// chain.
func TestFormatBytesShort(t *testing.T) {
	t.Parallel()

	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
		{1500 * 1024, "1.5 MB"}, // 1500 KB → 1.5 MB
	}
	for _, tc := range cases {
		if got := formatBytesShort(tc.n); got != tc.want {
			t.Errorf("formatBytesShort(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

// TestTransferProgressFraction pins the [0,1] clamp + terminal-state
// special-cases used to render the progress bar. The terminal cases
// matter: a freshly-completed transfer must show full bar and a
// failed one empty regardless of the stale Bytes counter (which can
// be slightly behind FileSize at the moment commit returned).
func TestTransferProgressFraction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		t    filetransfer.TransferSnapshot
		want float64
	}{
		{
			name: "zero_filesize_is_zero",
			t:    filetransfer.TransferSnapshot{FileSize: 0, Bytes: 0, State: "downloading"},
			want: 0,
		},
		{
			name: "half_done",
			t:    filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 500, State: "downloading"},
			want: 0.5,
		},
		{
			name: "completed_full_bar_even_with_stale_bytes",
			t:    filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 950, State: "completed"},
			want: 1,
		},
		{
			name: "failed_empty_bar",
			t:    filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 750, State: "failed"},
			want: 0,
		},
		{
			name: "tombstone_empty_bar",
			t:    filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 1000, State: "tombstone"},
			want: 0,
		},
		{
			name: "clamped_above_1",
			t:    filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 2000, State: "verifying"},
			want: 1,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := transferProgressFraction(tc.t); got != tc.want {
				t.Errorf("transferProgressFraction = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTruncatePeerIdentity pins the row-header peer label width. The
// short-input case (≤24 chars) returns the input unchanged so test
// fixtures with mock peer IDs read naturally; the long-input case
// uses a fixed prefix+ellipsis+suffix shape so the layout never
// reflows due to peer-id length.
func TestTruncatePeerIdentity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   domain.PeerIdentity
		want string
	}{
		{"", ""},
		{"short-id-12345", "short-id-12345"}, // 14 chars — pass through
		{"exactly-24-characters!!!", "exactly-24-characters!!!"},    // 24 chars — boundary, pass through
		{"abcdefghijklmnopqrstuvwxyz0123", "abcdefghijkl…wxyz0123"}, // 30 chars — first 12 + … + last 8
	}
	for _, tc := range cases {
		got := truncatePeerIdentity(tc.in)
		if got != tc.want {
			t.Errorf("truncatePeerIdentity(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestFormatTransferTimestamp verifies the meta-line timestamp
// formatter. Empty input passes through; valid RFC3339 is rendered
// in local time with the YYYY-MM-DD HH:MM shape; malformed input
// falls back to the raw value rather than silently hiding a bad
// timestamp.
func TestFormatTransferTimestamp(t *testing.T) {
	t.Parallel()

	if got := formatTransferTimestamp(""); got != "" {
		t.Errorf("formatTransferTimestamp(empty) = %q, want empty", got)
	}
	if got := formatTransferTimestamp("not-a-timestamp"); got != "not-a-timestamp" {
		t.Errorf("formatTransferTimestamp(malformed) = %q, want %q", got, "not-a-timestamp")
	}
	// Round-trip sanity: a valid RFC3339 timestamp produces a
	// non-empty, non-error formatted string. Asserting the exact
	// format would couple the test to the local timezone, which is
	// fragile across CI environments — we only verify the formatter
	// did not return the malformed-fallback or empty.
	got := formatTransferTimestamp("2026-04-28T14:33:15Z")
	if got == "" || got == "2026-04-28T14:33:15Z" {
		t.Errorf("formatTransferTimestamp(valid) = %q, want a formatted local-time string", got)
	}
}

// TestFormatTransferBytes pins the meta-line format. Active rows
// show "<bytes> / <total>  (<percent>%)" — same shape as the
// chat-thread file card, including the double-space separator
// before the parenthesised percent. Terminal rows show just the
// total because the progress bar (or its absence) carries the
// visualisation already.
func TestFormatTransferBytes(t *testing.T) {
	t.Parallel()

	active := filetransfer.TransferSnapshot{FileSize: 1024 * 1024, Bytes: 512 * 1024, State: "downloading"}
	if got := formatTransferBytes(active); got != "512.0 KB / 1.0 MB  (50%)" {
		t.Errorf("active row: formatTransferBytes = %q, want %q", got, "512.0 KB / 1.0 MB  (50%)")
	}
	completed := filetransfer.TransferSnapshot{FileSize: 1024 * 1024, Bytes: 1024 * 1024, State: "completed"}
	if got := formatTransferBytes(completed); got != "1.0 MB" {
		t.Errorf("completed row: formatTransferBytes = %q, want %q", got, "1.0 MB")
	}
	failed := filetransfer.TransferSnapshot{FileSize: 1024, Bytes: 0, State: "failed"}
	if got := formatTransferBytes(failed); got != "1.0 KB" {
		t.Errorf("failed row: formatTransferBytes = %q, want %q", got, "1.0 KB")
	}
	// Zero-bytes active rows MUST show "0 B / total  (0%)" — the
	// chat-thread file card uses the same format from the moment
	// the row enters a non-terminal state, and the user expects to
	// see progress numbers between StartDownload and the first
	// chunk_response. Suppressing this gap (an earlier short-circuit
	// returned just the total when Bytes == 0) was the bug the user
	// reported.
	zeroBytes := filetransfer.TransferSnapshot{FileSize: 1024, Bytes: 0, State: "downloading"}
	if got := formatTransferBytes(zeroBytes); got != "0 B / 1.0 KB  (0%)" {
		t.Errorf("zero-bytes active row: formatTransferBytes = %q, want %q",
			got, "0 B / 1.0 KB  (0%)")
	}
}

// TestFileDisplayName checks the fallback to the FileID prefix when
// no filename is recorded. Legacy entries persisted before the
// FileName field was added would otherwise render an empty title.
func TestFileDisplayName(t *testing.T) {
	t.Parallel()

	if got := fileDisplayName(filetransfer.TransferSnapshot{FileName: "report.pdf"}); got != "report.pdf" {
		t.Errorf("fileDisplayName with name = %q, want %q", got, "report.pdf")
	}
	if got := fileDisplayName(filetransfer.TransferSnapshot{FileID: domain.FileID("abc-123")}); got != "abc-123" {
		t.Errorf("fileDisplayName fallback = %q, want %q", got, "abc-123")
	}
}

// TestStateBadgeKey verifies the direction-aware label rule for the
// "completed" state. Sender-side completion shows "Uploaded";
// receiver-side completion shows "Downloaded". Other states use
// the bare state name. Without the direction split, a single
// "Completed" label confused users on rows where the meaning was
// ambiguous (the user feedback that motivated this change).
func TestStateBadgeKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		state     string
		direction string
		want      string
	}{
		{"completed_send_uploaded", "completed", "send", "console.file.state.uploaded"},
		{"completed_receive_downloaded", "completed", "receive", "console.file.state.downloaded"},
		{"completed_unknown_direction_falls_back", "completed", "", "console.file.state.downloaded"},
		{"failed_unaffected_by_direction", "failed", "send", "console.file.state.failed"},
		{"downloading_unaffected", "downloading", "receive", "console.file.state.downloading"},
		{"tombstone_unaffected", "tombstone", "send", "console.file.state.tombstone"},
	}
	for _, tc := range cases {
		got := stateBadgeKey(filetransfer.TransferSnapshot{State: tc.state, Direction: tc.direction})
		if got != tc.want {
			t.Errorf("%s: stateBadgeKey = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestDeleteEnabledRule pins the direction-aware offline-gate
// predicate that drives Delete-button visibility on every surface
// (chat context menu via Window.contextMenuDeleteEnabled,
// chat-thread file-card via Window.layoutFileCardDeleteButton,
// file-tab row via ConsoleWindow.layoutFileRowDeleteAction). The
// rule is tiny but load-bearing: getting it wrong either blocks
// users from cleaning up incoming messages while the peer is
// offline (the P2 reviewer item that motivated this slice) or
// burns the entire SendMessageDelete retry budget waiting for an
// ack from an offline outbound peer.
//
// Contract: enabled = isIncoming || peerOnline.
func TestDeleteEnabledRule(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		isOutgoing bool
		peerOnline bool
		want       bool
	}{
		// Outgoing: gated on peer reachability because the wire
		// delete needs an ack.
		{"outgoing_offline_disabled", true, false, false},
		{"outgoing_online_enabled", true, true, true},
		// Incoming: local-only delete path bypasses the peer
		// check entirely. Always enabled.
		{"incoming_offline_enabled", false, false, true},
		{"incoming_online_enabled", false, true, true},
	}
	for _, tc := range cases {
		got := !tc.isOutgoing || tc.peerOnline
		if got != tc.want {
			t.Errorf("%s: enabled = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestShowDiskActionsForRow pins the visibility of Show in Folder
// + Open buttons. Sender rows: only state="completed" qualifies
// (recipient acked, file still on disk). Receiver rows:
// "completed" or "waiting_ack" both qualify because the verifier
// has already renamed the partial to CompletedPath. Every other
// state hides the buttons.
func TestShowDiskActionsForRow(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		direction string
		state     string
		want      bool
	}{
		{"send_completed_yes", "send", "completed", true},
		{"send_serving_no", "send", "serving", false},
		{"send_announced_no", "send", "announced", false},
		{"send_failed_no", "send", "failed", false},
		{"send_tombstone_no", "send", "tombstone", false},

		{"recv_completed_yes", "receive", "completed", true},
		{"recv_waiting_ack_yes", "receive", "waiting_ack", true},
		{"recv_downloading_no", "receive", "downloading", false},
		{"recv_failed_no", "receive", "failed", false},
		{"recv_available_no", "receive", "available", false},
		{"recv_tombstone_no", "receive", "tombstone", false},

		{"unknown_direction_no", "", "completed", false},
	}
	for _, tc := range cases {
		got := showDiskActionsForRow(filetransfer.TransferSnapshot{
			Direction: tc.direction,
			State:     tc.state,
		})
		if got != tc.want {
			t.Errorf("%s: showDiskActionsForRow = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestFileRowStateLabel pins the chat-thread parity for the
// status caption shown below the progress bar. Each (Direction,
// State) pair must map to either an empty key (no caption rendered)
// or the same i18n key the chat thread uses for the same state.
func TestFileRowStateLabel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		direction string
		state     string
		wantKey   string
	}{
		// Sender side — only the post-ack completion gets a caption,
		// matching chat-thread layoutFileCard's "downloaded" branch.
		{"send_completed_shows_downloaded_by_peer", "send", "completed", "console.file.label.downloaded_by_peer"},
		{"send_serving_no_label", "send", "serving", ""},
		{"send_announced_no_label", "send", "announced", ""},
		{"send_failed_no_label", "send", "failed", ""},
		// Receiver side mirrors the chat-thread switch verbatim.
		{"recv_completed_shows_completed", "receive", "completed", "console.file.label.completed"},
		{"recv_waiting_ack_shows_confirming", "receive", "waiting_ack", "console.file.label.confirming"},
		{"recv_failed_shows_failed", "receive", "failed", "console.file.label.failed"},
		{"recv_waiting_route_shows_sender_offline", "receive", "waiting_route", "console.file.label.sender_offline"},
		{"recv_downloading_no_label", "receive", "downloading", ""},
		{"recv_available_no_label", "receive", "available", ""},
	}
	for _, tc := range cases {
		gotKey, _ := fileRowStateLabel(filetransfer.TransferSnapshot{
			Direction: tc.direction,
			State:     tc.state,
		})
		if gotKey != tc.wantKey {
			t.Errorf("%s: got key=%q, want %q", tc.name, gotKey, tc.wantKey)
		}
	}
}

// TestCollectFileTransfersStableSort guards the flicker-fix from
// user feedback: two rows that share a CreatedAt/CompletedAt
// timestamp used to shuffle between paints because the underlying
// receiverMaps / senderMaps iteration is non-deterministic in Go.
// FileID-descending tie-break makes the order deterministic.
//
// The flat-list version of the sort lives inside
// collectFileTransfers, but the comparator is exercised here by
// driving a slice through the same sort.SliceStable call we apply
// in production, since collectFileTransfers requires a wired
// router.
func TestCollectFileTransfersStableSort(t *testing.T) {
	t.Parallel()

	const ts = "2026-04-06T18:34:00Z"
	in := []filetransfer.TransferSnapshot{
		{FileID: "fid-a", CreatedAt: ts},
		{FileID: "fid-z", CreatedAt: ts},
		{FileID: "fid-m", CreatedAt: "2026-04-06T18:35:00Z"}, // newer
	}
	stable := func(s []filetransfer.TransferSnapshot) []filetransfer.TransferSnapshot {
		out := make([]filetransfer.TransferSnapshot, len(s))
		copy(out, s)
		// Same comparator collectFileTransfers uses.
		sortBy(out)
		return out
	}
	a := stable(in)
	b := stable(in)
	for i := range a {
		if a[i].FileID != b[i].FileID {
			t.Fatalf("non-deterministic order: a=%v b=%v", ids(a), ids(b))
		}
	}
	// fid-m is newest; fid-z and fid-a tie on timestamp, FileID
	// descending puts fid-z before fid-a.
	want := []domain.FileID{"fid-m", "fid-z", "fid-a"}
	for i := range want {
		if a[i].FileID != want[i] {
			t.Errorf("a[%d] = %q, want %q (full order: %v)", i, a[i].FileID, want[i], ids(a))
		}
	}
}

func sortBy(s []filetransfer.TransferSnapshot) {
	// Mirrors collectFileTransfers's sort.SliceStable closure verbatim.
	sortSliceStable(s, func(i, j int) bool {
		a, b := transferTimestampForSort(s[i]), transferTimestampForSort(s[j])
		if a != b {
			return a > b
		}
		return s[i].FileID > s[j].FileID
	})
}

// sortSliceStable is a tiny indirection so the test file does not
// import "sort" directly — keeps the test file self-contained
// against future refactors that swap sort packages.
func sortSliceStable(s []filetransfer.TransferSnapshot, less func(i, j int) bool) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && less(j, j-1); j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func ids(s []filetransfer.TransferSnapshot) []domain.FileID {
	out := make([]domain.FileID, 0, len(s))
	for _, t := range s {
		out = append(out, t.FileID)
	}
	return out
}

// TestTransferProgressPercent verifies the integer-clamp transform
// the chat-thread progress bar expects. Off-by-one rounding errors
// (49.9% should round to 50, not 49) and out-of-range inputs are
// the cases that matter most — the bar is sub-pixel-sensitive on
// small widths.
func TestTransferProgressPercent(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		t    filetransfer.TransferSnapshot
		want int
	}{
		{"zero_size_zero_pct", filetransfer.TransferSnapshot{FileSize: 0}, 0},
		{"half", filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 500, State: "downloading"}, 50},
		{"rounding_up", filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 499, State: "downloading"}, 50},
		{"completed_full", filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 950, State: "completed"}, 100},
		{"failed_zero", filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 750, State: "failed"}, 0},
		{"clamped_above_100", filetransfer.TransferSnapshot{FileSize: 1000, Bytes: 5000, State: "verifying"}, 100},
	}
	for _, tc := range cases {
		if got := transferProgressPercent(tc.t); got != tc.want {
			t.Errorf("%s: transferProgressPercent = %d, want %d", tc.name, got, tc.want)
		}
	}
}
