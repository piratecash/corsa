package desktop

import (
	"fmt"
	"image"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/service"

	"gioui.org/f32"
	"gioui.org/gpu/headless"
	"gioui.org/io/input"
	"gioui.org/io/key"
	"gioui.org/io/pointer"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/text"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
)

func TestMergeRecipientOrder(t *testing.T) {
	recipients := []domain.PeerIdentity{domaintest.ID("a"), domaintest.ID("b"), domaintest.ID("c"), domaintest.ID("d")}
	order := []domain.PeerIdentity{domaintest.ID("c"), domaintest.ID("a")}

	merged := mergeRecipientOrder(recipients, order)

	// "c" and "a" should come first (in order), then "b" and "d" (sorted).
	if len(merged) != 4 {
		t.Fatalf("expected 4, got %d: %v", len(merged), merged)
	}
	if merged[0] != domaintest.ID("c") || merged[1] != domaintest.ID("a") {
		t.Fatalf("expected [c, a, ...], got %v", merged)
	}
}

func TestMergeRecipientOrderEmpty(t *testing.T) {
	merged := mergeRecipientOrder(nil, []domain.PeerIdentity{domaintest.ID("a")})
	if merged != nil {
		t.Fatalf("expected nil for empty recipients, got %v", merged)
	}
}

func TestSearchKnownIdentities(t *testing.T) {
	// knownIDs must be valid 40-char hex so searchKnownIdentities can
	// decode them via PeerIdentityFromWire. The query "abc" is matched
	// against the raw hex text, so each ID embeds (or omits) that substring.
	const (
		listedHex  = "abc0000000000000000000000000000000000000" // already listed, contains "abc"
		matchHex   = "00abc00000000000000000000000000000000000" // contains "abc" → included
		noMatchHex = "ffffffffffffffffffffffffffffffffffffffff" // no "abc"
		selfHex    = "1111111111111111111111111111111111111111"
	)
	knownIDs := []string{listedHex, matchHex, noMatchHex}
	recipients := []domain.PeerIdentity{domain.PeerIdentityFromWire(listedHex)} // already listed
	self := domain.PeerIdentityFromWire(selfHex)

	results := searchKnownIdentities(knownIDs, nil, recipients, self, "abc")

	// listedHex is already listed → excluded.
	// matchHex matches query → included.
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}
	if results[0] != domain.PeerIdentityFromWire(matchHex) {
		t.Fatalf("expected %s, got %s", domain.PeerIdentityFromWire(matchHex), results[0])
	}
}

func TestSearchKnownIdentitiesEmptyQuery(t *testing.T) {
	results := searchKnownIdentities([]string{"a", "b"}, nil, nil, domaintest.ID("self"), "")
	if results != nil {
		t.Fatalf("expected nil for empty query, got %v", results)
	}
}

func newIdentityLayoutTestWindow(t *testing.T) *Window {
	t.Helper()
	icons, err := loadWindowIcons()
	if err != nil {
		t.Fatalf("load embedded UI icons: %v", err)
	}
	return &Window{
		theme:                    newAppTheme(),
		language:                 "en",
		recipientButtons:         make(map[domain.PeerIdentity]*widget.Clickable),
		recipientRightClick:      make(map[domain.PeerIdentity]*rightClickState),
		recipientMenuBtns:        make(map[domain.PeerIdentity]*widget.Clickable),
		identityPanelList:        layout.List{Axis: layout.Vertical},
		searchIcon:               icons.search,
		fingerprintIcon:          icons.fingerprint,
		personIcon:               icons.person,
		personOutlineIcon:        icons.personOutline,
		peerLastOnlineByIdentity: make(map[domain.PeerIdentity]time.Time),
		chevronIcon:              icons.chevron,
		copyIcon:                 icons.copy,
		shareIcon:                icons.share,
		closeIcon:                icons.close,
		shieldIcon:               icons.shield,
		consoleIcon:              icons.console,
	}
}

func TestLoadUIIconReturnsAnErrorForInvalidData(t *testing.T) {
	if _, err := loadUIIcon("broken-test-icon", []byte("not IconVG")); err == nil {
		t.Fatal("invalid embedded icon data was accepted")
	}
}

func TestContactPresenceStates(t *testing.T) {
	peer := domaintest.ID("presence-peer")
	tests := []struct {
		name   string
		status service.NodeStatus
		want   contactPresenceState
	}{
		{name: "probe unavailable", status: service.NodeStatus{}, want: contactPresenceUnknown},
		{name: "reachable", status: service.NodeStatus{ReachableIDs: map[domain.PeerIdentity]bool{peer: true}}, want: contactPresenceOnline},
		{name: "known unreachable", status: service.NodeStatus{ReachableIDs: map[domain.PeerIdentity]bool{}}, want: contactPresenceOffline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := contactPresence(tt.status, peer); got != tt.want {
				t.Fatalf("contactPresence() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContactPresenceAvatarUsesReferenceSize(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	peer := domaintest.ID("avatar-peer")
	for name, reachable := range map[string]map[domain.PeerIdentity]bool{
		"online":  {peer: true},
		"offline": {},
		"unknown": nil,
	} {
		t.Run(name, func(t *testing.T) {
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Constraints{Max: image.Pt(100, 100)},
			}
			dims := w.layoutContactPresenceAvatar(gtx, service.NodeStatus{ReachableIDs: reachable}, peer)
			if dims.Size != image.Pt(38, 38) {
				t.Fatalf("avatar size = %v, want 38x38", dims.Size)
			}
		})
	}
}

func TestContactPresenceAvatarCentersPersonIcon(t *testing.T) {
	const side = 38
	window, err := headless.NewWindow(side, side)
	if err != nil {
		t.Skipf("no headless GPU context here: %v", err)
	}
	defer window.Release()

	w := newIdentityLayoutTestWindow(t)
	peer := domaintest.ID("centered-avatar-peer")
	ops := new(op.Ops)
	w.layoutContactPresenceAvatar(layout.Context{
		Ops:         ops,
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(side, side)),
	}, service.NodeStatus{ReachableIDs: map[domain.PeerIdentity]bool{}}, peer)
	if err := window.Frame(ops); err != nil {
		t.Fatalf("render avatar: %v", err)
	}
	shot := image.NewRGBA(image.Rect(0, 0, side, side))
	if err := window.Screenshot(shot); err != nil {
		t.Fatalf("screenshot avatar: %v", err)
	}

	ink := brightPixelBounds(shot)
	if ink.Empty() {
		t.Fatal("person icon drew no bright pixels")
	}
	if got, want := ink.Min.X+ink.Max.X, side; got < want-2 || got > want+2 {
		t.Fatalf("person icon spans x %d..%d: centre %.1f, want %.1f", ink.Min.X, ink.Max.X, float32(got)/2, float32(want)/2)
	}
	if got, want := ink.Min.Y+ink.Max.Y, side; got < want-2 || got > want+2 {
		t.Fatalf("person icon spans y %d..%d: centre %.1f, want %.1f", ink.Min.Y, ink.Max.Y, float32(got)/2, float32(want)/2)
	}
}

func brightPixelBounds(img *image.RGBA) image.Rectangle {
	bounds := img.Bounds()
	ink := image.Rectangle{Min: bounds.Max, Max: bounds.Min}
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, a := img.At(x, y).RGBA()
			if a < 0x2000 || r < 0xc000 || g < 0xc000 || b < 0xc000 {
				continue
			}
			ink.Min.X = min(ink.Min.X, x)
			ink.Min.Y = min(ink.Min.Y, y)
			ink.Max.X = max(ink.Max.X, x+1)
			ink.Max.Y = max(ink.Max.Y, y+1)
		}
	}
	if ink.Min.X >= ink.Max.X || ink.Min.Y >= ink.Max.Y {
		return image.Rectangle{}
	}
	return ink
}

func TestContactLastOnlineAtPrefersPresenceEvidence(t *testing.T) {
	peer := domaintest.ID("last-online-peer")
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	preview := &service.RouterPeerState{
		Preview: service.ConversationPreview{
			Sender:    peer,
			Timestamp: now.Add(-2 * time.Hour),
		},
		LastIncomingAt: domain.TimeOf(now.Add(-2 * time.Hour)),
	}

	if got := contactLastOnlineAt(service.NodeStatus{
		ReachableIDs: map[domain.PeerIdentity]bool{peer: true},
	}, preview, peer, time.Time{}); !got.IsZero() {
		t.Fatalf("online contact exposed a moving wall-clock timestamp: %v", got)
	}

	disconnected := now.Add(-30 * time.Minute)
	persisted := now.Add(-10 * time.Minute)
	status := service.NodeStatus{
		ReachableIDs: map[domain.PeerIdentity]bool{},
		Contacts: map[string]service.Contact{
			peer.String(): {LastOnlineAt: domain.TimeOf(persisted)},
		},
		PeerHealth: []service.PeerHealth{{
			PeerID:             peer.String(),
			LastDisconnectedAt: domain.TimeOf(disconnected),
		}},
	}
	if got := contactLastOnlineAt(status, preview, peer, disconnected); !got.Equal(persisted) {
		t.Fatalf("offline timestamp = %v, want persisted transition %v", got, persisted)
	}

	newerHealth := now.Add(-5 * time.Minute)
	if got := contactLastOnlineAt(status, preview, peer, newerHealth); !got.Equal(newerHealth) {
		t.Fatalf("newer peer-health evidence = %v, want %v", got, newerHealth)
	}

	delete(status.Contacts, peer.String())
	if got := contactLastOnlineAt(status, preview, peer, disconnected); !got.Equal(disconnected) {
		t.Fatalf("legacy peer-health fallback = %v, want disconnect %v", got, disconnected)
	}

	if got := contactLastOnlineAt(service.NodeStatus{}, preview, peer, time.Time{}); !got.Equal(preview.LastIncomingAt.Time()) {
		t.Fatalf("fallback with no node observation = %v, want the peer's message %v", got, preview.LastIncomingAt.Time())
	}

	// A conversation where only we have written carries no incoming stamp, so
	// our own message must not become evidence about the peer.
	outgoingOnly := &service.RouterPeerState{Preview: service.ConversationPreview{
		Sender:    domaintest.ID("self"),
		Timestamp: now.Add(-5 * time.Minute),
	}}
	if got := contactLastOnlineAt(service.NodeStatus{}, outgoingOnly, peer, time.Time{}); !got.IsZero() {
		t.Fatalf("outgoing message became peer presence evidence: got %v, want zero", got)
	}
}

// TestContactLastOnlineAtUsesLastIncomingBehindOwnReply covers the ordinary
// conversation shape: the peer wrote, we answered. The thread's last message is
// then ours, so a preview-derived fallback sees no peer evidence at all and the
// row goes blank — while the peer's own message is still the newest thing we
// know about them.
func TestContactLastOnlineAtUsesLastIncomingBehindOwnReply(t *testing.T) {
	peer := domaintest.ID("replied-peer")
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	incoming := now.Add(-3 * time.Hour)

	state := &service.RouterPeerState{
		Preview: service.ConversationPreview{
			Sender:    domaintest.ID("self"),
			Timestamp: now.Add(-1 * time.Hour),
		},
		LastIncomingAt: domain.TimeOf(incoming),
	}

	if got := contactLastOnlineAt(service.NodeStatus{}, state, peer, time.Time{}); !got.Equal(incoming) {
		t.Fatalf("last-online behind own reply = %v, want the peer's message %v", got, incoming)
	}

	// A node-owned observation wins outright — not by being newer. The chat
	// stamp comes from the sender's clock, so letting it compete would let a
	// peer overwrite what this node itself observed by claiming a later time.
	staleHealth := now.Add(-9 * time.Hour)
	if got := contactLastOnlineAt(service.NodeStatus{}, state, peer, staleHealth); !got.Equal(staleHealth) {
		t.Fatalf("chat activity overrode an older node observation: got %v, want %v", got, staleHealth)
	}
	observedContact := service.NodeStatus{Contacts: map[string]service.Contact{
		peer.String(): {LastOnlineAt: domain.TimeOf(staleHealth)},
	}}
	if got := contactLastOnlineAt(observedContact, state, peer, time.Time{}); !got.Equal(staleHealth) {
		t.Fatalf("chat activity overrode an older durable observation: got %v, want %v", got, staleHealth)
	}
	online := service.NodeStatus{ReachableIDs: map[domain.PeerIdentity]bool{peer: true}}
	if got := contactLastOnlineAt(online, state, peer, time.Time{}); !got.IsZero() {
		t.Fatalf("online contact exposed a timestamp: %v", got)
	}
}

// TestContactLastOnlineAtRanksChatBelowObservations pins the class boundary.
// Chat activity carries the SENDER's timestamp, so it must not outrank what
// this node saw with its own clock — otherwise dating a message forward would
// beat a route loss the node actually witnessed.
func TestContactLastOnlineAtRanksChatBelowObservations(t *testing.T) {
	peer := domaintest.ID("chatty-but-unseen-peer")
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)

	observed := now.Add(-6 * time.Hour)
	claimed := now.Add(-1 * time.Hour)
	state := &service.RouterPeerState{LastIncomingAt: domain.TimeOf(claimed)}

	// No observation anywhere: the message is what the row has.
	if got := contactLastOnlineAt(service.NodeStatus{}, state, peer, time.Time{}); !got.Equal(claimed) {
		t.Fatalf("chat-only contact = %v, want %v", got, claimed)
	}

	// An OLDER observation still wins — the ranking is by class, not by time.
	if got := contactLastOnlineAt(service.NodeStatus{}, state, peer, observed); !got.Equal(observed) {
		t.Fatalf("newer chat activity beat an older peer-health observation: got %v, want %v", got, observed)
	}
	withObservation := service.NodeStatus{Contacts: map[string]service.Contact{
		peer.String(): {LastOnlineAt: domain.TimeOf(observed)},
	}}
	if got := contactLastOnlineAt(withObservation, state, peer, time.Time{}); !got.Equal(observed) {
		t.Fatalf("newer chat activity beat the durable observation: got %v, want %v", got, observed)
	}
}

func TestCompactContactRowHidesLastOnlineBeforeContactName(t *testing.T) {
	gtx := layout.Context{
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(279, 100)},
	}
	if shouldShowContactLastOnlineLabel(gtx, true) {
		t.Fatal("compact contact row kept the long visual last-online label")
	}
	gtx.Constraints.Max.X = 280
	if !shouldShowContactLastOnlineLabel(gtx, true) {
		t.Fatal("regular contact row hid the last-online label")
	}
	if shouldShowContactLastOnlineLabel(gtx, false) {
		t.Fatal("non-contact row exposed a last-online label")
	}
}

func TestRebuildPeerLastOnlineIndexKeepsNewestIdentityActivity(t *testing.T) {
	peer := domaintest.ID("peer-health-index")
	older := time.Date(2026, time.August, 21, 8, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	w := &Window{snap: service.RouterSnapshot{NodeStatus: service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{PeerID: peer.String(), LastDisconnectedAt: domain.TimeOf(older)},
			{PeerID: peer.String(), LastUsefulReceiveAt: domain.TimeOf(newer)},
			{PeerID: "not-an-identity", LastConnectedAt: domain.TimeOf(newer.Add(time.Hour))},
		},
	}}}

	w.rebuildPeerLastOnlineIndex()
	if got := w.peerLastOnlineByIdentity[peer]; !got.Equal(newer) {
		t.Fatalf("indexed peer activity = %v, want newest %v", got, newer)
	}
	if len(w.peerLastOnlineByIdentity) != 1 {
		t.Fatalf("peer activity index has %d rows, want only the valid identity", len(w.peerLastOnlineByIdentity))
	}
}

func TestFormatContactLastOnline(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name     string
		language string
		last     time.Time
		want     string
	}{
		{name: "today", language: "ru", last: now.Add(-time.Hour), want: "11:30"},
		{name: "yesterday Russian", language: "ru", last: now.AddDate(0, 0, -1), want: "Вчера"},
		{name: "three days Russian", language: "ru", last: now.AddDate(0, 0, -3), want: "3 дня назад"},
		{name: "three days English", language: "en", last: now.AddDate(0, 0, -3), want: "3 days ago"},
		{name: "three days Spanish", language: "es", last: now.AddDate(0, 0, -3), want: "hace 3 días"},
		{name: "three days French", language: "fr", last: now.AddDate(0, 0, -3), want: "il y a 3 jours"},
		{name: "three days Arabic", language: "ar", last: now.AddDate(0, 0, -3), want: "منذ 3 أيام"},
		{name: "older", language: "ru", last: now.AddDate(0, 0, -7), want: "14.08.26"},
		{name: "older English", language: "en", last: now.AddDate(0, 0, -7), want: "08/14/26"},
		{name: "older Chinese", language: "zh", last: now.AddDate(0, 0, -7), want: "2026.08.14"},
		{name: "missing", language: "ru", last: time.Time{}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatContactLastOnline(now, tt.last, tt.language); got != tt.want {
				t.Fatalf("formatContactLastOnline() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatContactLastOnlineUsesCalendarDaysAcrossDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	now := time.Date(2026, time.March, 10, 12, 0, 0, 0, location)
	last := time.Date(2026, time.March, 8, 12, 0, 0, 0, location)
	if got := formatContactLastOnline(now, last, "en"); got != "2 days ago" {
		t.Fatalf("DST-spanning calendar difference = %q, want %q", got, "2 days ago")
	}
}

func TestOnlineContactRowHasOnePresenceDescription(t *testing.T) {
	peer := domaintest.ID("semantic-online-peer")
	w := newIdentityLayoutTestWindow(t)
	w.snap.Peers = map[domain.PeerIdentity]*service.RouterPeerState{
		peer: {Preview: service.ConversationPreview{Sender: peer, Timestamp: time.Date(2026, time.August, 21, 8, 14, 0, 0, time.UTC)}},
	}

	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Now:         time.Date(2026, time.August, 21, 9, 0, 0, 0, time.UTC),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(320, 100)},
	}
	w.layoutRecipientButton(gtx, service.NodeStatus{
		ReachableIDs: map[domain.PeerIdentity]bool{peer: true},
	}, peer, true)
	router.Frame(gtx.Ops)

	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class != semantic.Button || node.Desc.Description == "Open contact menu" {
			continue
		}
		if node.Desc.Description != "Online" {
			t.Fatalf("online contact description = %q, want one authoritative presence description %q", node.Desc.Description, "Online")
		}
		return
	}
	t.Fatal("contact row button is absent from the semantic tree")
}

func TestIdentitySearchUsesCompactSingleRow(t *testing.T) {
	w := newIdentityLayoutTestWindow(t)
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(320, 200)},
	}

	dims := w.identitySearchCard(gtx, service.NodeStatus{}, nil)

	if dims.Size.Y != 48 {
		t.Fatalf("empty identity search height = %d, want one compact 48dp row", dims.Size.Y)
	}
}

func TestMyIdentityAddressWrapsWithoutTruncation(t *testing.T) {
	const address = "ae47201ce461599cbb1562eb159fcee0075fdcdd"
	w := &Window{
		theme:    newAppTheme(),
		language: "en",
		snap: service.RouterSnapshot{
			MyAddress: domain.PeerIdentityFromWire(address),
		},
	}
	layoutAt := func(width, height int) widget.TextInfo {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(width, height)},
		}
		_, info := w.layoutMyIdentityAddress(gtx)
		return info
	}

	// About 181dp is what the address receives in a 1024dp-wide window after
	// the sidebar card's avatar, chevron, padding and gaps take their share.
	if got := layoutAt(181, 200).Truncated; got != 0 {
		t.Fatalf("identity address still truncates %d runes at the normal sidebar width", got)
	}
	// Around 70dp remains for the address in the sidebar of a 640dp-wide
	// two-pane window. The card may grow vertically, but the identity must stay
	// complete instead of losing its tail.
	if got := layoutAt(70, 200).Truncated; got != 0 {
		t.Fatalf("identity address truncates %d runes in a narrow two-pane sidebar", got)
	}
	// Control: the former two-line policy must report truncation under the same
	// constraints, proving that Truncated observes rendered glyph layout rather
	// than the always-complete semantic label.
	controlGTX := layout.Context{
		Ops:         new(op.Ops),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Constraints{Max: image.Pt(70, 200)},
	}
	style := material.Caption(w.theme, address)
	_, control := (widget.Label{MaxLines: 2, WrapPolicy: text.WrapGraphemes}).LayoutDetailed(
		controlGTX, style.Shaper, style.Font, style.TextSize, style.Text, op.CallOp{},
	)
	if control.Truncated == 0 {
		t.Fatal("two-line control did not report truncation")
	}
}

func TestMyIdentityCardGrowsForNarrowTwoPaneWindows(t *testing.T) {
	const address = "ae47201ce461599cbb1562eb159fcee0075fdcdd"
	for _, width := range []int{174, 222} {
		t.Run(fmt.Sprintf("card-width-%d", width), func(t *testing.T) {
			w := newIdentityLayoutTestWindow(t)
			w.snap.MyAddress = domain.PeerIdentityFromWire(address)
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Constraints{Max: image.Pt(width, 400)},
			}

			dims := w.layoutMyIdentityButton(gtx, 12)
			if dims.Size.Y <= 96 {
				t.Fatalf("narrow identity card stayed at %ddp and cannot expose all wrapped address lines", dims.Size.Y)
			}
		})
	}
}

func TestIdentitySearchEditorLayoutAppliesOpticalOffset(t *testing.T) {
	contextFor := func() layout.Context {
		return layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(240, 26)},
		}
	}
	shiftedWindow := &Window{theme: newAppTheme(), language: "en"}
	shifted := shiftedWindow.layoutIdentitySearchEditor(contextFor())
	rawWindow := &Window{theme: newAppTheme(), language: "en"}
	raw := rawWindow.identitySearchEditorStyle().Layout(contextFor())

	shiftedBaselineFromTop := shifted.Size.Y - shifted.Baseline
	rawBaselineFromTop := raw.Size.Y - raw.Baseline
	if shifted.Size.Y != raw.Size.Y+2 || shiftedBaselineFromTop != rawBaselineFromTop+2 {
		t.Fatalf("search editor optical layout = size %v baseline %d; raw = size %v baseline %d, want a real 2dp downward shift",
			shifted.Size, shifted.Baseline, raw.Size, raw.Baseline)
	}
}

func TestIdentityPanelSizeStaysInsideWindow(t *testing.T) {
	tests := []struct {
		name   string
		window image.Point
		want   image.Point
	}{
		{name: "desktop", window: image.Pt(1000, 700), want: image.Pt(384, 520)},
		{name: "phone", window: image.Pt(360, 640), want: image.Pt(328, 520)},
		{name: "landscape phone", window: image.Pt(640, 320), want: image.Pt(384, 288)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := identityPanelSize(tt.window, 1); got != tt.want {
				t.Fatalf("identityPanelSize(%v) = %v, want %v", tt.window, got, tt.want)
			}
		})
	}
}

func TestIdentityPanelFillsCompactWindow(t *testing.T) {
	window := image.Pt(390, 720)
	if got := identityPanelSizeForMode(window, 1, true); got != window {
		t.Fatalf("compact identity panel size = %v, want full client area %v", got, window)
	}

	if got := identityPanelSizeForMode(window, 1, false); got == window {
		t.Fatalf("desktop identity panel unexpectedly fills the window: %v", got)
	}
}

func TestDesktopIdentityPanelCentersItsCloseButton(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	frame := func() layout.Context {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.layoutIdentityPanelOverlay(gtx)
		router.Frame(gtx.Ops)
		return gtx
	}

	frame()
	router.Queue(
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: f32.Pt(660, 124)},
		pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: f32.Pt(660, 124)},
	)
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	if !w.identityPanelClose.Clicked(gtx) {
		t.Fatal("close button was not hit at the centered desktop-panel coordinates")
	}
}

func TestIdentityPanelBackdropHasNoButtonSemantics(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	w.layoutIdentityPanelOverlay(gtx)
	router.Frame(gtx.Ops)

	windowBounds := image.Rect(0, 0, 1000, 700)
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Class == semantic.Button && node.Desc.Bounds == windowBounds {
			t.Fatalf("identity panel exposes its full-window backdrop as a button: %+v", node.Desc)
		}
	}
}

func TestIdentityPanelBackdropDismissesOnlyOutsideTheCard(t *testing.T) {
	runPress := func(t *testing.T, position f32.Point) bool {
		t.Helper()
		var router input.Router
		w := newIdentityLayoutTestWindow(t)
		w.identityPanelVisible = true
		frame := func() {
			gtx := layout.Context{
				Ops:         new(op.Ops),
				Source:      router.Source(),
				Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
				Constraints: layout.Exact(image.Pt(1000, 700)),
			}
			w.layoutIdentityPanelOverlay(gtx)
			router.Frame(gtx.Ops)
		}

		frame()
		router.Queue(pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: position})
		frame()
		return w.identityPanelVisible
	}

	if runPress(t, f32.Pt(4, 4)) {
		t.Fatal("press on the desktop backdrop did not close the identity panel")
	}
	if !runPress(t, f32.Pt(500, 350)) {
		t.Fatal("press on blank space inside the identity card closed the panel")
	}
}

func TestIdentityPanelBlocksClicksThroughBlankPanelAreas(t *testing.T) {
	tests := []struct {
		name     string
		window   image.Point
		position f32.Point
	}{
		{name: "desktop inner padding", window: image.Pt(1000, 700), position: f32.Pt(315, 97)},
		{name: "desktop bottom padding", window: image.Pt(1000, 700), position: f32.Pt(500, 605)},
		{name: "compact edge", window: image.Pt(390, 720), position: f32.Pt(5, 5)},
		{name: "compact bottom padding", window: image.Pt(390, 720), position: f32.Pt(200, 715)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var router input.Router
			var underneath widget.Clickable
			w := newIdentityLayoutTestWindow(t)
			w.identityPanelVisible = true
			clickedUnderneath := false
			frame := func() {
				gtx := layout.Context{
					Ops:         new(op.Ops),
					Source:      router.Source(),
					Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
					Constraints: layout.Exact(tt.window),
				}
				for underneath.Clicked(gtx) {
					clickedUnderneath = true
				}
				underneath.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Dimensions{Size: gtx.Constraints.Max}
				})
				w.layoutIdentityPanelOverlay(gtx)
				router.Frame(gtx.Ops)
			}

			frame()
			router.Queue(
				pointer.Event{Source: pointer.Mouse, Kind: pointer.Press, Buttons: pointer.ButtonPrimary, Position: tt.position},
				pointer.Event{Source: pointer.Mouse, Kind: pointer.Release, Position: tt.position},
			)
			frame()
			if clickedUnderneath {
				t.Fatal("click reached the application underneath the identity panel")
			}
		})
	}
}

func TestContactLinkForClipboardRejectsEmptyPayload(t *testing.T) {
	if link, ok := contactLinkForClipboard(" \n\t "); ok || link != "" {
		t.Fatalf("blank contact link accepted as %q", link)
	}
	if link, ok := contactLinkForClipboard("  corsa:test  "); !ok || link != "corsa:test" {
		t.Fatalf("valid contact link = (%q, %v), want trimmed payload", link, ok)
	}
}

func TestIdentityPanelClaimsAndTrapsKeyboardFocus(t *testing.T) {
	var router input.Router
	w := newIdentityLayoutTestWindow(t)
	frame := func() {
		ops := new(op.Ops)
		gtx := layout.Context{
			Ops:         ops,
			Source:      router.Source(),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Exact(image.Pt(1000, 700)),
		}
		w.identitySearchCard(gtx, service.NodeStatus{}, nil)
		if w.identityPanelVisible {
			w.layoutIdentityPanelOverlay(gtx)
		}
		router.Frame(ops)
	}

	// Reproduce the reported state: search owns focus before the panel opens.
	ops := new(op.Ops)
	gtx := layout.Context{
		Ops:         ops,
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(1000, 700)),
	}
	gtx.Execute(key.FocusCmd{Tag: &w.identitySearchEditor})
	w.identitySearchCard(gtx, service.NodeStatus{}, nil)
	router.Frame(ops)
	if !router.Source().Focused(&w.identitySearchEditor) {
		t.Fatal("test setup failed: search editor did not gain focus")
	}

	w.openIdentityPanel("corsa:test", widget.Image{})
	frame()
	if !router.Source().Focused(&w.identityPanelClose) {
		t.Fatal("opening identity details left keyboard focus in the search editor underneath")
	}

	router.Queue(key.Event{Name: key.NameTab, State: key.Press})
	frame()
	if !router.Source().Focused(&w.copyIdentityButton) {
		t.Fatal("Tab escaped identity details instead of moving to its next action")
	}
}

// TestSearchUnionIncludesReachable is the §4.9 search fix: an identity with
// a live route but no observed keys must be findable — the routing table
// learns about a fresh node seconds after it joins, the epidemic contact
// plane only much later.
func TestSearchUnionIncludesReachable(t *testing.T) {
	const routedHex = "00abcf0000000000000000000000000000000000"
	routed := domain.PeerIdentityFromWire(routedHex)
	reachable := map[domain.PeerIdentity]bool{
		routed:               true,
		domaintest.ID("off"): false, // a dead route is not a search hit
	}

	results := searchKnownIdentities(nil, reachable, nil, domaintest.ID("self"), "abc")
	if len(results) != 1 || results[0] != routed {
		t.Fatalf("expected the routed identity, got %v", results)
	}

	// The same identity in both sets stays a single row.
	results = searchKnownIdentities([]string{routedHex}, reachable, nil, domaintest.ID("self"), "abc")
	if len(results) != 1 {
		t.Fatalf("union produced duplicates: %v", results)
	}
}

// TestSearchFullAddressCandidateRow: a complete valid 40-hex absent from
// BOTH sets yields a selectable candidate row — absence from ReachableIDs
// does not prove absence of a route, and opening the chat is what starts
// key discovery.
func TestSearchFullAddressCandidateRow(t *testing.T) {
	const strangerHex = "aabbccddeeff00112233445566778899aabbccdd"

	results := searchKnownIdentities(nil, nil, nil, domaintest.ID("self"), strangerHex)
	if len(results) != 1 || results[0] != domain.PeerIdentityFromWire(strangerHex) {
		t.Fatalf("expected the candidate row, got %v", results)
	}

	// The search normalises case, so an uppercase paste still resolves to
	// the same candidate; a partial prefix never fabricates one.
	if got := searchKnownIdentities(nil, nil, nil, domaintest.ID("self"), strings.ToUpper(strangerHex)); len(got) != 1 {
		t.Fatalf("uppercase paste lost the candidate: %v", got)
	}
	if got := searchKnownIdentities(nil, nil, nil, domaintest.ID("self"), strangerHex[:39]); len(got) != 0 {
		t.Fatalf("partial query fabricated a candidate: %v", got)
	}

	// A candidate equal to self stays hidden.
	if got := searchKnownIdentities(nil, nil, nil, domain.PeerIdentityFromWire(strangerHex), strangerHex); len(got) != 0 {
		t.Fatalf("self appeared as its own candidate: %v", got)
	}
}

func TestShortFingerprint(t *testing.T) {
	short := "abc"
	if got := shortFingerprint(short); got != short {
		t.Fatalf("expected %q, got %q", short, got)
	}

	long := "abcdefghijklmnopqrstuvwxyz"
	got := shortFingerprint(long)
	if got != "abcdefgh...uvwxyz" {
		t.Fatalf("expected 'abcdefgh...uvwxyz', got %q", got)
	}
}

func TestEllipsize(t *testing.T) {
	if got := ellipsize("hello", 10); got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
	if got := ellipsize("hello world", 5); got != "hell…" {
		t.Fatalf("expected 'hell…', got %q", got)
	}
	if got := ellipsize("", 5); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestScheduleTransferInvalidateCoalesces(t *testing.T) {
	w := &Window{}

	w.scheduleTransferInvalidate(10 * time.Millisecond)
	w.scheduleTransferInvalidate(10 * time.Millisecond)

	w.transferInvalidateMu.Lock()
	pending := w.transferInvalidatePending
	w.transferInvalidateMu.Unlock()
	if !pending {
		t.Fatalf("expected transfer invalidate to be pending")
	}

	time.Sleep(30 * time.Millisecond)

	w.transferInvalidateMu.Lock()
	pending = w.transferInvalidatePending
	w.transferInvalidateMu.Unlock()
	if pending {
		t.Fatalf("expected transfer invalidate pending flag to clear after timer fires")
	}
}

// TestNetworkStatusSummary verifies that the aggregate network status is based
// on the number of usable peers (healthy + degraded) among currently live
// peers. Stalled peers count as connected-but-not-usable, while reconnecting
// peers are diagnostic only unless there are no live peers at all.
// TestNodeUpdateAvailable verifies that the Desktop UI reads the pre-computed
// update_available flag from AggregateStatus rather than computing it locally.
// The policy decision (which peers, thresholds, dedup) lives in the node layer;
// the UI only renders the result.
func TestNodeUpdateAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		agg  *service.AggregateStatus
		want bool
	}{
		{
			name: "nil aggregate status",
			agg:  nil,
			want: false,
		},
		{
			name: "update not available",
			agg:  &service.AggregateStatus{UpdateAvailable: false},
			want: false,
		},
		{
			name: "update available from node policy — peer build",
			agg:  &service.AggregateStatus{UpdateAvailable: true, UpdateReason: "peer_build_newer"},
			want: true,
		},
		{
			name: "update available from node policy — incompatible reporters",
			agg:  &service.AggregateStatus{UpdateAvailable: true, UpdateReason: "incompatible_version_reporters"},
			want: true,
		},
		{
			name: "update available from node policy — both signals",
			agg:  &service.AggregateStatus{UpdateAvailable: true, UpdateReason: "peer_build_and_incompatible_version"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Window{
				snap: service.RouterSnapshot{
					NodeStatus: service.NodeStatus{AggregateStatus: tt.agg},
				},
			}
			got := w.nodeUpdateAvailable()
			if got != tt.want {
				t.Errorf("nodeUpdateAvailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNetworkStatusSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		peers         []service.PeerHealth
		wantState     string
		wantConnected int
		wantTotal     int
	}{
		{
			name:      "no peers is offline",
			peers:     nil,
			wantState: "offline",
		},
		{
			name: "all reconnecting",
			peers: []service.PeerHealth{
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "reconnecting",
			wantConnected: 0,
			wantTotal:     2,
		},
		{
			name: "single healthy peer is limited",
			peers: []service.PeerHealth{
				{State: "healthy"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "single stalled peer is limited (connected but not usable)",
			peers: []service.PeerHealth{
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 1,
			wantTotal:     1,
		},
		{
			name: "two usable peers are healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     2,
		},
		{
			name: "all stalled is limited not healthy (P2 regression)",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "stalled"},
				{State: "stalled"},
			},
			wantState:     "limited",
			wantConnected: 3,
			wantTotal:     3,
		},
		{
			name: "reconnecting peers do not downgrade live healthy quorum",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     7,
		},
		{
			name: "half usable is healthy",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "reconnecting"},
				{State: "reconnecting"},
			},
			wantState:     "healthy",
			wantConnected: 2,
			wantTotal:     4,
		},
		{
			name: "mix of stalled and degraded uses only usable for status",
			peers: []service.PeerHealth{
				{State: "stalled"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "degraded"},
			},
			wantState:     "healthy",
			wantConnected: 4,
			wantTotal:     4,
		},
		{
			name: "less than half of live peers usable is warning",
			peers: []service.PeerHealth{
				{State: "healthy"},
				{State: "degraded"},
				{State: "stalled"},
				{State: "stalled"},
				{State: "stalled"},
			},
			wantState:     "warning",
			wantConnected: 5,
			wantTotal:     5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := service.NodeStatus{PeerHealth: tt.peers}
			gotState, gotConnected, gotTotal, _ := networkStatusSummary(status)
			if gotState != tt.wantState {
				t.Errorf("state: got %q, want %q", gotState, tt.wantState)
			}
			if gotConnected != tt.wantConnected {
				t.Errorf("connected: got %d, want %d", gotConnected, tt.wantConnected)
			}
			if gotTotal != tt.wantTotal {
				t.Errorf("total: got %d, want %d", gotTotal, tt.wantTotal)
			}
		})
	}
}

// TestNetworkStatusSummary_AggregateStatusTakesPrecedence verifies the key
// contract of step 2a: when NodeStatus contains a non-nil AggregateStatus
// (from fetch_aggregate_status), networkStatusSummary uses it directly and
// ignores the PeerHealth entries. The test intentionally feeds conflicting
// values so that any fallback to local computation would produce a different
// result and be caught.
func TestNetworkStatusSummary_AggregateStatusTakesPrecedence(t *testing.T) {
	t.Parallel()

	status := service.NodeStatus{
		// PeerHealth says: 2 healthy → would produce "healthy", connected=2, total=2.
		PeerHealth: []service.PeerHealth{
			{State: "healthy", PendingCount: 1},
			{State: "healthy", PendingCount: 1},
		},
		// AggregateStatus from node says: "warning" with different counters.
		AggregateStatus: &service.AggregateStatus{
			Status:          "warning",
			UsablePeers:     1,
			ConnectedPeers:  3,
			TotalPeers:      5,
			PendingMessages: 42,
		},
	}

	gotState, gotConnected, gotTotal, gotPending := networkStatusSummary(status)

	if gotState != "warning" {
		t.Errorf("state: got %q, want %q (AggregateStatus should take precedence over PeerHealth)", gotState, "warning")
	}
	if gotConnected != 3 {
		t.Errorf("connected: got %d, want %d", gotConnected, 3)
	}
	if gotTotal != 5 {
		t.Errorf("total: got %d, want %d", gotTotal, 5)
	}
	if gotPending != 42 {
		t.Errorf("pending: got %d, want %d", gotPending, 42)
	}
}

// TestNetworkStatusSummary_FallbackWhenAggregateStatusNil verifies that when
// AggregateStatus is nil (older node version), the function falls back to
// local computation from PeerHealth.
func TestNetworkStatusSummary_FallbackWhenAggregateStatusNil(t *testing.T) {
	t.Parallel()

	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{State: "healthy", PendingCount: 3},
			{State: "stalled", PendingCount: 0},
			{State: "reconnecting", PendingCount: 0},
		},
		AggregateStatus: nil, // older node — command not available
	}

	gotState, gotConnected, gotTotal, gotPending := networkStatusSummary(status)

	// 1 usable out of 2 connected → "limited"
	if gotState != "limited" {
		t.Errorf("state: got %q, want %q", gotState, "limited")
	}
	if gotConnected != 2 {
		t.Errorf("connected: got %d, want %d", gotConnected, 2)
	}
	if gotTotal != 3 {
		t.Errorf("total: got %d, want %d", gotTotal, 3)
	}
	if gotPending != 3 {
		t.Errorf("pending: got %d, want %d", gotPending, 3)
	}
}

func TestCompactContactsPaneShowsNetworkStatus(t *testing.T) {
	t.Parallel()

	var router input.Router
	gtx := layout.Context{
		Ops:         new(op.Ops),
		Source:      router.Source(),
		Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
		Constraints: layout.Exact(image.Pt(360, 720)),
	}
	w := newIdentityLayoutTestWindow(t)
	w.contactsList = widget.List{List: layout.List{Axis: layout.Vertical}}
	w.snap = service.RouterSnapshot{NodeStatus: service.NodeStatus{
		AggregateStatus: &service.AggregateStatus{
			Status:          "healthy",
			ConnectedPeers:  2,
			TotalPeers:      4,
			PendingMessages: 3,
		},
	}}

	w.layoutMainCompact(gtx, w.snap.NodeStatus, nil)
	router.Frame(gtx.Ops)

	want := "NET HEALTHY | 2/4 peers | 3 pending"
	for _, node := range router.AppendSemantics(nil) {
		if node.Desc.Label == want {
			return
		}
	}
	t.Fatalf("compact contacts pane does not expose network status %q", want)
}

func TestFindMessageBody(t *testing.T) {
	t.Parallel()

	w := &Window{
		snap: service.RouterSnapshot{
			ActiveMessages: []service.DirectMessage{
				{ID: "aaa", Body: "hello"},
				{ID: "bbb", Body: "world"},
			},
		},
	}
	w.rebuildMsgCache()

	if got := w.findMessageBody("aaa"); got != "hello" {
		t.Errorf("findMessageBody(aaa) = %q, want %q", got, "hello")
	}
	if got := w.findMessageBody("bbb"); got != "world" {
		t.Errorf("findMessageBody(bbb) = %q, want %q", got, "world")
	}
	if got := w.findMessageBody("nonexistent"); got != "" {
		t.Errorf("findMessageBody(nonexistent) = %q, want empty", got)
	}
}

func TestFindCachedMsg(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 3, 12, 30, 0, 0, time.UTC)
	w := &Window{
		snap: service.RouterSnapshot{
			ActiveMessages: []service.DirectMessage{
				{ID: "aaa", Body: "hello", Sender: domaintest.ID("alice"), Timestamp: ts},
				{ID: "bbb", Body: "world", Sender: domaintest.ID("bob"), Timestamp: ts.Add(time.Minute)},
			},
		},
	}
	w.rebuildMsgCache()

	cm, ok := w.findCachedMsg("aaa")
	if !ok {
		t.Fatal("findCachedMsg(aaa) not found")
	}
	if cm.Sender != domaintest.ID("alice") {
		t.Errorf("Sender = %q, want %q", cm.Sender, "alice")
	}
	if cm.Index != 0 {
		t.Errorf("Index = %d, want 0", cm.Index)
	}
	if cm.Timestamp != ts {
		t.Errorf("Timestamp = %v, want %v", cm.Timestamp, ts)
	}

	cm2, ok := w.findCachedMsg("bbb")
	if !ok {
		t.Fatal("findCachedMsg(bbb) not found")
	}
	if cm2.Index != 1 {
		t.Errorf("Index = %d, want 1", cm2.Index)
	}

	_, ok = w.findCachedMsg("nonexistent")
	if ok {
		t.Error("findCachedMsg(nonexistent) should return false")
	}
}

func TestTriggerSendSetsReplyTo(t *testing.T) {
	t.Parallel()

	replyMsg := &service.DirectMessage{
		ID:   "reply-target-id",
		Body: "original message",
	}
	w := &Window{
		replyToMsg: replyMsg,
	}

	// Verify triggerSend maps replyToMsg into OutgoingDM.ReplyTo. (triggerSend
	// itself clears the composer synchronously after a successful dispatch;
	// this test only checks the ReplyTo extraction, not the clear.)
	outgoing := domain.OutgoingDM{Body: "my reply"}
	if w.replyToMsg != nil {
		outgoing.ReplyTo = domain.MessageID(w.replyToMsg.ID)
	}

	if outgoing.ReplyTo != "reply-target-id" {
		t.Errorf("ReplyTo = %q, want %q", outgoing.ReplyTo, "reply-target-id")
	}
	if w.replyToMsg == nil {
		t.Error("replyToMsg should remain set until the send completes")
	}
}

func TestResetReplyOnPeerChange(t *testing.T) {
	t.Parallel()

	w := &Window{
		replyToMsg: &service.DirectMessage{ID: "msg-1", Body: "hello"},
		snap: service.RouterSnapshot{
			ActivePeer: domaintest.ID("peer-b"),
		},
		lastChatPeer: domaintest.ID("peer-a"),
	}

	w.resetReplyOnPeerChange()

	if w.replyToMsg != nil {
		t.Error("replyToMsg should be nil after peer change")
	}
	if w.msgContextMsg != nil {
		t.Error("msgContextMsg should be nil after peer change")
	}
}

func TestResetReplyOnPeerChangeSamePeer(t *testing.T) {
	t.Parallel()

	replyMsg := &service.DirectMessage{ID: "msg-1", Body: "hello"}
	w := &Window{
		replyToMsg: replyMsg,
		snap: service.RouterSnapshot{
			ActivePeer: domaintest.ID("peer-a"),
		},
		lastChatPeer: domaintest.ID("peer-a"),
	}

	w.resetReplyOnPeerChange()

	if w.replyToMsg != replyMsg {
		t.Error("replyToMsg should remain unchanged when peer is the same")
	}
}

// TestRebuildMsgCacheSkipsWhenUnchanged verifies that rebuildMsgCache
// does not reallocate the map when the snapshot generation has not changed.
func TestRebuildMsgCacheSkipsWhenUnchanged(t *testing.T) {
	t.Parallel()

	now := time.Now()
	w := &Window{}
	w.snap.DMGeneration = 1
	w.snap.ActiveMessages = []service.DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("alice"), Timestamp: now},
		{ID: "msg-2", Body: "world", Sender: domaintest.ID("bob"), Timestamp: now.Add(time.Second)},
	}

	// First call — builds cache.
	w.rebuildMsgCache()
	if w.msgCacheByID == nil {
		t.Fatal("msgCacheByID should be populated after first call")
	}
	if len(w.msgCacheByID) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(w.msgCacheByID))
	}
	if w.msgCacheGen != 1 {
		t.Fatalf("msgCacheGen = %d, want 1", w.msgCacheGen)
	}

	// Second call with same generation — should skip rebuild.
	w.rebuildMsgCache()
	if w.msgCacheGen != 1 {
		t.Fatalf("msgCacheGen = %d after no-op call, want 1", w.msgCacheGen)
	}

	// A status-only notify advances Generation and nothing else: the DM half
	// is reused byte-for-byte, so the cache must NOT be rebuilt. This is the
	// whole point of gating on DMGeneration — over a long conversation the
	// old gate re-hashed every ID two or three times a second.
	w.snap.Generation = 99
	w.rebuildMsgCache()
	if w.msgCacheGen != 1 {
		t.Fatalf("msgCacheGen = %d after a status-only Generation bump, want 1", w.msgCacheGen)
	}

	// Bump the DM generation and append a message — cache must rebuild.
	w.snap.DMGeneration = 2
	w.snap.ActiveMessages = append(w.snap.ActiveMessages, service.DirectMessage{
		ID: "msg-3", Body: "new", Sender: domaintest.ID("carol"), Timestamp: now.Add(2 * time.Second),
	})
	w.rebuildMsgCache()
	if len(w.msgCacheByID) != 3 {
		t.Fatalf("expected 3 entries after append, got %d", len(w.msgCacheByID))
	}
	if w.msgCacheGen != 2 {
		t.Fatalf("msgCacheGen = %d after rebuild, want 2", w.msgCacheGen)
	}
}

// TestRebuildMsgCacheDetectsGenerationChange verifies that the cache
// is rebuilt when the snapshot's DM generation changes, even if message
// count and IDs remain the same (e.g. receipt status update, body
// edit, or same-shape conversation reload). Those all reach the UI through
// a DM-typed notify, so they move DMGeneration and are still caught.
func TestRebuildMsgCacheDetectsGenerationChange(t *testing.T) {
	t.Parallel()

	now := time.Now()
	w := &Window{}
	w.snap.DMGeneration = 10
	w.snap.ActiveMessages = []service.DirectMessage{
		{ID: "msg-1", Body: "hello", Sender: domaintest.ID("alice"), Timestamp: now, ReceiptStatus: ""},
	}

	w.rebuildMsgCache()
	if w.msgCacheGen != 10 {
		t.Fatalf("msgCacheGen = %d, want 10", w.msgCacheGen)
	}

	// Simulate receipt arrival: same IDs, new DM generation (applyReceiptRepair
	// notifies UIEventMessagesUpdated, which rebuilds the DM half).
	w.snap.DMGeneration = 11
	w.snap.ActiveMessages[0].ReceiptStatus = "delivered"
	w.rebuildMsgCache()
	if w.msgCacheGen != 11 {
		t.Fatalf("msgCacheGen = %d after update, want 11", w.msgCacheGen)
	}

	// Same generation — should skip.
	w.rebuildMsgCache()
	if w.msgCacheGen != 11 {
		t.Fatalf("msgCacheGen = %d after no-op, want 11", w.msgCacheGen)
	}
}

// TestPressWindowPosRequiresUniqueFrame locks in the rule that a press is
// resolved by the FRAME it began on (widget.Press carries no PointerID in Gio
// v0.10) and that an ambiguous frame resolves to nothing rather than to a
// guess. Lives here rather than in touch_input_test.go because that file is
// copied into the widget-free harness.
func TestPressWindowPosRequiresUniqueFrame(t *testing.T) {
	f1 := time.Unix(0, 1000)
	f2 := time.Unix(0, 2000)
	w := &Window{pointerPressPos: map[pointer.ID]pressPoint{
		3: {pos: image.Pt(40, 60), at: f1},
		7: {pos: image.Pt(90, 10), at: f2},
	}}
	// Each press resolves to its OWN position, never the other pointer's —
	// however much later that one moved within the same frame.
	if got, ok := w.pressWindowPos(widget.Press{Start: f1}); !ok || got != image.Pt(40, 60) {
		t.Fatalf("press on f1 = %v, %v; want (40,60), true", got, ok)
	}
	if got, ok := w.pressWindowPos(widget.Press{Start: f2}); !ok || got != image.Pt(90, 10) {
		t.Fatalf("press on f2 = %v, %v; want (90,10), true", got, ok)
	}
	// A frame that recorded no press is not resolvable.
	if _, ok := w.pressWindowPos(widget.Press{Start: time.Unix(0, 3000)}); ok {
		t.Fatal("unknown frame must not resolve")
	}
	// Two pointers pressed on ONE frame: nothing here distinguishes them, so
	// report failure instead of picking one. Callers fall back rather than
	// anchoring a menu under the wrong finger.
	w.pointerPressPos[7] = pressPoint{pos: image.Pt(90, 10), at: f1}
	if _, ok := w.pressWindowPos(widget.Press{Start: f1}); ok {
		t.Fatal("ambiguous frame must report false")
	}
}

// A "⋯" rectangle cached from an identity-search row must not outlive the
// results it was measured in. The hits are sorted by identity, so a new query
// can put a different peer in a given row while the count, the row heights,
// both scrollable lists and the window size stay exactly as they were — and
// these rows carry the ordinary per-contact buttons, so a rectangle that
// survives opens a REAL menu beside the row that took the old place. See
// menuRectSig.
func TestMenuRectCacheDropsWhenIdentitySearchRowsChange(t *testing.T) {
	const (
		hexA = "11ab110000000000000000000000000000000000"
		hexB = "22ab220000000000000000000000000000000000"
	)
	status := service.NodeStatus{KnownIDs: []string{hexA, hexB}}
	w := &Window{menuBtnRects: make(map[*widget.Clickable]image.Rectangle)}
	btn := new(widget.Clickable)
	cached := func() bool { _, ok := w.menuBtnRects[btn]; return ok }

	w.identitySearchEditor.SetText("11")
	if got := w.resolveIdentitySearchRows(status, nil); len(got) != 1 || got[0] != domain.PeerIdentityFromWire(hexA) {
		t.Fatalf("query \"11\" gave %v, want exactly [%s]", got, domain.PeerIdentityFromWire(hexA))
	}
	w.menuBtnRects[btn] = image.Rect(0, 0, 10, 10)

	// Same query, same row: nothing moved, and the cache is what makes a
	// keyboard or Narrator menu land on the button instead of the fallback
	// corner. Clearing it here would be deleting the feature.
	w.resolveIdentitySearchRows(status, nil)
	if !cached() {
		t.Fatal("an unchanged result set moves no row; the rectangle must survive")
	}

	// One hit swapped for another: same count, same heights, different peer in
	// the only row there is.
	w.identitySearchEditor.SetText("22")
	if got := w.resolveIdentitySearchRows(status, nil); len(got) != 1 || got[0] != domain.PeerIdentityFromWire(hexB) {
		t.Fatalf("query \"22\" gave %v, want exactly [%s]", got, domain.PeerIdentityFromWire(hexB))
	}
	if cached() {
		t.Fatal("the search row now holds a different peer: a rectangle kept from the old query anchors the menu at a row that is gone")
	}
}

// The cap and the digest must describe the SAME rows. Capping after
// fingerprinting would clear the cache for hits that never had a row; capping
// somewhere the digest cannot see would let a change inside the visible rows
// pass unnoticed.
func TestIdentitySearchCapAndDigestDescribeTheSameRows(t *testing.T) {
	const (
		hex0 = "00ab000000000000000000000000000000000000"
		hex1 = "01ab000000000000000000000000000000000000"
		hex2 = "02ab000000000000000000000000000000000000"
		hex3 = "03ab000000000000000000000000000000000000"
		hex4 = "04ab000000000000000000000000000000000000"
		hex5 = "05ab000000000000000000000000000000000000"
	)
	w := &Window{menuBtnRects: make(map[*widget.Clickable]image.Rectangle)}
	btn := new(widget.Clickable)
	cached := func() bool { _, ok := w.menuBtnRects[btn]; return ok }
	w.identitySearchEditor.SetText("ab")

	rows := w.resolveIdentitySearchRows(service.NodeStatus{KnownIDs: []string{hex1, hex2, hex3, hex4, hex5}}, nil)
	if len(rows) != identitySearchMaxRows {
		t.Fatalf("laid-out rows = %d, want the cap %d", len(rows), identitySearchMaxRows)
	}
	if rows[len(rows)-1] != domain.PeerIdentityFromWire(hex4) {
		t.Fatalf("last row = %s, want %s — the hits are sorted and the tail is cut", rows[len(rows)-1], domain.PeerIdentityFromWire(hex4))
	}
	w.menuBtnRects[btn] = image.Rect(0, 0, 10, 10)

	// hex5 was over the cap and never had a row, so losing it moves nothing.
	w.resolveIdentitySearchRows(service.NodeStatus{KnownIDs: []string{hex1, hex2, hex3, hex4}}, nil)
	if !cached() {
		t.Fatal("a hit beyond the cap has no row: dropping it must not cost every cached rectangle")
	}

	// A hit that sorts ahead of all of them pushes every row down one.
	w.resolveIdentitySearchRows(service.NodeStatus{KnownIDs: []string{hex0, hex1, hex2, hex3, hex4}}, nil)
	if cached() {
		t.Fatal("a new first hit moved every search row down one; the cached rectangle now names the row above")
	}
}

// A ⋯ rectangle must not survive the search block MOVING, even when the hits
// in it are untouched. Every other term of the signature describes content or
// viewport, and both are blind to a translation: the touch keyboard taking the
// window header away, or a language change re-wrapping the labels the contacts
// card carries above its search box, slides every row while the query, the
// count, the order and the window size stay exactly as they were. See
// menuRectSig.
func TestMenuRectCacheDropsWhenSearchRowsSlide(t *testing.T) {
	w := &Window{menuBtnRects: make(map[*widget.Clickable]image.Rectangle)}
	btn := new(widget.Clickable)
	cached := func() bool { _, ok := w.menuBtnRects[btn]; return ok }
	gtx := func(availY int) layout.Context {
		return layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(320, availY)},
		}
	}

	w.recordSearchRowAnchor(gtx(644), 1)
	w.menuBtnRects[btn] = image.Rect(0, 0, 10, 10)

	// The same frame again: nothing moved, and a clear here would spend a
	// whole keyboard/Narrator activation for nothing.
	w.recordSearchRowAnchor(gtx(644), 1)
	if !cached() {
		t.Fatal("an unmoved row must keep its rectangle: the cache IS the keyboard/Narrator anchor")
	}

	// The header yields to the keyboard: the same single hit, 66dp higher.
	w.recordSearchRowAnchor(gtx(710), 1)
	if cached() {
		t.Fatal("the rows rose with the header the keyboard took away; the kept rectangle now names the row's old place")
	}

	// No hits records no edge, and goes on recording none however the empty
	// card is sized — there is no row to anchor, so an empty search box
	// drifting must not cost the contacts and chat rectangles.
	w.recordSearchRowAnchor(gtx(710), 0)
	w.menuBtnRects[btn] = image.Rect(0, 0, 10, 10)
	w.recordSearchRowAnchor(gtx(300), 0)
	if !cached() {
		if w.searchAvail != 0 {
			t.Fatalf("with no hits searchAvail = %d, want 0: an empty block has no top edge to record", w.searchAvail)
		}
		t.Fatal("with no hits there is no row to anchor; the empty card's size must not be recorded at all")
	}
}

// The same claim through the REAL layout, which is where it has to hold: the
// number recorded is the space beneath the rows' top edge, so a taller header
// above the card pushes the block down and changes it — while the hits, and
// every other term of the signature, stay identical. A test on the recorder
// alone would pass even if it were called somewhere that cannot see the
// block move.
func TestSearchRowAnchorTracksTheBlockAndNotItsContents(t *testing.T) {
	const (
		hexA     = "11ab110000000000000000000000000000000000"
		headerDp = 66
	)
	status := service.NodeStatus{KnownIDs: []string{hexA}}
	w := newIdentityLayoutTestWindow(t)
	w.recipientButtons = make(map[domain.PeerIdentity]*widget.Clickable)
	w.recipientRightClick = make(map[domain.PeerIdentity]*rightClickState)
	w.recipientMenuBtns = make(map[domain.PeerIdentity]*widget.Clickable)
	w.menuBtnRects = make(map[*widget.Clickable]image.Rectangle)
	w.identitySearchEditor.SetText("11")
	btn := new(widget.Clickable)
	cached := func() bool { _, ok := w.menuBtnRects[btn]; return ok }

	// One frame of the sidebar column: a header of the given height, then the
	// search card under it, both Rigid in a vertical Flex the height of the
	// window — which is the shape the real card sits in.
	frame := func(header int) {
		gtx := layout.Context{
			Ops:         new(op.Ops),
			Metric:      unit.Metric{PxPerDp: 1, PxPerSp: 1},
			Constraints: layout.Constraints{Max: image.Pt(320, 800)},
		}
		results := w.resolveIdentitySearchRows(status, nil)
		if len(results) != 1 {
			t.Fatalf("hits = %d, want the one identity every frame here matches", len(results))
		}
		layout.Flex{Axis: layout.Vertical}.Layout(gtx,
			layout.Rigid(layout.Spacer{Height: unit.Dp(header)}.Layout),
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return w.identitySearchCard(gtx, status, results)
			}),
		)
	}

	frame(headerDp)
	settled := w.searchAvail
	if settled == 0 {
		t.Fatal("a laid-out hit must record where it was laid out")
	}
	w.menuBtnRects[btn] = image.Rect(0, 0, 10, 10)

	frame(headerDp)
	if w.searchAvail != settled || !cached() {
		t.Fatalf("an identical frame moved nothing: searchAvail %d -> %d, cached = %v", settled, w.searchAvail, cached())
	}

	// The keyboard came up and the header went away.
	frame(0)
	if w.searchAvail != settled+headerDp {
		t.Fatalf("searchAvail = %d with the header gone, want %d — the rows rise by exactly its height", w.searchAvail, settled+headerDp)
	}
	if cached() {
		t.Fatal("the block slid up while its one row stayed the same peer; a kept rectangle anchors the menu where the row no longer is")
	}
}
