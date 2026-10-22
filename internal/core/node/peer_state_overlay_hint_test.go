package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func overlayHintFixture(t *testing.T) (peerStateFile, *identity.Identity) {
	t.Helper()
	self, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	until := time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second)
	return peerStateFile{
		Version:             peerStateVersion,
		Peers:               []peerEntry{{Address: "203.0.113.51:64646", Identity: domain.PeerIdentityFromWire(peer.Address), AddedAt: &at, Source: domain.PeerSourcePeerExchange, Score: 7}},
		BannedIPs:           []bannedIPStateEntry{{IP: "198.51.100.8", BannedUntil: until, BanOrigin: "local", BanReason: "fixture"}},
		RemoteBannedIPs:     []remoteBannedIPStateEntry{{IP: "198.51.100.9", Until: until, Reason: "fixture"}},
		FirstHopGuards:      []firstHopGuardRow{{Identity: peer.Address, SampledAt: at, Failures: 2}},
		FirstHopGuardsOwner: self.Address,
	}, self
}

func overlayHintPayload(t *testing.T, state peerStateFile, fields string) []byte {
	t.Helper()
	state.OverlayDWin = overlayDegreeHint{}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if fields == "" {
		return raw
	}
	return []byte(strings.TrimSuffix(string(raw), "}") + "," + fields + "}")
}

func overlayHintWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func overlayHintAssertPayload(t *testing.T, got, want peerStateFile) {
	t.Helper()
	if !reflect.DeepEqual(got.Peers, want.Peers) || !reflect.DeepEqual(got.BannedIPs, want.BannedIPs) || !reflect.DeepEqual(got.RemoteBannedIPs, want.RemoteBannedIPs) || !reflect.DeepEqual(got.FirstHopGuards, want.FirstHopGuards) || got.FirstHopGuardsOwner != want.FirstHopGuardsOwner {
		t.Fatal("optional hint damaged existing peer, ban or guard payload")
	}
}

func overlayHintServiceWithoutRun(t *testing.T, state peerStateFile, self *identity.Identity, fields string) *Service {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	overlayHintWrite(t, path, overlayHintPayload(t, state, fields))
	// The real common constructor is used without Run, bootstrapLoop or dials.
	svc := NewService(config.Node{ListenAddress: "127.0.0.1:0", PeersStatePath: path, TrustStorePath: filepath.Join(dir, "trust.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json"), AllowPrivatePeers: true, Type: domain.NodeTypeClient}, self, nil)
	t.Cleanup(func() { svc.runCancel(); svc.closeBackgroundAdmission(); svc.WaitBackground() })
	return svc
}

func TestOverlayDegreeHintNumberDomainAndValueMarshal(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, c := range []struct {
		name, number string
		valid        bool
		want         uint8
	}{
		{"zero", "0", true, 0}, {"below", "63", true, 63}, {"exact64", "64", true, 64}, {"above", "65", true, 64},
		{"maxuint64", "18446744073709551615", true, 64}, {"overflow", "18446744073709551616", false, 0},
		{"negative", "-1", false, 0}, {"negative_zero", "-0", false, 0}, {"fraction", "1.5", false, 0}, {"exponent", "1e1", false, 0}, {"string", `"64"`, false, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			var state peerStateFile
			raw := fmt.Sprintf(`{"overlay_d_win":{"value":%s,"at":%q}}`, c.number, at.Format(time.RFC3339Nano))
			if err := json.Unmarshal([]byte(raw), &state); err != nil {
				t.Fatal("optional number escaped as whole-state error", err)
			}
			if state.OverlayDWin.stat.valid != c.valid || state.OverlayDWin.stat.value != c.want {
				t.Fatal("optional uint64 domain or clip64 changed", c.name, state.OverlayDWin.stat)
			}
			// Passing the DTO by value exercises the value MarshalJSON receiver.
			encoded, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if !c.valid {
				if bytes.Contains(encoded, []byte("overlay_d_win")) {
					t.Fatal("invalid optional hint was serialized")
				}
				return
			}
			var again peerStateFile
			if err := json.Unmarshal(encoded, &again); err != nil {
				t.Fatal(err)
			}
			if !again.OverlayDWin.stat.valid || again.OverlayDWin.stat.value != c.want || !again.OverlayDWin.stat.at.Equal(at) {
				t.Fatal("value DTO marshal lost clipped scalar or original instant")
			}
			if bytes.Contains(encoded, []byte(`"seen"`)) || bytes.Contains(encoded, []byte(`"stat"`)) || bytes.Contains(encoded, []byte(`"valid"`)) {
				t.Fatal("transient/private hint state escaped JSON")
			}
		})
	}
}

func TestOverlayDegreeHintMalformedOptionalPreservesPayload(t *testing.T) {
	state, _ := overlayHintFixture(t)
	at := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	for _, c := range []struct{ name, value string }{
		{"null", "null"}, {"bool", "true"}, {"array", "[]"}, {"string", `"bad"`}, {"empty", "{}"},
		{"missing_at", `{"value":2}`}, {"missing_value", fmt.Sprintf(`{"at":%q}`, at)},
		{"bad_at", `{"value":2,"at":"not-a-time"}`}, {"null_at", `{"value":2,"at":null}`}, {"zero_at", `{"value":2,"at":"0001-01-01T00:00:00Z"}`},
		{"unknown", fmt.Sprintf(`{"value":2,"at":%q,"other":0}`, at)},
		{"duplicate_value", fmt.Sprintf(`{"value":2,"value":3,"at":%q}`, at)},
		{"escaped_duplicate_value", fmt.Sprintf(`{"value":2,"\u0076alue":3,"at":%q}`, at)},
		{"duplicate_at", fmt.Sprintf(`{"value":2,"at":%q,"at":%q}`, at, at)},
		{"oversize", fmt.Sprintf(`{"value":2,"at":%q,%q:0}`, at, strings.Repeat("x", 256))},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "peers.json")
			overlayHintWrite(t, path, overlayHintPayload(t, state, `"overlay_d_win":`+c.value))
			got, err := loadPeerState(path)
			if err != nil {
				t.Fatal("malformed optional value poisoned valid peer state", err)
			}
			overlayHintAssertPayload(t, got, state)
			if got.OverlayDWin.stat.valid {
				t.Fatal("malformed optional hint became valid", c.name)
			}
		})
	}
}

func TestOverlayDegreeHintDuplicateOuterKeysStayInvalid(t *testing.T) {
	state, _ := overlayHintFixture(t)
	valid := fmt.Sprintf(`{"value":64,"at":%q}`, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano))
	for _, c := range []struct{ name, fields string }{
		{"twice", `"overlay_d_win":` + valid + `,"overlay_d_win":` + valid},
		{"invalid_then_valid", `"overlay_d_win":false,"overlay_d_win":` + valid},
		{"null_then_valid", `"overlay_d_win":null,"overlay_d_win":` + valid},
		{"valid_null_valid", `"overlay_d_win":` + valid + `,"overlay_d_win":null,"overlay_d_win":` + valid},
		{"case_alias", `"overlay_d_win":` + valid + `,"OVERLAY_D_WIN":` + valid},
		{"escaped_alias", `"overlay_d_win":` + valid + `,"overlay_d_\u0077in":` + valid},
		{"oversize_then_valid", `"overlay_d_win":{"value":2,"at":"` + strings.Repeat("x", 256) + `"},"overlay_d_win":` + valid},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "peers.json")
			overlayHintWrite(t, path, overlayHintPayload(t, state, c.fields))
			got, err := loadPeerState(path)
			if err != nil {
				t.Fatal(err)
			}
			overlayHintAssertPayload(t, got, state)
			if got.OverlayDWin.stat.valid {
				t.Fatal("duplicate outer hint recovered validity", c.name)
			}
		})
	}
}

func TestOverlayDegreeHintEncodedValueBound(t *testing.T) {
	state, _ := overlayHintFixture(t)
	value := fmt.Sprintf(`{"value":8,"at":%q}`, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano))
	for _, n := range []int{256, 257} {
		t.Run(fmt.Sprintf("bytes%d", n), func(t *testing.T) {
			padded := strings.TrimSuffix(value, "}") + strings.Repeat(" ", n-len(value)) + "}"
			if len(padded) != n {
				t.Fatal("encoded bound fixture length")
			}
			path := filepath.Join(t.TempDir(), "peers.json")
			overlayHintWrite(t, path, overlayHintPayload(t, state, `"overlay_d_win":`+padded))
			got, err := loadPeerState(path)
			if err != nil {
				t.Fatal(err)
			}
			overlayHintAssertPayload(t, got, state)
			if got.OverlayDWin.stat.valid != (n == 256) {
				t.Fatal("optional encoded value bound changed", n)
			}
		})
	}
}

func TestOverlayDegreeHintFreshWallBoundaries(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	for _, c := range []struct {
		name  string
		at    time.Time
		valid bool
	}{
		{"fresh", now.Add(-24*time.Hour + time.Nanosecond), true}, {"exact24h", now.Add(-24 * time.Hour), false},
		{"old", now.Add(-25 * time.Hour), false}, {"future", now.Add(time.Nanosecond), false}, {"zero", time.Time{}, false}, {"now", now, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := freshOverlayDegreeStatistic(overlayDegreeStatistic{value: 64, at: c.at, valid: true}, now)
			if got.valid != c.valid {
				t.Fatal("optional wall freshness boundary changed", c.name)
			}
			if got.valid && (got.value != 64 || !got.at.Equal(c.at)) {
				t.Fatal("fresh validation refreshed original at or value")
			}
		})
	}
	if freshOverlayDegreeStatistic(overlayDegreeStatistic{value: 65, at: now, valid: true}, now).valid || freshOverlayDegreeStatistic(overlayDegreeStatistic{at: now}, now).valid {
		t.Fatal("invalid private scalar acquired validity")
	}
}

func TestOverlayDegreeHintWholeFileErrorsAndOldMigration(t *testing.T) {
	for _, raw := range []string{`{"peers":[],"overlay_d_win":{`, `{"peers":"wrong","overlay_d_win":null}`, `{"banned_ips":"wrong","overlay_d_win":null}`} {
		path := filepath.Join(t.TempDir(), "peers.json")
		overlayHintWrite(t, path, []byte(raw))
		if _, err := loadPeerState(path); err == nil {
			t.Fatal("optional permissiveness masked old whole-file error")
		}
	}
	path := filepath.Join(t.TempDir(), "legacy.json")
	stamp := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	raw := fmt.Sprintf(`{"version":4,"peers":[{"address":"203.0.113.51:64646","last_connected_at":%q}],"overlay_d_win":false}`, stamp.Format(time.RFC3339))
	overlayHintWrite(t, path, []byte(raw))
	got, err := loadPeerState(path)
	if err != nil || got.Version != peerStateVersion || len(got.Peers) != 1 || got.Peers[0].LastSessionAt == nil || !got.Peers[0].LastSessionAt.Equal(stamp) || got.Peers[0].AddedAt == nil {
		t.Fatal("optional hint disturbed pre-v5 migration", got, err)
	}
	overlayHintWrite(t, path, []byte(`{"version":5,"peers":null,"overlay_d_win":false}`))
	got, err = loadPeerState(path)
	if err != nil || got.Peers == nil || got.OverlayDWin.stat.valid {
		t.Fatal("null peers normalization changed", err)
	}
}

func TestOverlayDegreeHintCommonConstructorActualFlushAndReload(t *testing.T) {
	state, self := overlayHintFixture(t)
	at := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	svc := overlayHintServiceWithoutRun(t, state, self, fmt.Sprintf(`"overlay_d_win":{"value":63,"at":%q}`, at.Format(time.RFC3339Nano)))
	if svc.degree != nil || svc.findNode != nil {
		t.Fatal("ordinary constructor activated degree or FindNode")
	}
	if !svc.overlayDWin.valid || svc.overlayDWin.value != 63 || !svc.overlayDWin.at.Equal(at) {
		t.Fatal("common constructor dropped loaded scalar")
	}
	for pass := 0; pass < 2; pass++ {
		svc.markPeerStateDirty()
		svc.flushPeerState()
		got, err := loadPeerState(svc.peersStatePath)
		if err != nil {
			t.Fatal(err)
		}
		if !got.OverlayDWin.stat.valid || got.OverlayDWin.stat.value != 63 || !got.OverlayDWin.stat.at.Equal(at) {
			t.Fatal("actual flush dropped or refreshed original hint")
		}
		if got.UpdatedAt.IsZero() || !got.UpdatedAt.After(at) || got.UpdatedAt.Equal(got.OverlayDWin.stat.at) {
			t.Fatal("file UpdatedAt replaced hint observation time")
		}
		if len(got.Peers) != 1 || got.Peers[0].Address != state.Peers[0].Address || got.Peers[0].Identity != state.Peers[0].Identity || len(got.BannedIPs) != 1 || got.BannedIPs[0].IP != state.BannedIPs[0].IP || len(got.RemoteBannedIPs) != 1 || got.RemoteBannedIPs[0].IP != state.RemoteBannedIPs[0].IP || len(got.FirstHopGuards) != 1 || got.FirstHopGuards[0].Identity != state.FirstHopGuards[0].Identity || got.FirstHopGuardsOwner != self.Address {
			t.Fatal("actual ordinary flush lost unrelated peer state")
		}
	}
}

func TestOverlayDegreeHintExpiredAndAbsentOrdinaryFlush(t *testing.T) {
	state, self := overlayHintFixture(t)
	for _, fields := range []string{"", `"overlay_d_win":false`, fmt.Sprintf(`"overlay_d_win":{"value":64,"at":%q}`, time.Now().UTC().Add(2*time.Hour).Format(time.RFC3339Nano))} {
		svc := overlayHintServiceWithoutRun(t, state, self, fields)
		if svc.overlayDWin.valid {
			t.Fatal("absent invalid or future hint survived constructor")
		}
		svc.flushPeerState()
		raw, err := os.ReadFile(svc.peersStatePath)
		if err != nil || bytes.Contains(raw, []byte("overlay_d_win")) {
			t.Fatal("ordinary absent shape gained hint", err)
		}
	}
	svc := overlayHintServiceWithoutRun(t, state, self, fmt.Sprintf(`"overlay_d_win":{"value":64,"at":%q}`, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)))
	// Private elapsed-time fixture, before any Run: it is not a new clock seam.
	svc.peerMu.Lock()
	svc.overlayDWin.at = time.Now().UTC().Add(-25 * time.Hour)
	svc.peerMu.Unlock()
	svc.flushPeerState()
	raw, err := os.ReadFile(svc.peersStatePath)
	if err != nil || bytes.Contains(raw, []byte("overlay_d_win")) {
		t.Fatal("actual flush refreshed expired hint", err)
	}
}

func TestOverlayDegreeHintActualWriteFailureRetainsDirtyAndScalar(t *testing.T) {
	state, self := overlayHintFixture(t)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	svc := overlayHintServiceWithoutRun(t, state, self, fmt.Sprintf(`"overlay_d_win":{"value":12,"at":%q}`, at.Format(time.RFC3339Nano)))
	goodPath := svc.peersStatePath
	blocker := filepath.Join(t.TempDir(), "regular-file-not-directory")
	overlayHintWrite(t, blocker, []byte("blocker"))
	svc.peerMu.Lock()
	before := svc.lastPeerSave
	svc.peersStatePath = filepath.Join(blocker, "peers.json")
	svc.peerStateDirty = true
	svc.peerMu.Unlock()
	svc.flushPeerState() // Existing error log is expected for this deliberate I/O refusal.
	svc.peerMu.RLock()
	kept := svc.overlayDWin
	dirty, last := svc.peerStateDirty, svc.lastPeerSave
	svc.peerMu.RUnlock()
	if !dirty || !last.Equal(before) || !kept.valid || kept.value != 12 || !kept.at.Equal(at) {
		t.Fatal("failed existing writer lost scalar or retry state")
	}
	svc.peerMu.Lock()
	svc.peersStatePath = goodPath
	svc.peerMu.Unlock()
	svc.flushPeerState()
	got, err := loadPeerState(goodPath)
	if err != nil || !got.OverlayDWin.stat.valid || !got.OverlayDWin.stat.at.Equal(at) {
		t.Fatal("existing writer retry did not retain original hint", err)
	}
}

// A finite syntax inventory supplements JSON/load/actual-writer checks; it
// does not claim universal dataflow or process hermeticity.
func overlayHintSourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"overlayDegreeHint":           {"peer_state_overlay_hint.go:<package>": 1, "peer_state_overlay_hint.go:UnmarshalJSON": 2, "peer_state_overlay_hint.go:IsZero": 1, "peer_state_overlay_hint.go:MarshalJSON": 1, "peer_state.go:<package>": 1, "peer_state.go:loadPeerState": 1, "peer_management.go:flushPeerState": 1},
		"overlayDegreeStatistic":      {"peer_state_overlay_hint.go:<package>": 2, "peer_state_overlay_hint.go:UnmarshalJSON": 1, "peer_state_overlay_hint.go:freshOverlayDegreeStatistic": 3, "service.go:<package>": 1},
		"freshOverlayDegreeStatistic": {"peer_state_overlay_hint.go:freshOverlayDegreeStatistic": 1, "peer_state.go:loadPeerState": 1, "peer_management.go:flushPeerState": 1},
		"overlayDWin":                 {"service.go:<package>": 1, "service.go:newServiceWithConstruction": 1, "peer_management.go:flushPeerState": 1},
		"OverlayDWin":                 {"peer_state.go:<package>": 1, "peer_state.go:loadPeerState": 2, "service.go:newServiceWithConstruction": 1, "peer_management.go:flushPeerState": 1},
		"savePeerState":               {"peer_state.go:savePeerState": 1, "peer_management.go:flushPeerState": 1},
	}

	// Only the unused producer and the same actual flush snapshot may add references.
	for name, sites := range map[string]map[string]int{
		"overlayDegreeStatistic":      {"peer_state_degree_statistic.go:captureOverlayDWinForFlush": 6, "peer_state_degree_statistic.go:selectOverlayDegreeStatistic": 2, "peer_state_owned_degree_statistic.go:captureOwnedOverlayDWinForFlush": 8},
		"freshOverlayDegreeStatistic": {"peer_state_degree_statistic.go:selectOverlayDegreeStatistic": 1},
		"overlayDWin":                 {"peer_management.go:flushPeerState": 3},
	} {
		for site, count := range sites {
			expected[name][site] = count
		}
	}
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["freshOverlayDegreeStatistic"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 1
	expected["OverlayDWin"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 1
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	var failures []string
	for name, raw := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			failures = append(failures, "overlay hint source parse error")
			continue
		}
		if name == "peer_state_overlay_hint.go" {
			imports := map[string]bool{}
			for _, imp := range file.Imports {
				imports[strings.Trim(imp.Path.Value, "\"")] = true
			}
			if !reflect.DeepEqual(imports, map[string]bool{"bytes": true, "encoding/json": true, "io": true, "strconv": true, "time": true}) {
				failures = append(failures, "overlay hint acquired extra dependency")
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if _, ok := n.(*ast.GoStmt); ok {
					failures = append(failures, "overlay hint acquired a worker")
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			site := name + ":<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = name + ":" + fn.Name.Name
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if sites, tracked := actual[id.Name]; tracked {
						sites[site]++
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		failures = append(failures, "overlay hint closed source inventory changed")
	}
	flush := string(sources["peer_management.go"])
	start := strings.Index(flush, "func (s *Service) flushPeerState()")
	if start >= 0 {
		flush = flush[start:]
	}
	held := strings.Index(flush, "overlayHint := s.overlayDWin")
	unlock := strings.Index(flush, "s.peerMu.Unlock()")
	fresh := strings.Index(flush, "freshOverlayDegreeStatistic(overlayHint,")
	dto := strings.Index(flush, "OverlayDWin:")
	save := strings.Index(flush, "savePeerState(path, state)")
	clear := strings.Index(flush, "s.peerStateDirty = false")
	if start < 0 || clear < 0 || held <= clear || unlock <= held || fresh <= unlock || dto <= fresh || save <= dto {
		failures = append(failures, "overlay hint snapshot validation or dirty order changed")
	}
	return failures
}

func overlayHintReadSources(t *testing.T) map[string][]byte {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = raw
	}
	return sources
}

func TestOverlayDegreeHintClosedScalarDTOAndWriter(t *testing.T) {
	for _, c := range []struct {
		typ   reflect.Type
		names []string
	}{
		{reflect.TypeFor[overlayDegreeStatistic](), []string{"value", "at", "valid"}},
		{reflect.TypeFor[overlayDegreeHint](), []string{"stat", "seen"}},
	} {
		if c.typ.NumField() != len(c.names) {
			t.Fatal("overlay hint retained unbounded or extra state")
		}
		for i, name := range c.names {
			if c.typ.Field(i).Name != name {
				t.Fatal("overlay hint retained state changed")
			}
		}
	}
	field, ok := reflect.TypeFor[peerStateFile]().FieldByName("OverlayDWin")
	if !ok || field.PkgPath != "" || field.Tag.Get("json") != "overlay_d_win,omitzero" {
		t.Fatal("optional DTO field export/tag changed")
	}
	if failures := overlayHintSourceErrors(overlayHintReadSources(t)); len(failures) != 0 {
		t.Fatal(failures)
	}
}

func TestOverlayDegreeHintGuardRejectsConsumerAndMissingFreshSnapshot(t *testing.T) {
	base := overlayHintReadSources(t)
	for _, c := range []struct{ name, path, before, after string }{
		{"consumer", "peer_state_overlay_hint.go", "", "\nvar overlayHintEscape = freshOverlayDegreeStatistic\n"},
		{"fresh_snapshot", "peer_management.go", "overlayHint = freshOverlayDegreeStatistic(overlayHint, time.Now().UTC())", "overlayHint = overlayHint"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sources := map[string][]byte{}
			for name, raw := range base {
				sources[name] = append([]byte(nil), raw...)
			}
			raw := string(sources[c.path])
			if c.before == "" {
				raw += c.after
			} else {
				if strings.Count(raw, c.before) != 1 {
					t.Fatal("overlay source fixture anchor")
				}
				raw = strings.Replace(raw, c.before, c.after, 1)
			}
			sources[c.path] = []byte(raw)
			if len(overlayHintSourceErrors(sources)) == 0 {
				t.Fatal("overlay guard accepted consumer or missing post-lock freshness")
			}
		})
	}
}
