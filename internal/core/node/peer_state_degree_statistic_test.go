package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func statisticService(t *testing.T, profile degreeProfile, prior overlayDegreeStatistic) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	if prior.valid {
		if err := savePeerState(cfg.EffectivePeersStatePath(), peerStateFile{Version: peerStateVersion, OverlayDWin: overlayDegreeHint{stat: prior}}); err != nil {
			t.Fatal(err)
		}
	}
	s, err := newDegreeStatisticCandidateService(cfg, id, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}

func TestDegreeStatisticRuntimePartialPreservesPriorWithGenuineP1(t *testing.T) {
	prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), valid: true}
	s := statisticService(t, degreeProfile{MaxActive: 2}, prior)
	d := statisticService(t, degreeProfile{MaxActive: 2}, overlayDegreeStatistic{})
	wire, tr := degreeActualPair(t, s, d) // genuine Node Accept/Dial, not synthetic ProvenSession
	if wire.common.Load() == nil || tr.common == nil {
		t.Fatal("statistic actual P1 missing")
	}
	s.degree.mu.Lock()
	observed := 0
	for _, row := range s.degree.active {
		if row.life != nil {
			observed++
		}
	}
	start := s.degreeStatisticProducer.coverageStart
	s.degree.mu.Unlock()
	if observed != 1 {
		t.Fatal("genuine P1 bypassed installed statistic observer")
	}
	stat, err := s.captureOverlayDWinForFlush()
	if !errors.Is(err, errDegreeUnavailable) || stat.valid {
		t.Fatal("partial runtime observation overwrote preserved hint")
	}
	s.degree.mu.Lock()
	last := s.degree.last
	s.degree.mu.Unlock()
	if last < start {
		t.Fatal("partial runtime did not record monotonic high water")
	}
	s.flushPeerState()
	loaded, err := loadPeerState(s.peersStatePath)
	if err != nil || !loaded.OverlayDWin.stat.valid || loaded.OverlayDWin.stat.value != 16 || !loaded.OverlayDWin.stat.at.Equal(prior.at) {
		t.Fatal("actual runtime partial flush refreshed or replaced original hint", err)
	}
}

func TestDegreeStatisticShortRestartsNeverRefreshOriginalAt(t *testing.T) {
	prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second), valid: true}
	for pass := 0; pass < 3; pass++ {
		s := statisticService(t, degreeProfile{MaxActive: 1}, prior)
		s.flushPeerState()
		got, err := loadPeerState(s.peersStatePath)
		if err != nil || !got.OverlayDWin.stat.valid || got.OverlayDWin.stat.value != 16 || !got.OverlayDWin.stat.at.Equal(prior.at) {
			t.Fatal("short restart renewed or zeroed prior hint", err)
		}
		prior = got.OverlayDWin.stat
	}
}

func TestDegreeStatisticOrdinaryOldDegreeAndC5RootsStayInactive(t *testing.T) {
	for _, kind := range []string{"ordinary", "old_degree", "C5"} {
		t.Run(kind, func(t *testing.T) {
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", AllowPrivatePeers: true, Type: domain.NodeTypeClient})
			prior := overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), valid: true}
			if err := savePeerState(cfg.EffectivePeersStatePath(), peerStateFile{Version: peerStateVersion, OverlayDWin: overlayDegreeHint{stat: prior}}); err != nil {
				t.Fatal(err)
			}
			var s *Service
			switch kind {
			case "ordinary":
				s = NewService(cfg, id, nil)
			case "old_degree":
				s, err = newDegreeCandidateService(cfg, id, nil, degreeProfile{MaxActive: 1})
			case "C5":
				s, err = newFindNodeCandidateService(cfg, id, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
			if s.degreeStatisticProducer != nil {
				t.Fatal("old constructor activated statistic producer", kind)
			}
			s.flushPeerState()
			got, err := loadPeerState(s.peersStatePath)
			if err != nil || !got.OverlayDWin.stat.valid || got.OverlayDWin.stat.value != 16 || !got.OverlayDWin.stat.at.Equal(prior.at) {
				t.Fatal("inactive root refreshed or replaced stored statistic", kind, err)
			}
		})
	}
}

func TestDegreeStatisticRuntimeRegressionPoisonsWithoutOriginRewrite(t *testing.T) {
	s := statisticService(t, degreeProfile{MaxActive: 1}, overlayDegreeStatistic{})
	s.degree.mu.Lock()
	s.degree.last = time.Duration(math.MaxInt64) // corrupt future HWM fixture; original runtime origin untouched
	_, _, ok := sampleDegreeStatisticClockLocked(s.degree)
	poisoned, last := s.degree.poisoned, s.degree.last
	s.degree.mu.Unlock()
	if ok || !poisoned || last != time.Duration(math.MaxInt64) {
		t.Fatal("runtime regression advanced or failed to poison")
	}
}

func TestDegreeStatisticRequiredProfileAndCopiedBound(t *testing.T) {
	for _, m := range []int{0, -1, int(^uint(0) >> 1)} {
		if s, err := newDegreeStatisticCandidateService(config.Node{}, nil, nil, degreeProfile{MaxActive: m}); s != nil || !errors.Is(err, errDegreeProfile) {
			t.Fatal("statistic root accepted invalid or default profile")
		}
	}
	profile := degreeProfile{MaxActive: 2}
	s := statisticService(t, profile, overlayDegreeStatistic{})
	profile.MaxActive = 17
	if s.degree.profile.MaxActive != 2 || len(s.degree.active) != 2 {
		t.Fatal("statistic root retained mutable profile")
	}
}

func TestDegreeStatisticActualPartialExpiryDoesNotRenewPrior(t *testing.T) {
	s := statisticService(t, degreeProfile{MaxActive: 1}, overlayDegreeStatistic{value: 16, at: time.Now().UTC().Add(-time.Hour), valid: true})
	// Expired historical-statistic fixture; the actual runtime origin and
	// coverage start are unchanged and still partial.
	expired := time.Now().UTC().Add(-25 * time.Hour)
	s.peerMu.Lock()
	s.overlayDWin.at = expired
	s.peerMu.Unlock()
	s.flushPeerState()
	got, err := loadPeerState(s.peersStatePath)
	if err != nil || got.OverlayDWin.stat.valid || !s.overlayDWin.at.Equal(expired) {
		t.Fatal("partial observation renewed expired prior", err)
	}
}

func TestDegreeStatisticPureCoverageExcludesConstructionAndExact600(t *testing.T) {
	// Explicit arithmetic fixtures: not a runtime origin rewind or evidence that
	// any actual candidate has already observed600 seconds.
	start := 1000 * time.Second
	for _, c := range []struct {
		name  string
		q     time.Duration
		ready bool
	}{
		{"construction_only", 1001 * time.Second, false}, {"just_before", 1600*time.Second - time.Nanosecond, false},
		{"exact600", 1600 * time.Second, true}, {"later", 1601 * time.Second, true}, {"before_start", 999 * time.Second, false},
		{"negative", -1, false}, {"saturated", time.Duration(math.MaxInt64), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if degreeStatisticCoverageReady(start, c.q) != c.ready {
				t.Fatal("coverage boundary or construction exclusion changed")
			}
		})
	}
	if degreeStatisticCoverageReady(-1, degreeRecent) {
		t.Fatal("negative coverage start accepted")
	}
}

func statisticPureOwner(maximum int) *degreeOwner {
	return &degreeOwner{token: &proofTransportServiceToken{}, profile: degreeProfile{MaxActive: maximum}, active: make([]degreeActiveRow, maximum)}
}
func statisticPureKey(n byte, outgoing bool, group string) degreeKey {
	var id domain.PeerIdentity
	id[0], id[len(id)-1] = n, n
	return degreeKey{outgoing: outgoing, group: domain.SourceGroup(group), identity: id}
}
func statisticPureActive(o *degreeOwner, slot int, key degreeKey) {
	life := &proofTransportLifetime{token: o.token}
	witness := &degreeWitness{owner: o, key: key, slot: slot, generation: 1}
	o.active[slot] = degreeActiveRow{life: life, witness: witness, generation: 1}
}

func TestDegreeStatisticPureProjectionHistoryUnchangedAndCurrentBasis(t *testing.T) {
	o := statisticPureOwner(4)
	q := 900 * time.Second
	// Old OUT expires exactly at600. Active duplicate cannot count twice.
	o.history[0] = degreeHistoryRow{key: statisticPureKey(1, true, ""), last: 300 * time.Second}
	o.history[1] = degreeHistoryRow{key: statisticPureKey(2, false, "g"), last: 301 * time.Second}
	o.historyN = 2
	statisticPureActive(o, 0, statisticPureKey(2, false, "g"))
	statisticPureActive(o, 1, statisticPureKey(3, false, "g"))
	statisticPureActive(o, 2, statisticPureKey(4, false, "g"))
	statisticPureActive(o, 3, statisticPureKey(5, true, ""))
	before, n := o.history, o.historyN
	out, in, win, err := projectDegreeStatisticLocked(o, q)
	if err != nil || out != 1 || in != 2 || win != 3 {
		t.Fatal("pure current basis group distinct or expiry changed", out, in, win, err)
	}
	if o.history != before || o.historyN != n {
		t.Fatal("statistic projection mutated terminal history")
	}
}

func TestDegreeStatisticPureActiveDuplicateCountsOnce(t *testing.T) {
	o := statisticPureOwner(2)
	key := statisticPureKey(1, true, "")
	statisticPureActive(o, 0, key)
	statisticPureActive(o, 1, key)
	out, in, win, err := projectDegreeStatisticLocked(o, degreeRecent)
	if err != nil || out != 1 || in != 0 || win != 1 {
		t.Fatal("duplicate original active identities counted twice", out, in, win, err)
	}
}

func TestDegreeStatisticPureProjectionClipsRankWithoutInventingHistory(t *testing.T) {
	o := statisticPureOwner(70)
	for i := range o.active {
		statisticPureActive(o, i, statisticPureKey(byte(i+1), true, ""))
	}
	out, in, win, err := projectDegreeStatisticLocked(o, degreeRecent)
	if err != nil || out != 64 || in != 0 || win != 64 || o.historyN != 0 {
		t.Fatal("fixed basis rank or history purity changed", out, in, win, err)
	}
}

func TestDegreeStatisticPureCompleteZeroReplacesNonzeroWithoutMax(t *testing.T) {
	o := statisticPureOwner(1)
	o.history[0] = degreeHistoryRow{key: statisticPureKey(1, true, ""), last: 0}
	o.historyN = 1
	_, _, win, err := projectDegreeStatisticLocked(o, degreeRecent)
	if err != nil || win != 0 {
		t.Fatal("complete expired window did not produce genuine zero", err)
	}
	now := time.Unix(1800000000, 0).UTC()
	prior := overlayDegreeStatistic{value: 16, at: now.Add(-time.Hour), valid: true}
	candidate := overlayDegreeStatistic{value: uint8(win), at: now, valid: true}
	got := selectOverlayDegreeStatistic(prior, candidate, now)
	if !got.valid || got.value != 0 || !got.at.Equal(now) {
		t.Fatal("complete zero failed to replace nonzero prior; no max policy")
	}
}

func TestDegreeStatisticPureRefusalsNeverBecomeZero(t *testing.T) {
	for _, kind := range []string{"unknown", "poison", "bad_slot", "foreign_owner", "wrong_token", "closed", "untracked", "generation", "bad_history_time", "duplicate", "group_quota", "history_count", "active_count"} {
		t.Run(kind, func(t *testing.T) {
			o := statisticPureOwner(1)
			statisticPureActive(o, 0, statisticPureKey(1, true, ""))
			switch kind {
			case "unknown":
				o.unknown = 1
			case "poison":
				o.poisoned = true
			case "bad_slot":
				o.active[0].witness.slot = 1
			case "foreign_owner":
				o.active[0].witness.owner = statisticPureOwner(1)
			case "wrong_token":
				o.active[0].life.token = &proofTransportServiceToken{}
			case "closed":
				o.active[0].witness.closed = true
			case "untracked":
				o.active[0].witness.untracked = true
			case "generation":
				o.active[0].generation = 2
			case "bad_history_time":
				o.historyN = 1
				o.history[0] = degreeHistoryRow{key: statisticPureKey(2, true, ""), last: degreeRecent + 1}
			case "duplicate":
				o.historyN = 2
				o.history[0] = degreeHistoryRow{key: statisticPureKey(2, true, ""), last: 0}
				o.history[1] = o.history[0]
			case "group_quota":
				o.historyN = 3
				for i := 0; i < 3; i++ {
					o.history[i] = degreeHistoryRow{key: statisticPureKey(byte(i+2), false, "g"), last: 0}
				}
			case "history_count":
				o.historyN = 65
			case "active_count":
				o.profile.MaxActive = 2
			}
			if _, _, _, err := projectDegreeStatisticLocked(o, degreeRecent); !errors.Is(err, errDegreeUnavailable) {
				t.Fatal("unavailable projection became valid zero", kind)
			}
		})
	}
}

func TestDegreeStatisticRuntimeClockRejectsFixtureAndRecordsBeforeUnknown(t *testing.T) {
	fixture, _ := degreeTestService(t, 1)
	fixture.degree.mu.Lock()
	_, _, ok := sampleDegreeStatisticClockLocked(fixture.degree)
	fixture.degree.mu.Unlock()
	if ok {
		t.Fatal("fixture clock presented as paired runtime wall evidence")
	}
	s := statisticService(t, degreeProfile{MaxActive: 1}, overlayDegreeStatistic{})
	s.degree.mu.Lock()
	s.degree.unknown = 1 // private refusal fixture, not a synthetic600s clock
	before := s.degree.last
	_, _, valid := sampleDegreeStatisticClockLocked(s.degree)
	after := s.degree.last
	_, _, _, err := projectDegreeStatisticLocked(s.degree, after)
	s.degree.mu.Unlock()
	if !valid || after < before || !errors.Is(err, errDegreeUnavailable) {
		t.Fatal("unknown refusal lost genuine runtime HWM ordering")
	}
}

func TestDegreeStatisticUnavailableExpiredAndFutureNeverRefreshAt(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	prior := overlayDegreeStatistic{value: 16, at: now.Add(-time.Hour), valid: true}
	for _, candidate := range []overlayDegreeStatistic{{}, {value: 0, at: now.Add(1), valid: true}, {value: 0, at: now.Add(-24 * time.Hour), valid: true}, {value: 0, at: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), valid: true}} {
		got := selectOverlayDegreeStatistic(prior, candidate, now)
		if got != prior {
			t.Fatal("unavailable statistic refreshed prior tuple")
		}
	}
	expired := prior
	expired.at = now.Add(-24 * time.Hour)
	kept := selectOverlayDegreeStatistic(expired, overlayDegreeStatistic{}, now)
	if !kept.at.Equal(expired.at) || freshOverlayDegreeStatistic(kept, now).valid {
		t.Fatal("expired observing hint was renewed instead of omitted")
	}
}

func TestDegreeStatisticActualPartialFailureRetryKeepsOriginalAt(t *testing.T) {
	prior := overlayDegreeStatistic{value: 12, at: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), valid: true}
	s := statisticService(t, degreeProfile{MaxActive: 1}, prior)
	good := s.peersStatePath
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("blocker"), 0600); err != nil {
		t.Fatal(err)
	}
	before := s.lastPeerSave
	s.peersStatePath = filepath.Join(blocker, "peers.json")
	s.flushPeerState() // deliberate existing IO refusal; no new persistence seam
	if !s.peerStateDirty || !s.lastPeerSave.Equal(before) || s.overlayDWin != prior {
		t.Fatal("partial failed flush lost dirty or original scalar")
	}
	s.degree.mu.Lock()
	s.degree.poisoned = true
	s.degree.mu.Unlock()
	s.peersStatePath = good
	s.flushPeerState()
	got, err := loadPeerState(good)
	if err != nil || !got.OverlayDWin.stat.valid || !got.OverlayDWin.stat.at.Equal(prior.at) || got.OverlayDWin.stat.value != 12 {
		t.Fatal("unavailable retry renewed accepted original at", err)
	}
}

// Exact SOURCE closure, not a typed semantic graph or a600s runtime experiment.
func degreeStatisticSourceErrors(sources map[string][]byte) []string {
	var failures []string
	compact := func(s string) string { return strings.Join(strings.Fields(s), "") }
	producer := compact(string(sources["peer_state_degree_statistic.go"]))
	flush := compact(string(sources["peer_management.go"]))
	require := func(text, fragment, label string) {
		if !strings.Contains(text, compact(fragment)) {
			failures = append(failures, label)
		}
	}
	require(producer, "s, err := newDegreeService(cfg, id, bus, profile, nil)", "statistic root must use closed production degree root")
	require(producer, "o.mu.Lock() start, _, valid := sampleDegreeStatisticClockLocked(o)", "coverage must start after installed owner lock")
	require(producer, "s.degreeStatisticProducer = &degreeStatisticProducer{owner: o, coverageStart: start}", "coverage must exclude pre-install construction")
	require(producer, "q := sample.Sub(o.clock.origin)", "paired original monotonic q")
	require(producer, "wall := sample.UTC()", "paired original wall timestamp")
	if strings.Count(producer, "sample:=time.Now()") != 1 {
		failures = append(failures, "statistic clock source broadened")
	}
	require(producer, "o.last = q wall := sample.UTC()", "HWM must precede later refusal")
	require(producer, "if q < 0 || q == time.Duration(math.MaxInt64) || q < o.last", "statistic saturated clock refusal")
	require(producer, "if o == nil || o.poisoned || o.token == nil || o.clock.fixture != nil || o.clock.origin.IsZero()", "runtime observer must reject fixture clocks")
	require(producer, "q-start >= degreeRecent", "full600 coverage embargo")
	require(producer, "rows, used := o.history, o.historyN", "projection must clone terminal basis")
	if strings.Contains(producer, "o.degreeSnapshot(") || strings.Contains(producer, "o.history=") || strings.Contains(producer, "o.historyN=") {
		failures = append(failures, "projection changed shared history or old cut")
	}
	require(producer, "if validDegreeStatisticWall(candidate.at) { if fresh := freshOverlayDegreeStatistic(candidate, now); fresh.valid { return fresh } } return prior", "preserve unavailable original tuple; genuine zero replaces")
	capture := strings.Index(flush, "liveStatistic,_:=s.captureOverlayDWinForFlush()")
	provider := strings.Index(flush, "s.peerProvider.StaticSnapshotAll()")
	lock := -1
	if capture >= 0 {
		lock = strings.Index(flush[capture:], "s.peerMu.Lock()")
	}
	if capture < 0 || provider <= capture || lock < 0 {
		failures = append(failures, "statistic capture must precede provider and peer locks")
	}
	// Existing parser checks identifier references, direct-call escapes and
	// writes. Types/functions below are admitted only at these exact sites.
	expected := degreeStatisticReferenceInventory()
	actual := map[string]map[string]int{}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	for path, raw := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range f.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
			}
			calls := map[ast.Node]bool{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					calls[c.Fun] = true
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok {
						calls[sel.Sel] = true
					}
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != declaration {
					if sites, tracked := actual[id.Name]; tracked {
						sites[site]++
					}
				}
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if _, tracked := expected[sel.Sel.Name]; tracked && sel.Sel.Name != "degreeStatisticProducer" && !calls[sel] {
						failures = append(failures, "statistic method value escape")
					}
				}
				if id, ok := n.(*ast.Ident); ok && id != declaration {
					if _, tracked := expected[id.Name]; tracked && id.Name != "degreeStatisticProducer" && !calls[id] {
						failures = append(failures, "statistic root value escape")
					}
				}
				if assign, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range assign.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "degreeStatisticProducer" && site != "peer_state_degree_statistic.go:newDegreeStatisticCandidateService" {
							failures = append(failures, "statistic late producer setter")
						}
					}
				}
				if path == "peer_state_degree_statistic.go" {
					switch n.(type) {
					case *ast.GoStmt, *ast.SendStmt:
						failures = append(failures, "statistic worker or producer channel")
					}
				}
				return true
			})
		}
	}
	if !reflect.DeepEqual(expected, actual) {
		failures = append(failures, "statistic exact reference inventory changed")
	}
	return failures
}

// Filled from the authored finite plain TEXT inventory; evaluated only by
// future Go tests, never by the SOURCE authoring script.
func degreeStatisticReferenceInventory() map[string]map[string]int {
	return map[string]map[string]int{
		"degreeStatisticProducer":            {"peer_state_degree_statistic.go:<package>": 1, "peer_state_degree_statistic.go:captureOverlayDWinForFlush": 2, "peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 2, "service.go:<package>": 2},
		"newDegreeStatisticCandidateService": {},
		"sampleDegreeStatisticClockLocked":   {"peer_state_degree_statistic.go:captureOverlayDWinForFlush": 1, "peer_state_degree_statistic.go:newDegreeStatisticCandidateService": 1},
		"degreeStatisticCoverageReady":       {"peer_state_degree_statistic.go:captureOverlayDWinForFlush": 1},
		"captureOverlayDWinForFlush":         {"peer_management.go:flushPeerState": 1},
		"validDegreeStatisticKey":            {"peer_state_degree_statistic.go:projectDegreeStatisticLocked": 2},
		"insertDegreeStatisticKey":           {"peer_state_degree_statistic.go:projectDegreeStatisticLocked": 1},
		"projectDegreeStatisticLocked":       {"peer_state_degree_statistic.go:captureOverlayDWinForFlush": 1},
		"selectOverlayDegreeStatistic":       {"peer_management.go:flushPeerState": 1},
		"validDegreeStatisticWall":           {"peer_state_degree_statistic.go:sampleDegreeStatisticClockLocked": 1, "peer_state_degree_statistic.go:selectOverlayDegreeStatistic": 1},
	}
}

func TestDegreeStatisticClosedProducerClockProjectionAndWriter(t *testing.T) {
	shape := reflect.TypeFor[degreeStatisticProducer]()
	if shape.NumField() != 2 || shape.Field(0).Name != "owner" || shape.Field(1).Name != "coverageStart" {
		t.Fatal("statistic producer retained extra clock or authority state")
	}
	if failures := degreeStatisticSourceErrors(ninN1Sources(t)); len(failures) != 0 {
		t.Fatal("closed degree statistic producer", failures)
	}
}

func TestDegreeStatisticConstructorGraphCannotEscapeBeforeOwner(t *testing.T) {
	// Compact source pins freeze the actual concrete graph, including the
	// external constructors storing Service-bearing callbacks without running
	// them. This does not authorize arbitrary replacement builders.
	pins := degreeStatisticConstructionPins()
	for path, want := range pins {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(strings.Join(strings.Fields(string(raw)), "")))
		if hex.EncodeToString(sum[:]) != want {
			t.Fatal("closed constructor graph source changed before observer installation", path)
		}
	}
}
func degreeStatisticConstructionPins() map[string]string {
	return map[string]string{
		"internal/core/node/service.go":                         "a26fa0022c6857720a2ca7c2c95d18242529a91f0dce536612c8ebcb6bf964cd",
		"internal/core/node/find_node_degree.go":                "20b419376b2f6687db507cc7df75ffe74d8a6dedd11f288c82ef5206bc54e8f0",
		"internal/core/node/datagram_runtime_state.go":          "7f8b75b1ff5998ffe6ebeeb43e975eeaa6f58b2121dd787e6d0483f08c3eaf7b",
		"internal/core/node/datagram_layer.go":                  "6436e98109e483ddd769623175434c17d21653fbe97ddc7f45b9f6566ba66d34",
		"internal/core/node/identity_discovery.go":              "aefc83c8cb9154c6fa53db3276295943c850c7e2abe6410b4c71f895c75dd3c0",
		"internal/core/node/dm_control.go":                      "ce8cb1973ebfe8ae1bf857d026128fcb1810a69b4f51ddd9c1f097154334fa7e",
		"internal/core/node/dm_control_send.go":                 "3db22b81c0a7d5830c75c3ed100b4b1ba7282a88fa1462113b2a821f1f363c09",
		"internal/core/node/datagram_resolver.go":               "1540af57acade8d6fdfdef0859a294fc9eb8ca8e380a0ab7b01c00f83045d84c",
		"internal/core/node/peer_provider.go":                   "6b8f3bf3d4b7d1be92378de5e0665de969416fdc47a104aa91a9b94d0cb37610",
		"internal/core/node/connection_manager.go":              "d26f5bbf3c11867559d4818337463ffbcd0f5ebdcf1fc9229156a75ae3c8fabe",
		"internal/core/node/dial_pacer.go":                      "dcfe581adb4c9b70a0978c69c19c80b54eb9e5a389d10b63e92c8c167bf9c206",
		"internal/core/node/table_router.go":                    "3f5abfb597302d3e9ab7e4f6687e2f62ee51ebac8ea3118158ce99a9ba012465",
		"internal/core/node/identity_resolver.go":               "ef6d489f0bfec8ae172b5415324072785017155cef8a219646f04cfc9af304b3",
		"internal/core/node/identity_pin_selected_lifecycle.go": "19c6b1f49aa6ec5d5f1fa2bcf82974b0b35587d4533807e49e176939743377b9",
		"internal/core/node/identity_pin_contact.go":            "372902826b744f3cddd4028bdf5fc814e51108f73e8e49dac811179c81f0cefa",
		"internal/core/node/presence_prober.go":                 "acfef8a845330f775bb76306ebfdaa53fc4b42e8e9dbb73a55b0835f1451fef9",
		"internal/core/node/routing_probe_loop.go":              "bdd2f189647ddf821bd1bf2486e4af4f11cc5ac2f23b712e39edb44f478d3594",
		"internal/core/node/first_hop_guards.go":                "4ad9f9c6e1f1d3081714c100c9e5361be200f0dfcfc26e710fcb595bda2628b8",
		"internal/core/node/traffic_capture_bridge.go":          "b7ec5423e60dc7f150f19c023ca1e98efd64e17c5863a5fc4850d5ec5de61643",
		"internal/core/node/session_secure.go":                  "c2181b64d36effff1dac3f449c16ed18912e9ab21e76541b36c942029629614f",
		"internal/core/node/proof_transport_lifetime.go":        "bcfde8a6cd6a8bc8b5e22b6494df15854c44d20f76ed944127f58ef0cea7ca6a",
		"internal/core/node/datagram_integration.go":            "f295766d6d349c59b14810c35b5ab6fc4d63c926885e01a2c1df5bedfab37c4c",
		"internal/core/capture/manager.go":                      "47ab9c32173a273258240b1251de2b22017a21387dd09d0e31b837c038dddd29",
		"internal/core/routing/announce.go":                     "8e71419e4e1a5c4cd2eba4b936b2713b5f3bec3706c9de231a192a392a0220a4",
		"internal/core/datagram/planner.go":                     "aa68213e9728cdcb3b17133d3cdec3360d4f5b89c5bdfebbd8ae317b5ecd33f5",
		"internal/core/datagram/pipeline.go":                    "be5053c5d6be9afa25cc7b03c7e379b6346738156a77cab9b8fd3a59d22612b2",
		"internal/core/datagram/composite_resolver.go":          "d908a8e352f3e336a1534bc741c8bd9ad021175048d5a9ee933719c03d0196a6",
	}
}

func TestDegreeStatisticGuardRejectsEarlyOverwriteAndClockEscape(t *testing.T) {
	for _, c := range []struct{ name, path, before, after string }{
		{"early", "peer_state_degree_statistic.go", "q-start >= degreeRecent", "q-start >= 0"},
		{"restamp", "peer_state_degree_statistic.go", "return prior", "prior.at = now; return prior"},
		{"late_clock", "peer_state_degree_statistic.go", "q := sample.Sub(o.clock.origin)", "q := time.Now().Sub(o.clock.origin)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sources := ninN1Sources(t)
			raw := string(sources[c.path])
			if strings.Count(raw, c.before) != 1 {
				t.Fatal("statistic guard control anchor")
			}
			sources[c.path] = []byte(strings.Replace(raw, c.before, c.after, 1))
			if len(degreeStatisticSourceErrors(sources)) == 0 {
				t.Fatal("statistic source guard accepted policy or clock escape")
			}
		})
	}
}
