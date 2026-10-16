//go:build sessiontraffic

package node

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// Session background traffic: what a node's sessions cost while nobody uses
// them, measured apart from the computed cost because idle sessions are part
// of what a neighbour degree really costs. The method and its parameters are
// fixed before the run, so a result cannot tune the method that produced it.
//
// One observed full node and k neighbour full nodes that bootstrap to it,
// every one a REAL Service of this process on loopback — load-stand nodes,
// so the configuration is config.Default() plus the declared loopback hooks
// and the sessions are whatever production negotiates (v2). After a warm-up
// the stand is left alone for a window — no DM, no gossip, no churn — and
// what moved is recorded: the observed node's transport totals, every
// node's, the process's CPU and heap. Before the stand starts, the same
// number of nodes is run WITHOUT any peer, so the heap and CPU a node costs
// by itself can be told apart from what its sessions cost.
//
// The test measures and records; it judges only whether a window is valid
// (a ban, a legacy session, a byte balance that does not close, a counter
// family that restarted). The figures are read from the records by
// analyse_traffic.py in the same log folder.
//
// The build tag keeps it out of every ordinary go test: one repeat is
// minutes of wall time on purpose.

const (
	sessionTrafficEnvNeighbours = "SESSION_TRAFFIC_NEIGHBOURS"
	sessionTrafficEnvRepeats    = "SESSION_TRAFFIC_REPEATS"
	sessionTrafficEnvWarmup     = "SESSION_TRAFFIC_WARMUP"
	sessionTrafficEnvWindow     = "SESSION_TRAFFIC_WINDOW"
	sessionTrafficEnvTick       = "SESSION_TRAFFIC_TICK"
	sessionTrafficEnvIdleSettle = "SESSION_TRAFFIC_IDLE_SETTLE"
	sessionTrafficEnvIdleWindow = "SESSION_TRAFFIC_IDLE_WINDOW"
	sessionTrafficEnvMode       = "SESSION_TRAFFIC_MODE"
	sessionTrafficEnvOut        = "SESSION_TRAFFIC_OUT"
)

// sessionTrafficStopBudget bounds the stop of one stand node. It is a hang
// guard: an idle node stops in milliseconds.
const sessionTrafficStopBudget = 30 * time.Second

var (
	errSessionTrafficParams  = errors.New("session traffic: invalid parameters")
	errSessionTrafficInvalid = errors.New("session traffic: the window is not a valid measurement")
)

// sessionTrafficMode says what a repeat is for. A measure repeat gives the
// cost figures; an attribution repeat records the observed node's frames by
// type and is NOT a cost figure — the capture writes every frame to disk.
type sessionTrafficMode string

const (
	sessionTrafficModeMeasure     sessionTrafficMode = "measure"
	sessionTrafficModeAttribution sessionTrafficMode = "attribution"
)

var sessionTrafficModes = map[string]sessionTrafficMode{
	string(sessionTrafficModeMeasure):     sessionTrafficModeMeasure,
	string(sessionTrafficModeAttribution): sessionTrafficModeAttribution,
}

// sessionTrafficNeighbourCount is k, the observed node's neighbours.
type sessionTrafficNeighbourCount int

type sessionTrafficParams struct {
	Neighbours []sessionTrafficNeighbourCount
	Repeats    int
	Warmup     time.Duration
	Window     time.Duration
	Tick       time.Duration
	// IdleSettle and IdleWindow are the peerless phase: how long the nodes
	// run before its CPU window opens, and how long the window is.
	IdleSettle time.Duration
	IdleWindow time.Duration
	Mode       sessionTrafficMode
	// OutDir is where the records go; absolute, and it must exist.
	OutDir string
}

// readSessionTrafficParams reads the parameters from lookup (os.Getenv in
// the run). Every parameter is required: a default here would be a
// measurement parameter nobody wrote into the plan.
func readSessionTrafficParams(lookup func(string) string) (sessionTrafficParams, error) {
	var errs []error
	collect := func(err error) { errs = append(errs, err) }

	neighbours, err := parseSessionTrafficNeighbours(lookup(sessionTrafficEnvNeighbours))
	collect(err)
	repeats, err := parseSessionTrafficPositive(sessionTrafficEnvRepeats, lookup(sessionTrafficEnvRepeats))
	collect(err)
	durations := map[string]*time.Duration{}
	params := sessionTrafficParams{Neighbours: neighbours, Repeats: repeats}
	durations[sessionTrafficEnvWarmup] = &params.Warmup
	durations[sessionTrafficEnvWindow] = &params.Window
	durations[sessionTrafficEnvTick] = &params.Tick
	durations[sessionTrafficEnvIdleSettle] = &params.IdleSettle
	durations[sessionTrafficEnvIdleWindow] = &params.IdleWindow
	for name, target := range durations {
		*target, err = parseSessionTrafficDuration(name, lookup(name))
		collect(err)
	}
	mode, known := sessionTrafficModes[lookup(sessionTrafficEnvMode)]
	if !known {
		collect(fmt.Errorf("%s = %q, want measure or attribution", sessionTrafficEnvMode, lookup(sessionTrafficEnvMode)))
	}
	params.Mode = mode
	params.OutDir = lookup(sessionTrafficEnvOut)
	collect(requireSessionTrafficOutDir(params.OutDir))
	collect(requireSessionTrafficTickFits(params.Tick, params.Window))
	if err := errors.Join(errs...); err != nil {
		return sessionTrafficParams{}, fmt.Errorf("%w: %w", errSessionTrafficParams, err)
	}
	return params, nil
}

func parseSessionTrafficNeighbours(raw string) ([]sessionTrafficNeighbourCount, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%s is empty", sessionTrafficEnvNeighbours)
	}
	var counts []sessionTrafficNeighbourCount
	for field := range strings.SplitSeq(raw, ",") {
		count, err := parseSessionTrafficPositive(sessionTrafficEnvNeighbours, strings.TrimSpace(field))
		if err != nil {
			return nil, err
		}
		counts = append(counts, sessionTrafficNeighbourCount(count))
	}
	return counts, nil
}

func parseSessionTrafficPositive(name, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s = %q, want a positive whole number", name, raw)
	}
	return value, nil
}

func parseSessionTrafficDuration(name, raw string) (time.Duration, error) {
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s = %q, want a positive duration", name, raw)
	}
	return value, nil
}

func requireSessionTrafficOutDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%s = %q, want an absolute directory", sessionTrafficEnvOut, dir)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%s = %q is not an existing directory", sessionTrafficEnvOut, dir)
	}
	return nil
}

// requireSessionTrafficTickFits refuses a tick that does not divide the
// window: the tick series would otherwise end short of the closing sweep.
func requireSessionTrafficTickFits(tick, window time.Duration) error {
	if tick <= 0 || window <= 0 || window%tick != 0 {
		return fmt.Errorf("%s = %s does not divide %s = %s", sessionTrafficEnvTick, tick, sessionTrafficEnvWindow, window)
	}
	return nil
}

// TestSessionBackgroundTraffic runs every repeat of every k in turn and
// writes one record per repeat, valid or not: an invalid repeat is evidence
// too, and the analysis leaves it out by its own flag.
func TestSessionBackgroundTraffic(t *testing.T) {
	isolateFromCorsaEnvironment(t)
	params, err := readSessionTrafficParams(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	routeSessionTrafficLogs(t, params)

	for _, k := range params.Neighbours {
		for repeat := 1; repeat <= params.Repeats; repeat++ {
			record := measureSessionTraffic(t, params, k, repeat)
			if err := writeSessionTrafficRecord(params.OutDir, record); err != nil {
				t.Fatalf("k=%d repeat %d: %v", k, repeat, err)
			}
			if len(record.Invalid) > 0 {
				t.Errorf("k=%d repeat %d: %v: %s", k, repeat, errSessionTrafficInvalid, strings.Join(record.Invalid, "; "))
			}
			t.Logf("k=%d repeat %d: observed sent %d received %d bytes over %.0f s, %d sessions; process CPU %.2f s",
				k, repeat, record.Observed.BytesSent, record.Observed.BytesReceived, record.WindowSeconds,
				record.Observed.ConnectionsEnd, record.Process.WindowCPUSeconds)
		}
	}
}

// routeSessionTrafficLogs gives the nodes production's log level (warn, the
// crashlog default) and sends their lines to a file beside the records, so
// a warning in the window can be read after the run without flooding the
// test output.
func routeSessionTrafficLogs(t *testing.T, params sessionTrafficParams) {
	t.Helper()

	path := filepath.Join(params.OutDir, fmt.Sprintf("%s.nodes.log", params.Mode))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open node log: %v", err)
	}
	previousLogger, previousLevel := log.Logger, zerolog.GlobalLevel()
	log.Logger = zerolog.New(file).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	t.Cleanup(func() {
		log.Logger = previousLogger
		zerolog.SetGlobalLevel(previousLevel)
		if err := file.Close(); err != nil {
			t.Errorf("close node log: %v", err)
		}
	})
}

// --- record ---

// sessionTrafficRecord is one repeat. Every figure is a raw reading or a
// difference of two; rates and fits are the analysis's.
type sessionTrafficRecord struct {
	Mode            sessionTrafficMode           `json:"mode"`
	Neighbours      sessionTrafficNeighbourCount `json:"neighbours"`
	Repeat          int                          `json:"repeat"`
	StandNodes      int                          `json:"stand_nodes"`
	GoVersion       string                       `json:"go_version"`
	GOOS            string                       `json:"goos"`
	GOARCH          string                       `json:"goarch"`
	GOMAXPROCS      int                          `json:"gomaxprocs"`
	ClientVersion   string                       `json:"client_version"`
	ProtocolVersion int                          `json:"protocol_version"`
	MinimumProtocol int                          `json:"minimum_protocol_version"`
	WarmupSeconds   float64                      `json:"warmup_seconds"`
	WindowFrom      time.Time                    `json:"window_from"`
	WindowTo        time.Time                    `json:"window_to"`
	WindowSeconds   float64                      `json:"window_seconds"`
	Observed        sessionTrafficObserved       `json:"observed"`
	Stand           sessionTrafficStand          `json:"stand"`
	Isolated        sessionTrafficIsolated       `json:"isolated"`
	Process         sessionTrafficProcess        `json:"process"`
	Attribution     *sessionTrafficAttribution   `json:"attribution,omitempty"`
	// Invalid makes the repeat unusable as a measurement; Notes are
	// observations the analysis weighs (churn inside the window).
	Invalid      []string `json:"invalid"`
	Notes        []string `json:"notes"`
	StopErrors   []string `json:"stop_errors"`
	NodeLogLevel string   `json:"node_log_level"`
}

// sessionTrafficObserved is the observed node over the window.
type sessionTrafficObserved struct {
	// PeriodSeconds is between the observed node's own two readings: the
	// byte figures' period.
	PeriodSeconds     float64            `json:"period_seconds"`
	BytesSent         loadStandByteCount `json:"bytes_sent"`
	BytesReceived     loadStandByteCount `json:"bytes_received"`
	ConnectionsStart  int                `json:"connections_start"`
	ConnectionsEnd    int                `json:"connections_end"`
	PeersEnd          int                `json:"peers_end"`
	RouteEntriesStart int                `json:"route_entries_start"`
	RouteEntriesEnd   int                `json:"route_entries_end"`
	// LiveSessions is the observed node's live session gauge at the
	// window's end, by kind and direction.
	LiveSessions domain.LiveSessionKinds `json:"live_sessions"`
	// SessionAttempts and SessionsSucceeded in the window: an idle stand
	// dials nobody, so a non-zero count is churn inside the window.
	SessionAttempts   loadStandEventCount            `json:"session_attempts"`
	SessionsSucceeded loadStandEventCount            `json:"sessions_succeeded"`
	AnnounceModes     map[string]loadStandEventCount `json:"announce_modes"`
	// FloorBytes is the node's own resource breakdown at the window's end —
	// what the node SAYS it holds, recorded beside the heap measurement.
	FloorBytes loadStandByteCount   `json:"floor_bytes"`
	Ticks      []sessionTrafficTick `json:"ticks"`
}

// sessionTrafficTick is the observed node's cumulative traffic and every
// node's connection count, read every Tick inside the window.
type sessionTrafficTick struct {
	OffsetSeconds       float64            `json:"offset_seconds"`
	BytesSent           loadStandByteCount `json:"bytes_sent"`
	BytesReceived       loadStandByteCount `json:"bytes_received"`
	ObservedConnections int                `json:"observed_connections"`
	StandConnectionEnds int                `json:"stand_connection_ends"`
}

// sessionTrafficStand is every node of the stand over the window. Each
// connection between two stand nodes has two ends, both in this process.
type sessionTrafficStand struct {
	BytesSent             loadStandByteCount  `json:"bytes_sent"`
	BytesReceived         loadStandByteCount  `json:"bytes_received"`
	ConnectionEndsStart   int                 `json:"connection_ends_start"`
	ConnectionEndsEnd     int                 `json:"connection_ends_end"`
	SessionAttempts       loadStandEventCount `json:"session_attempts"`
	LegacySessionsEnd     int                 `json:"legacy_sessions_end"`
	V2SessionsEnd         int                 `json:"v2_sessions_end"`
	BalanceSent           loadStandByteCount  `json:"balance_sent"`
	BalanceReceived       loadStandByteCount  `json:"balance_received"`
	BalanceImbalance      loadStandByteCount  `json:"balance_imbalance"`
	BalanceAllowance      loadStandByteCount  `json:"balance_allowance"`
	BalanceAllowanceShare float64             `json:"balance_allowance_share"`
}

// sessionTrafficIsolated is the peerless phase: the same number of nodes,
// none with a peer, so a node's own heap and idle CPU are known.
type sessionTrafficIsolated struct {
	Nodes             int                `json:"nodes"`
	HeapBefore        loadStandByteCount `json:"heap_live_before"`
	HeapRunning       loadStandByteCount `json:"heap_live_running"`
	HeapAfterStop     loadStandByteCount `json:"heap_live_after_stop"`
	WindowSeconds     float64            `json:"window_seconds"`
	WindowCPUSeconds  float64            `json:"window_cpu_seconds"`
	ConnectionEndsEnd int                `json:"connection_ends_end"`
}

// sessionTrafficProcess is the process hosting the stand over the window.
// CPU is the kernel's count (rusage), user plus system; the GC share is the
// runtime's, which moves only when a cycle completes.
type sessionTrafficProcess struct {
	WindowCPUSeconds     float64                 `json:"window_cpu_seconds"`
	WindowUserCPUSeconds float64                 `json:"window_user_cpu_seconds"`
	WindowGCCPUSeconds   float64                 `json:"window_gc_cpu_seconds"`
	WindowGCCycles       loadStandEventCount     `json:"window_gc_cycles"`
	WindowHeapAllocs     loadStandByteCount      `json:"window_heap_allocs"`
	HeapLiveEnd          loadStandByteCount      `json:"heap_live_end"`
	GoMappedEnd          loadStandByteCount      `json:"go_mapped_end"`
	MaxRSSEnd            loadStandByteCount      `json:"max_rss_end"`
	GoroutinesEnd        loadStandGoroutineCount `json:"goroutines_end"`
	DescriptorsEnd       int                     `json:"descriptors_end"`
}

func writeSessionTrafficRecord(dir string, record sessionTrafficRecord) error {
	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	name := fmt.Sprintf("%s-k%02d-r%d.json", record.Mode, record.Neighbours, record.Repeat)
	if err := os.WriteFile(filepath.Join(dir, name), append(encoded, '\n'), 0o600); err != nil {
		return fmt.Errorf("write record %s: %w", name, err)
	}
	return nil
}

// --- one repeat ---

// measureSessionTraffic runs one repeat: the peerless phase, then the stand,
// its warm-up and its window. Whatever fails is recorded in Invalid; only a
// failure that leaves nothing to record stops the test.
func measureSessionTraffic(t *testing.T, params sessionTrafficParams, k sessionTrafficNeighbourCount, repeat int) sessionTrafficRecord {
	t.Helper()

	record := newSessionTrafficRecord(params, k, repeat)
	isolated, err := measureIsolatedNodes(t, record.StandNodes, params)
	if err != nil {
		t.Fatalf("k=%d repeat %d: peerless phase: %v", k, repeat, err)
	}
	record.Isolated = isolated

	// A Fatalf below leaves the stand to t.Context(): every node was
	// started under it, and its cancellation stops them.
	stand := startSessionTrafficStand(t, k)
	if err := sleepSessionTraffic(t.Context(), params.Warmup); err != nil {
		t.Fatalf("k=%d repeat %d: warm-up: %v", k, repeat, err)
	}
	window, err := observeSessionTrafficWindow(t.Context(), stand, params)
	if err != nil {
		t.Fatalf("k=%d repeat %d: window: %v", k, repeat, err)
	}
	record.fill(window)
	record.StopErrors = stand.stop(t)
	return record
}

func newSessionTrafficRecord(params sessionTrafficParams, k sessionTrafficNeighbourCount, repeat int) sessionTrafficRecord {
	return sessionTrafficRecord{
		Mode:            params.Mode,
		Neighbours:      k,
		Repeat:          repeat,
		StandNodes:      int(k) + 1,
		GoVersion:       runtime.Version(),
		GOOS:            runtime.GOOS,
		GOARCH:          runtime.GOARCH,
		GOMAXPROCS:      runtime.GOMAXPROCS(0),
		ClientVersion:   config.CorsaVersion,
		ProtocolVersion: config.ProtocolVersion,
		MinimumProtocol: config.MinimumProtocolVersion,
		WarmupSeconds:   params.Warmup.Seconds(),
		NodeLogLevel:    zerolog.WarnLevel.String(),
		Invalid:         []string{},
		Notes:           []string{},
		StopErrors:      []string{},
	}
}

// settledProcessSample collects twice before sampling, so HeapLive is what
// the forced cycle marked live — the heap held right now, not at whatever
// cycle ran last. Twice, because objects with finalizers survive the first.
func settledProcessSample() (loadStandProcessSample, error) {
	runtime.GC()
	runtime.GC()
	return takeLoadStandProcessSample(runjournal.SystemClock{})
}

// --- peerless phase ---

func measureIsolatedNodes(t *testing.T, count int, params sessionTrafficParams) (sessionTrafficIsolated, error) {
	t.Helper()

	before, err := settledProcessSample()
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	nodes := make([]*loadStandNode, 0, count)
	for i := range count {
		node := newSessionTrafficNode(t, loadStandNodeID(fmt.Sprintf("isolated-%02d", i)), ports, nil)
		nodes = append(nodes, node)
	}
	stand := &sessionTrafficStandNodes{all: nodes}
	if err := stand.start(t.Context()); err != nil {
		stand.stop(t)
		return sessionTrafficIsolated{}, err
	}
	result, err := readIsolatedWindow(t.Context(), stand, params)
	if stopErrs := stand.stop(t); len(stopErrs) > 0 {
		return sessionTrafficIsolated{}, fmt.Errorf("stop peerless nodes: %s", strings.Join(stopErrs, "; "))
	}
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	after, err := settledProcessSample()
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	result.Nodes = count
	result.HeapBefore = before.HeapLive
	result.HeapAfterStop = after.HeapLive
	return result, nil
}

// readIsolatedWindow measures the running peerless nodes: their CPU over
// IdleWindow after IdleSettle, then their heap.
func readIsolatedWindow(ctx context.Context, stand *sessionTrafficStandNodes, params sessionTrafficParams) (sessionTrafficIsolated, error) {
	if err := sleepSessionTraffic(ctx, params.IdleSettle); err != nil {
		return sessionTrafficIsolated{}, err
	}
	from, err := takeLoadStandProcessSample(runjournal.SystemClock{})
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	if err := sleepSessionTraffic(ctx, params.IdleWindow); err != nil {
		return sessionTrafficIsolated{}, err
	}
	to, err := takeLoadStandProcessSample(runjournal.SystemClock{})
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	cpu, err := sessionTrafficKernelCPU(from, to)
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	running, err := settledProcessSample()
	if err != nil {
		return sessionTrafficIsolated{}, err
	}
	return sessionTrafficIsolated{
		HeapRunning:       running.HeapLive,
		WindowSeconds:     to.ReadAt.Sub(from.ReadAt).Seconds(),
		WindowCPUSeconds:  cpu.Seconds(),
		ConnectionEndsEnd: stand.connectionEnds(),
	}, nil
}

// sessionTrafficKernelCPU is the kernel's user+system CPU between two
// process samples; a platform without rusage cannot give the figure.
func sessionTrafficKernelCPU(from, to loadStandProcessSample) (time.Duration, error) {
	before, err := from.Rusage.Get()
	if err != nil {
		return 0, err
	}
	after, err := to.Rusage.Get()
	if err != nil {
		return 0, err
	}
	return (after.UserCPU + after.SystemCPU) - (before.UserCPU + before.SystemCPU), nil
}

// --- the stand ---

// sessionTrafficStandNodes is the observed node and its neighbours, owned
// by the test goroutine. observed is nil in the peerless phase.
type sessionTrafficStandNodes struct {
	observed *loadStandNode
	all      []*loadStandNode
}

func newSessionTrafficNode(t *testing.T, id loadStandNodeID, ports *loadStandPortRegistry, bootstrap []domain.PeerAddress) *loadStandNode {
	t.Helper()

	opts := loadStandNodeOpts{BootstrapPeers: bootstrap, Hooks: loadStandLoopbackHooks()}
	node, err := newLoadStandNode(id, loadStandRoleFull, t.TempDir(), ports, opts)
	if err != nil {
		t.Fatalf("new stand node %s: %v", id, err)
	}
	return node
}

// startSessionTrafficStand starts the observed node, then k neighbours that
// each bootstrap to it. What else they connect to is production's choice.
func startSessionTrafficStand(t *testing.T, k sessionTrafficNeighbourCount) *sessionTrafficStandNodes {
	t.Helper()

	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	observed := newSessionTrafficNode(t, "observed", ports, nil)
	address, listens := observed.DialAddress()
	if !listens {
		t.Fatalf("observed node has no dial address")
	}
	stand := &sessionTrafficStandNodes{observed: observed, all: []*loadStandNode{observed}}
	for i := range int(k) {
		id := loadStandNodeID(fmt.Sprintf("neighbour-%02d", i+1))
		stand.all = append(stand.all, newSessionTrafficNode(t, id, ports, []domain.PeerAddress{address}))
	}
	if err := stand.start(t.Context()); err != nil {
		stand.stop(t)
		t.Fatalf("start stand k=%d: %v", k, err)
	}
	return stand
}

func (s *sessionTrafficStandNodes) start(ctx context.Context) error {
	for _, node := range s.all {
		if err := node.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// stop stops every running node and returns what refused, so a repeat
// records a slow stop instead of hiding it.
func (s *sessionTrafficStandNodes) stop(t *testing.T) []string {
	t.Helper()

	failures := []string{}
	for _, node := range s.all {
		err := node.Stop(withBudget(t, sessionTrafficStopBudget))
		if err != nil && !errors.Is(err, errLoadStandNodeNotRunning) {
			failures = append(failures, err.Error())
		}
	}
	return failures
}

// connectionEnds is every live neighbour connection of every running node,
// as each node's census last counted it.
func (s *sessionTrafficStandNodes) connectionEnds() int {
	ends := 0
	for _, node := range s.all {
		if svc, running := node.Service(); running {
			ends += svc.NeighbourComposition().Connections
		}
	}
	return ends
}

func (s *sessionTrafficStandNodes) observedService() *Service {
	svc, _ := s.observed.Service()
	return svc
}

// --- the window ---

// sessionTrafficWindow is what the window collected, before it is folded
// into a record.
type sessionTrafficWindow struct {
	Before      loadStandSweep
	After       loadStandSweep
	AfterGC     loadStandProcessSample
	Ticks       []sessionTrafficTick
	Kinds       domain.SessionKindStats
	Attribution *sessionTrafficAttribution
	Problems    []string
}

// observeSessionTrafficWindow sweeps the stand, ticks through the window
// reading the observed node's counters, and sweeps again. In attribution
// mode the observed node captures its frames for exactly the window.
func observeSessionTrafficWindow(ctx context.Context, stand *sessionTrafficStandNodes, params sessionTrafficParams) (sessionTrafficWindow, error) {
	var window sessionTrafficWindow
	capturing := params.Mode == sessionTrafficModeAttribution
	if capturing {
		stand.observedService().captureManager.StartAll(domain.CaptureFormatCompact)
	}
	before, err := takeLoadStandSweep(ctx, runjournal.SystemClock{}, stand.all, loadStandBanPolicy{})
	if err != nil {
		return sessionTrafficWindow{}, err
	}
	window.Before = before
	window.Ticks, err = tickSessionTrafficWindow(ctx, stand, params, before.Begin)
	if err != nil {
		return sessionTrafficWindow{}, err
	}
	after, err := takeLoadStandSweep(ctx, runjournal.SystemClock{}, stand.all, loadStandBanPolicy{})
	if err != nil {
		return sessionTrafficWindow{}, err
	}
	window.After = after
	window.Kinds = stand.observedService().SessionKindStats()
	if capturing {
		window.Attribution, err = collectSessionTrafficAttribution(stand.observedService())
		if err != nil {
			window.Problems = append(window.Problems, err.Error())
		}
	}
	window.AfterGC, err = settledProcessSample()
	if err != nil {
		return sessionTrafficWindow{}, err
	}
	return window, nil
}

func tickSessionTrafficWindow(ctx context.Context, stand *sessionTrafficStandNodes, params sessionTrafficParams, opened time.Time) ([]sessionTrafficTick, error) {
	ticker := time.NewTicker(params.Tick)
	defer ticker.Stop()
	ticks := make([]sessionTrafficTick, 0, int(params.Window/params.Tick))
	for range int(params.Window / params.Tick) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
		traffic := stand.observedService().TransportTrafficStats()
		ticks = append(ticks, sessionTrafficTick{
			OffsetSeconds:       traffic.ReadAt.Sub(opened).Seconds(),
			BytesSent:           loadStandByteCount(traffic.BytesSent),
			BytesReceived:       loadStandByteCount(traffic.BytesReceived),
			ObservedConnections: stand.observedService().NeighbourComposition().Connections,
			StandConnectionEnds: stand.connectionEnds(),
		})
	}
	return ticks, nil
}

func sleepSessionTraffic(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// --- folding the window into the record ---

func (r *sessionTrafficRecord) fill(window sessionTrafficWindow) {
	r.Invalid = append(r.Invalid, window.Problems...)
	r.WindowFrom, r.WindowTo = window.Before.Begin, window.After.End
	r.WindowSeconds = window.After.End.Sub(window.Before.Begin).Seconds()
	r.Observed.Ticks = window.Ticks
	r.Attribution = window.Attribution
	r.noteProblem(checkLoadStandSweep(window.Before))
	r.noteProblem(checkLoadStandSweep(window.After))
	r.noteProblem(checkLoadStandProcessWindow(window.Before.Process, window.After.Process))
	r.fillNodes(window.Before, window.After)
	r.fillBalance(window.Before, window.After)
	r.fillSessionKinds(window.Kinds)
	r.fillProcess(window)
}

func (r *sessionTrafficRecord) noteProblem(err error) {
	if err != nil {
		r.Invalid = append(r.Invalid, err.Error())
	}
}

// fillNodes subtracts every node's two samples. The observed node is the
// first node of every sweep; the stand figures sum all of them.
func (r *sessionTrafficRecord) fillNodes(before, after loadStandSweep) {
	if len(before.Nodes) != len(after.Nodes) || len(after.Nodes) != r.StandNodes {
		r.Invalid = append(r.Invalid, fmt.Sprintf("sweeps sampled %d and %d nodes, the stand has %d", len(before.Nodes), len(after.Nodes), r.StandNodes))
		return
	}
	for i := range after.Nodes {
		nodeWindow, err := newLoadStandNodeWindow(before.Nodes[i], after.Nodes[i])
		if err != nil {
			r.noteProblem(err)
			continue
		}
		r.Stand.BytesSent += nodeWindow.Bytes.Sent
		r.Stand.BytesReceived += nodeWindow.Bytes.Received
		r.Stand.SessionAttempts += nodeWindow.Sessions.Delta.Attempts
		r.Stand.ConnectionEndsStart += before.Nodes[i].Neighbours.Connections
		r.Stand.ConnectionEndsEnd += after.Nodes[i].Neighbours.Connections
		if i == 0 {
			r.fillObserved(before.Nodes[i], after.Nodes[i], nodeWindow)
		}
	}
	if r.Stand.SessionAttempts > 0 {
		r.Notes = append(r.Notes, fmt.Sprintf("%d session attempts inside the idle window", r.Stand.SessionAttempts))
	}
	r.noteConnectionChanges()
}

// noteConnectionChanges notes a window in which a census moved: the
// per-session figures divide by the count, so the analysis has to know.
func (r *sessionTrafficRecord) noteConnectionChanges() {
	observed := []int{r.Observed.ConnectionsStart, r.Observed.ConnectionsEnd}
	stand := []int{r.Stand.ConnectionEndsStart, r.Stand.ConnectionEndsEnd}
	for _, tick := range r.Observed.Ticks {
		observed = append(observed, tick.ObservedConnections)
		stand = append(stand, tick.StandConnectionEnds)
	}
	if slices.Min(observed) != slices.Max(observed) {
		r.Notes = append(r.Notes, fmt.Sprintf("observed connections moved within %d..%d", slices.Min(observed), slices.Max(observed)))
	}
	if slices.Min(stand) != slices.Max(stand) {
		r.Notes = append(r.Notes, fmt.Sprintf("stand connection ends moved within %d..%d", slices.Min(stand), slices.Max(stand)))
	}
}

func (r *sessionTrafficRecord) fillObserved(before, after loadStandNodeSample, window loadStandNodeWindow) {
	r.Observed.PeriodSeconds = window.Bytes.Period.Length().Seconds()
	r.Observed.BytesSent = window.Bytes.Sent
	r.Observed.BytesReceived = window.Bytes.Received
	r.Observed.ConnectionsStart = before.Neighbours.Connections
	r.Observed.ConnectionsEnd = after.Neighbours.Connections
	r.Observed.PeersEnd = after.Neighbours.Peers
	r.Observed.RouteEntriesStart = before.RouteEntries
	r.Observed.RouteEntriesEnd = after.RouteEntries
	r.Observed.SessionAttempts = window.Sessions.Delta.Attempts
	r.Observed.SessionsSucceeded = window.Sessions.Delta.Succeeded
	r.Observed.FloorBytes = loadStandByteCount(after.Resources.FloorBytes())
	r.Observed.AnnounceModes = map[string]loadStandEventCount{}
	for key, count := range window.Modes.Counts {
		r.Observed.AnnounceModes[fmt.Sprintf("%s/%s/%s", key.Operation, key.Mode, key.Reason)] = count
	}
	if r.Observed.ConnectionsStart < int(r.Neighbours) || r.Observed.ConnectionsEnd < int(r.Neighbours) {
		r.Invalid = append(r.Invalid, fmt.Sprintf("observed node has %d then %d connections, fewer than its %d neighbours",
			r.Observed.ConnectionsStart, r.Observed.ConnectionsEnd, r.Neighbours))
	}
}

func (r *sessionTrafficRecord) fillBalance(before, after loadStandSweep) {
	balance, err := checkLoadStandByteBalance(before, after, nil, loadStandDefaultBalanceTolerance())
	switch {
	case errors.Is(err, errLoadStandBalanceUndefined):
		// An idle stand carries less than the in-flight allowance, so the
		// balance cannot judge it; the sums are still recorded.
		r.Notes = append(r.Notes, err.Error())
	default:
		r.noteProblem(err)
	}
	r.Stand.BalanceSent = balance.Sent
	r.Stand.BalanceReceived = balance.Received
	r.Stand.BalanceImbalance = balance.Imbalance
	r.Stand.BalanceAllowance = balance.Allowance
	r.Stand.BalanceAllowanceShare = float64(balance.AllowanceShare)
}

// fillSessionKinds records the observed node's live sessions; a legacy one
// means the window did not measure what production runs.
func (r *sessionTrafficRecord) fillSessionKinds(kinds domain.SessionKindStats) {
	live := kinds.Live
	r.Observed.LiveSessions = live
	r.Stand.LegacySessionsEnd = live.InboundLegacy.Sessions + live.OutboundLegacy.Sessions
	r.Stand.V2SessionsEnd = live.InboundV2.Sessions + live.OutboundV2.Sessions
	switch {
	case !live.Ready:
		r.Invalid = append(r.Invalid, "observed node's live session gauge was never refreshed")
	case r.Stand.LegacySessionsEnd > 0:
		r.Invalid = append(r.Invalid, fmt.Sprintf("observed node holds %d legacy sessions", r.Stand.LegacySessionsEnd))
	}
}

func (r *sessionTrafficRecord) fillProcess(window sessionTrafficWindow) {
	from, to := window.Before.Process, window.After.Process
	cpu, err := sessionTrafficKernelCPU(from, to)
	r.noteProblem(err)
	r.Process.WindowCPUSeconds = cpu.Seconds()
	// Without rusage the kernel CPU above is already noted as a problem;
	// the zero values below then stand for "not read".
	before, _ := from.Rusage.Get()
	after, _ := to.Rusage.Get()
	r.Process.WindowUserCPUSeconds = (after.UserCPU - before.UserCPU).Seconds()
	r.Process.MaxRSSEnd = after.MaxRSS
	r.Process.WindowGCCPUSeconds = (to.CPUGC - from.CPUGC).Seconds()
	r.Process.WindowGCCycles = to.GCCycles - from.GCCycles
	r.Process.WindowHeapAllocs = to.HeapAllocs - from.HeapAllocs
	r.Process.HeapLiveEnd = window.AfterGC.HeapLive
	r.Process.GoMappedEnd = window.AfterGC.GoMemory
	r.Process.GoroutinesEnd = window.AfterGC.Goroutines
	if descriptors, err := window.AfterGC.Descriptors.Get(); err == nil {
		r.Process.DescriptorsEnd = descriptors
	}
}

// --- frame attribution ---

// sessionTrafficFrameType is the "type" field of a frame as the capture
// recorded it; a line that is not a JSON object with one is counted under
// sessionTrafficUntypedFrame.
type sessionTrafficFrameType string

const sessionTrafficUntypedFrame sessionTrafficFrameType = "<untyped>"

// sessionTrafficFrameRow is one direction and frame type. Bytes are the
// frame's plaintext line as captured plus its newline — what the session
// carries inside TLS, not what crossed the socket.
type sessionTrafficFrameRow struct {
	Direction domain.WireDirection    `json:"direction"`
	Type      sessionTrafficFrameType `json:"type"`
	Frames    loadStandEventCount     `json:"frames"`
	Bytes     loadStandByteCount      `json:"bytes"`
}

type sessionTrafficAttribution struct {
	Files         int                      `json:"files"`
	DroppedEvents int64                    `json:"dropped_events"`
	Rows          []sessionTrafficFrameRow `json:"rows"`
}

// collectSessionTrafficAttribution stops the observed node's capture — which
// flushes and closes its files — and tallies them.
func collectSessionTrafficAttribution(svc *Service) (*sessionTrafficAttribution, error) {
	var dropped int64
	for _, snapshot := range svc.captureManager.AllSessionSnapshots() {
		dropped += snapshot.DroppedEvents
	}
	svc.captureManager.StopAll()
	root := filepath.Join(svc.cfg.EffectiveDataDir(), "debug", "traffic-captures")
	tally := newSessionTrafficFrameTally()
	files := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".jsonl" {
			return err
		}
		files++
		return tally.addFile(path)
	})
	if err != nil {
		return nil, fmt.Errorf("tally captures: %w", err)
	}
	return &sessionTrafficAttribution{Files: files, DroppedEvents: dropped, Rows: tally.rows()}, nil
}

type sessionTrafficFrameKey struct {
	direction domain.WireDirection
	frameType sessionTrafficFrameType
}

type sessionTrafficFrameTally struct {
	counts map[sessionTrafficFrameKey]*sessionTrafficFrameRow
}

func newSessionTrafficFrameTally() *sessionTrafficFrameTally {
	return &sessionTrafficFrameTally{counts: map[sessionTrafficFrameKey]*sessionTrafficFrameRow{}}
}

func (t *sessionTrafficFrameTally) addFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }() // read-only: a close error loses nothing
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for scanner.Scan() {
		t.addLine(scanner.Text())
	}
	return scanner.Err()
}

// addLine counts one compact capture line: "<ts> <dir>: <payload>" or
// "<ts> <dir> [<outcome>]: <payload>". A line of any other shape is not a
// frame and is skipped.
func (t *sessionTrafficFrameTally) addLine(line string) {
	header, payload, found := strings.Cut(line, ": ")
	fields := strings.Fields(header)
	if !found || len(fields) < 2 {
		return
	}
	key := sessionTrafficFrameKey{direction: domain.WireDirection(fields[1]), frameType: sessionTrafficFrameTypeOf(payload)}
	row, seen := t.counts[key]
	if !seen {
		row = &sessionTrafficFrameRow{Direction: key.direction, Type: key.frameType}
		t.counts[key] = row
	}
	row.Frames++
	row.Bytes += loadStandByteCount(len(payload) + 1)
}

func sessionTrafficFrameTypeOf(payload string) sessionTrafficFrameType {
	var frame struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(payload), &frame); err != nil || frame.Type == "" {
		return sessionTrafficUntypedFrame
	}
	return sessionTrafficFrameType(frame.Type)
}

// rows returns the tally ordered by direction, then by bytes, largest first.
func (t *sessionTrafficFrameTally) rows() []sessionTrafficFrameRow {
	rows := make([]sessionTrafficFrameRow, 0, len(t.counts))
	for _, row := range t.counts {
		rows = append(rows, *row)
	}
	slices.SortFunc(rows, func(a, b sessionTrafficFrameRow) int {
		return cmp.Or(
			strings.Compare(string(a.Direction), string(b.Direction)),
			cmp.Compare(b.Bytes, a.Bytes),
			strings.Compare(string(a.Type), string(b.Type)),
		)
	})
	return rows
}
