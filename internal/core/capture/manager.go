package capture

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// maxSessionsPerProcess is the global soft limit on concurrent capture
// writer goroutines (plan §9).
const maxSessionsPerProcess = 1024

// ---------------------------------------------------------------------------
// Capture rule — a standing instruction to capture future traffic
// ---------------------------------------------------------------------------

type rule struct {
	scope     domain.CaptureScope
	target    netip.Addr // meaningful for by_ip; zero for all
	format    domain.CaptureFormat
	createdAt time.Time
}

func (r *rule) key() string {
	switch r.scope {
	case domain.CaptureScopeIP:
		return "ip:" + r.target.String()
	case domain.CaptureScopeAll:
		return "all"
	default:
		return "conn_id"
	}
}

// ---------------------------------------------------------------------------
// ManagerOpts — required dependencies for the Manager
// ---------------------------------------------------------------------------

// ManagerOpts bundles the required dependencies for the Manager constructor.
type ManagerOpts struct {
	// BaseDir is the root directory for capture files (plan §6.1).
	// e.g. cfg.EffectiveDataDir() + "/debug/traffic-captures"
	BaseDir string

	// Clock returns the current time. Injected so business logic never
	// calls time.Now() directly (CLAUDE.md + plan §4.5).
	Clock func() time.Time

	// ConnResolver resolves connection metadata from the node registry.
	ConnResolver ConnResolver

	// Factory creates a Writer for the given path.
	// If nil, DefaultWriterFactory is used.
	Factory WriterFactory
}

// ---------------------------------------------------------------------------
// Manager — single source of truth for recording state (plan §4.1)
// ---------------------------------------------------------------------------

// Manager owns active capture rules, capture sessions, and file writers.
//
// Lifecycle: created by node.Service in Run(), ctx-bound to Service.runCtx.
type Manager struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	clock    func() time.Time
	baseDir  string
	resolver ConnResolver
	factory  WriterFactory

	// sessions is keyed by ConnID — one capture session per live connection.
	sessions map[domain.ConnID]*session

	// rules is the set of standing capture instructions (by_ip, all).
	// by_conn_id does not produce rules — it is a one-shot action.
	rules map[string]*rule
}

// NewManager creates a capture Manager bound to the given context.
// The context should derive from the Service run context so that
// graceful shutdown triggers drain (plan §4.5).
func NewManager(ctx context.Context, opts ManagerOpts) *Manager {
	derived, cancel := context.WithCancel(ctx)
	factory := opts.Factory
	if factory == nil {
		factory = DefaultWriterFactory
	}
	return &Manager{
		ctx:      derived,
		cancel:   cancel,
		clock:    opts.Clock,
		baseDir:  opts.BaseDir,
		resolver: opts.ConnResolver,
		factory:  factory,
		sessions: make(map[domain.ConnID]*session),
		rules:    make(map[string]*rule),
	}
}

// Close stops all active capture sessions and waits for writers to drain.
func (m *Manager) Close() {
	m.cancel()

	m.mu.Lock()
	toStop := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		s.initiateStop()
		toStop = append(toStop, s)
	}
	m.mu.Unlock()

	for _, s := range toStop {
		s.waitDone()
	}
}

// -----------------------------------------------------------------------
// Start commands (plan §5.1, §5.2)
// -----------------------------------------------------------------------

// StartByConnIDs starts capture for the given conn_ids.
func (m *Manager) StartByConnIDs(connIDs []domain.ConnID, format domain.CaptureFormat) StartResult {
	sessionDir := m.newSessionDir()
	var result StartResult

	for _, id := range connIDs {
		info, ok := m.resolver.ConnInfoByID(id)
		if !ok {
			result.NotFound = append(result.NotFound, fmt.Sprintf("conn_id=%d", id))
			continue
		}
		m.classifyStart(&result, info, format, domain.CaptureScopeConnID, sessionDir)
	}
	return result
}

// StartByIPs installs by_ip rules and starts capture for current matches.
func (m *Manager) StartByIPs(ips []netip.Addr, format domain.CaptureFormat) StartResult {
	sessionDir := m.newSessionDir()
	var result StartResult

	for _, ip := range ips {
		r := &rule{
			scope:     domain.CaptureScopeIP,
			target:    ip,
			format:    format,
			createdAt: m.clock(),
		}

		m.mu.Lock()
		key := r.key()
		if existing, ok := m.rules[key]; ok && existing.format != format {
			m.mu.Unlock()
			result.Conflicts = append(result.Conflicts,
				fmt.Sprintf("ip=%s: rule exists with format=%s", ip, existing.format))
			continue
		}
		m.rules[key] = r
		m.mu.Unlock()

		conns := m.resolver.ConnInfoByIP(ip)
		ruleEntry := RuleEntry{
			Scope:     domain.CaptureScopeIP,
			Target:    ip,
			Format:    format,
			CreatedAt: r.createdAt,
		}

		for _, info := range conns {
			status := m.classifyStart(&result, info, format, domain.CaptureScopeIP, sessionDir)
			if status == startCreated || status == startAlreadyActive {
				ruleEntry.MatchedConnIDs = append(ruleEntry.MatchedConnIDs, info.ConnID)
			}
		}

		if len(conns) == 0 {
			result.NotFound = append(result.NotFound, fmt.Sprintf("ip=%s: no current connections", ip))
		}
		result.InstalledRules = append(result.InstalledRules, ruleEntry)
	}
	return result
}

// StartAll installs the global all rule and starts capture for every live connection.
func (m *Manager) StartAll(format domain.CaptureFormat) StartResult {
	sessionDir := m.newSessionDir()
	var result StartResult

	r := &rule{
		scope:     domain.CaptureScopeAll,
		format:    format,
		createdAt: m.clock(),
	}

	m.mu.Lock()
	key := r.key()
	if existing, ok := m.rules[key]; ok && existing.format != format {
		m.mu.Unlock()
		result.Conflicts = append(result.Conflicts,
			fmt.Sprintf("all: rule exists with format=%s", existing.format))
		return result
	}
	m.rules[key] = r
	m.mu.Unlock()

	conns := m.resolver.AllConnInfo()
	ruleEntry := RuleEntry{
		Scope:     domain.CaptureScopeAll,
		Format:    format,
		CreatedAt: r.createdAt,
	}

	for _, info := range conns {
		status := m.classifyStart(&result, info, format, domain.CaptureScopeAll, sessionDir)
		if status == startCreated || status == startAlreadyActive {
			ruleEntry.MatchedConnIDs = append(ruleEntry.MatchedConnIDs, info.ConnID)
		}
	}

	result.InstalledRules = append(result.InstalledRules, ruleEntry)
	return result
}

// -----------------------------------------------------------------------
// Stop commands (plan §5.3)
// -----------------------------------------------------------------------

// StopByConnIDs stops capture for the given conn_ids.
func (m *Manager) StopByConnIDs(connIDs []domain.ConnID) StopResult {
	var result StopResult
	for _, id := range connIDs {
		m.mu.Lock()
		s, ok := m.sessions[id]
		if !ok {
			m.mu.Unlock()
			result.NotFound = append(result.NotFound, fmt.Sprintf("conn_id=%d", id))
			continue
		}
		delete(m.sessions, id)
		m.mu.Unlock()

		s.initiateStop()
		s.waitDone()

		snap := s.snapshot()
		result.Stopped = append(result.Stopped, snapshotToStartEntry(snap))
	}
	return result
}

// StopByIPs stops capture sessions and removes by_ip rules for the given IPs.
func (m *Manager) StopByIPs(ips []netip.Addr) StopResult {
	var result StopResult
	for _, ip := range ips {
		key := "ip:" + ip.String()

		m.mu.Lock()
		r, ruleExists := m.rules[key]
		delete(m.rules, key)

		var toStop []*session
		for id, s := range m.sessions {
			if s.remoteIP == ip {
				toStop = append(toStop, s)
				delete(m.sessions, id)
			}
		}
		m.mu.Unlock()

		if ruleExists {
			re := RuleEntry{
				Scope:     r.scope,
				Target:    ip,
				Format:    r.format,
				CreatedAt: r.createdAt,
			}
			for _, s := range toStop {
				re.MatchedConnIDs = append(re.MatchedConnIDs, s.connID)
			}
			result.RemovedRules = append(result.RemovedRules, re)
		}

		for _, s := range toStop {
			s.initiateStop()
			s.waitDone()
			result.Stopped = append(result.Stopped, snapshotToStartEntry(s.snapshot()))
		}

		if !ruleExists && len(toStop) == 0 {
			result.NotFound = append(result.NotFound, fmt.Sprintf("ip=%s", ip))
		}
	}
	return result
}

// StopAll stops all capture sessions and removes all rules (plan §5.3: scope=all).
func (m *Manager) StopAll() StopResult {
	var result StopResult

	m.mu.Lock()
	for _, r := range m.rules {
		result.RemovedRules = append(result.RemovedRules, RuleEntry{
			Scope:     r.scope,
			Target:    r.target,
			Format:    r.format,
			CreatedAt: r.createdAt,
		})
	}
	m.rules = make(map[string]*rule)

	toStop := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		toStop = append(toStop, s)
	}
	m.sessions = make(map[domain.ConnID]*session)
	m.mu.Unlock()

	for _, s := range toStop {
		s.initiateStop()
		s.waitDone()
		result.Stopped = append(result.Stopped, snapshotToStartEntry(s.snapshot()))
	}
	return result
}

// -----------------------------------------------------------------------
// Lifecycle hooks for future connections (plan §4.6)
// -----------------------------------------------------------------------

// OnNewConnection is called from connection lifecycle hooks (inbound
// registerInboundConn, outbound attachOutboundNetCore) to check if any
// standing rule matches the new connection.
//
// Must be lock-safe and short — called from paths that may hold the
// service mutex. Heavy work (file creation, goroutine start) is deferred.
func (m *Manager) OnNewConnection(info ConnInfo) {
	m.mu.Lock()
	if len(m.rules) == 0 {
		m.mu.Unlock()
		return
	}

	// Priority: by_ip > all (plan §5.2).
	var matched *rule
	ipKey := "ip:" + info.RemoteIP.String()
	if r, ok := m.rules[ipKey]; ok {
		matched = r
	} else if r, ok := m.rules["all"]; ok {
		matched = r
	}

	if matched == nil {
		m.mu.Unlock()
		return
	}

	if _, exists := m.sessions[info.ConnID]; exists {
		m.mu.Unlock()
		return
	}

	if len(m.sessions) >= maxSessionsPerProcess {
		m.mu.Unlock()
		log.Warn().
			Uint64("conn_id", uint64(info.ConnID)).
			Msg("capture: session limit reached, cannot auto-start")
		return
	}

	rCopy := *matched
	now := m.clock()

	// Phase 1: register a pending session synchronously so that
	// EnqueueRecv/EnqueueSend can buffer events immediately.
	// This eliminates the first-frame loss window.
	sessionDir := m.newSessionDir()
	fileName := buildFileName(now, info.RemoteIP, info.ConnID, info.PeerDir)
	filePath := filepath.Join(sessionDir, fileName)

	s := newSession(sessionOpts{
		connID:    info.ConnID,
		remoteIP:  info.RemoteIP,
		peerDir:   info.PeerDir,
		format:    rCopy.format,
		scope:     rCopy.scope,
		filePath:  filePath,
		startedAt: now,
		writer:    nil, // pending — no file yet
		onFailed:  m.evictFailedSession,
	})
	m.sessions[info.ConnID] = s
	m.mu.Unlock()

	// Phase 2: defer heavy I/O (mkdir + file create) to a goroutine.
	// Once the writer is ready, attachWriter transitions to active and
	// starts the writer goroutine which drains any buffered events.
	go func() {
		if err := os.MkdirAll(sessionDir, 0o755); err != nil {
			log.Error().Err(err).Str("dir", sessionDir).Msg("capture: mkdir failed for auto-start")
			m.removeSessionIfOwner(info.ConnID, s)
			s.initiateStop()
			return
		}

		w, err := m.factory(filePath)
		if err != nil {
			log.Error().Err(err).Str("path", filePath).Msg("capture: open writer failed for auto-start")
			m.removeSessionIfOwner(info.ConnID, s)
			s.initiateStop()
			return
		}

		if !s.attachWriter(w, m.ctx.Done()) {
			// Session was stopped before writer was ready (connection closed).
			m.removeSessionIfOwner(info.ConnID, s)
			return
		}

		log.Info().
			Uint64("conn_id", uint64(info.ConnID)).
			Str("remote_ip", info.RemoteIP.String()).
			Str("scope", rCopy.scope.String()).
			Str("file", filePath).
			Msg("capture: auto-started by standing rule")
	}()
}

// OnConnectionClosed is called when a connection is torn down.
// Stops the capture session (if any) for the given conn_id.
func (m *Manager) OnConnectionClosed(connID domain.ConnID) {
	m.mu.Lock()
	s, ok := m.sessions[connID]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.sessions, connID)
	m.mu.Unlock()

	s.initiateStop()
	// Let the writer drain in the background — don't block teardown.
}

// -----------------------------------------------------------------------
// Enqueue helpers — called from wire taps
// -----------------------------------------------------------------------

// EnqueueRecv records an inbound wire event. Non-blocking.
func (m *Manager) EnqueueRecv(connID domain.ConnID, raw string, kind domain.PayloadKind) {
	m.mu.Lock()
	s, ok := m.sessions[connID]
	m.mu.Unlock()
	if !ok {
		return
	}
	s.enqueue(Event{
		Ts:      m.clock(),
		WireDir: domain.WireDirectionIn,
		Kind:    kind,
		Raw:     raw,
	})
}

// EnqueueSend records an outbound wire event with its send outcome. Non-blocking.
func (m *Manager) EnqueueSend(connID domain.ConnID, raw string, kind domain.PayloadKind, outcome domain.SendOutcome) {
	m.mu.Lock()
	s, ok := m.sessions[connID]
	m.mu.Unlock()
	if !ok {
		return
	}
	s.enqueue(Event{
		Ts:      m.clock(),
		WireDir: domain.WireDirectionOut,
		Outcome: outcome,
		Kind:    kind,
		Raw:     raw,
	})
}

// -----------------------------------------------------------------------
// Snapshot for PeerHealth / getActivePeers
// -----------------------------------------------------------------------

// SessionSnapshotByID returns the diagnostic snapshot for a specific conn_id.
func (m *Manager) SessionSnapshotByID(connID domain.ConnID) (SessionSnapshot, bool) {
	m.mu.Lock()
	s, ok := m.sessions[connID]
	m.mu.Unlock()
	if !ok {
		return SessionSnapshot{}, false
	}
	return s.snapshot(), true
}

// AllSessionSnapshots returns diagnostic snapshots for all active captures.
func (m *Manager) AllSessionSnapshots() []SessionSnapshot {
	m.mu.Lock()
	list := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		list = append(list, s)
	}
	m.mu.Unlock()

	out := make([]SessionSnapshot, 0, len(list))
	for _, s := range list {
		out = append(out, s.snapshot())
	}
	return out
}

// HasActiveCaptures reports whether any capture is currently running.
func (m *Manager) HasActiveCaptures() bool {
	m.mu.Lock()
	n := len(m.sessions)
	m.mu.Unlock()
	return n > 0
}

// removeSessionIfOwner atomically removes connID from m.sessions only if the
// current map entry still points to the given session. This prevents a stale
// async cleanup goroutine from wiping out a replacement session that was
// installed after the original was stopped.
func (m *Manager) removeSessionIfOwner(connID domain.ConnID, owner *session) {
	m.mu.Lock()
	if m.sessions[connID] == owner {
		delete(m.sessions, connID)
	}
	m.mu.Unlock()
}

// evictFailedSession removes a session that has entered sessionFailed.
// Called by the session's onFailed callback from the writer goroutine.
func (m *Manager) evictFailedSession(connID domain.ConnID) {
	m.mu.Lock()
	s, ok := m.sessions[connID]
	if ok {
		s.mu.Lock()
		failed := s.state == sessionFailed
		s.mu.Unlock()
		if failed {
			delete(m.sessions, connID)
		}
	}
	m.mu.Unlock()
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

type startStatus int

const (
	startCreated startStatus = iota
	startAlreadyActive
	startConflict
	startError
	startLimitReached
)

// classifyStart attempts to start a session for a single connection and
// appends the result to the appropriate bucket of the StartResult.
// Returns the status for the caller's rule-entry bookkeeping.
func (m *Manager) classifyStart(
	result *StartResult,
	info ConnInfo,
	format domain.CaptureFormat,
	scope domain.CaptureScope,
	sessionDir string,
) startStatus {
	entry, status := m.startSessionForConn(info, format, scope, sessionDir)
	switch status {
	case startCreated:
		result.Started = append(result.Started, entry)
	case startAlreadyActive:
		result.AlreadyActive = append(result.AlreadyActive, entry)
	case startConflict:
		result.Conflicts = append(result.Conflicts, fmt.Sprintf("conn_id=%d: format conflict", info.ConnID))
	case startLimitReached:
		result.Errors = append(result.Errors, fmt.Sprintf("conn_id=%d: session limit reached (%d)", info.ConnID, maxSessionsPerProcess))
	case startError:
		result.Errors = append(result.Errors, fmt.Sprintf("conn_id=%d: %s", info.ConnID, entry.FilePath))
	}
	return status
}

func (m *Manager) startSessionForConn(
	info ConnInfo,
	format domain.CaptureFormat,
	scope domain.CaptureScope,
	sessionDir string,
) (StartEntry, startStatus) {
	m.mu.Lock()

	if existing, ok := m.sessions[info.ConnID]; ok {
		snap := existing.snapshot()
		entry := snapshotToStartEntry(snap)
		if snap.Format == format {
			m.mu.Unlock()
			return entry, startAlreadyActive
		}
		m.mu.Unlock()
		return entry, startConflict
	}

	if len(m.sessions) >= maxSessionsPerProcess {
		m.mu.Unlock()
		return StartEntry{}, startLimitReached
	}
	m.mu.Unlock()

	now := m.clock()
	fileName := buildFileName(now, info.RemoteIP, info.ConnID, info.PeerDir)
	filePath := filepath.Join(sessionDir, fileName)

	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		log.Error().Err(err).Str("dir", sessionDir).Msg("capture: mkdir failed")
		return StartEntry{FilePath: err.Error()}, startError
	}

	w, err := m.factory(filePath)
	if err != nil {
		log.Error().Err(err).Str("path", filePath).Msg("capture: open writer failed")
		return StartEntry{FilePath: err.Error()}, startError
	}

	s := newSession(sessionOpts{
		connID:    info.ConnID,
		remoteIP:  info.RemoteIP,
		peerDir:   info.PeerDir,
		format:    format,
		scope:     scope,
		filePath:  filePath,
		startedAt: now,
		writer:    w,
		onFailed:  m.evictFailedSession,
	})

	m.mu.Lock()
	// Double-check after unlock gap: another goroutine may have
	// created a session for the same connID or pushed past the limit.
	if existing, ok := m.sessions[info.ConnID]; ok {
		m.mu.Unlock()
		_ = w.Close()
		snap := existing.snapshot()
		entry := snapshotToStartEntry(snap)
		if snap.Format == format {
			return entry, startAlreadyActive
		}
		return entry, startConflict
	}
	if len(m.sessions) >= maxSessionsPerProcess {
		m.mu.Unlock()
		_ = w.Close()
		return StartEntry{}, startLimitReached
	}
	m.sessions[info.ConnID] = s
	m.mu.Unlock()

	go s.runWriter(m.ctx.Done())

	return StartEntry{
		ConnID:   info.ConnID,
		RemoteIP: info.RemoteIP,
		PeerDir:  info.PeerDir,
		Format:   format,
		FilePath: filePath,
	}, startCreated
}

func (m *Manager) newSessionDir() string {
	ts := m.clock().UTC().Format("2006-01-02T15-04-05Z")
	return filepath.Join(m.baseDir, ts)
}

// buildFileName constructs a filesystem-safe capture file name (plan §6.2).
func buildFileName(ts time.Time, ip netip.Addr, connID domain.ConnID, dir domain.PeerDirection) string {
	tsStr := ts.UTC().Format("2006-01-02T15-04-05.000Z")
	ipStr := domain.FormatIPForFilename(ip)
	return fmt.Sprintf("%s__ip=%s__conn=%d__peerdir=%s.jsonl",
		tsStr, ipStr, connID, dir)
}

func snapshotToStartEntry(snap SessionSnapshot) StartEntry {
	return StartEntry{
		ConnID:   snap.ConnID,
		RemoteIP: snap.RemoteIP,
		PeerDir:  snap.PeerDirection,
		Format:   snap.Format,
		FilePath: snap.FilePath,
	}
}
