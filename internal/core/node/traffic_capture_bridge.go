package node

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"

	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// capture.ConnResolver adapter — thin bridge between node.Service and
// capture.Manager that resolves connection metadata from the live registry.
//
// All registry access goes through ConnID-first helpers (connInfoByIDLocked,
// forEachConnLocked) so this file never touches s.conns, entry.core or
// net.Conn directly. The §2.9 boundary requires the bridge to depend only on
// the value-typed connInfo snapshot the registry hands out.
// ---------------------------------------------------------------------------

type serviceCaptureResolver struct {
	svc *Service
}

func (r *serviceCaptureResolver) ConnInfoByID(id domain.ConnID) (capture.ConnInfo, bool) {
	r.svc.peerMu.RLock()
	defer r.svc.peerMu.RUnlock()

	info, ok := r.svc.connInfoByIDLocked(id)
	if !ok {
		return capture.ConnInfo{}, false
	}
	return capture.ConnInfo{
		ConnID:   info.id,
		RemoteIP: info.remoteIP,
		PeerDir:  info.peerDir,
	}, true
}

func (r *serviceCaptureResolver) ConnInfoByIP(ip netip.Addr) []capture.ConnInfo {
	r.svc.peerMu.RLock()
	defer r.svc.peerMu.RUnlock()

	var result []capture.ConnInfo
	r.svc.forEachConnLocked(func(info connInfo) bool {
		if info.remoteIP == ip {
			result = append(result, capture.ConnInfo{
				ConnID:   info.id,
				RemoteIP: info.remoteIP,
				PeerDir:  info.peerDir,
			})
		}
		return true
	})
	return result
}

func (r *serviceCaptureResolver) AllConnInfo() []capture.ConnInfo {
	r.svc.peerMu.RLock()
	defer r.svc.peerMu.RUnlock()

	result := make([]capture.ConnInfo, 0, r.svc.connCountLocked())
	r.svc.forEachConnLocked(func(info connInfo) bool {
		result = append(result, capture.ConnInfo{
			ConnID:   info.id,
			RemoteIP: info.remoteIP,
			PeerDir:  info.peerDir,
		})
		return true
	})
	return result
}

// ---------------------------------------------------------------------------
// Service integration — init, accessor, lifecycle hooks
// ---------------------------------------------------------------------------

// initCaptureManager creates and stores the capture.Manager bound to the
// service run context. Called from Service.Run().
func (s *Service) initCaptureManager() {
	baseDir := filepath.Join(s.cfg.EffectiveDataDir(), "debug", "traffic-captures")
	s.captureManager = capture.NewManager(s.runCtx, capture.ManagerOpts{
		BaseDir:      baseDir,
		Clock:        time.Now,
		ConnResolver: &serviceCaptureResolver{svc: s},
		// Sessions the manager tears down on its own (auto-start setup
		// failures, runtime writer failures) must still surface as Stopped
		// on the bus, or NodeStatusMonitor keeps showing a recording that
		// silently died. Stop*/StopAll teardown is published separately by
		// publishCaptureStopped; duplicates are idempotent for subscribers.
		OnSessionEvicted: s.publishCaptureSessionEvicted,
	})
}

// CaptureManager returns the capture manager (nil before Run).
func (s *Service) CaptureManager() *capture.Manager {
	return s.captureManager
}

// startConfiguredCapture starts recording all peer traffic at startup when
// cfg.RecordAllTraffic is set (env: CORSA_RECORD_ALL_TRAFFIC, default off).
// Called from Service.Run right after initCaptureManager. It goes through
// StartCaptureAll — the same path as the recordAllPeerTraffic RPC command —
// so the standing scope=all rule is installed (capturing every connection
// established later) and ebus Started events are published identically.
// A bad CORSA_RECORD_TRAFFIC_FORMAT is logged and ignored rather than
// failing startup: a diagnostic feature must not take the node down.
func (s *Service) startConfiguredCapture() {
	if !s.cfg.RecordAllTraffic {
		return
	}
	if _, err := s.StartCaptureAll(s.cfg.RecordTrafficFormat); err != nil {
		log.Error().
			Err(err).
			Str("format", s.cfg.RecordTrafficFormat).
			Msg("capture: startup traffic recording failed (CORSA_RECORD_ALL_TRAFFIC)")
		return
	}
	// Log the effective format, not the raw env value: the default empty
	// string parses to compact and would otherwise show up as format="".
	// StartCaptureAll succeeded, so the parse cannot fail here.
	format, _ := domain.ParseCaptureFormat(s.cfg.RecordTrafficFormat)
	log.Info().
		Str("format", format.String()).
		Msg("capture: startup traffic recording enabled (CORSA_RECORD_ALL_TRAFFIC)")
}

// notifyCaptureNewConn tells the capture manager about a new connection
// (for rule matching) and attaches the outbound capture sink to the NetCore.
// Called from handleConn / attachOutboundNetCore after the connection has
// been registered, so the registry lookup always succeeds in production —
// the bridge no longer needs the raw net.Conn to read the remote IP.
func (s *Service) notifyCaptureNewConn(connID domain.ConnID) {
	if s.captureManager == nil {
		return
	}

	sink := &captureSinkAdapter{
		connID:  connID,
		manager: s.captureManager,
	}
	remoteIP, peerDir, ok := s.attachCaptureSinkByID(connID, sink)
	if !ok {
		// The connection raced away between the lifecycle hook and the
		// registry lookup. Without registry metadata the manager cannot
		// match standing rules anyway, so skip the OnNewConnection call.
		return
	}

	s.captureManager.OnNewConnection(capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: remoteIP,
		PeerDir:  peerDir,
	})

	// Standing rules (recordAllPeerTraffic / recordPeerTrafficByIP, including
	// the CORSA_RECORD_ALL_TRAFFIC startup rule) auto-start sessions inside
	// OnNewConnection. That path bypasses StartCapture* and would otherwise
	// never reach publishCaptureStarted, breaking the topics.go contract that
	// every session start emits TopicCaptureSessionStarted — NodeStatusMonitor
	// relies on it for CaptureSessions / the recording UI state.
	// OnNewConnection registers the (pending) session synchronously, so a
	// successful auto-start is observable here; if no rule matched there is
	// no session and nothing is published.
	s.publishAutoStartedCapture(connID)
}

// publishAutoStartedCapture emits TopicCaptureSessionStarted for a session
// that a standing rule auto-started for connID inside OnNewConnection.
// No-op when no session exists (no rule matched) or no bus is wired.
func (s *Service) publishAutoStartedCapture(connID domain.ConnID) {
	if s.eventBus == nil || s.captureManager == nil {
		return
	}
	snap, ok := s.captureManager.SessionSnapshotByID(connID)
	if !ok {
		return
	}
	s.publishOneCaptureStarted(capture.StartEntry{
		ConnID:   snap.ConnID,
		RemoteIP: snap.RemoteIP,
		PeerDir:  snap.PeerDirection,
		Format:   snap.Format,
		FilePath: snap.FilePath,
	})
}

// notifyCaptureConnClosed tells the capture manager that a connection is
// being torn down, and publishes the paired Stopped event when a capture
// session existed for it. Auto-started sessions (standing rules) have no
// Stop* RPC call to publish their teardown, so without this the status
// monitor would keep showing an active recording after the peer disconnects.
// The existed-check races an explicit Stop* only in the duplicate direction
// (both publish Stopped), which is idempotent for subscribers.
func (s *Service) notifyCaptureConnClosed(connID domain.ConnID) {
	if s.captureManager == nil {
		return
	}
	snap, hadSession := s.captureManager.SessionSnapshotByID(connID)
	s.captureManager.OnConnectionClosed(connID)
	if hadSession {
		s.publishCaptureSessionEvicted(snap, nil)
	}
}

// publishCaptureSessionEvicted emits TopicCaptureSessionStopped for a session
// torn down outside the Stop* RPC paths: connection close, auto-start setup
// failure, or runtime writer failure (wired as capture.ManagerOpts.
// OnSessionEvicted). The snapshot's terminal diagnostics flow into the
// payload per the CaptureSessionStopped contract; cause covers setup
// failures where the session never recorded its own error.
func (s *Service) publishCaptureSessionEvicted(snap capture.SessionSnapshot, cause error) {
	if s.eventBus == nil {
		return
	}
	errMsg := snap.Error
	if errMsg == "" && cause != nil {
		errMsg = cause.Error()
	}
	s.eventBus.Publish(ebus.TopicCaptureSessionStopped, ebus.CaptureSessionStopped{
		ConnID:        snap.ConnID,
		Error:         errMsg,
		DroppedEvents: snap.DroppedEvents,
	})
}

// ---------------------------------------------------------------------------
// netcore.CaptureSink adapter — bridges NetCore outbound tap to Manager
// ---------------------------------------------------------------------------

// captureSinkAdapter implements netcore.CaptureSink. It is created per
// conn_id and bridges the writer goroutine's OnSendAttempt calls to
// capture.Manager.EnqueueSend.
type captureSinkAdapter struct {
	connID  domain.ConnID
	manager *capture.Manager
}

func (a *captureSinkAdapter) OnSendAttempt(data []byte, ok bool) {
	outcome := domain.SendOutcomeSent
	if !ok {
		outcome = domain.SendOutcomeWriteFailed
	}
	raw := strings.TrimRight(string(data), "\n")
	kind := capture.ClassifyPayload(raw)
	a.manager.EnqueueSend(a.connID, raw, kind, outcome)
}

// ---------------------------------------------------------------------------
// Inbound recv tap helper — called from handleConn read loop
// ---------------------------------------------------------------------------

// captureInboundRecv enqueues an inbound recv event if capture is active.
// Called from handleConn after readFrameLine, before dispatch.
func (s *Service) captureInboundRecv(connID domain.ConnID, rawLine string) {
	if s.captureManager == nil {
		return
	}
	kind := capture.ClassifyPayload(rawLine)
	s.captureManager.EnqueueRecv(connID, rawLine, kind)
}

// captureInboundRecvFrameTooLarge records a frame_too_large event.
func (s *Service) captureInboundRecvFrameTooLarge(connID domain.ConnID) {
	if s.captureManager == nil {
		return
	}
	s.captureManager.EnqueueRecv(connID, "<frame_too_large>", domain.PayloadKindFrameTooLarge)
}

// ---------------------------------------------------------------------------
// Outbound recv tap helper — called from readPeerSession read loop
// ---------------------------------------------------------------------------

// captureOutboundRecv enqueues an outbound-session recv event if capture
// is active. Called from readPeerSession after readFrameLine, before parse.
func (s *Service) captureOutboundRecv(connID domain.ConnID, rawLine string) {
	if s.captureManager == nil {
		return
	}
	kind := capture.ClassifyPayload(rawLine)
	s.captureManager.EnqueueRecv(connID, rawLine, kind)
}

// captureOutboundRecvFrameTooLarge records a frame_too_large event for outbound sessions.
func (s *Service) captureOutboundRecvFrameTooLarge(connID domain.ConnID) {
	if s.captureManager == nil {
		return
	}
	s.captureManager.EnqueueRecv(connID, "<frame_too_large>", domain.PayloadKindFrameTooLarge)
}

// ---------------------------------------------------------------------------
// rpc.CaptureProvider implementation — transport boundary adapter
// ---------------------------------------------------------------------------

func (s *Service) StartCaptureByConnIDs(connIDs []uint64, format string) (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	f, ok := domain.ParseCaptureFormat(format)
	if !ok {
		return nil, fmt.Errorf("invalid format: %q", format)
	}
	ids := make([]domain.ConnID, len(connIDs))
	for i, v := range connIDs {
		ids[i] = domain.ConnID(v)
	}
	result := s.captureManager.StartByConnIDs(ids, f)
	s.publishCaptureStarted(result)
	return marshalStartResult(result)
}

func (s *Service) StartCaptureByIPs(ips []string, format string) (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	f, ok := domain.ParseCaptureFormat(format)
	if !ok {
		return nil, fmt.Errorf("invalid format: %q", format)
	}
	addrs := make([]netip.Addr, 0, len(ips))
	for _, raw := range ips {
		addr, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid IP %q: %w", raw, err)
		}
		addrs = append(addrs, addr)
	}
	result := s.captureManager.StartByIPs(addrs, f)
	s.publishCaptureStarted(result)
	return marshalStartResult(result)
}

func (s *Service) StartCaptureAll(format string) (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	f, ok := domain.ParseCaptureFormat(format)
	if !ok {
		return nil, fmt.Errorf("invalid format: %q", format)
	}
	result := s.captureManager.StartAll(f)
	s.publishCaptureStarted(result)
	return marshalStartResult(result)
}

func (s *Service) StopCaptureByConnIDs(connIDs []uint64) (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	ids := make([]domain.ConnID, len(connIDs))
	for i, v := range connIDs {
		ids[i] = domain.ConnID(v)
	}
	result := s.captureManager.StopByConnIDs(ids)
	s.publishCaptureStopped(result)
	return marshalStopResult(result)
}

func (s *Service) StopCaptureByIPs(ips []string) (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	addrs := make([]netip.Addr, 0, len(ips))
	for _, raw := range ips {
		addr, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid IP %q: %w", raw, err)
		}
		addrs = append(addrs, addr)
	}
	result := s.captureManager.StopByIPs(addrs)
	s.publishCaptureStopped(result)
	return marshalStopResult(result)
}

func (s *Service) StopCaptureAll() (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	result := s.captureManager.StopAll()
	s.publishCaptureStopped(result)
	return marshalStopResult(result)
}

// ---------------------------------------------------------------------------
// ebus publishing — keeps NodeStatusMonitor's Recording* fields live without
// re-polling fetchPeerHealth. Handlers on the subscriber side update peer
// rows identified by ConnID (globally unique) so no address lookup is needed.
// ---------------------------------------------------------------------------

// publishCaptureStarted emits TopicCaptureSessionStarted for every session
// that became (or remained) active as a result of a Start*/StartAll call.
// Both Started and AlreadyActive entries are emitted: re-emitting a Started
// for an existing session is idempotent on the subscriber side (setting
// Recording=true twice is a no-op) and self-heals a monitor that lost the
// original event because of a full inbox.
//
// StartedAt and Scope are read from SessionSnapshotByID because
// capture.StartEntry does not carry them; the snapshot is thread-safe and
// returns the same startedAt the session stamped at construction time.
func (s *Service) publishCaptureStarted(result capture.StartResult) {
	if s.eventBus == nil || s.captureManager == nil {
		return
	}
	for _, e := range result.Started {
		s.publishOneCaptureStarted(e)
	}
	for _, e := range result.AlreadyActive {
		s.publishOneCaptureStarted(e)
	}
}

func (s *Service) publishOneCaptureStarted(entry capture.StartEntry) {
	snap, ok := s.captureManager.SessionSnapshotByID(entry.ConnID)
	if !ok {
		// Session raced away between the Start call and the snapshot lookup
		// (writer failure, OnConnectionClosed). The paired Stopped publish
		// will follow from whichever path evicted the session.
		return
	}

	// Resolve overlay identity (Address, PeerID, Direction) from the
	// connection registry so NodeStatusMonitor can materialize a missing
	// PeerHealth row when this event races ahead of TopicPeerHealthChanged.
	// Empty values are acceptable — the subscriber treats empty Address as
	// "cannot recover, wait for a future delta" rather than inventing a
	// phantom row.
	address, peerID, direction := s.resolveOverlayIdentityByConnID(entry.ConnID)

	s.eventBus.Publish(ebus.TopicCaptureSessionStarted, ebus.CaptureSessionStarted{
		ConnID:    entry.ConnID,
		Address:   address,
		PeerID:    peerID,
		Direction: direction,
		FilePath:  entry.FilePath,
		StartedAt: ebus.TimePtr(snap.StartedAt),
		Scope:     snap.Scope,
		Format:    entry.Format,
	})
}

// resolveOverlayIdentityByConnID returns the overlay address, identity, and
// direction recorded on the live connection, or zero values when the
// connection is not registered (or has no core attached). Thin adapter over
// the registry helper that keeps s.conns / entry.core out of this file.
func (s *Service) resolveOverlayIdentityByConnID(id domain.ConnID) (domain.PeerAddress, domain.PeerIdentity, domain.PeerDirection) {
	return s.overlayIdentityByID(id)
}

// publishCaptureStopped emits TopicCaptureSessionStopped for every session
// that was torn down by a Stop*/StopAll call. Error and DroppedEvents are
// left zero — capture.StopResult does not surface terminal diagnostics today.
// If they become observable we fill them here, not on the subscriber side,
// so the monitor remains a pure applicator of remote state.
func (s *Service) publishCaptureStopped(result capture.StopResult) {
	if s.eventBus == nil {
		return
	}
	for _, e := range result.Stopped {
		s.eventBus.Publish(ebus.TopicCaptureSessionStopped, ebus.CaptureSessionStopped{
			ConnID: e.ConnID,
		})
	}
}

// ---------------------------------------------------------------------------
// DTO serialization — transforms domain result types to RPC wire JSON
// ---------------------------------------------------------------------------

type startEntryDTO struct {
	ConnID   domain.ConnID `json:"conn_id"`
	RemoteIP string        `json:"remote_ip"`
	PeerDir  string        `json:"peer_direction"`
	Format   string        `json:"format"`
	FilePath string        `json:"file_path"`
}

type ruleEntryDTO struct {
	Scope          string   `json:"scope"`
	Target         string   `json:"target"`
	Format         string   `json:"format"`
	CreatedAt      string   `json:"created_at"`
	MatchedConnIDs []uint64 `json:"matched_conn_ids,omitempty"`
}

func marshalStartResult(r capture.StartResult) (json.RawMessage, error) {
	dto := struct {
		Started        []startEntryDTO `json:"started,omitempty"`
		AlreadyActive  []startEntryDTO `json:"already_active,omitempty"`
		InstalledRules []ruleEntryDTO  `json:"installed_rules,omitempty"`
		NotFound       []string        `json:"not_found,omitempty"`
		Conflicts      []string        `json:"conflicts,omitempty"`
		Errors         []string        `json:"errors,omitempty"`
	}{
		Started:        toStartEntryDTOs(r.Started),
		AlreadyActive:  toStartEntryDTOs(r.AlreadyActive),
		InstalledRules: toRuleEntryDTOs(r.InstalledRules),
		NotFound:       r.NotFound,
		Conflicts:      r.Conflicts,
		Errors:         r.Errors,
	}
	return json.Marshal(dto)
}

func marshalStopResult(r capture.StopResult) (json.RawMessage, error) {
	dto := struct {
		Stopped      []startEntryDTO `json:"stopped,omitempty"`
		RemovedRules []ruleEntryDTO  `json:"removed_rules,omitempty"`
		NotFound     []string        `json:"not_found,omitempty"`
		Errors       []string        `json:"errors,omitempty"`
	}{
		Stopped:      toStartEntryDTOs(r.Stopped),
		RemovedRules: toRuleEntryDTOs(r.RemovedRules),
		NotFound:     r.NotFound,
		Errors:       r.Errors,
	}
	return json.Marshal(dto)
}

func toStartEntryDTOs(entries []capture.StartEntry) []startEntryDTO {
	if len(entries) == 0 {
		return nil
	}
	out := make([]startEntryDTO, len(entries))
	for i, e := range entries {
		out[i] = startEntryDTO{
			ConnID:   e.ConnID,
			RemoteIP: e.RemoteIP.String(),
			PeerDir:  e.PeerDir.String(),
			Format:   e.Format.String(),
			FilePath: e.FilePath,
		}
	}
	return out
}

func toRuleEntryDTOs(entries []capture.RuleEntry) []ruleEntryDTO {
	if len(entries) == 0 {
		return nil
	}
	out := make([]ruleEntryDTO, len(entries))
	for i, e := range entries {
		target := "all"
		if e.Target.IsValid() {
			target = e.Target.String()
		}
		ids := make([]uint64, len(e.MatchedConnIDs))
		for j, id := range e.MatchedConnIDs {
			ids[j] = uint64(id)
		}
		out[i] = ruleEntryDTO{
			Scope:          e.Scope.String(),
			Target:         target,
			Format:         e.Format.String(),
			CreatedAt:      e.CreatedAt.UTC().Format(time.RFC3339Nano),
			MatchedConnIDs: ids,
		}
	}
	return out
}
