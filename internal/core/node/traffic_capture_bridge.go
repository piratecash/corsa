package node

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// ---------------------------------------------------------------------------
// capture.ConnResolver adapter — thin bridge between node.Service and
// capture.Manager that resolves connection metadata from the live registry.
// ---------------------------------------------------------------------------

type serviceCaptureResolver struct {
	svc *Service
}

func (r *serviceCaptureResolver) ConnInfoByID(id domain.ConnID) (capture.ConnInfo, bool) {
	r.svc.mu.RLock()
	defer r.svc.mu.RUnlock()

	entry, ok := r.svc.conns[netcore.ConnID(id)]
	if !ok || entry.core == nil {
		return capture.ConnInfo{}, false
	}

	return capture.ConnInfo{
		ConnID:   id,
		RemoteIP: resolveRemoteIP(entry.core.Conn()),
		PeerDir:  coreToPeerDirection(entry.core.Dir()),
	}, true
}

func (r *serviceCaptureResolver) ConnInfoByIP(ip netip.Addr) []capture.ConnInfo {
	r.svc.mu.RLock()
	defer r.svc.mu.RUnlock()

	var result []capture.ConnInfo
	for id, entry := range r.svc.conns {
		if entry.core == nil {
			continue
		}
		entryIP := resolveRemoteIP(entry.core.Conn())
		if entryIP == ip {
			result = append(result, capture.ConnInfo{
				ConnID:   domain.ConnID(id),
				RemoteIP: ip,
				PeerDir:  coreToPeerDirection(entry.core.Dir()),
			})
		}
	}
	return result
}

func (r *serviceCaptureResolver) AllConnInfo() []capture.ConnInfo {
	r.svc.mu.RLock()
	defer r.svc.mu.RUnlock()

	result := make([]capture.ConnInfo, 0, len(r.svc.conns))
	for id, entry := range r.svc.conns {
		if entry.core == nil {
			continue
		}
		result = append(result, capture.ConnInfo{
			ConnID:   domain.ConnID(id),
			RemoteIP: resolveRemoteIP(entry.core.Conn()),
			PeerDir:  coreToPeerDirection(entry.core.Dir()),
		})
	}
	return result
}

// ---------------------------------------------------------------------------
// IP resolution helpers
// ---------------------------------------------------------------------------

// resolveRemoteIP extracts a typed netip.Addr from a net.Conn's remote
// address. Plan §4.4 mandates netip.Addr for runtime model, not string.
func resolveRemoteIP(conn net.Conn) netip.Addr {
	if conn == nil {
		return netip.Addr{}
	}
	return resolveRemoteIPFromAddr(conn.RemoteAddr())
}

func resolveRemoteIPFromAddr(addr net.Addr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok && tcpAddr.IP != nil {
		if mapped, ok := netip.AddrFromSlice(tcpAddr.IP); ok {
			return mapped.Unmap()
		}
	}
	addrPort, err := netip.ParseAddrPort(addr.String())
	if err == nil {
		return addrPort.Addr().Unmap()
	}
	return netip.Addr{}
}

// coreToPeerDirection maps netcore.Direction to domain.PeerDirection.
func coreToPeerDirection(d netcore.Direction) domain.PeerDirection {
	if d == netcore.Outbound {
		return domain.PeerDirectionOutbound
	}
	return domain.PeerDirectionInbound
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
	})
}

// CaptureManager returns the capture manager (nil before Run).
func (s *Service) CaptureManager() *capture.Manager {
	return s.captureManager
}

// notifyCaptureNewConn tells the capture manager about a new connection
// (for rule matching) and attaches the outbound capture sink to the NetCore.
// Called outside s.mu from handleConn / attachOutboundNetCore.
func (s *Service) notifyCaptureNewConn(connID domain.ConnID, conn net.Conn) {
	if s.captureManager == nil {
		return
	}

	info := capture.ConnInfo{
		ConnID:   connID,
		RemoteIP: resolveRemoteIP(conn),
	}
	// Determine direction from the registry.
	s.mu.RLock()
	if entry, ok := s.conns[netcore.ConnID(connID)]; ok && entry.core != nil {
		info.PeerDir = coreToPeerDirection(entry.core.Dir())
		// Attach capture sink to the NetCore so writerLoop emits events.
		entry.core.SetCaptureSink(&captureSinkAdapter{
			connID:  connID,
			manager: s.captureManager,
		})
	}
	s.mu.RUnlock()

	s.captureManager.OnNewConnection(info)
}

// notifyCaptureConnClosed tells the capture manager that a connection is
// being torn down.
func (s *Service) notifyCaptureConnClosed(connID domain.ConnID) {
	if s.captureManager == nil {
		return
	}
	s.captureManager.OnConnectionClosed(connID)
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
	return marshalStopResult(result)
}

func (s *Service) StopCaptureAll() (json.RawMessage, error) {
	if s.captureManager == nil {
		return nil, fmt.Errorf("capture manager not available")
	}
	result := s.captureManager.StopAll()
	return marshalStopResult(result)
}

// ---------------------------------------------------------------------------
// DTO serialization — transforms domain result types to RPC wire JSON
// ---------------------------------------------------------------------------

type startEntryDTO struct {
	ConnID   uint64 `json:"conn_id"`
	RemoteIP string `json:"remote_ip"`
	PeerDir  string `json:"peer_direction"`
	Format   string `json:"format"`
	FilePath string `json:"file_path"`
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
			ConnID:   uint64(e.ConnID),
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
