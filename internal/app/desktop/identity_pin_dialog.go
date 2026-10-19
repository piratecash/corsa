package desktop

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gioui.org/layout"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// This version deliberately does not follow the domain constant automatically:
// a new warning needs a reviewed translation and a fresh explicit gesture.
const identityPinUIWarningVersion domain.IdentityPinConsequenceVersion = 2

// Deliberately snapshot the reviewed wire wording as well as its version:
// changing the domain constant must not silently reuse an older translation.
const identityPinUIKnownConsequence = "Remove this identity pin, one of the grounds for refusing downgrade. Endpoint bindings and all other requirements to use v2 remain. A later successful v2 proof pins this identity again. The inactive candidate workflow requires a node stop and permits only one removal before external operator finalize. A receipt alone never authorizes network restart."
const identityPinUIRequestTimeout = 10 * time.Second
const identityPinUIResponseLimit = 16 * 1024

type identityPinDialogPhase uint8

const (
	identityPinDraft identityPinDialogPhase = iota
	identityPinLoading
	identityPinPreviewReady
	identityPinUnavailable
	identityPinInvalidWarning
	identityPinConfirming
	identityPinLocalPending
	identityPinProtectionRetained
	identityPinRecoveryRequired
	identityPinRequestFailed
	identityPinInvalidIdentity
	identityPinStalePreview
	identityPinNotPinned
)

type identityPinPreviewState struct {
	identity  domain.PeerIdentity
	pinnedAt  time.Time
	version   domain.IdentityPinConsequenceVersion
	token     domain.IdentityPinConfirmationToken
	available bool
}

func (identityPinPreviewState) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("[redacted pin preview]"))
}
func (identityPinPreviewState) MarshalJSON() ([]byte, error) {
	return nil, identity.ErrSecretSerialization
}
func (*identityPinPreviewState) UnmarshalJSON([]byte) error { return identity.ErrSecretSerialization }

type identityPinUIGeneration uint64

// Widgets/model below belong only to the UI goroutine. Workers receive immutable
// snapshots and publish once to their own bounded reply channel; they never
// mutate dialog state. Replaced/cancelled channels cannot publish a later view.
type identityPinDialog struct {
	visible           bool
	editor            widget.Editor
	phase             identityPinDialogPhase
	preview           *identityPinPreviewState
	warningShown      bool
	acknowledged      bool
	generation        identityPinUIGeneration
	draft             string
	pending           *identityPinUIRequest
	operation         domain.IdentityPinOperationID
	focusPending      bool
	list              widget.List
	previewButton     widget.Clickable
	confirmButton     widget.Clickable
	acknowledgeButton widget.Clickable
	cancelButton      widget.Clickable
	closeButton       widget.Clickable
	dismissTag        struct{}
}

func (identityPinDialog) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("[redacted pin dialog]"))
}
func (identityPinDialog) MarshalJSON() ([]byte, error) { return nil, identity.ErrSecretSerialization }
func (*identityPinDialog) UnmarshalJSON([]byte) error  { return identity.ErrSecretSerialization }

type identityPinUIRequest struct {
	generation identityPinUIGeneration
	identity   domain.PeerIdentity
	confirm    bool
	cancel     context.CancelFunc
	replies    <-chan identityPinUIReply
	stopping   <-chan struct{}
}
type identityPinUIReply struct {
	generation identityPinUIGeneration
	identity   domain.PeerIdentity
	phase      identityPinDialogPhase
	preview    *identityPinPreviewState
	operation  domain.IdentityPinOperationID
}

func (identityPinUIReply) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("[redacted pin reply]"))
}
func (identityPinUIReply) MarshalJSON() ([]byte, error) { return nil, identity.ErrSecretSerialization }
func (*identityPinUIReply) UnmarshalJSON([]byte) error  { return identity.ErrSecretSerialization }

func newIdentityPinDialog() *identityPinDialog {
	d := &identityPinDialog{list: widget.List{List: layout.List{Axis: layout.Vertical}}}
	d.editor.SingleLine, d.editor.Submit = true, true
	return d
}
func (d *identityPinDialog) invalidate() {
	if d.pending != nil {
		d.pending.cancel()
		d.pending = nil
	}
	d.generation++
	d.preview = nil
	d.warningShown, d.acknowledged = false, false
	d.operation = ""
}
func (d *identityPinDialog) open() {
	d.close()
	d.visible = true
	d.focusPending = true
}
func (d *identityPinDialog) close() {
	d.invalidate()
	d.visible = false
	d.focusPending = false
	d.phase = identityPinDraft
	d.editor.SetText("")
	d.draft = ""
	d.list.Position = layout.Position{}
	d.previewButton, d.confirmButton, d.acknowledgeButton = widget.Clickable{}, widget.Clickable{}, widget.Clickable{}
	d.cancelButton, d.closeButton = widget.Clickable{}, widget.Clickable{}
}
func (d *identityPinDialog) syncIdentity() {
	if current := d.editor.Text(); current != d.draft {
		d.invalidate()
		d.draft = current
		d.phase = identityPinDraft
	}
}
func (d *identityPinDialog) identity() (domain.PeerIdentity, error) {
	id, err := domain.ParsePeerIdentity(strings.TrimSpace(d.editor.Text()))
	if err != nil || id.IsZero() {
		return domain.PeerIdentity{}, domain.ErrIdentityPinIdentityRequired
	}
	return id, nil
}
func (d *identityPinDialog) canConfirm() bool {
	if !d.visible || d.pending != nil || d.phase != identityPinPreviewReady || d.preview == nil || !d.preview.available || d.preview.version != identityPinUIWarningVersion || !d.warningShown || !d.acknowledged {
		return false
	}
	id, err := d.identity()
	return err == nil && d.draft == d.editor.Text() && id == d.preview.identity
}
func (d *identityPinDialog) canAcknowledge() bool {
	return d.visible && d.pending == nil && d.phase == identityPinPreviewReady && d.preview != nil && d.preview.available && d.warningShown
}
func (d *identityPinDialog) canPreview() bool {
	return d.visible && d.pending == nil && d.phase != identityPinConfirming && d.phase != identityPinLocalPending && d.phase != identityPinRecoveryRequired
}
func (d *identityPinDialog) requestPreview(w *Window) {
	d.syncIdentity()
	if !d.canPreview() {
		return
	}
	id, err := d.identity()
	if err != nil {
		d.invalidate()
		d.phase = identityPinInvalidIdentity
		return
	}
	d.invalidate()
	d.phase = identityPinLoading
	d.start(w, id, "", false)
}
func (d *identityPinDialog) requestConfirm(w *Window) {
	d.syncIdentity()
	if !d.canConfirm() {
		return
	}
	id, token := d.preview.identity, d.preview.token
	d.invalidate()
	d.phase = identityPinConfirming
	d.start(w, id, token, true)
}
func (d *identityPinDialog) start(w *Window, id domain.PeerIdentity, token domain.IdentityPinConfirmationToken, confirm bool) {
	name := "previewIdentityPinRemovalUI"
	if confirm {
		name = "confirmIdentityPinRemovalUI"
	}
	if w.cmdTable == nil {
		d.phase = identityPinUnavailable
		return
	}
	policy, exists := w.cmdTable.TransportPolicyFor(name)
	if !exists || policy != rpc.TransportInProcessOnly || !w.beginUIOp() {
		d.phase = identityPinUnavailable
		return
	}
	base, stop := w.uiOpContext()
	ctx, cancel := context.WithTimeout(base, identityPinUIRequestTimeout)
	replies := make(chan identityPinUIReply, 1)
	generation := d.generation
	d.pending = &identityPinUIRequest{generation: generation, identity: id, confirm: confirm, cancel: func() { cancel(); stop() }, replies: replies, stopping: w.uiStop()}
	table := w.cmdTable
	go func() {
		defer w.endUIOp()
		defer cancel()
		defer stop()
		args := map[string]interface{}{"identity": id.String()}
		if confirm {
			args["confirm"] = string(token)
		}
		// Direct typed dispatch never enters console history, output or clipboard.
		response := table.Execute(rpc.CommandRequest{Name: name, Args: args, Ctx: ctx})
		reply := identityPinUIReply{generation: generation, identity: id, phase: identityPinRequestFailed}
		if response.Error != nil {
			reply.phase = identityPinUIError(response)
		} else if confirm {
			reply = identityPinUIConfirmReply(response, id, generation)
		} else if ctx.Err() == nil {
			reply = identityPinUIPreviewReply(response, id, generation)
		}
		replies <- reply
		w.invalidate()
	}()
}
func (d *identityPinDialog) poll() {
	d.syncIdentity()
	if !d.visible || d.pending == nil {
		return
	}
	request := d.pending
	select {
	case <-request.stopping:
		d.invalidate()
		d.phase = identityPinUnavailable
		return
	default:
	}
	select {
	case reply := <-request.replies:
		request.cancel()
		d.pending = nil
		if reply.generation != d.generation || reply.identity != request.identity {
			return
		}
		d.phase, d.preview, d.operation = reply.phase, reply.preview, reply.operation
		d.warningShown, d.acknowledged = false, false
	default:
	}
}
func identityPinUIError(response rpc.CommandResponse) identityPinDialogPhase {
	switch {
	case errors.Is(response.Error, domain.ErrIdentityPinRecoveryRequired):
		return identityPinRecoveryRequired
	case errors.Is(response.Error, domain.ErrIdentityPinConfirmationStale):
		return identityPinStalePreview
	case errors.Is(response.Error, domain.ErrIdentityPinNotFound):
		return identityPinNotPinned
	case errors.Is(response.Error, domain.ErrIdentityPinIdentityRequired):
		return identityPinInvalidIdentity
	case response.ErrorKind == rpc.ErrUnavailable || response.ErrorKind == rpc.ErrForbidden:
		return identityPinUnavailable
	default:
		return identityPinRequestFailed
	}
}
func identityPinUIPreviewReply(response rpc.CommandResponse, id domain.PeerIdentity, generation identityPinUIGeneration) identityPinUIReply {
	reply := identityPinUIReply{generation: generation, identity: id, phase: identityPinInvalidWarning}
	var dto rpc.IdentityPinRemovalPreviewDTO
	if len(response.Data) > identityPinUIResponseLimit || json.Unmarshal(response.Data, &dto) != nil || dto.Identity != id || dto.PinnedAt.IsZero() || dto.ConsequenceVersion != identityPinUIWarningVersion || dto.Consequence != identityPinUIKnownConsequence {
		return reply
	}
	raw, err := hex.DecodeString(string(dto.ConfirmToken))
	if err != nil || len(raw) != 32 {
		return reply
	}
	preview := &identityPinPreviewState{identity: id, pinnedAt: dto.PinnedAt, version: dto.ConsequenceVersion, available: dto.RemovalAvailable}
	reply.phase = identityPinUnavailable
	if dto.RemovalAvailable {
		preview.token = dto.ConfirmToken
		reply.phase = identityPinPreviewReady
	}
	reply.preview = preview
	return reply
}
func identityPinUIConfirmReply(response rpc.CommandResponse, id domain.PeerIdentity, generation identityPinUIGeneration) identityPinUIReply {
	reply := identityPinUIReply{generation: generation, identity: id, phase: identityPinRequestFailed}
	var dto rpc.IdentityPinRemovalResultDTO
	if len(response.Data) > identityPinUIResponseLimit || json.Unmarshal(response.Data, &dto) != nil || dto.Identity != id || len(dto.OperationID) == 0 || len(dto.OperationID) > 128 {
		return reply
	}
	switch dto.Outcome {
	case domain.IdentityPinRemovalCommitted:
		reply.phase = identityPinLocalPending
	case domain.IdentityPinRemovalNotCommitted:
		reply.phase = identityPinProtectionRetained
	case domain.IdentityPinRemovalIndeterminate, domain.IdentityPinRemovalRecoveryRequired:
		reply.phase = identityPinRecoveryRequired
	default:
		return reply
	}
	reply.operation = dto.OperationID
	return reply
}
