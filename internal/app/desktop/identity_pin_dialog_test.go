package desktop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/stretchr/testify/mock"
)

func pinDialogFixture(t *testing.T) (*identityPinDialog, *Window, *rpcmocks.MockIdentityPinProvider, domain.IdentityPinRemovalPreview) {
	t.Helper()
	provider := rpcmocks.NewMockIdentityPinProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterIdentityPinCommands(table, provider)
	w := newConsoleModalTestWindow(t)
	w.cmdTable = table
	d := newIdentityPinDialog()
	d.open()
	preview := domain.IdentityPinRemovalPreview{Identity: domain.PeerIdentity{21}, PinnedAt: time.Unix(12, 0).UTC(), Consequence: domain.IdentityPinRemovalConsequence, ConsequenceVersion: domain.CurrentIdentityPinConsequenceVersion, Token: domain.IdentityPinConfirmationToken(strings.Repeat("ab", 32)), RemovalAvailable: true}
	d.editor.SetText(preview.Identity.String())
	t.Cleanup(func() {
		d.close()
		if !w.drainUIOps(time.Second) {
			t.Error("pin dialog UI worker failed to drain")
		}
	})
	return d, w, provider, preview
}

func TestIdentityPinDialogRequiresPresentedWarningAndExplicitConfirmation(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
	d.requestPreview(w)
	w.sendWG.Wait()
	d.poll()
	if d.preview == nil || d.phase != identityPinPreviewReady {
		t.Fatal("exact identity preview was not delivered")
	}
	d.acknowledged = true
	d.requestConfirm(w)
	provider.AssertNotCalled(t, "ConfirmIdentityPinRemoval", mock.Anything, mock.Anything, mock.Anything)
	d.warningShown = true
	if !d.canConfirm() {
		t.Fatal("presented supported warning plus explicit acknowledgement did not permit confirmation")
	}
	provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, preview.Identity, preview.Token).Return(domain.IdentityPinRemovalResult{OperationID: "one-operation", Identity: preview.Identity, Outcome: domain.IdentityPinRemovalCommitted}, nil).Once()
	d.requestConfirm(w)
	w.sendWG.Wait()
	d.poll()
	if d.phase != identityPinLocalPending {
		t.Fatalf("local durability was presented as full success: %v", d.phase)
	}
	if d.preview != nil {
		t.Fatal("completion retained confirmation credential")
	}
}

func TestIdentityPinDialogUnknownWarningAndUnavailableNeverConfirm(t *testing.T) {
	for _, mode := range []string{"unknown-version", "changed-warning-same-version", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			d, w, provider, preview := pinDialogFixture(t)
			switch mode {
			case "unknown-version":
				preview.ConsequenceVersion++
			case "changed-warning-same-version":
				preview.Consequence = "different server warning"
			default:
				preview.RemovalAvailable = false
			}
			provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
			d.requestPreview(w)
			w.sendWG.Wait()
			d.poll()
			expected := identityPinInvalidWarning
			if mode == "unavailable" {
				expected = identityPinUnavailable
			}
			if d.phase != expected {
				t.Fatalf("wrong refusal state: %v", d.phase)
			}
			d.acknowledged, d.warningShown = true, true
			d.requestConfirm(w)
			if d.canConfirm() {
				t.Fatal("unsupported confirmation became available")
			}
			provider.AssertNotCalled(t, "ConfirmIdentityPinRemoval", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestIdentityPinDialogIdentityEditCancelsAndRejectsLatePreview(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).RunAndReturn(func(ctx context.Context, _ domain.PeerIdentity) (domain.IdentityPinRemovalPreview, error) {
		close(started)
		<-release
		return preview, nil
	}).Once()
	d.requestPreview(w)
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("preview worker never started")
	}
	d.editor.SetText((domain.PeerIdentity{22}).String())
	d.syncIdentity()
	close(release)
	w.sendWG.Wait()
	d.poll()
	if d.preview != nil || d.canConfirm() {
		t.Fatal("late reply restored a credential for the old identity")
	}
}

func TestIdentityPinDialogMapsRecoveryWithoutReportingRemoval(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).Return(preview, nil).Once()
	d.requestPreview(w)
	w.sendWG.Wait()
	d.poll()
	d.warningShown, d.acknowledged = true, true
	provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, preview.Identity, preview.Token).Return(domain.IdentityPinRemovalResult{}, errors.Join(errors.New("storage uncertainty"), domain.ErrIdentityPinRecoveryRequired)).Once()
	d.requestConfirm(w)
	w.sendWG.Wait()
	d.poll()
	if d.phase != identityPinRecoveryRequired {
		t.Fatalf("storage recovery boundary lost: %v", d.phase)
	}
}

func TestIdentityPinDialogCloseReopenDiscardsLateReply(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).RunAndReturn(func(context.Context, domain.PeerIdentity) (domain.IdentityPinRemovalPreview, error) {
		close(started)
		<-release
		return preview, nil
	}).Once()
	d.requestPreview(w)
	<-started
	d.close()
	d.open()
	close(release)
	w.sendWG.Wait()
	d.poll()
	if d.preview != nil || d.editor.Text() != "" || d.pending != nil || d.phase != identityPinDraft {
		t.Fatal("reopened dialog accepted old result/input")
	}
}
func TestIdentityPinDialogCancellationAndWindowShutdownDrainWorkers(t *testing.T) {
	d, w, provider, preview := pinDialogFixture(t)
	started := make(chan struct{})
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, preview.Identity).RunAndReturn(func(ctx context.Context, _ domain.PeerIdentity) (domain.IdentityPinRemovalPreview, error) {
		close(started)
		<-ctx.Done()
		return preview, nil
	}).Once()
	d.requestPreview(w)
	<-started
	if !w.drainUIOps(time.Second) {
		t.Fatal("UI shutdown did not cancel request")
	}
	d.poll()
	if d.phase != identityPinUnavailable || d.preview != nil {
		t.Fatal("shutdown accepted cancelled preview")
	}
}
func TestIdentityPinDialogRejectsWrongScopeAndBadCredential(t *testing.T) {
	for _, variant := range []string{"identity", "zero-time", "token", "oversized"} {
		t.Run(variant, func(t *testing.T) {
			_, _, _, preview := pinDialogFixture(t)
			dto := rpc.IdentityPinRemovalPreviewDTO{Identity: preview.Identity, PinnedAt: preview.PinnedAt, Consequence: preview.Consequence, ConsequenceVersion: preview.ConsequenceVersion, ConfirmToken: preview.Token, RemovalAvailable: true}
			switch variant {
			case "identity":
				dto.Identity = domain.PeerIdentity{44}
			case "zero-time":
				dto.PinnedAt = time.Time{}
			case "token":
				dto.ConfirmToken = "no-token"
			case "oversized":
				dto.Consequence = strings.Repeat("a", identityPinUIResponseLimit)
			}
			data, err := json.Marshal(dto)
			if err != nil {
				t.Fatal(err)
			}
			reply := identityPinUIPreviewReply(rpc.CommandResponse{Data: data}, preview.Identity, 1)
			if reply.preview != nil || reply.phase != identityPinInvalidWarning {
				t.Fatal("invalid preview accepted")
			}
		})
	}
}
func TestIdentityPinDialogNeverSerializesOrFormatsCredentials(t *testing.T) {
	d, _, _, preview := pinDialogFixture(t)
	d.preview = &identityPinPreviewState{identity: preview.Identity, token: preview.Token}
	for _, value := range []any{d, *d, d.preview, *d.preview, identityPinUIReply{preview: d.preview}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%x"} {
			if strings.Contains(fmt.Sprintf(format, value), string(preview.Token)) {
				t.Fatal("diagnostic formatter leaked confirmation token")
			}
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("credential-bearing UI state serialized")
		}
	}
}
func TestIdentityPinDialogTerminalOutcomesCannotRestartTransaction(t *testing.T) {
	for _, outcome := range []domain.IdentityPinRemovalOutcome{domain.IdentityPinRemovalCommitted, domain.IdentityPinRemovalNotCommitted, domain.IdentityPinRemovalRecoveryRequired, domain.IdentityPinRemovalIndeterminate, "unknown"} {
		t.Run(string(outcome), func(t *testing.T) {
			d, w, _, preview := pinDialogFixture(t)
			data, err := json.Marshal(rpc.IdentityPinRemovalResultDTO{Identity: preview.Identity, OperationID: "op", Outcome: outcome})
			if err != nil {
				t.Fatal(err)
			}
			reply := identityPinUIConfirmReply(rpc.CommandResponse{Data: data}, preview.Identity, 1)
			expected := identityPinRequestFailed
			switch outcome {
			case domain.IdentityPinRemovalCommitted:
				expected = identityPinLocalPending
			case domain.IdentityPinRemovalNotCommitted:
				expected = identityPinProtectionRetained
			case domain.IdentityPinRemovalRecoveryRequired, domain.IdentityPinRemovalIndeterminate:
				expected = identityPinRecoveryRequired
			}
			if reply.phase != expected {
				t.Fatal("result misclassified")
			}
			d.syncIdentity()
			d.phase = reply.phase
			d.requestConfirm(w)
			if d.canConfirm() {
				t.Fatal("terminal outcome retained confirmation")
			}
			if (expected == identityPinLocalPending || expected == identityPinRecoveryRequired) && d.canPreview() {
				t.Fatal("pending external workflow permitted a new operation")
			}
		})
	}
}
func TestIdentityPinDialogRequiresInProcessPolicy(t *testing.T) {
	d, w, provider, _ := pinDialogFixture(t)
	w.cmdTable = rpc.NewCommandTable()
	w.cmdTable.Register(rpc.CommandInfo{Name: "previewIdentityPinRemovalUI"}, func(rpc.CommandRequest) rpc.CommandResponse {
		t.Error("non-UI transport policy dispatched")
		return rpc.CommandResponse{}
	})
	d.requestPreview(w)
	if d.phase != identityPinUnavailable || d.pending != nil {
		t.Fatal("non-inprocess preview was allowed")
	}
	provider.AssertNotCalled(t, "PreviewIdentityPinRemoval", mock.Anything, mock.Anything)
}

func TestIdentityPinDialogReviewedWarningMatchesCurrentContract(t *testing.T) {
	if identityPinUIWarningVersion != domain.CurrentIdentityPinConsequenceVersion || identityPinUIKnownConsequence != domain.IdentityPinRemovalConsequence {
		t.Fatal("domain consequence changed: review UI translations and warning snapshot explicitly")
	}
	for _, lang := range []string{"en", "ru"} {
		if text := translate(lang, "pin.warning_v2"); text == "" || text == "pin.warning_v2" {
			t.Fatalf("missing reviewed warning for %s", lang)
		}
	}
}
