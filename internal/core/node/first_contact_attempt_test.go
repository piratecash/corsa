//go:build s6_first_contact_probe

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestFirstContactEventsJoinSocketsToTheAttempt(t *testing.T) {
	var output bytes.Buffer
	saved := log.Logger
	log.Logger = zerolog.New(&output)
	t.Cleanup(func() { log.Logger = saved })

	dial := func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
		return local, nil
	}
	// Unmanaged dials must stay outside the diagnostic sequence.
	_, timing, err := openFirstContactSocket(context.Background(), dial)
	if err != nil {
		t.Fatal(err)
	}
	timing.record(99, "opened")

	ctx := beginFirstContactAttempt(context.Background())
	for _, connID := range []domain.ConnID{101, 102} {
		_, timing, err := openFirstContactSocket(ctx, dial)
		if err != nil {
			t.Fatal(err)
		}
		timing.record(connID, "opened")
	}
	recordFirstContactSetup(ctx, 102, nil)
	firstContactFromContext(ctx).recordPublication(102, "published", 30, false)

	ctx = beginFirstContactAttempt(context.Background())
	recordFirstContactSetup(ctx, 0, context.Canceled)

	type event struct {
		Name    string `json:"message"`
		Attempt uint64 `json:"attempt_id"`
		Conn    uint64 `json:"conn_id"`
		Elapsed int64  `json:"elapsed_ns"`
		Outcome string `json:"outcome"`
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	wantNames := []string{"attempt", "transport", "transport", "setup", "publication", "attempt", "setup"}
	wantConns := []uint64{0, 101, 102, 102, 102, 0, 0}
	wantOutcomes := []string{"", "opened", "opened", "ready_for_cm", "published", "", "setup_failed"}
	if len(lines) != len(wantNames) {
		t.Fatalf("got %d events, want %d", len(lines), len(wantNames))
	}
	var first uint64
	for i, line := range lines {
		var got event
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = got.Attempt
		}
		wantAttempt := first
		if i >= 5 {
			wantAttempt++
		}
		if got.Attempt == 0 || got.Attempt != wantAttempt || got.Conn != wantConns[i] ||
			got.Name != "s6_first_contact_"+wantNames[i] || got.Outcome != wantOutcomes[i] || got.Elapsed < 0 {
			t.Fatalf("event %d: %+v", i, got)
		}
	}
}

func TestFirstContactTLSTimeoutLogsPhaseAndDialOutcome(t *testing.T) {
	var output bytes.Buffer
	saved := log.Logger
	log.Logger = zerolog.New(&output)
	t.Cleanup(func() { log.Logger = saved })

	const connID = 101
	const budget = 5 * time.Second
	synctest.Test(t, func(t *testing.T) {
		local, err := sessionv2.NewLocal(generateProbeIdentity(t), "gazeta-devnet")
		if err != nil {
			t.Fatal(err)
		}
		wire, peer := net.Pipe()
		t.Cleanup(func() { _ = wire.Close(); _ = peer.Close() })
		ctx := beginFirstContactAttempt(context.Background())
		_, err = dialSessionV2(ctx, connID, wire, local, protocol.Frame{Version: 31}, sessionv2.AnyPeer(),
			sessionv2.Timeouts{TLS: budget, Proof: time.Second})
		if !errors.Is(err, sessionv2.ErrNotV2) {
			t.Fatalf("dial error = %v, want ErrNotV2", err)
		}
	})

	type event struct {
		Message   string `json:"message"`
		AttemptID uint64 `json:"attempt_id"`
		ConnID    uint64 `json:"conn_id"`
		Phase     string `json:"phase"`
		Budget    int64  `json:"budget_ns"`
		Elapsed   int64  `json:"elapsed_ns"`
		Phases    int    `json:"phases"`
		Outcome   string `json:"outcome"`
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 3 {
		t.Fatalf("got %d events, want attempt, TLS phase and dial result", len(lines))
	}
	var events [3]event
	for i, line := range lines {
		if err := json.Unmarshal(line, &events[i]); err != nil {
			t.Fatal(err)
		}
	}
	start, phase, result := events[0], events[1], events[2]
	if start.Message != "s6_first_contact_attempt" || start.AttemptID == 0 || start.ConnID != 0 {
		t.Fatalf("unexpected attempt event: %+v", start)
	}
	if phase != (event{
		Message: "s6_first_contact_v2_phase", AttemptID: start.AttemptID, ConnID: connID,
		Phase: "tls", Budget: budget.Nanoseconds(), Elapsed: budget.Nanoseconds(), Outcome: "timeout",
	}) {
		t.Fatalf("unexpected TLS phase event: %+v", phase)
	}
	if result != (event{
		Message: "s6_first_contact_v2_result", AttemptID: start.AttemptID, ConnID: connID, Phases: 1, Outcome: "failed",
	}) {
		t.Fatalf("unexpected dial result: %+v", result)
	}
}
