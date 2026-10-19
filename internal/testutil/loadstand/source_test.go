package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func protocolClient(t *testing.T, status int, body string) *RPCClient {
	t.Helper()
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != credentials.username || password != credentials.passwordText() || r.URL.Path != "/rpc/v1/exec" || r.Method != http.MethodPost {
			t.Error("invalid authenticated protocol request")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}
func TestProcessSourceBanFamiliesFailClosed(t *testing.T) {
	now := time.Now().UTC()
	baseline := map[string]any{"banned_ips": []any{}, "count": 0, "version": 2, "ready": true, "fresh": true, "started_at": now, "observation_started_at": now, "generated_at": now, "local_bans": []any{}, "local_ip_bans": []any{}, "peer_bans": []any{}, "remote_peer_bans": []any{}, "remote_ip_bans": []any{}, "loopback_identities_scored": 0, "loopback_identities_banned": 0, "loopback_identity_penalties_available": true}
	for _, missing := range []string{"", "banned_ips", "count", "version", "ready", "fresh", "local_bans", "local_ip_bans", "peer_bans", "remote_peer_bans", "remote_ip_bans", "loopback_identities_scored", "loopback_identities_banned", "loopback_identity_penalties_available"} {
		t.Run("missing_"+missing, func(t *testing.T) {
			copy := make(map[string]any, len(baseline))
			for key, value := range baseline {
				copy[key] = value
			}
			if missing != "" {
				delete(copy, missing)
			}
			payload, err := json.Marshal(copy)
			if err != nil {
				t.Fatal(err)
			}
			source, constructErr := NewProcessSourceWithClock(protocolClient(t, 200, string(payload)), adapterClock{now})
			if constructErr != nil {
				t.Fatal(constructErr)
			}
			_, err = source.BanObservation(t.Context())
			if missing == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrFamilyUnavailable) {
				t.Fatalf("missing must not be zero: %v", err)
			}
		})
	}
	for _, stale := range []string{"ready", "fresh", "loopback_identity_penalties_available"} {
		baseline[stale] = false
		payload, err := json.Marshal(baseline)
		if err != nil {
			t.Fatal(err)
		}
		source, constructErr := NewProcessSourceWithClock(protocolClient(t, 200, string(payload)), adapterClock{now})
		if constructErr != nil {
			t.Fatal(constructErr)
		}
		if _, err := source.BanObservation(t.Context()); !errors.Is(err, ErrFamilyUnavailable) {
			t.Fatal(err)
		}
		baseline[stale] = true
	}
}
func TestProcessSourceRuntimeMissingIsUnavailable(t *testing.T) {
	_, err := NewProcessSource(protocolClient(t, 200, `{"version":1,"started_at":"2026-10-18T00:00:00Z","read_at":"2026-10-18T00:00:01Z"}`)).Runtime(t.Context())
	if !errors.Is(err, ErrFamilyUnavailable) {
		t.Fatal(err)
	}
	for _, status := range []int{404, 503} {
		_, err := NewProcessSource(protocolClient(t, status, `{"password":"must never be echoed"}`)).Runtime(t.Context())
		if !errors.Is(err, ErrFamilyUnavailable) {
			t.Fatal(err)
		}
	}
}
func TestRPCFailuresNeverEchoRemoteBodies(t *testing.T) {
	for _, status := range []int{200, 401, 403, 500, 302} {
		err := protocolClient(t, status, `{"secret":"REMOTE_SECRET",invalid`).Call(t.Context(), "getNodeStatus", nil, new(any))
		if err == nil || strings.Contains(err.Error(), "REMOTE_SECRET") {
			t.Fatalf("status %d: unsafe error", status)
		}
	}
	client := protocolClient(t, 200, strings.Repeat("x", (1<<20)+1))
	if err := client.Call(t.Context(), "x", nil, new(any)); !errors.Is(err, ErrInvalidSample) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.Call(ctx, "x", nil, new(any)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRPCClientClosePermanentlyFencesPreviousProcessHandle(t *testing.T) {
	client := protocolClient(t, 200, `{}`)
	client.Close()
	if err := client.Call(t.Context(), "getNodeStatus", nil, new(any)); !errors.Is(err, ErrRPCClosed) {
		t.Fatalf("old handle reopened after Close: %v", err)
	}
}
func TestRPCClientCloseAbortsInFlightRead(t *testing.T) {
	entered := make(chan struct{})
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress(strings.TrimPrefix(server.URL, "http://")), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	finished := make(chan error, 1)
	go func() { finished <- client.Call(t.Context(), "getNodeStatus", nil, new(any)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("read did not enter server")
	}
	client.Close()
	select {
	case err := <-finished:
		if !errors.Is(err, ErrRPCClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close left in-flight read alive")
	}
}
