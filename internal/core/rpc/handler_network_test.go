package rpc_test

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestNetworkPeers(t *testing.T) {
	expectedPeers := []string{"peer1:8000", "peer2:8000", "peer3:8000"}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "get_peers" {
			return protocol.Frame{
				Type:  "peers_response",
				Peers: expectedPeers,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/peers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "peers_response")

	peers, ok := result["peers"].([]interface{})
	if !ok {
		t.Errorf("expected peers to be array, got %T", result["peers"])
		return
	}
	if len(peers) != len(expectedPeers) {
		t.Errorf("expected %d peers, got %d", len(expectedPeers), len(peers))
	}
}

func TestNetworkPeersEmpty(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "get_peers" {
			return protocol.Frame{
				Type:  "peers_response",
				Peers: []string{},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/peers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "peers_response")
}

func TestNetworkHealth(t *testing.T) {
	healthData := []protocol.PeerHealthFrame{
		{
			Address:         "peer1:8000",
			State:           "active",
			Connected:       true,
			LastConnectedAt: "2026-03-26T10:00:00Z",
		},
	}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_peer_health" {
			return protocol.Frame{
				Type:       "health_response",
				PeerHealth: healthData,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/health", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "health_response")
	expectFieldExists(t, result, "peer_health")
}

func TestNetworkAddPeerValidAddress(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "add_peer" && len(frame.Peers) > 0 {
			return protocol.Frame{
				Type:   "add_peer_response",
				Status: "success",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "newpeer:8000",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "add_peer_response")
	expectField(t, result, "status", "success")
}

func TestNetworkAddPeerMissingAddress(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerMissingBody(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerWhitespaceAddress(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "   ",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerNodeError(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "add_peer" {
			return protocol.Frame{
				Type:  "error",
				Error: "peer already connected",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "existingpeer:8000",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
}

func TestNetworkConnectOnlyEnable(t *testing.T) {
	var captured protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			captured = frame
			return protocol.Frame{Type: "ok", Status: "connect-only pinned to peer:8000"}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{
		"address": "peer:8000",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ok")
	if captured.Type != "connect_only" {
		t.Fatalf("expected connect_only frame, got %q", captured.Type)
	}
	if len(captured.Peers) != 1 || captured.Peers[0] != "peer:8000" {
		t.Errorf("expected Peers=[peer:8000], got %v", captured.Peers)
	}
}

func TestNetworkConnectOnlyDisableEmptyBody(t *testing.T) {
	captured := protocol.Frame{Type: "unset"}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			captured = frame
			return protocol.Frame{Type: "ok", Status: "connect-only disabled"}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	// Empty body must reach the node as a connect_only frame with no Peers —
	// the disable intent.
	code, result := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ok")
	if captured.Type != "connect_only" {
		t.Fatalf("expected connect_only frame, got %q", captured.Type)
	}
	if len(captured.Peers) != 0 {
		t.Errorf("expected empty Peers for disable, got %v", captured.Peers)
	}
}

func TestNetworkConnectOnlyDisableOffToken(t *testing.T) {
	var captured protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			captured = frame
			return protocol.Frame{Type: "ok", Status: "connect-only disabled"}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	// "off" is forwarded verbatim; the node layer interprets the disable token.
	code, result := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{
		"address": "off",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ok")
	if len(captured.Peers) != 1 || captured.Peers[0] != "off" {
		t.Errorf("expected Peers=[off], got %v", captured.Peers)
	}
}

func TestNetworkConnectOnlyWhitespaceDisables(t *testing.T) {
	var captured protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			captured = frame
			return protocol.Frame{Type: "ok"}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	// A whitespace-only address is trimmed to empty → disable (no Peers).
	code, _ := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{
		"address": "   ",
	})

	expectStatusCode(t, code, 200)
	if len(captured.Peers) != 0 {
		t.Errorf("expected empty Peers for whitespace address, got %v", captured.Peers)
	}
}

func TestNetworkConnectOnlyMalformedAddressRejected(t *testing.T) {
	called := false
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			called = true
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	// A present-but-non-string address must be a 400, not a silent disable.
	code, result := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{
		"address": 123,
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
	if called {
		t.Error("node must not be invoked on a malformed enable request")
	}
}

func TestNetworkConnectOnlyMalformedPeersRejected(t *testing.T) {
	called := false
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "connect_only" {
			called = true
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	// Raw wire frame with a non-string peer element must also be a 400.
	code, result := postJSON(t, server, "/rpc/v1/network/connect_only", map[string]interface{}{
		"peers": []interface{}{123},
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
	if called {
		t.Error("node must not be invoked on a malformed peers array")
	}
}

func TestNetworkAddPeerComplexAddress(t *testing.T) {
	tests := []string{
		"192.168.1.1:8000",
		"example.com:64646",
		"[::1]:8000",
		"peer-name-123:9000",
	}

	for _, addr := range tests {
		t.Run(addr, func(t *testing.T) {
			node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
				if frame.Type == "add_peer" && len(frame.Peers) > 0 {
					return protocol.Frame{
						Type:   "add_peer_response",
						Status: "added",
					}
				}
				return protocol.Frame{Type: "ok"}
			})
			server := setupTestServer(t, node, nil)

			code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
				"address": addr,
			})

			expectStatusCode(t, code, 200)
			expectField(t, result, "type", "add_peer_response")
		})
	}
}
