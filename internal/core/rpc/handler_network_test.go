package rpc_test

import (
	"corsa/internal/core/protocol"
	"testing"
)

func TestNetworkPeers(t *testing.T) {
	expectedPeers := []string{"peer1:8000", "peer2:8000", "peer3:8000"}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "get_peers" {
				return protocol.Frame{
					Type:  "peers_response",
					Peers: expectedPeers,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/peers", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "peers_response")

	// Check peers array
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
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "get_peers" {
				return protocol.Frame{
					Type:  "peers_response",
					Peers: []string{},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
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
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_peer_health" {
				return protocol.Frame{
					Type:       "health_response",
					PeerHealth: healthData,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/health", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "health_response")
	expectFieldExists(t, result, "peer_health")
}

func TestNetworkAddPeerValidAddress(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "add_peer" && len(frame.Peers) > 0 {
				return protocol.Frame{
					Type:   "add_peer_response",
					Status: "success",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "newpeer:8000",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "add_peer_response")
	expectField(t, result, "status", "success")
}

func TestNetworkAddPeerMissingAddress(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerMissingBody(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerWhitespaceAddress(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "   ",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestNetworkAddPeerNodeError(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "add_peer" {
				return protocol.Frame{
					Type:  "error",
					Error: "peer already connected",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
		"address": "existingpeer:8000",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
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
			node := &mockNodeProvider{
				handleFunc: func(frame protocol.Frame) protocol.Frame {
					if frame.Type == "add_peer" && len(frame.Peers) > 0 {
						return protocol.Frame{
							Type:   "add_peer_response",
							Status: "added",
						}
					}
					return protocol.Frame{Type: "ok"}
				},
			}
			server := setupTestServer(t, node, nil)

			code, result := postJSON(t, server, "/rpc/v1/network/add_peer", map[string]interface{}{
				"address": addr,
			})

			expectStatusCode(t, code, 200)
			expectField(t, result, "type", "add_peer_response")
		})
	}
}
