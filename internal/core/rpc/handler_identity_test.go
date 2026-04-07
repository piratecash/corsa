package rpc_test

import (
	"github.com/piratecash/corsa/internal/core/protocol"
	"testing"
)

func TestIdentityFetchIdentities(t *testing.T) {
	identities := []string{"identity-1", "identity-2", "identity-3"}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_identities" {
				return protocol.Frame{
					Type:       "identities_response",
					Identities: identities,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/identities", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "identities_response")

	ids, ok := result["identities"].([]interface{})
	if !ok {
		t.Errorf("expected identities to be array, got %T", result["identities"])
		return
	}
	if len(ids) != len(identities) {
		t.Errorf("expected %d identities, got %d", len(identities), len(ids))
	}
}

func TestIdentityFetchIdentitiesEmpty(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_identities" {
				return protocol.Frame{
					Type:       "identities_response",
					Identities: []string{},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/identities", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "identities_response")
}

func TestIdentityFetchContacts(t *testing.T) {
	contacts := []protocol.ContactFrame{
		{
			Address: "contact-addr-1",
			PubKey:  "pubkey-1",
			BoxKey:  "boxkey-1",
			BoxSig:  "boxsig-1",
		},
		{
			Address: "contact-addr-2",
			PubKey:  "pubkey-2",
			BoxKey:  "boxkey-2",
			BoxSig:  "boxsig-2",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_contacts" {
				return protocol.Frame{
					Type:     "contacts_response",
					Contacts: contacts,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/contacts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "contacts_response")

	contactsResult, ok := result["contacts"].([]interface{})
	if !ok {
		t.Errorf("expected contacts to be array, got %T", result["contacts"])
		return
	}
	if len(contactsResult) != len(contacts) {
		t.Errorf("expected %d contacts, got %d", len(contacts), len(contactsResult))
	}

	// Verify first contact structure
	if len(contactsResult) > 0 {
		contact, ok := contactsResult[0].(map[string]interface{})
		if !ok {
			t.Errorf("expected contact to be map, got %T", contactsResult[0])
			return
		}
		if _, hasAddress := contact["address"]; !hasAddress {
			t.Error("contact missing 'address' field")
		}
		if _, hasPubKey := contact["pubkey"]; !hasPubKey {
			t.Error("contact missing 'pubkey' field")
		}
	}
}

func TestIdentityFetchContactsEmpty(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_contacts" {
				return protocol.Frame{
					Type:     "contacts_response",
					Contacts: []protocol.ContactFrame{},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/contacts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "contacts_response")
}

func TestIdentityFetchTrustedContacts(t *testing.T) {
	contacts := []protocol.ContactFrame{
		{
			Address: "trusted-addr-1",
			PubKey:  "trusted-pubkey-1",
			BoxKey:  "trusted-boxkey-1",
			BoxSig:  "trusted-boxsig-1",
		},
	}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_trusted_contacts" {
				return protocol.Frame{
					Type:     "trusted_contacts_response",
					Contacts: contacts,
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/trusted_contacts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "trusted_contacts_response")

	contactsResult, ok := result["contacts"].([]interface{})
	if !ok {
		t.Errorf("expected contacts to be array, got %T", result["contacts"])
		return
	}
	if len(contactsResult) != len(contacts) {
		t.Errorf("expected %d trusted contacts, got %d", len(contacts), len(contactsResult))
	}
}

func TestIdentityFetchTrustedContactsEmpty(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_trusted_contacts" {
				return protocol.Frame{
					Type:     "trusted_contacts_response",
					Contacts: []protocol.ContactFrame{},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/trusted_contacts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "trusted_contacts_response")
}

func TestIdentityFetchContactsNodeError(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_contacts" {
				return protocol.Frame{
					Type:  "error",
					Error: "contacts not available",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/contacts", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
}

func TestIdentityFetchIdentitiesNodeError(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "fetch_identities" {
				return protocol.Frame{
					Type:  "error",
					Error: "identities not available",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/identity/identities", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "error")
	expectFieldExists(t, result, "error")
}
