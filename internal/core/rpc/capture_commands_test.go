package rpc

import (
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------
// Stub CaptureProvider for tests
// ---------------------------------------------------------------------------

type stubCaptureProvider struct {
	startConnIDsCalled bool
	startIPsCalled     bool
	startAllCalled     bool
	stopConnIDsCalled  bool
	stopIPsCalled      bool
	stopAllCalled      bool
}

func (s *stubCaptureProvider) StartCaptureByConnIDs(_ []uint64, _ string) (json.RawMessage, error) {
	s.startConnIDsCalled = true
	return json.RawMessage(`{"started":[]}`), nil
}

func (s *stubCaptureProvider) StartCaptureByIPs(_ []string, _ string) (json.RawMessage, error) {
	s.startIPsCalled = true
	return json.RawMessage(`{"started":[]}`), nil
}

func (s *stubCaptureProvider) StartCaptureAll(_ string) (json.RawMessage, error) {
	s.startAllCalled = true
	return json.RawMessage(`{"started":[]}`), nil
}

func (s *stubCaptureProvider) StopCaptureByConnIDs(_ []uint64) (json.RawMessage, error) {
	s.stopConnIDsCalled = true
	return json.RawMessage(`{"stopped":[]}`), nil
}

func (s *stubCaptureProvider) StopCaptureByIPs(_ []string) (json.RawMessage, error) {
	s.stopIPsCalled = true
	return json.RawMessage(`{"stopped":[]}`), nil
}

func (s *stubCaptureProvider) StopCaptureAll() (json.RawMessage, error) {
	s.stopAllCalled = true
	return json.RawMessage(`{"stopped":[]}`), nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestCaptureCommands_StartByConnIDs(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "recordPeerTrafficByConnID",
		Args: map[string]interface{}{"conn_ids": "41,42"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.startConnIDsCalled {
		t.Fatal("expected StartCaptureByConnIDs to be called")
	}
}

func TestCaptureCommands_StartByConnIDs_MissingArg(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "recordPeerTrafficByConnID",
		Args: map[string]interface{}{},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error for missing conn_ids")
	}
}

func TestCaptureCommands_StartByIPs(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "recordPeerTrafficByIP",
		Args: map[string]interface{}{"ips": "203.0.113.10"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.startIPsCalled {
		t.Fatal("expected StartCaptureByIPs to be called")
	}
}

func TestCaptureCommands_StartAll(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "recordAllPeerTraffic",
		Args: map[string]interface{}{},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.startAllCalled {
		t.Fatal("expected StartCaptureAll to be called")
	}
}

func TestCaptureCommands_StopByConnIDs(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"conn_ids": "41,42"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.stopConnIDsCalled {
		t.Fatal("expected StopCaptureByConnIDs to be called")
	}
}

func TestCaptureCommands_StopByIPs(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"ips": "203.0.113.10"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.stopIPsCalled {
		t.Fatal("expected StopCaptureByIPs to be called")
	}
}

func TestCaptureCommands_StopAll(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"scope": "all"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if !provider.stopAllCalled {
		t.Fatal("expected StopCaptureAll to be called")
	}
}

func TestCaptureCommands_StopNoArgs(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error when stop called without arguments")
	}
}

func TestCaptureCommands_StopInvalidScope(t *testing.T) {
	table := NewCommandTable()
	provider := &stubCaptureProvider{}
	RegisterCaptureCommands(table, provider)

	resp := table.Execute(CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"scope": "partial"},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error for invalid scope")
	}
}

func TestCaptureCommands_NilProvider(t *testing.T) {
	table := NewCommandTable()
	RegisterCaptureCommands(table, nil)

	resp := table.Execute(CommandRequest{
		Name: "recordAllPeerTraffic",
		Args: map[string]interface{}{},
	})

	if resp.Error == nil {
		t.Fatal("expected error when provider is nil")
	}
}

func TestParseUint64CSV(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []uint64
		wantErr bool
	}{
		{"single", "42", []uint64{42}, false},
		{"multiple", "1,2,3", []uint64{1, 2, 3}, false},
		{"spaces", " 1 , 2 , 3 ", []uint64{1, 2, 3}, false},
		{"trailing comma", "1,2,", []uint64{1, 2}, false},
		{"empty", "", nil, true},
		{"all empty", ",,", nil, true},
		{"invalid", "abc", nil, true},
		{"mixed", "1,abc,3", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUint64CSV(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUint64CSV(%q) err=%v, wantErr=%v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && len(got) != len(tt.want) {
				t.Errorf("parseUint64CSV(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
