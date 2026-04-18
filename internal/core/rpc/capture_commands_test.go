package rpc_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// newMockCaptureProvider creates a MockCaptureProvider with default expectations
// for all methods. Each expectation is marked Maybe() so that tests only need
// to assert the calls they care about.
func newMockCaptureProvider(t *testing.T) *rpcmocks.MockCaptureProvider {
	t.Helper()
	m := rpcmocks.NewMockCaptureProvider(t)
	m.On("StartCaptureByConnIDs", mock.Anything, mock.Anything).Return(json.RawMessage(`{"started":[]}`), nil).Maybe()
	m.On("StartCaptureByIPs", mock.Anything, mock.Anything).Return(json.RawMessage(`{"started":[]}`), nil).Maybe()
	m.On("StartCaptureAll", mock.Anything).Return(json.RawMessage(`{"started":[]}`), nil).Maybe()
	m.On("StopCaptureByConnIDs", mock.Anything).Return(json.RawMessage(`{"stopped":[]}`), nil).Maybe()
	m.On("StopCaptureByIPs", mock.Anything).Return(json.RawMessage(`{"stopped":[]}`), nil).Maybe()
	m.On("StopCaptureAll").Return(json.RawMessage(`{"stopped":[]}`), nil).Maybe()
	return m
}

func TestCaptureCommands_StartByConnIDs(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "recordPeerTrafficByConnID",
		Args: map[string]interface{}{"conn_ids": "41,42"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StartCaptureByConnIDs", mock.Anything, mock.Anything)
}

func TestCaptureCommands_StartByConnIDs_MissingArg(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "recordPeerTrafficByConnID",
		Args: map[string]interface{}{},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error for missing conn_ids")
	}
	provider.AssertNotCalled(t, "StartCaptureByConnIDs", mock.Anything, mock.Anything)
}

func TestCaptureCommands_StartByIPs(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "recordPeerTrafficByIP",
		Args: map[string]interface{}{"ips": "203.0.113.10"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StartCaptureByIPs", mock.Anything, mock.Anything)
}

func TestCaptureCommands_StartAll(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "recordAllPeerTraffic",
		Args: map[string]interface{}{},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StartCaptureAll", mock.Anything)
}

func TestCaptureCommands_StopByConnIDs(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"conn_ids": "41,42"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StopCaptureByConnIDs", mock.Anything)
}

func TestCaptureCommands_StopByIPs(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"ips": "203.0.113.10"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StopCaptureByIPs", mock.Anything)
}

func TestCaptureCommands_StopAll(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"scope": "all"},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	provider.AssertCalled(t, "StopCaptureAll")
}

func TestCaptureCommands_StopNoArgs(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error when stop called without arguments")
	}
}

func TestCaptureCommands_StopInvalidScope(t *testing.T) {
	table := rpc.NewCommandTable()
	provider := newMockCaptureProvider(t)
	rpc.RegisterCaptureCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "stopPeerTrafficRecording",
		Args: map[string]interface{}{"scope": "partial"},
	})

	if resp.Error == nil {
		t.Fatal("expected validation error for invalid scope")
	}
}

func TestCaptureCommands_NilProvider(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterCaptureCommands(table, nil)

	resp := table.Execute(rpc.CommandRequest{
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
			got, err := rpc.ParseUint64CSVForTest(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUint64CSV(%q) err=%v, wantErr=%v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && len(got) != len(tt.want) {
				t.Errorf("parseUint64CSV(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
