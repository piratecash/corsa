package netcore

import (
	"errors"
	"testing"
)

// TestSendStatusToErrorMapping verifies that every concrete SendStatus value
// maps to its expected public sentinel. The mapping is the compile-time
// bridge between the internal enum and the Network interface's error
// contract; any new SendStatus value must extend this test and the switch
// in SendStatusToError simultaneously.
func TestSendStatusToErrorMapping(t *testing.T) {
	cases := []struct {
		name   string
		status SendStatus
		want   error // nil means "expect nil"
	}{
		{"ok_maps_to_nil", SendOK, nil},
		{"buffer_full", SendBufferFull, ErrSendBufferFull},
		{"writer_done", SendWriterDone, ErrSendWriterDone},
		{"timeout", SendTimeout, ErrSendTimeout},
		{"chan_closed", SendChanClosed, ErrSendChanClosed},
		{"marshal_error", SendMarshalError, ErrSendMarshalError},
		{"ctx_cancelled", SendCtxCancelled, ErrSendCtxCancelled},
		{"invalid_zero", SendStatusInvalid, ErrSendInvalidStatus},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SendStatusToError(tc.status)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("SendStatusToError(%v) = %v; want nil", tc.status, got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("SendStatusToError(%v) = %v; want errors.Is == %v", tc.status, got, tc.want)
			}
		})
	}
}

// TestSentinelErrorsAreDistinct guards against accidental aliasing between
// the Network sentinels — errors.Is must return false for mismatched pairs,
// otherwise callers that switch on them would collapse separate
// partial-failure cases into one.
func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrUnknownConn,
		ErrSendBufferFull,
		ErrSendWriterDone,
		ErrSendTimeout,
		ErrSendChanClosed,
		ErrSendMarshalError,
		ErrSendCtxCancelled,
		ErrSendInvalidStatus,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			if errors.Is(a, b) {
				t.Fatalf("sentinel aliasing: errors.Is(%v, %v) returned true", a, b)
			}
		}
	}
}
