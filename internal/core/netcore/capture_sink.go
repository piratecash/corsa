package netcore

// CaptureSink is the narrow interface through which NetCore.writerLoop emits
// outbound traffic events. The implementation is provided by the capture
// subsystem; NetCore does not know about rules, files or the Manager.
//
// OnSendAttempt is called after every conn.Write attempt in writerLoop with
// the raw payload and a boolean indicating whether the write succeeded.
// Implementations must be non-blocking — the writerLoop must not stall on
// capture I/O.
type CaptureSink interface {
	// OnSendAttempt is called synchronously from the writer goroutine.
	// data is the raw bytes that were (or would have been) written.
	// ok is true if conn.Write succeeded, false on write error.
	OnSendAttempt(data []byte, ok bool)
}

// SetCaptureSink attaches a capture sink to this connection. Thread-safe:
// the sink is read only by the writer goroutine, and this setter is called
// from the lifecycle layer before any send can race with the read.
//
// Passing nil disables capture for this connection.
func (pc *NetCore) SetCaptureSink(sink CaptureSink) {
	pc.mu.Lock()
	pc.captureSink = sink
	pc.mu.Unlock()
}

// captureSinkLocked returns the current capture sink under read lock.
// Called from writerLoop which does not hold mu — so we take RLock briefly.
func (pc *NetCore) loadCaptureSink() CaptureSink {
	pc.mu.RLock()
	sink := pc.captureSink
	pc.mu.RUnlock()
	return sink
}
