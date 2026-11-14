package netcore

import "time"

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

// loadstandWriteStartSink is used only by explicitly measured connections.
// It brackets the original socket attempt with the existing completion tap.
type loadstandWriteStartSink interface {
	OnLoadstandWriteStart(data []byte)
}

// loadstandQueueSink may copy a selected frame to give its exact queue item
// a private identity. The ordinary queue stores no extra metadata.
type loadstandQueueSink interface {
	PrepareLoadstandQueue(data []byte, first time.Duration) []byte
	DiscardLoadstandQueue(data []byte, admitted bool)
}

func (pc *NetCore) prepareLoadstandQueue(item *sendItem, first time.Duration) loadstandQueueSink {
	if pc.loadstandSessionID == nil || item.owned != nil {
		return nil
	}
	sink, ok := pc.loadCaptureSink().(loadstandQueueSink)
	if !ok {
		return nil
	}
	item.data = sink.PrepareLoadstandQueue(item.data, first)
	return sink
}

func (pc *NetCore) discardLoadstandQueue(item sendItem) {
	if pc.loadstandSessionID != nil && item.owned == nil {
		if sink, ok := pc.loadCaptureSink().(loadstandQueueSink); ok {
			sink.DiscardLoadstandQueue(item.data, true)
		}
	}
}

// SetCaptureSink attaches a capture sink to this connection. Thread-safe:
// the lifecycle layer calls this before it exposes a connection to senders.
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
