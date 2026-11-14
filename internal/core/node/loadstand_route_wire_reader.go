package node

import (
	"bufio"
	"io"
	"time"
)

// routeWireReader belongs to one plaintext reader goroutine. A positive fill
// takes the timestamp from before the underlying call, since the first byte
// can arrive any time while the call blocks. Readahead in bufio keeps that
// timestamp when its bytes are consumed by later frame reads.
type routeWireReader struct {
	source    io.Reader
	observer  *LoadstandRouteWireObservation
	lastFill  time.Duration
	firstFill time.Duration
	active    bool
	hasBytes  bool
}

// trackPlaintextReader is used only for a newly constructed TLS application
// reader. If bytes already reached bufio their acquisition time is unknown.
func (o *LoadstandRouteWireObservation) trackPlaintextReader(reader *bufio.Reader, source io.Reader) *routeWireReader {
	if o == nil || o.window == nil {
		return nil
	}
	if reader.Buffered() != 0 {
		o.fail()
		return nil
	}
	tracker := &routeWireReader{source: source, observer: o}
	reader.Reset(tracker)
	return tracker
}

func (r *routeWireReader) Read(p []byte) (int, error) {
	o := r.observer
	o.mu.Lock()
	err := o.window.advance()
	at := o.window.last
	if err != nil || o.sealed || !r.active {
		o.incomplete = true
	}
	o.mu.Unlock()

	n, readErr := r.source.Read(p)
	if n > 0 {
		r.lastFill = at
		if !r.hasBytes {
			r.firstFill = at
			r.hasBytes = true
		}
	}
	return n, readErr
}

func (r *routeWireReader) beginFrame(reader *bufio.Reader) {
	if r == nil {
		return
	}
	r.active = true
	r.hasBytes = reader.Buffered() != 0
	r.firstFill = 0
	if r.hasBytes {
		r.firstFill = r.lastFill
	}
}

func (r *routeWireReader) endFrame() (time.Duration, bool) {
	if r == nil {
		return 0, false
	}
	r.active = false
	return r.firstFill, r.hasBytes
}

func (r *routeWireReader) finish() {
	if r != nil && r.active {
		r.observer.fail()
	}
}
