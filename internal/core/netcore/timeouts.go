package netcore

import "time"

// sessionWriteTimeout is the per-write deadline applied by NetCore.writerLoop
// for outbound (dialled) connections — fire-and-forget frames, request-reply
// handshake and steady-state traffic all share this 3-second window.
// WriteDeadlineFor(Outbound) returns this value; inbound uses connWriteTimeout.
const sessionWriteTimeout = 3 * time.Second

// connWriteTimeout is the per-write deadline applied by NetCore.writerLoop
// for inbound (accepted) connections — historically 30s to give inbound
// peers more time to respond to writes without eviction.
const connWriteTimeout = 30 * time.Second

// syncFlushTimeout is the deadline applied to synchronous send operations
// (SendSync, sendRawSyncBlocking) that wait for the writer goroutine to
// flush data to the socket. A caller waiting longer than this means the
// writer is stuck, which indicates a serious problem upstream.
const syncFlushTimeout = 5 * time.Second
