package node

import "testing"

// TestShouldAckOnStoreResult locks in the ack-policy decision table
// that drives the push_message handler in both peer_management.go
// (outbound session path) and service.go (inbound conn path).
//
// The table covers the four (stored, errCode) outcomes that
// storeIncomingMessage can return:
//
//   - stored=true,  errCode="":         newly stored — ack so the sender
//     can drop the message from its outbound queue.
//   - stored=false, errCode="":         duplicate — ack so a re-sender
//     stops looping the same id forever (this is the dedup-storm fix).
//   - stored=false, errCode!="":        real failure — DO NOT ack so the
//     sender retries after the underlying cause is fixed (unknown
//     sender key triggers a sync upstream, etc.).
//   - stored=true,  errCode!="":        impossible-by-contract pair, but
//     the predicate must still ack on stored=true regardless of the
//     residual errCode so a future contract change cannot silently
//     suppress acks for stored messages.
func TestShouldAckOnStoreResult(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		stored  bool
		errCode string
		wantAck bool
	}{
		{name: "stored_no_error", stored: true, errCode: "", wantAck: true},
		{name: "duplicate_no_error", stored: false, errCode: "", wantAck: true},
		{name: "unknown_sender_key", stored: false, errCode: "unknown-sender-key", wantAck: false},
		{name: "timestamp_out_of_range", stored: false, errCode: "message-timestamp-out-of-range", wantAck: false},
		{name: "stored_with_residual_errcode", stored: true, errCode: "some-warning-code", wantAck: true},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := shouldAckOnStoreResult(c.stored, c.errCode)
			if got != c.wantAck {
				t.Fatalf("shouldAckOnStoreResult(stored=%v, errCode=%q) = %v, want %v",
					c.stored, c.errCode, got, c.wantAck)
			}
		})
	}
}

// TestARefusedReDeliveryReleasesThePreviousHop is the transport half of the
// deletion contract, pinned where the decision is actually made.
//
// A message the user deleted is refused by the store adapter, which reports it
// as a DUPLICATE — we had it and destroyed it, which is exactly what a
// duplicate is. This test is here to say what that reporting BUYS: the
// duplicate branch acks, `ack_delete` goes back to the hop that pushed the
// frame, and that hop releases the message from its backlog. Answering is what
// stops a replay at its source; remembering the id would only stop it here, and
// remembering is the thing a deletion may not do.
//
// If this table ever stopped acking duplicates, a deleted message would be
// re-pushed by every hop that still held it, on every reconnect, for as long as
// the sender's reseed horizon — and nothing in the service layer would notice.
func TestARefusedReDeliveryReleasesThePreviousHop(t *testing.T) {
	t.Parallel()

	// What MessageStoreAdapter.StoreMessage returns for a deleted id, as the
	// node sees it after storeIncomingMessage maps StoreDuplicate.
	const (
		storedByUs = false
		noFailure  = ""
	)
	if !shouldAckOnStoreResult(storedByUs, noFailure) {
		t.Fatal("a refused re-delivery of a deleted message is not acked: the previous hop keeps it and pushes it again")
	}
}
