package node

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestRefusedStatusWriteCommitsNothing is the invariant the previous four
// rounds kept violating one exit at a time.
//
// Handling a receipt ends three retries: the SENDER's (deleting the retry
// entry), this node's suppression of the peer's next copy (the dedup key),
// and the peer's own (the ack). All three used to be applied before the
// durable write that makes them true, and each had to be walked back by
// hand when the write failed — the entry never was, so a failed write left
// the row at `sent` with nothing anywhere able to correct it.
//
// Now they are one commit, after the write. A refused write leaves the
// sender still retrying, which is a local recovery that does not depend on
// the peer's receipt queue — capped, expiring after three minutes, and
// gone on their restart.
func TestRefusedStatusWriteCommitsNothing(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	store := &receiptStatusStore{accept: false}
	svc.RegisterMessageStore(store)

	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const target = protocol.MessageID("refused-status-write")
	now := time.Now().UTC()
	svc.sentDMIDs.Add(string(target))
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient, CreatedAt: now,
	}, now, true)
	svc.deliveryMu.Unlock()

	receipt := protocol.DeliveryReceipt{
		MessageID:   target,
		Sender:      recipient,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: now,
	}

	if outcome := svc.storeDeliveryReceipt(receipt); outcome.ackable {
		t.Error("a receipt whose status write failed was declared ackable")
	}
	if svc.receiptAlreadySeen(receipt) {
		t.Error("the dedup key was committed over a refused write; the peer's next copy would be swallowed")
	}
	svc.deliveryMu.RLock()
	_, stillRetrying := svc.awaitingDelivered[target]
	backlog := len(svc.receipts[svc.Address()])
	svc.deliveryMu.RUnlock()
	if !stillRetrying {
		t.Fatal("the sender's retry was stopped by a receipt this node failed to record; nothing will ever correct the row")
	}
	if backlog != 0 {
		t.Errorf("the receipt is in the backlog after a refused write; fetch_delivery_receipts serves that list to the desktop, which would show delivered over a row still reading sent")
	}

	// The database recovers and the message, still being retried, earns
	// another receipt.
	store.mu.Lock()
	store.accept = true
	store.mu.Unlock()
	outcome := svc.storeDeliveryReceipt(receipt)
	svc.WaitBackground()

	if !outcome.ackable {
		t.Error("the second receipt was not accepted, so the peer keeps it forever")
	}
	if !svc.receiptAlreadySeen(receipt) {
		t.Error("a handled receipt left no dedup key, so the peer's next copy is processed again")
	}
	svc.deliveryMu.RLock()
	_, retryingAfter := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if retryingAfter {
		t.Error("a recorded receipt left the sender re-sending a message the peer holds")
	}
	svc.deliveryMu.RLock()
	backlogAfter := len(svc.receipts[svc.Address()])
	svc.deliveryMu.RUnlock()
	if backlogAfter != 1 {
		t.Errorf("the backlog holds %d copies of one receipt; a repeatedly refused write must not stack duplicates", backlogAfter)
	}
	if got := store.updates(); got != 2 {
		t.Errorf("the store saw %d attempts; the second copy was suppressed by a dedup key it should not have had", got)
	}
}

// TestLocalReceiptRpcReportsAFailedWrite: the client clears the unread
// mark on this reply, and nothing else writes that row.
//
// The network re-send of a `seen` goes to the PEER; our own chatlog is
// written here or not at all. Answering `receipt_stored` over a refused
// write leaves a read conversation unread again after the next reload.
func TestLocalReceiptRpcReportsAFailedWrite(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.RegisterMessageStore(&receiptStatusStore{accept: false})

	reply := svc.storeDeliveryReceiptFrame(protocol.Frame{
		Type:        "send_delivery_receipt",
		ID:          "local-seen-1",
		Address:     svc.Address(),
		Recipient:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})
	if reply.Type != "error" || reply.Code != protocol.ErrCodeStoreFailed {
		t.Errorf("reply was %s/%s; a refused chatlog write must not read as success, or the client clears unread over it",
			reply.Type, reply.Code)
	}
	svc.WaitBackground()
}

// TestEveryReceiptAckConsultsTheOutcome ends the class rather than the
// instance.
//
// Four separate places tell someone they may stop keeping a receipt: the
// two ack_delete doors (outbound session and inbound connection), the
// seen_ack that stops a reader's durable retry, and the local RPC reply
// the client clears unread on. Each was written at a different time, the
// gate was added to one of them, and the other three had to be found by
// review.
//
// So the check is on the GUARD, not on a list of blessed functions: every
// call that ends a receipt's life must sit under a condition that reads
// the outcome. A list of names would have passed happily while the guard
// inside one of those names was deleted.
func TestEveryReceiptAckConsultsTheOutcome(t *testing.T) {
	t.Parallel()

	// Every way to tell someone a receipt is finished with.
	enders := map[string]struct{}{
		"enqueueAckDeleteOnSession": {},
		"sendAckDeleteToPeer":       {},
		"sendAckDeleteByID":         {},
		"sendSeenAck":               {},
	}

	// The names that answer "may this receipt be forgotten?".
	//
	// seenReceipts is one of them BECAUSE of the commit rule: the dedup
	// key is written only by finishReceipt, so a branch conditioned on it
	// is already standing on a durable write that landed.
	outcomeReaders := map[string]struct{}{
		"ackable":        {},
		"committed":      {},
		"receiptStoreOK": {},
		"seenReceipts":   {},
	}

	// Call sites that provably end something ELSE, with the reason.
	permitted := map[string]string{
		"handleInboundRelayDeliveryReceipt": "acks the RELAY hop, not the end-to-end receipt",
		"handleAckDeleteFrame":              "the RECEIVING side of an ack: someone else's decision, not ours",
		"drainAckDeleteQueue":               "flushes acks already decided on",
		"enqueueAckDeleteOnSession":         "the helper itself",
		"sendAckDeleteToPeer":               "the helper itself",
		"sendAckDeleteByID":                 "the helper itself",
		"sendSeenAck":                       "the helper itself",
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var offenders []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if _, allowed := permitted[fn.Name.Name]; allowed {
				continue
			}
			var stack []ast.Node
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return false
				}
				stack = append(stack, n)
				if lit, ok := n.(*ast.KeyValueExpr); ok {
					// The local RPC's success reply is an ack too: the
					// client clears the unread mark on it.
					if !repliesReceiptSuccess(lit) || guardedBy(stack, outcomeReaders) {
						return true
					}
					offenders = append(offenders, fmt.Sprintf("%s:%d %s answers %s unguarded",
						name, fset.Position(lit.Pos()).Line, fn.Name.Name, replyType(lit)))
					return true
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, ends := enders[sel.Sel.Name]; !ends {
					return true
				}
				if !endsAReceipt(sel.Sel.Name, call) {
					// The same helpers ack MESSAGES, which have their
					// own durable gate in storeIncomingMessage.
					return true
				}
				if guardedBy(stack, outcomeReaders) {
					return true
				}
				offenders = append(offenders, fmt.Sprintf("%s:%d %s calls %s unguarded",
					name, fset.Position(call.Pos()).Line, fn.Name.Name, sel.Sel.Name))
				return true
			})
		}
	}

	for _, offender := range offenders {
		t.Errorf("%s — a receipt may only be acked once its durable consequence is recorded; "+
			"put the call under a condition that reads the outcome, or list it above with the reason it ends something else", offender)
	}
}

// repliesReceiptSuccess spots the local RPC's success reply, which is an
// ack in every sense that matters: the client clears the unread mark on it
// and nothing else ever writes that row.
func repliesReceiptSuccess(kv *ast.KeyValueExpr) bool {
	key, ok := kv.Key.(*ast.Ident)
	if !ok || key.Name != "Type" {
		return false
	}
	lit, ok := kv.Value.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	return lit.Value == `"receipt_stored"` || lit.Value == `"receipt_known"`
}

func replyType(kv *ast.KeyValueExpr) string {
	if lit, ok := kv.Value.(*ast.BasicLit); ok {
		return lit.Value
	}
	return "a receipt reply"
}

// endsAReceipt tells a receipt ack from a message ack: the ack helpers
// take the kind as a string literal, and sendSeenAck is only ever about a
// receipt.
func endsAReceipt(name string, call *ast.CallExpr) bool {
	if name == "sendSeenAck" {
		return true
	}
	for _, arg := range call.Args {
		if lit, ok := arg.(*ast.BasicLit); ok && lit.Kind == token.STRING && lit.Value == `"receipt"` {
			return true
		}
	}
	return false
}

// guardedBy reports whether the outcome is actually TESTED on the path to
// this node, rather than merely mentioned somewhere above it.
//
// Two shapes count, and nothing else does:
//
//   - an enclosing `if` whose condition is SAFE-WHEN-TRUE holding the node
//     in its body, or SAFE-WHEN-FALSE holding it in its else;
//   - an earlier guard clause in the same block: a SAFE-WHEN-FALSE
//     condition whose body leaves.
//
// The classification is the point. Three versions of this test accepted
// unsafe guards in turn: any mention of the name (so `if !ackable { ack }`
// passed), then any positive-looking read (so `if ackable == false { ack }`
// passed), then any negation (so `if !ackable && other { return }` passed,
// though it leaves the ack reachable whenever `other` is false).
func guardedBy(stack []ast.Node, readers map[string]struct{}) bool {
	for i, node := range stack {
		if i+1 >= len(stack) {
			break
		}
		if branch, ok := node.(*ast.IfStmt); ok && guardsBranch(branch, stack[i+1], readers) {
			return true
		}
		if block, ok := node.(*ast.BlockStmt); ok && guardClauseBefore(block, stack[i+1], readers) {
			return true
		}
	}
	return false
}

// guardsBranch reports whether this if-statement tests the outcome and the
// node sits in the branch that test protects.
func guardsBranch(branch *ast.IfStmt, child ast.Node, readers map[string]struct{}) bool {
	switch test := classifyGuard(branch.Cond, readers); {
	case child == ast.Node(branch.Body):
		return test == safeWhenTrue
	case branch.Else != nil && child == branch.Else:
		return test == safeWhenFalse
	default:
		return false
	}
}

// guardClauseBefore reports whether a statement earlier in this block
// refuses the unsafe case and leaves.
func guardClauseBefore(block *ast.BlockStmt, child ast.Node, readers map[string]struct{}) bool {
	for _, stmt := range block.List {
		if ast.Node(stmt) == child {
			return false
		}
		branch, ok := stmt.(*ast.IfStmt)
		if !ok || branch.Else != nil || !terminates(branch.Body) {
			continue
		}
		if classifyGuard(branch.Cond, readers) == safeWhenFalse {
			return true
		}
	}
	return false
}

// terminates reports whether a block leaves rather than falls through.
func terminates(body *ast.BlockStmt) bool {
	if body == nil || len(body.List) == 0 {
		return false
	}
	switch body.List[len(body.List)-1].(type) {
	case *ast.ReturnStmt, *ast.BranchStmt:
		return true
	default:
		return false
	}
}

type guardKind int

const (
	// notAGuard: the condition says nothing binding about the outcome.
	notAGuard guardKind = iota
	// safeWhenTrue: the condition being TRUE implies the outcome is safe.
	safeWhenTrue
	// safeWhenFalse: the condition being FALSE implies it.
	safeWhenFalse
)

// classifyGuard decides what a condition promises about the outcome.
//
// The asymmetry between the two connectives is the whole reason this is a
// classification rather than a search. `a && ackable` being true implies
// ackable, so it is safe-when-true; but its being FALSE implies nothing,
// so it is not safe-when-false — which is exactly the hole an
// `if !ackable && other { return }` guard clause leaves. `||` promises
// nothing in either direction.
func classifyGuard(cond ast.Expr, readers map[string]struct{}) guardKind {
	switch expr := cond.(type) {
	case *ast.ParenExpr:
		return classifyGuard(expr.X, readers)
	case *ast.UnaryExpr:
		if expr.Op != token.NOT {
			return notAGuard
		}
		return flipGuard(classifyGuard(expr.X, readers))
	case *ast.BinaryExpr:
		switch expr.Op {
		case token.LAND:
			// Safe-when-true survives a conjunction; safe-when-false
			// does not, because either operand can be the false one.
			if classifyGuard(expr.X, readers) == safeWhenTrue || classifyGuard(expr.Y, readers) == safeWhenTrue {
				return safeWhenTrue
			}
			return notAGuard
		case token.EQL, token.NEQ:
			return classifyComparison(expr, readers)
		default:
			return notAGuard
		}
	}
	if readsOutcome(cond, readers) {
		return safeWhenTrue
	}
	return notAGuard
}

// classifyComparison handles `x == false` and its three siblings, which are
// negations written the long way.
func classifyComparison(expr *ast.BinaryExpr, readers map[string]struct{}) guardKind {
	operand, literal := expr.X, expr.Y
	value, ok := boolLiteral(literal)
	if !ok {
		operand, literal = expr.Y, expr.X
		if value, ok = boolLiteral(literal); !ok {
			return notAGuard
		}
	}
	kind := classifyGuard(operand, readers)
	if kind == notAGuard {
		return notAGuard
	}
	if (expr.Op == token.EQL) != value {
		// `x == false` and `x != true` both mean "not x".
		kind = flipGuard(kind)
	}
	return kind
}

func boolLiteral(expr ast.Expr) (value, ok bool) {
	ident, isIdent := expr.(*ast.Ident)
	if !isIdent {
		return false, false
	}
	switch ident.Name {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func flipGuard(kind guardKind) guardKind {
	switch kind {
	case safeWhenTrue:
		return safeWhenFalse
	case safeWhenFalse:
		return safeWhenTrue
	default:
		return notAGuard
	}
}

// readsOutcome reports whether an expression reads one of the names that
// answer "may this receipt be forgotten?".
func readsOutcome(node ast.Node, readers map[string]struct{}) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		if _, reads := readers[ident.Name]; reads {
			found = true
		}
		return true
	})
	return found
}

// TestCommitIsTheSerialisationPoint: the conditions a receipt is admitted
// on are re-checked where it is committed, because everything between the
// two happens outside the lock.
//
// Two things were decided at the door and acted on later: that this
// receipt was not a duplicate, and that its recipient still had a live
// subscriber. Two copies could both pass the first check while the first
// was inside its write and then both append to the backlog; a subscriber
// could disconnect during the write, its teardown find an empty backlog,
// and the commit then create one that no ack_delete will ever drain.
func TestCommitIsTheSerialisationPoint(t *testing.T) {
	t.Parallel()

	t.Run("count is taken after the append", func(t *testing.T) {
		t.Parallel()
		svc := newTestService(t, config.NodeTypeFull)
		svc.RegisterMessageStore(&receiptStatusStore{accept: true})

		reply := svc.storeDeliveryReceiptFrame(protocol.Frame{
			Type:        "send_delivery_receipt",
			ID:          "count-after-commit",
			Address:     svc.Address(),
			Recipient:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Status:      protocol.ReceiptStatusSeen,
			DeliveredAt: time.Now().UTC().Format(time.RFC3339),
		})
		if reply.Type != "receipt_stored" {
			t.Fatalf("reply was %s/%s, want receipt_stored", reply.Type, reply.Code)
		}
		if reply.Count != 1 {
			t.Errorf("reply carries count %d for the receipt it just stored", reply.Count)
		}
		svc.WaitBackground()
	})

	t.Run("concurrent copies commit once", func(t *testing.T) {
		t.Parallel()
		svc := newTestService(t, config.NodeTypeFull)
		store := &receiptStatusStore{accept: true}
		svc.RegisterMessageStore(store)

		recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		const target = protocol.MessageID("concurrent-copies")
		svc.sentDMIDs.Add(string(target))
		receipt := protocol.DeliveryReceipt{
			MessageID:   target,
			Sender:      recipient,
			Recipient:   svc.Address(),
			Status:      protocol.ReceiptStatusDelivered,
			DeliveredAt: time.Now().UTC(),
		}

		entered, release := store.park()
		outcomes := make(chan receiptOutcome, 2)
		go func() { outcomes <- svc.storeDeliveryReceipt(receipt) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			release()
			t.Fatal("the first copy never reached the store")
		}
		// The second copy passes the door's dedup check: the first has
		// not committed its key yet. Wait for it to be INSIDE the store —
		// on a slow machine a sleep would let the first copy finish and
		// commit, and the second would then take the ordinary duplicate
		// fast-path, which is not what this test is about.
		go func() { outcomes <- svc.storeDeliveryReceipt(receipt) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			release()
			t.Fatal("the second copy never reached the store")
		}
		release()

		stored := 0
		for range 2 {
			select {
			case outcome := <-outcomes:
				if outcome.stored {
					stored++
				}
				if !outcome.ackable {
					t.Error("a copy of a receipt that was recorded is not ackable")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a copy never completed")
			}
		}
		if stored != 1 {
			t.Errorf("%d copies reported themselves stored; one receipt is stored once", stored)
		}
		svc.deliveryMu.RLock()
		backlog := len(svc.receipts[svc.Address()])
		svc.deliveryMu.RUnlock()
		if backlog != 1 {
			t.Errorf("the backlog holds %d copies of one receipt", backlog)
		}
		svc.WaitBackground()
	})

	t.Run("a subscriber lost during the write takes its backlog", func(t *testing.T) {
		t.Parallel()
		svc := newTestService(t, config.NodeTypeFull)
		store := &receiptStatusStore{accept: true}
		svc.RegisterMessageStore(store)

		const listener = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		svc.gossipMu.Lock()
		svc.subs[listener] = map[string]*subscriber{"conn-1": {recipient: listener}}
		svc.gossipMu.Unlock()

		receipt := protocol.DeliveryReceipt{
			MessageID:   "subscriber-lost-mid-write",
			Sender:      "cccccccccccccccccccccccccccccccccccccccc",
			Recipient:   listener,
			Status:      protocol.ReceiptStatusDelivered,
			DeliveredAt: time.Now().UTC(),
		}

		// Delivery state this receipt would settle, seeded so the drop
		// can be checked against it.
		svc.deliveryMu.Lock()
		svc.outbound[string(receipt.MessageID)] = outboundDelivery{}
		svc.relayRetry[relayMessageKey(receipt.MessageID)] = relayAttempt{}
		svc.deliveryMu.Unlock()

		entered, release := store.park()
		done := make(chan receiptOutcome, 1)
		go func() { done <- svc.storeDeliveryReceipt(receipt) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			release()
			t.Fatal("the receipt never reached the store")
		}

		// The subscriber goes away while the write runs; its teardown
		// finds nothing to reclaim.
		svc.gossipMu.Lock()
		delete(svc.subs, listener)
		svc.gossipMu.Unlock()
		release()

		var outcome receiptOutcome
		select {
		case outcome = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the receipt never completed")
		}
		svc.deliveryMu.RLock()
		orphaned := len(svc.receipts[listener])
		_, tracked := svc.relayRetry[relayReceiptKey(receipt)]
		_, outboundKept := svc.outbound[string(receipt.MessageID)]
		_, messageRetryKept := svc.relayRetry[relayMessageKey(receipt.MessageID)]
		svc.deliveryMu.RUnlock()
		if !outboundKept || !messageRetryKept {
			// The drop promises to touch nothing. Settling delivery state
			// at the door and dropping afterwards cannot be undone — and
			// the pending counters it publishes have already been drawn.
			t.Error("a receipt dropped at commit still settled the message's delivery state")
		}
		if orphaned != 0 {
			t.Errorf("the commit created a backlog of %d for a recipient with no subscriber; nothing drains or expires it", orphaned)
		}
		if tracked {
			// retryableRelayReceipts only walks receipts that are in
			// s.receipts, so an entry for one that never got there is
			// never revisited and never expires — it just sits in the
			// shared cap, evicting live ones.
			t.Error("a receipt dropped at commit left a relayRetry entry that nothing will ever clean up")
		}
		if outcome.stored {
			t.Error("a receipt dropped at commit reported itself stored")
		}
		svc.WaitBackground()
	})
}

// TestSeenAfterRestartIsAcceptedForADeliveredMessage: the gate that rejects
// receipts for messages we never sent must not reject them for messages we
// sent BEFORE the last restart.
//
// The retry reseed loads rows still at `sent` — correctly, those are the
// ones still owed work — so a message that had reached `delivered` is not
// among them, and the memory-only "we sent this" set comes back empty for
// it. The recipient then opens the conversation days later, their `seen`
// arrives, and it is dropped as unsolicited: the row stays `delivered`, no
// seen_ack goes back, and their durable seen retry never stops.
func TestSeenAfterRestartIsAcceptedForADeliveredMessage(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.RegisterMessageStore(&receiptStatusStore{accept: true})

	const delivered = protocol.MessageID("delivered-before-restart")
	outbox := newEmissionOutbox()
	// Nothing to reseed for retry — the message is already delivered — but
	// the chatlog still holds it as ours.
	outbox.alsoSent = []protocol.MessageID{delivered}
	svc.RegisterDeliveryOutbox(outbox)
	svc.WaitBackground()

	reader := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	outcome := svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   delivered,
		Sender:      reader,
		Recipient:   svc.Address(),
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC(),
	})
	svc.WaitBackground()

	if !outcome.stored {
		t.Error("a genuine seen was rejected as unsolicited after a restart; the row stays at delivered and the reader's seen retry is never acked")
	}
	if !outcome.ackable {
		t.Error("the seen was not ackable, so the reader keeps retrying it")
	}
}

// TestReceiptFromAnyoneElseDoesNotEndTheDelivery: a receipt is a claim by
// the RECIPIENT, and the id alone does not make it one.
//
// The solicited gate only asked "is this message ours?", so any peer that
// learned an id could send `delivered` for it: the retry entry was deleted,
// the recipient's queue advanced to the next message, and the row went to
// `delivered` — while the person it was actually sent to had said nothing
// and may never have received it.
func TestReceiptFromAnyoneElseDoesNotEndTheDelivery(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.RegisterMessageStore(&receiptStatusStore{accept: true})

	const target = protocol.MessageID("addressed-to-one-peer")
	recipient := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	stranger := "cccccccccccccccccccccccccccccccccccccccc"
	now := time.Now().UTC()
	svc.sentDMIDs.Add(string(target))
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(protocol.Envelope{
		ID: target, Topic: "dm", Sender: svc.Address(), Recipient: recipient, CreatedAt: now,
	}, now, true)
	svc.deliveryMu.Unlock()

	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID: target, Sender: stranger, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: now,
	})
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, stillOwed := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if !stillOwed {
		t.Fatal("a stranger's receipt ended the delivery; the message it was addressed to may never arrive")
	}

	// The real recipient's word does end it.
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID: target, Sender: recipient, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: now,
	})
	svc.WaitBackground()

	svc.deliveryMu.RLock()
	_, owedAfter := svc.awaitingDelivered[target]
	svc.deliveryMu.RUnlock()
	if owedAfter {
		t.Error("the recipient's own receipt did not end the delivery")
	}
}

// A relay carries other people's receipts, so the sender binding that
// protects the end node cannot run here: nothing on a transit node is
// addressed to it. Its only defence is that a receipt's identity includes
// who claimed it — otherwise anyone who learned a message id could mark
// the key first and the recipient's real receipt would be forwarded
// nowhere, silently, as a duplicate.
func TestStrangerReceiptDoesNotShadowTheRealOneInTransit(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	const target = protocol.MessageID("in-transit-through-this-node")
	genuine := protocol.DeliveryReceipt{
		MessageID:   target,
		Sender:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Recipient:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
	stranger := genuine
	stranger.Sender = "cccccccccccccccccccccccccccccccccccccccc"

	if svc.markTransitReceiptSeen(stranger) {
		t.Fatal("the first receipt through this node was called a duplicate")
	}
	if svc.isTransitReceiptSeen(genuine) {
		t.Fatal("a stranger's receipt made the recipient's own look already forwarded")
	}
	if svc.markTransitReceiptSeen(genuine) {
		t.Fatal("the recipient's receipt was dropped as a duplicate of a stranger's")
	}
	// The suppression it does owe: the same peer's own re-send.
	if !svc.markTransitReceiptSeen(genuine) {
		t.Error("a re-send of one receipt was forwarded twice")
	}
}
