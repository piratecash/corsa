package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ackingPeer is a peer that can sign ack_delete frames and whose
// connection this node considers authenticated — everything the ack door
// checks before it deletes anything.
type ackingPeer struct {
	id     *identity.Identity
	connID domain.ConnID
}

func newAckingPeer(t *testing.T, svc *Service) ackingPeer {
	t.Helper()
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	conn := newSimpleMockConn(t)
	core := &netcore.NetCore{}
	core.SetAuth(&connauth.State{
		Verified: true,
		Hello: protocol.Frame{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
		},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: core})
	connID, _ := svc.connIDFor(conn)
	return ackingPeer{id: peer, connID: connID}
}

// ack builds the frame this peer would send, signed the way its own
// build path signs: v2 when it names the receipt's author, v1 when not.
func (p ackingPeer) ack(receiptSender string, messageID protocol.MessageID, status string) protocol.Frame {
	frame := protocol.Frame{
		Type:          "ack_delete",
		Address:       p.id.Address,
		AckType:       "receipt",
		ID:            string(messageID),
		Status:        status,
		ReceiptSender: receiptSender,
	}
	frame.Signature = identity.SignPayload(p.id, ackDeletePayloadForFrame(frame))
	return frame
}

func receiptFrom(sender, recipient string, id protocol.MessageID) protocol.DeliveryReceipt {
	return protocol.DeliveryReceipt{
		MessageID:   id,
		Sender:      sender,
		Recipient:   recipient,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
}

func seedReceiptBacklog(svc *Service, receipts ...protocol.DeliveryReceipt) {
	svc.deliveryMu.Lock()
	for _, receipt := range receipts {
		svc.receipts[receipt.Recipient] = append(svc.receipts[receipt.Recipient], receipt)
		svc.relayRetry[relayReceiptKey(receipt)] = relayAttempt{FirstSeen: time.Now().UTC()}
	}
	svc.deliveryMu.Unlock()
}

func heldReceipts(svc *Service, recipient string) []protocol.DeliveryReceipt {
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	return append([]protocol.DeliveryReceipt(nil), svc.receipts[recipient]...)
}

// The forgery is rejected at the end node — but the ack it answers with
// is legitimate traffic, and used to name only recipient+id+status. A
// relay applied it to every receipt that looked like that, so the real
// recipient's receipt and its retry entry were deleted on the way back
// and the message stayed at `sent` until the message itself was re-sent.
func TestAckForAForgedReceiptLeavesTheRealOneStanding(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peer := newAckingPeer(t, svc)

	const target = protocol.MessageID("waiting-for-confirmation")
	genuine := receiptFrom("the-actual-recipient", peer.id.Address, target)
	forged := receiptFrom("someone-who-knew-the-id", peer.id.Address, target)
	seedReceiptBacklog(svc, genuine, forged)

	reply, ok := svc.handleAckDeleteFrame(peer.connID, peer.ack(forged.Sender, target, forged.Status))
	if !ok {
		t.Fatalf("the ack was refused: %+v", reply)
	}
	if reply.Count != 1 {
		t.Fatalf("ack removed %d receipts, want exactly the one it named", reply.Count)
	}

	left := heldReceipts(svc, peer.id.Address)
	if len(left) != 1 || left[0].Sender != genuine.Sender {
		t.Fatalf("backlog after the ack = %+v, want only the genuine receipt", left)
	}
	svc.deliveryMu.RLock()
	_, retryKept := svc.relayRetry[relayReceiptKey(genuine)]
	_, retryDropped := svc.relayRetry[relayReceiptKey(forged)]
	svc.deliveryMu.RUnlock()
	if !retryKept {
		t.Error("the genuine receipt's retry entry was deleted by an ack for a different receipt")
	}
	if retryDropped {
		t.Error("the acked receipt's own retry entry survived")
	}
}

// A peer below ProtocolVersionReceiptSenderAck cannot say which one it
// holds. One candidate is unambiguous and goes; several are all kept,
// because a duplicate push the peer discards is cheaper than deleting
// the receipt somebody is still waiting for.
func TestLegacyAckDeletesOnlyWhenThereIsNothingToConfuseItWith(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peer := newAckingPeer(t, svc)

	const contested = protocol.MessageID("two-claims-one-message")
	genuine := receiptFrom("the-actual-recipient", peer.id.Address, contested)
	forged := receiptFrom("someone-who-knew-the-id", peer.id.Address, contested)
	const alone = protocol.MessageID("one-claim-only")
	single := receiptFrom("the-actual-recipient", peer.id.Address, alone)
	seedReceiptBacklog(svc, genuine, forged, single)

	reply, ok := svc.handleAckDeleteFrame(peer.connID, peer.ack("", contested, genuine.Status))
	if !ok {
		t.Fatalf("the legacy ack was refused: %+v", reply)
	}
	if reply.Count != 0 {
		t.Fatalf("an ack that cannot name its receipt removed %d of 2 candidates", reply.Count)
	}

	reply, ok = svc.handleAckDeleteFrame(peer.connID, peer.ack("", alone, single.Status))
	if !ok {
		t.Fatalf("the legacy ack was refused: %+v", reply)
	}
	if reply.Count != 1 {
		t.Fatalf("an unambiguous legacy ack removed %d receipts, want 1", reply.Count)
	}
	if len(heldReceipts(svc, peer.id.Address)) != 2 {
		t.Errorf("backlog = %+v, want both contested receipts kept", heldReceipts(svc, peer.id.Address))
	}
}

// The author is INSIDE the signature, so the two payload shapes must be
// told apart by the frame and not by either side's guess about the
// other's version — a verifier that rebuilt the wrong shape would score
// an honest ack as a forgery and ban the peer for it.
func TestAckSignatureShapeFollowsTheFrameItSigns(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	peer := newAckingPeer(t, svc)

	const target = protocol.MessageID("signature-shape")
	held := receiptFrom("the-actual-recipient", peer.id.Address, target)

	seedReceiptBacklog(svc, held)
	if reply, ok := svc.handleAckDeleteFrame(peer.connID, peer.ack(held.Sender, target, held.Status)); !ok || reply.Count != 1 {
		t.Fatalf("a v2-signed ack was not accepted: ok=%v reply=%+v", ok, reply)
	}

	seedReceiptBacklog(svc, held)
	if reply, ok := svc.handleAckDeleteFrame(peer.connID, peer.ack("", target, held.Status)); !ok || reply.Count != 1 {
		t.Fatalf("a legacy-signed ack was not accepted: ok=%v reply=%+v", ok, reply)
	}

	// And a frame whose author was substituted after signing is neither.
	seedReceiptBacklog(svc, held)
	tampered := peer.ack(held.Sender, target, held.Status)
	tampered.ReceiptSender = "someone-else-entirely"
	if _, ok := svc.handleAckDeleteFrame(peer.connID, tampered); ok {
		t.Error("an ack whose named author was swapped after signing was accepted")
	}
}

// Below the floor the frame must be byte-identical to what this node
// sent before the field existed: an older verifier rebuilds the v1
// payload, and a signature it cannot reproduce is scored as forgery.
func TestAckToAnOlderPeerCarriesNothingNew(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	receipt := receiptFrom("the-actual-recipient", svc.Address(), "older-peer")

	old := svc.buildAckDeleteFrameFor(ackDeleteForReceipt(receipt), false)
	if old.ReceiptSender != "" {
		t.Fatalf("frame for a pre-floor peer carries receipt_sender = %q", old.ReceiptSender)
	}
	if err := identity.VerifyPayload(svc.identity.Address, identity.PublicKeyBase64(svc.identity.PublicKey),
		ackDeletePayload(old.Address, old.AckType, old.ID, old.Status), old.Signature); err != nil {
		t.Errorf("a pre-floor peer cannot verify our ack: %v", err)
	}

	fresh := svc.buildAckDeleteFrameFor(ackDeleteForReceipt(receipt), true)
	if fresh.ReceiptSender != receipt.Sender {
		t.Errorf("frame for a current peer does not name the receipt's author: %q", fresh.ReceiptSender)
	}
}

// Identity is a type so that "one field missing" is unwritable, not
// merely reviewable: seven hand-spelled variants of the same tuple is
// how the sender came to be absent from four of them.
func TestReceiptIdentityIsBuiltInOnePlace(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "receipt_identity.go" {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			ident, ok := lit.Type.(*ast.Ident)
			if !ok || (ident.Name != "receiptIdentity" && ident.Name != "ackDelete") {
				return true
			}
			offenders = append(offenders, name+":"+fset.Position(lit.Pos()).String()+" builds "+ident.Name+" by hand")
			return true
		})
	}

	if len(offenders) > 0 {
		t.Errorf("receipt identity assembled outside receipt_identity.go:\n\t%s", strings.Join(offenders, "\n\t"))
	}
}

// Every relay-retry key of a receipt comes from the identity too. The
// map is shared with message keys, so a hand-built receipt key does not
// fail loudly — it simply addresses an entry nothing else will ever
// touch, and the retry it was meant to stop runs to its budget.
func TestEveryRelayRetryReceiptKeyComesFromTheIdentity(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	allowed := map[string]bool{"relayReceiptKey": true, "relayMessageKey": true, "retryKey": true, "dedupKey": true}
	var offenders []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "receipt_identity.go" {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			decl, ok := n.(*ast.FuncDecl)
			if !ok || decl.Body == nil {
				return true
			}
			built := map[string]bool{}
			ast.Inspect(decl.Body, func(inner ast.Node) bool {
				assign, ok := inner.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					return true
				}
				lhs, isIdent := assign.Lhs[0].(*ast.Ident)
				if isIdent && callsAllowedKeyBuilder(assign.Rhs[0], allowed) {
					built[lhs.Name] = true
				}
				return true
			})
			// Both ways a key reaches the map: m[k] and delete(m, k).
			// The delete form is a call, not an index expression, and an
			// earlier version of this sentinel checked only the latter —
			// which is exactly where the receipt keys are removed.
			check := func(pos token.Pos, key ast.Expr, how string) {
				if callsAllowedKeyBuilder(key, allowed) {
					return
				}
				if ident, ok := key.(*ast.Ident); ok && (built[ident.Name] || isParamNamed(decl, ident.Name)) {
					return
				}
				offenders = append(offenders, name+":"+fset.Position(pos).String()+" "+how+" relayRetry with a hand-built key")
			}
			ast.Inspect(decl.Body, func(inner ast.Node) bool {
				switch node := inner.(type) {
				case *ast.IndexExpr:
					if sel, ok := node.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "relayRetry" {
						check(node.Pos(), node.Index, "indexes")
					}
				case *ast.CallExpr:
					fun, ok := node.Fun.(*ast.Ident)
					if !ok || fun.Name != "delete" || len(node.Args) != 2 {
						return true
					}
					if sel, ok := node.Args[0].(*ast.SelectorExpr); ok && sel.Sel.Name == "relayRetry" {
						check(node.Pos(), node.Args[1], "deletes from")
					}
				}
				return true
			})
			return true
		})
	}

	if len(offenders) > 0 {
		t.Errorf("relay-retry keys built outside the identity:\n\t%s", strings.Join(offenders, "\n\t"))
	}
}

func callsAllowedKeyBuilder(expr ast.Expr, allowed map[string]bool) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return allowed[fun.Name]
	case *ast.SelectorExpr:
		return allowed[fun.Sel.Name]
	}
	return false
}
