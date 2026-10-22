package netcore

import (
	"github.com/piratecash/corsa/internal/core/findnode"
	"sync/atomic"
	"time"
)

// Separate from immutable/shareable legacy WriteTicket. This owned one-shot
// queue item is never shared with another socket or resolved by a later ID.
type ownedFindNodeSend struct {
	custodyClaim    atomic.Uint32                 // 0unclaimed,1soleconsumer,2terminalproducer; metadata ownership only
	resourcesHeld   bool                          // assigned preoffer, then owned only by nonoffer producer OR sole consumer
	resourceCustody findnode.OwnedResourceCustody // original intake owner, survives registry removal
	answer          findnode.OwnedQueuedAnswer
	core            *NetCore
	queued          findnode.OwnedQueuedForward
}

func (pc *NetCore) SendOwnedFindNode(work findnode.OwnedForwardWork, original findnode.Session) SendStatus {
	if pc == nil {
		_ = work.Cancel()
		return SendWriterDone
	}
	queued, err := work.QueueForOriginal(original)
	if err != nil {
		return SendMarshalError
	}
	st := pc.queueFrame(sendItem{owned: &ownedFindNodeSend{core: pc, queued: queued}})
	if st != SendOK {
		_ = queued.Cancel()
	}
	return st
}

// One closed typed union; both variants retain exact owner custody.
func (item *ownedFindNodeSend) cancelOwned() {
	if item == nil {
		return
	}
	if item.answer != (findnode.OwnedQueuedAnswer{}) {
		_ = item.answer.Cancel()
	} else {
		_ = item.queued.Cancel()
	}
}
func (pc *NetCore) writeOwnedFindNode(item *ownedFindNodeSend) bool {
	if item == nil || item.core != pc {
		if item != nil {
			item.cancelOwned()
		}
		return true
	}
	if item.answer != (findnode.OwnedQueuedAnswer{}) {
		return pc.writeOwnedFindNodeAnswer(item)
	}
	// A future reserved alias is a scheduled opportunity, not an early Take
	// refusal. This single writer timer only wakes; fresh Take authorizes start.
	delay, err := item.queued.WaitDelay()
	if err != nil {
		_ = item.queued.Cancel()
		return true
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-pc.closing:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			_ = item.queued.Cancel()
			return true
		}
	}
	// Deadline syscall/application waits precede the genuine writer-start LP.
	if err := pc.rawConn.SetWriteDeadline(time.Now().Add(pc.writeDeadline)); err != nil {
		_ = item.queued.Cancel()
		_ = pc.rawConn.Close()
		return false
	}
	var permit findnode.OwnedWritePermit
	needsFinish := false
	defer func() {
		if needsFinish {
			_ = permit.Finish(false)
			_ = pc.rawConn.Close()
		}
	}()
	data, permit, err := item.queued.Take()
	if err != nil {
		return true
	}
	needsFinish = true
	written, err := pc.rawConn.Write(data)
	writeOK := err == nil && written == len(data)
	finishErr := permit.Finish(writeOK)
	needsFinish = false
	if sink := pc.loadCaptureSink(); sink != nil {
		sink.OnSendAttempt(data, writeOK)
	}
	if !writeOK || finishErr != nil {
		_ = pc.rawConn.Close()
		return false
	}
	_ = pc.rawConn.SetWriteDeadline(time.Time{})
	return true
}
