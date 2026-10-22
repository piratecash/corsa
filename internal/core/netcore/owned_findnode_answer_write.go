package netcore

import (
	"github.com/piratecash/corsa/internal/core/findnode"
	"time"
)

func (pc *NetCore) SendOwnedFindNodeAnswer(work findnode.OwnedAnswerWork, original findnode.Session) SendStatus {
	if pc == nil {
		_ = work.CancelAnswer()
		return SendWriterDone
	}
	queued, err := work.QueueAnswerForOriginal(original)
	if err != nil {
		return SendMarshalError
	}
	status := pc.queueFrame(sendItem{owned: &ownedFindNodeSend{core: pc, answer: queued}})
	if status != SendOK {
		_ = queued.Cancel()
	}
	return status
}
func (pc *NetCore) writeOwnedFindNodeAnswer(item *ownedFindNodeSend) bool {
	if item == nil || item.core != pc || item.answer == (findnode.OwnedQueuedAnswer{}) {
		if item != nil {
			item.cancelOwned()
		}
		return true
	}
	if err := pc.rawConn.SetWriteDeadline(time.Now().Add(pc.writeDeadline)); err != nil {
		_ = item.answer.Cancel()
		_ = pc.rawConn.Close()
		return false
	}
	var permit findnode.OwnedAnswerPermit
	needsFinish := false
	defer func() {
		if needsFinish {
			_ = permit.Finish(false)
			_ = pc.rawConn.Close()
		}
	}()
	data, permit, err := item.answer.Take()
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
