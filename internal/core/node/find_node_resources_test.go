package node

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Genuine original Node/TLS/core receipts feed a separate fault writer. These
// tests prove owner accounting even for a core absent from current s.conns;
// they do not claim that the fault socket is the original registered writer.
func TestOwnedResourcesAnswerAfterPendingReleaseRetainsActualWriterCustody(t *testing.T) {
	relay, clock, _, source, answer := ownedAnswerActualWork(t)
	core, c := ownedWriterFaultCore(t, "success", clock)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.beforeWrite = func() { close(entered); <-release }
	if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("answer writer did not reach actual raw Write")
	}
	v := relay.ownedFindNodeResourceView()
	if v.Owner.Pending != 0 || v.AnswerItems != 1 || v.ForwardItems != 0 || v.CellBytes == 0 || v.PayloadBytes == 0 || v.ItemBytes == 0 {
		t.Fatal("released pending hid original in-flight answer", v)
	}
	if err := answer.CancelAnswer(); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("in-flight answer cancelled", err)
	}
	if relay.ownedFindNodeResourceView().AnswerItems != 1 {
		t.Fatal("logical cancellation released physical writer custody")
	}
	unblock()
	waitFor(t, 2*time.Second, "actual answer item last-use release", func() bool { return relay.ownedFindNodeResourceView().AnswerItems == 0 })
	if _, err := answer.QueueAnswerForOriginal(source); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("resource release revived answer", err)
	}
}
func TestOwnedResourcesCancelledButQueuedAnswerRemainsUntilActualDrain(t *testing.T) {
	forwardOwner, clock, _, target, forward := ownedWriterActualWork(t)
	answerOwner, _, _, source, answer := ownedAnswerActualWork(t)
	core, c := ownedWriterFaultCore(t, "write-error", clock)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.beforeWrite = func() { close(entered); <-release }
	if status := core.SendOwnedFindNode(forward, target); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first writer did not reach raw Write")
	}
	if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
		t.Fatal(status)
	}
	before := answerOwner.ownedFindNodeResourceView()
	if before.AnswerItems != 1 || before.PayloadBytes == 0 {
		t.Fatal("queued original answer missing", before)
	}
	if err := answer.CancelAnswer(); err != nil {
		t.Fatal(err)
	}
	after := answerOwner.ownedFindNodeResourceView()
	if after.AnswerItems != 1 || after.CellBytes != before.CellBytes || after.PayloadBytes != before.PayloadBytes {
		t.Fatal("logical cancel claimed queued bytes freed", before, after)
	}
	unblock()
	ownedWriterWaitDone(t, core, c)
	waitFor(t, 2*time.Second, "both original owners actual final-use release", func() bool {
		return answerOwner.ownedFindNodeResourceView().AnswerItems == 0 && forwardOwner.ownedFindNodeResourceView().ForwardItems == 0
	})
	if c.writes.Load() != 1 {
		t.Fatal("drained cancelled answer reached another Write")
	}
}
func TestOwnedResourcesClosedNonofferDoesNotRetainOwnerCells(t *testing.T) {
	owner, clock, _, target, work := ownedWriterActualWork(t)
	core, _ := ownedWriterFaultCore(t, "success", clock)
	core.Close()
	if status := core.SendOwnedFindNode(work, target); status == netcore.SendOK {
		t.Fatal("closed core accepted owned work")
	}
	held := owner.ownedFindNodeResourceView()
	if held.ForwardItems != 0 || held.CellBytes != 0 || held.PayloadBytes != 0 || held.NamedRoundEstimateBytes != 0 {
		t.Fatal("nonoffer retained original-owner resource charge", held)
	}
}
func TestOwnedResourcesBreakdownAndDTOExposeSeparateNamedEstimate(t *testing.T) {
	relay, clock, _, source, answer := ownedAnswerActualWork(t)
	core, c := ownedWriterFaultCore(t, "success", clock)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.beforeWrite = func() { close(entered); <-release }
	if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual owned answer did not start")
	}
	usage := relay.ownedFindNodeUsage()
	if usage.Subsystem() != domain.ResourceSubsystemOverlay || usage.FloorBytes() == 0 {
		t.Fatal("overlay owner omitted from breakdown")
	}
	var payload bool
	for _, g := range usage.Gauges() {
		if g.Name() == "owned_queue_payload_bytes" {
			payload = g.Count() > 0 && g.Kind() == domain.ResourceGaugeMemory
		}
		if g.Name() == "scheduler_batch_work_refs" && g.Kind() != domain.ResourceGaugeSaturation {
			t.Fatal("shared graph refs added to memory floor")
		}
	}
	if !payload {
		t.Fatal("immutable owned payload floor absent")
	}
	raw, err := relay.FetchResourceBreakdown()
	if err != nil {
		t.Fatal(err)
	}
	var dto struct {
		Overlay struct {
			Writer  map[string]any `json:"writer_custody"`
			Storage struct {
				Included bool   `json:"included_in_floor"`
				Sharing  string `json:"sharing"`
			} `json:"retained_named_storage"`
		} `json:"overlay"`
	}
	if err := json.Unmarshal(raw, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Overlay.Writer["answer_items"] != float64(1) || dto.Overlay.Storage.Included || dto.Overlay.Storage.Sharing == "" {
		t.Fatal("DTO lost custody or claimed shared estimate as floor", string(raw))
	}
	unblock()
}

// Finish precedes capture in the real typed writer; the wrapper still retains
// its opaque work until this callback and writeItem's release defer return.
type ownedResourceCompletionCapture struct {
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *ownedResourceCompletionCapture) OnSendAttempt(data []byte, ok bool) {
	kind, class := classifyFrameLine(string(data))
	if class != frameLineNamed || kind != protocol.FindNodeAnswerFrameType {
		return
	}
	c.once.Do(func() { close(c.entered) })
	<-c.release
}
func TestOwnedResourcesFinishedAnswerHeldThroughActualCaptureReturn(t *testing.T) {
	owner, clock, _, source, answer := ownedAnswerActualWork(t)
	core, _ := ownedWriterFaultCore(t, "success", clock)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	core.SetCaptureSink(&ownedResourceCompletionCapture{entered: entered, release: release})
	if status := core.SendOwnedFindNodeAnswer(answer, source); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("finished answer did not enter capture callback")
	}
	// Original Finish already happened. The one-use phase is terminal, while
	// operational writer custody must remain until the callback returns.
	if _, err := answer.QueueAnswerForOriginal(source); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("capture ran before original Finish", err)
	}
	view := owner.ownedFindNodeResourceView()
	if view.AnswerItems != 1 || view.CellBytes == 0 || view.PayloadBytes == 0 {
		t.Fatal("Finish claimed callback-held wrapper freed", view)
	}
	unblock()
	waitFor(t, 2*time.Second, "capture returned then exact item last-use release", func() bool { return owner.ownedFindNodeResourceView().AnswerItems == 0 })
}
