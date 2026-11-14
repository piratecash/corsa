package netcore

import (
	"io"
	"net"
	"testing"
	"time"
)

type loadstandWriteTap struct {
	started chan string
	ended   chan bool
}

func (s loadstandWriteTap) OnLoadstandWriteStart(data []byte) { s.started <- string(data) }
func (s loadstandWriteTap) OnSendAttempt(_ []byte, ok bool)   { s.ended <- ok }

func TestLoadstandWriteStartBracketsOriginalSocket(t *testing.T) {
	const payload = "frame\n"
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()
	pc := New(1, left, Outbound, Options{LoadstandSessionID: [32]byte{1}})
	defer pc.Close()
	tap := loadstandWriteTap{started: make(chan string, 1), ended: make(chan bool, 1)}
	pc.SetCaptureSink(tap)
	if pc.SendRaw([]byte(payload)) != SendOK {
		t.Fatal("write not accepted")
	}
	select {
	case got := <-tap.started:
		if got != payload {
			t.Fatalf("started with %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("socket blocked without a start callback")
	}
	select {
	case <-tap.ended:
		t.Fatal("write completed before peer read")
	default:
	}
	if err := right.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(right, got); err != nil || string(got) != payload {
		t.Fatalf("socket read %q, %v", got, err)
	}
	select {
	case ok := <-tap.ended:
		if !ok {
			t.Fatal("completed write reported failure")
		}
	case <-time.After(time.Second):
		t.Fatal("no completion callback")
	}
}

type loadstandQueueTap struct {
	started   chan []byte
	prepared  chan []byte
	discarded chan []byte
}

func (s loadstandQueueTap) OnLoadstandWriteStart(data []byte) { s.started <- data }
func (s loadstandQueueTap) OnSendAttempt(_ []byte, _ bool)    {}
func (s loadstandQueueTap) PrepareLoadstandQueue(data []byte, _ time.Duration) []byte {
	copyOfData := append([]byte(nil), data...)
	s.prepared <- copyOfData
	return copyOfData
}

func (s loadstandQueueTap) DiscardLoadstandQueue(data []byte, admitted bool) {
	if admitted {
		s.discarded <- data
	}
}

func TestLoadstandQueuePreservesOfferIdentityThroughWriteAndDrain(t *testing.T) {
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()
	pc := New(1, left, Outbound, Options{LoadstandSessionID: [32]byte{1}})
	defer pc.Close()
	tap := loadstandQueueTap{started: make(chan []byte, 2), prepared: make(chan []byte, 2), discarded: make(chan []byte, 2)}
	pc.SetCaptureSink(tap)
	data := []byte("frame\n")
	if pc.SendRaw(data) != SendOK {
		t.Fatal("first offer refused")
	}
	first := <-tap.prepared
	select {
	case started := <-tap.started:
		if &started[0] != &first[0] {
			t.Fatal("write did not retain the offer identity")
		}
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	if pc.SendRaw(data) != SendOK {
		t.Fatal("second offer refused")
	}
	second := <-tap.prepared
	pc.Close()
	select {
	case discarded := <-tap.discarded:
		if &discarded[0] != &second[0] || &first[0] == &second[0] {
			t.Fatal("drain did not retain the second offer identity")
		}
	default:
		t.Fatal("second offer was not reported discarded")
	}
}
