// Package ebus provides an in-process, topic-based, async event bus.
//
// Design:
//   - Topic-based pub/sub: publishers emit events by topic string,
//     subscribers register typed handlers for specific topics.
//   - Async by default: each async subscriber gets a dedicated goroutine
//     with a bounded inbox (default 64 slots). Publishers never block —
//     if the inbox is full the event is dropped and a warning is logged.
//   - Sync mode: handlers can opt in to WithSync() to run inline in the
//     publisher's goroutine (no inbox, no goroutine overhead).
//   - No global state: Bus instances are created via New() and passed
//     through constructors (dependency injection).
//
// Concurrency safety:
//   - Publish and Unsubscribe may run concurrently from different goroutines.
//     The inbox channel is never closed by Unsubscribe — instead a separate
//     quit channel signals the drainer to stop. This avoids a "send on closed
//     channel" panic when a Publish snapshot races with Unsubscribe.
//
// Usage:
//
//	bus := ebus.New()
//	id := bus.Subscribe("peer.connected", func(addr string, identity string) { ... })
//	bus.Publish("peer.connected", addr, identity)
//	bus.Unsubscribe(id) // removes the handler
//	bus.Shutdown()       // waits for in-flight handlers to finish
package ebus

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

// defaultInboxSize is the per-subscriber event buffer for async handlers.
// When the buffer is full, Publish drops the event (log warning) instead of
// blocking the publisher. 64 is large enough to absorb short bursts of
// peer-health / aggregate-status events without shedding under normal load.
const defaultInboxSize = 64

// SubscriptionID uniquely identifies a subscription for later removal.
type SubscriptionID uint64

// Bus is a topic-based async event bus. Zero value is not usable; create
// instances with New().
type Bus struct {
	mu     sync.RWMutex
	subs   map[string][]subscriber
	wg     sync.WaitGroup
	nextID uint64 // atomic counter for subscription IDs
}

type subscriber struct {
	id    SubscriptionID
	fn    reflect.Value
	async bool
	// inbox receives events for async subscribers. It is NEVER closed —
	// closing would race with concurrent Publish calls that hold a stale
	// subscriber snapshot. The channel is left open and becomes eligible
	// for GC once both the drainer goroutine (quit) and all publisher
	// references (snapshot copies) are gone.
	inbox chan []reflect.Value
	// quit signals the drainer goroutine to stop. Closed by Unsubscribe
	// or Shutdown. Closing quit is safe even while Publish writes to inbox
	// because they are independent channels.
	quit chan struct{}
}

// New creates a ready-to-use Bus.
func New() *Bus {
	return &Bus{
		subs: make(map[string][]subscriber),
	}
}

// Subscribe registers a handler for the given topic. The handler function
// signature must match the arguments passed to Publish for the same topic.
//
// By default, handlers run asynchronously via a dedicated drainer goroutine
// with a bounded inbox. Use WithSync() to run the handler synchronously in
// the publisher's goroutine.
//
// Returns a SubscriptionID that can be passed to Unsubscribe to remove the
// handler.
func (b *Bus) Subscribe(topic string, handler interface{}, opts ...SubOption) SubscriptionID {
	if b == nil {
		return 0
	}
	fn := reflect.ValueOf(handler)
	if fn.Kind() != reflect.Func {
		panic("ebus: handler must be a function")
	}

	var opt subOptions
	for _, o := range opts {
		o(&opt)
	}

	id := SubscriptionID(atomic.AddUint64(&b.nextID, 1))
	s := subscriber{
		id:    id,
		fn:    fn,
		async: !opt.sync,
	}

	if s.async {
		s.inbox = make(chan []reflect.Value, defaultInboxSize)
		s.quit = make(chan struct{})
		b.wg.Add(1)
		go b.drainLoop(topic, s)
	}

	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], s)
	b.mu.Unlock()

	return id
}

// drainLoop is the dedicated goroutine for an async subscriber. It
// processes events from the inbox until quit is closed, then drains any
// remaining buffered events before exiting. This two-phase approach
// guarantees that events already accepted into the inbox at the time of
// Shutdown/Unsubscribe are delivered — satisfying the contract that
// Shutdown waits for in-flight work to complete.
//
// Without the drain phase, Go's select would non-deterministically pick
// <-s.quit over <-s.inbox when both are ready, silently discarding an
// event that Publish already confirmed into the buffer.
func (b *Bus) drainLoop(topic string, s subscriber) {
	defer b.wg.Done()

	// Phase 1: normal operation — process events until quit is signalled.
	for {
		select {
		case args := <-s.inbox:
			b.safeCall(topic, s.fn, args)
		case <-s.quit:
			// Phase 2: drain remaining buffered events so nothing
			// accepted by Publish is silently lost.
			for {
				select {
				case args := <-s.inbox:
					b.safeCall(topic, s.fn, args)
				default:
					return
				}
			}
		}
	}
}

// Unsubscribe removes the handler identified by the given SubscriptionID.
// For async handlers the quit channel is closed, causing the drainer
// goroutine to exit. The inbox channel is intentionally left open to avoid
// racing with concurrent Publish calls.
// Safe to call with an ID that was already removed or zero — it is a no-op.
func (b *Bus) Unsubscribe(id SubscriptionID) {
	if b == nil || id == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, subs := range b.subs {
		for i, s := range subs {
			if s.id == id {
				b.subs[topic] = append(subs[:i], subs[i+1:]...)
				if s.quit != nil {
					close(s.quit)
				}
				return
			}
		}
	}
}

// UnsubscribeAll removes all handlers identified by the given IDs in one pass.
// Efficient for components that register many handlers and tear them all down
// at once (e.g. ConsoleWindow on close).
func (b *Bus) UnsubscribeAll(ids []SubscriptionID) {
	if b == nil || len(ids) == 0 {
		return
	}

	remove := make(map[SubscriptionID]struct{}, len(ids))
	for _, id := range ids {
		if id != 0 {
			remove[id] = struct{}{}
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, subs := range b.subs {
		filtered := subs[:0]
		for _, s := range subs {
			if _, ok := remove[s.id]; ok {
				if s.quit != nil {
					close(s.quit)
				}
			} else {
				filtered = append(filtered, s)
			}
		}
		b.subs[topic] = filtered
	}
}

// Publish sends an event to all subscribers of the given topic. Arguments
// are passed positionally to the handler function via reflection.
//
// Async subscribers receive the event via their bounded inbox channel.
// If the inbox is full the event is dropped and a warning is logged —
// this prevents slow consumers from causing unbounded goroutine growth.
// The inbox channel is never closed, so this send cannot panic.
//
// Sync handlers run inline before Publish returns.
//
// Publish never panics on subscriber errors — they are logged and skipped.
func (b *Bus) Publish(topic string, args ...interface{}) {
	if b == nil {
		return
	}
	b.mu.RLock()
	handlers := make([]subscriber, len(b.subs[topic]))
	copy(handlers, b.subs[topic])
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return
	}

	callArgs := make([]reflect.Value, len(args))
	for i, a := range args {
		callArgs[i] = reflect.ValueOf(a)
	}

	for _, s := range handlers {
		if s.async {
			select {
			case s.inbox <- callArgs:
			default:
				log.Warn().
					Str("topic", topic).
					Uint64("sub_id", uint64(s.id)).
					Msg("ebus: subscriber inbox full, event dropped")
			}
		} else {
			b.safeCall(topic, s.fn, callArgs)
		}
	}
}

// Shutdown signals all async subscriber drainers to stop and blocks until
// they finish processing all buffered events. Every event that Publish
// successfully placed into an inbox will be delivered before Shutdown returns.
func (b *Bus) Shutdown() {
	if b == nil {
		return
	}

	b.mu.Lock()
	for topic, subs := range b.subs {
		for _, s := range subs {
			if s.quit != nil {
				close(s.quit)
			}
		}
		b.subs[topic] = nil
	}
	b.mu.Unlock()

	b.wg.Wait()
}

// safeCall invokes the handler with panic recovery so a broken subscriber
// does not crash the publisher or the entire process.
func (b *Bus) safeCall(topic string, fn reflect.Value, args []reflect.Value) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Str("topic", topic).
				Interface("panic", r).
				Msg("ebus: handler panicked")
		}
	}()
	fn.Call(args)
}
