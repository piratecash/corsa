package node

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// A private provider-failure control, not a public switch or a claim that the
// native SinceBoot provider panics in an ordinary shipped constructor.
func k3ClockPanicAt(clock *k3ActualClock, read int) any {
	sentinel := &struct{ read int }{read: read}
	calls := 0
	clock.setHook(func() {
		calls++
		if calls == read {
			panic(sentinel)
		}
	})
	return sentinel
}

func k3ClockPanicCall(t *testing.T, sentinel any, call func()) {
	t.Helper()
	var observed any
	func() {
		defer func() { observed = recover() }()
		call()
	}()
	if observed != sentinel {
		t.Fatalf("original provider panic changed or was swallowed: got %v", observed)
	}
}

// The tested call has returned synchronously through the controlled panic.
// These fixtures run no Service.Run or competing peer-map owner. TryLock is a
// nonblocking oracle, so the known broken subject cannot hang the test suite.
// On RED only, release its stranded mutex for owned fixture cleanup AFTER
// recording the failure. That cleanup never supplies a passing observation.
func k3ClockPanicLocks(t *testing.T, s *Service, delivery bool) {
	t.Helper()
	peerFree := s.peerMu.TryLock()
	deliveryFree := true
	if delivery {
		deliveryFree = s.deliveryMu.TryLock()
		s.deliveryMu.Unlock()
	}
	s.peerMu.Unlock()
	if !peerFree || !deliveryFree {
		t.Errorf("provider panic stranded original owner locks: peer_free=%v delivery_free=%v", peerFree, deliveryFree)
	}
}

func k3ClockPanicFrozen(t *testing.T, s *Service, clock *k3ActualClock, before k3RegisteredSnapshot) {
	t.Helper()
	calls := clock.count()
	after := k3ActualFault(t, s, before)
	if after.Reason != "duration-clock-incomplete" {
		t.Fatal("provider panic did not retain the incomplete-read cause", after)
	}
	_ = k3ActualFault(t, s, before)
	if clock.count() != calls {
		t.Fatal("faulted observation called provider or advanced frozen prefix")
	}
}

func TestK3RegisteredSnapshotClockPanicPreservesOwner(t *testing.T) {
	for _, read := range []int{1, 2} {
		t.Run(fmt.Sprintf("read-%d", read), func(t *testing.T) {
			clock := newK3ActualClock()
			request := k3ActualRequest(clock, 8, 64)
			// No transport or background owners are needed to test the real
			// snapshot mutex and unchanged empty registered-proof prefix.
			s := &Service{k3Registered: newK3RegisteredSource(request)}
			before := k3ActualSnapshot(t, s, 0, 0)
			sentinel := k3ClockPanicAt(clock, read)
			k3ClockPanicCall(t, sentinel, func() { _ = s.k3RegisteredSnapshot() })
			k3ClockPanicLocks(t, s, false)
			k3ClockPanicFrozen(t, s, clock, before)
		})
	}
}

func TestK3RegisteredAuthClockPanicKeepsOriginalMutation(t *testing.T) {
	for _, operation := range []string{"clear", "set"} {
		for _, read := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s-read-%d", operation, read), func(t *testing.T) {
				clock := newK3ActualClock()
				request := k3ActualRequest(clock, 8, 64)
				s := k3ActualService(t, &request)
				peer := k3ActualService(t, nil)
				pair := k3ActualPair(t, s, peer)
				auth := k3Auth(t, s, pair.inbound.id)
				owners := 1
				if operation == "set" {
					s.clearConnAuth(pair.inbound.id)
					owners = 0
				}
				before := k3ActualSnapshot(t, s, owners, owners)
				sentinel := k3ClockPanicAt(clock, read)
				k3ClockPanicCall(t, sentinel, func() {
					if operation == "clear" {
						s.clearConnAuth(pair.inbound.id)
					} else {
						s.setConnAuthStateByID(pair.inbound.id, auth)
					}
				})
				k3ClockPanicLocks(t, s, false)
				want := auth
				if operation == "clear" && read == 2 || operation == "set" && read == 1 {
					want = nil
				}
				if got := s.connAuthStateByID(pair.inbound.id); got != want {
					t.Fatal("observer panic changed the original Before/After Auth write", got, want)
				}
				k3ClockPanicFrozen(t, s, clock, before)
			})
		}
	}
}

func TestK3RegisteredCMSessionClockPanicReleasesBothOwners(t *testing.T) {
	for _, operation := range []string{"publish", "withdraw"} {
		for _, read := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s-read-%d", operation, read), func(t *testing.T) {
				clock := newK3ActualClock()
				request := k3ActualRequest(clock, 8, 64)
				s := k3ActualService(t, &request)
				peer := k3ActualService(t, nil)
				pair := k3ActualPair(t, peer, s)
				owners := 0
				if operation == "withdraw" {
					k3Publish(t, s, pair.outbound)
					owners = 1
				}
				before := k3ActualSnapshot(t, s, owners, owners)
				sentinel := k3ClockPanicAt(clock, read)
				k3ClockPanicCall(t, sentinel, func() {
					if operation == "publish" {
						_ = s.registerCMSession(pair.outbound, pair.outbound.address)
					} else {
						k3Withdraw(s, pair.outbound)
					}
				})
				k3ClockPanicLocks(t, s, true)
				s.peerMu.RLock()
				published := s.sessions[pair.outbound.address] == pair.outbound
				s.peerMu.RUnlock()
				wantPublished := operation == "publish" && read == 2 || operation == "withdraw" && read == 1
				if published != wantPublished {
					t.Fatal("observer panic changed original Before/After session publication", published, wantPublished)
				}
				k3ClockPanicFrozen(t, s, clock, before)
				// The now-faulted observer must not obstruct original cleanup.
				k3Withdraw(s, pair.outbound)
			})
		}
	}
}

// Own transport only; Close supplies a synchronization observation, never a
// replacement for the original NetCore.Close or its writer-join contract.
type k3ClockBirthConn struct {
	net.Conn
	closed  chan struct{}
	once    sync.Once
	onClose func()
}

func (c *k3ClockBirthConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func TestK3ObservationClockPanicClosesUntransferredCore(t *testing.T) {
	for _, source := range []string{"registered", "advertised"} {
		for _, direction := range []string{"inbound", "outbound"} {
			for _, read := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s-%s-read-%d", source, direction, read), func(t *testing.T) {
					end := time.Now().Add(5 * time.Second)
					cfg, id := pendingConstructionInputs(t)
					clock := newK3ActualClock()
					terminal := terminalConstructionKey()
					options := ObservationOptions{Terminal: &terminal}
					registered, advertised := registeredBridgeRequest(), advertisedBridgeRequest()
					if source == "registered" {
						options.RegisteredProvenV2 = &registered
					} else {
						options.AdvertisedConnections = &advertised
					}
					s, readouts, err := newServiceWithObservations(cfg, id, nil, options, clock.read)
					if err != nil {
						t.Fatal(err)
					}
					ownTerminalConstruction(t, s, end)
					var before k3RegisteredSnapshot
					if source == "registered" {
						before = k3ActualSnapshot(t, s, 0, 0)
					}
					directionKind := connbudget.DirectionInbound
					if direction == "outbound" {
						directionKind = connbudget.DirectionOutbound
					}
					reservation, err := s.connBudget.Reserve(directionKind)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(reservation.Release)
					if v := s.connBudget.Snapshot(); v.Used != 1 {
						t.Fatal("fixture lacks an original owned connection reservation", v)
					}
					server, client := net.Pipe()
					base := server
					if direction == "outbound" {
						// Original dial ownership returns its reservation on Close.
						base = &budgetedConn{Conn: server, reservation: reservation}
					}
					raw := &k3ClockBirthConn{Conn: base, closed: make(chan struct{})}
					t.Cleanup(func() { _ = client.Close(); _ = raw.Close() })
					const connID domain.ConnID = 1
					var publishedCore *netcore.NetCore
					var closeUnderPeer, closeBeforeRemoval atomic.Bool
					var firstClose sync.Once
					raw.onClose = func() {
						if s.peerMu.TryLock() {
							firstClose.Do(func() {
								id, ok := s.connIDForLocked(raw)
								if read == 2 && ok && id == connID && publishedCore != nil && s.coreForIDLocked(id) == publishedCore {
									closeBeforeRemoval.Store(true)
								}
							})
							s.peerMu.Unlock()
						} else {
							closeUnderPeer.Store(true)
						}
					}
					sentinel := &struct{ read int }{read: read}
					calls := 0
					clock.setHook(func() {
						calls++
						if calls == read {
							// The original owner holds peerMu. The After case can
							// capture the actually published core without a second lock.
							publishedCore = s.coreForIDLocked(connID)
							panic(sentinel)
						}
					})
					k3ClockPanicCall(t, sentinel, func() {
						if direction == "inbound" {
							// The accept owner still owes Release on an incomplete
							// registration; After cleanup uses the same idempotent unit.
							defer reservation.Release()
							_ = s.registerInboundConn(raw, reservation)
						} else {
							session := &peerSession{address: "127.0.0.1:64646", connID: connID, conn: raw}
							_ = s.attachOutboundNetCore(session)
						}
					})
					k3ClockPanicLocks(t, s, false)
					select {
					case <-raw.closed:
					default:
						t.Error("untransferred core did not close its original socket before propagating panic")
					}
					if closeUnderPeer.Load() {
						t.Error("original NetCore.Close ran under peerMu")
					}
					if read == 2 {
						if publishedCore == nil {
							t.Fatal("After fixture failed to reach original registry publication")
						}
						if !closeBeforeRemoval.Load() {
							t.Error("incomplete birth removed registry ownership before original socket close")
						}
						select {
						case <-publishedCore.WriterExited():
						default:
							// RED-only cleanup prevents an already observed bug from
							// obstructing fixture cleanup; it cannot make this pass.
							publishedCore.Close()
							t.Error("original core writer was not joined before panic propagated")
						}
					} else if publishedCore != nil {
						t.Fatal("Before fixture unexpectedly reached registry publication")
					}
					s.peerMu.RLock()
					_, indexed := s.connIDForLocked(raw)
					_, registeredCore := s.conns[connID]
					s.peerMu.RUnlock()
					if indexed || registeredCore {
						t.Error("incomplete birth retained an original registry index")
					}
					if v := s.connBudget.Snapshot(); v.Used != 0 || v.Inbound != 0 || v.Outbound != 0 {
						t.Error("incomplete birth retained or double-released original connection accounting", v)
					}
					if source == "registered" {
						k3ClockPanicFrozen(t, s, clock, before)
					} else if v := readouts.AdvertisedConnections.Snapshot(); v.Status != "UNKNOWN" || v.Reason == "" {
						t.Fatal("incomplete advertised birth fabricated covered state", v)
					}
					if v := readouts.Terminal.Snapshot(); v.Status != "UNKNOWN" || v.Reason != "registry-birth-incomplete" {
						t.Fatal("partial K10 birth fabricated complete terminal coverage", v)
					}
				})
			}
		}
	}
}
