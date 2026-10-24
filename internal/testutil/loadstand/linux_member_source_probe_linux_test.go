//go:build linux

package loadstand

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A local value passed into the actual private helper. No global syscall hook
// is replaced, and no test opens a socket or changes a namespace.
type sourceProbeFake struct {
	trace        []string
	targets      []netip.AddrPort
	failAt       string
	failure      error
	closeError   error
	connectError error
	named        unix.Sockaddr
	closes       int
	afterName    func()
	afterClose   func()
}

func newSourceProbeFake() *sourceProbeFake {
	return &sourceProbeFake{failure: errors.New("original setup errno"), connectError: unix.ENETUNREACH,
		named: &unix.SockaddrInet6{Addr: netip.MustParseAddr("2001:db8::1").As16(), Port: 32123}}
}
func (f *sourceProbeFake) step(name string) error {
	f.trace = append(f.trace, name)
	if name == f.failAt {
		return f.failure
	}
	return nil
}
func (f *sourceProbeFake) ops() plannedProbeSocketOps {
	return plannedProbeSocketOps{
		socket: func(family, kind, protocol int) (int, error) {
			if kind != unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK || protocol != 0 {
				return -1, errors.New("changed socket parameters")
			}
			name := "socket4"
			if family == unix.AF_INET6 {
				name = "socket6"
			} else if family != unix.AF_INET {
				return -1, errors.New("changed socket family")
			}
			if err := f.step(name); err != nil {
				return -1, err
			}
			return 77, nil
		},
		option: func(fd, level, option, value int) error {
			if fd != 77 || level != unix.IPPROTO_IPV6 || option != unix.IPV6_FREEBIND || value != 1 {
				return errors.New("changed IPv6 option")
			}
			return f.step("option")
		},
		bind: func(fd int, addr unix.Sockaddr) error {
			a, ok := addr.(*unix.SockaddrInet6)
			if fd != 77 || !ok || a == nil || a.Addr != netip.MustParseAddr("2001:db8::1").As16() || a.Port != 0 || a.ZoneId != 0 {
				return errors.New("changed fixed nonlocal bind")
			}
			return f.step("bind")
		},
		name: func(fd int) (unix.Sockaddr, error) {
			if fd != 77 {
				return nil, errors.New("changed observed fd")
			}
			err := f.step("name")
			if f.afterName != nil {
				f.afterName()
			}
			return f.named, err
		},
		connect: func(fd int, addr unix.Sockaddr) error {
			if fd != 77 {
				return errors.New("changed connect fd")
			}
			if err := f.step("connect"); err != nil {
				return err
			}
			switch a := addr.(type) {
			case *unix.SockaddrInet4:
				f.targets = append(f.targets, netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)))
			case *unix.SockaddrInet6:
				f.targets = append(f.targets, netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port)))
			default:
				return errors.New("changed connect address type")
			}
			return f.connectError
		},
		close: func(fd int) error {
			if fd != 77 {
				return errors.New("changed close fd")
			}
			f.closes++
			f.trace = append(f.trace, "close")
			if f.afterClose != nil {
				f.afterClose()
			}
			return f.closeError
		},
	}
}

func TestPlannedSourceBoundProbeSocketSequence(t *testing.T) {
	for _, tc := range []struct {
		name, target string
		trace        []string
	}{
		{"IPv4", "8.8.8.8:443", []string{"socket4", "connect", "close"}},
		{"IPv6", "[2001:4860:4860::8888]:443", []string{"socket6", "option", "bind", "name", "connect", "close"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSourceProbeFake()
			target := netip.MustParseAddrPort(tc.target)
			if err := plannedProbeConnect(t.Context(), target, f.ops()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.trace, tc.trace) || !reflect.DeepEqual(f.targets, []netip.AddrPort{target}) || f.closes != 1 {
				t.Fatalf("changed original target/order/close: %+v", f)
			}
		})
	}
}

func TestPlannedSourceBoundProbeOnlyImmediateRoutingErrors(t *testing.T) {
	for _, target := range []string{"8.8.8.8:443", "[2001:4860:4860::8888]:443"} {
		for _, tc := range []struct {
			name     string
			err      error
			isolated bool
		}{
			{"network", unix.ENETUNREACH, true}, {"host", unix.EHOSTUNREACH, true},
			{"connected", nil, false}, {"progress", unix.EINPROGRESS, false},
			{"refused", unix.ECONNREFUSED, false}, {"address", unix.EADDRNOTAVAIL, false}, {"timeout", unix.ETIMEDOUT, false},
		} {
			t.Run(target+"/"+tc.name, func(t *testing.T) {
				f := newSourceProbeFake()
				f.connectError = tc.err
				err := plannedProbeConnect(t.Context(), netip.MustParseAddrPort(target), f.ops())
				if (err == nil) != tc.isolated || f.closes != 1 {
					t.Fatalf("changed strict routing/close law: err=%v closes=%d", err, f.closes)
				}
				if !tc.isolated && (!errors.Is(err, errPlannedNetwork) || tc.err != nil && !errors.Is(err, tc.err)) {
					t.Fatalf("lost original refusal/error identity: %v", err)
				}
			})
		}
	}
}

func TestPlannedSourceBoundProbeSetupAndCloseFailClosed(t *testing.T) {
	for _, stage := range []string{"socket6", "option", "bind", "name", "close"} {
		t.Run(stage, func(t *testing.T) {
			f := newSourceProbeFake()
			f.closeError = errors.New("original close errno")
			switch stage {
			case "socket6":
				f.failure = unix.EAFNOSUPPORT
			case "option":
				f.failure = unix.ENOPROTOOPT
			case "bind":
				f.failure = unix.EADDRNOTAVAIL
			case "name":
				f.failure = unix.EBADF
			}
			if stage != "close" {
				f.failAt = stage
			}
			err := plannedProbeConnect(t.Context(), netip.MustParseAddrPort("[2001:4860:4860::8888]:443"), f.ops())
			if err == nil {
				t.Fatal("setup/close failure accepted as isolation")
			}
			if stage != "close" && !errors.Is(err, f.failure) {
				t.Fatalf("lost original setup error: %v", err)
			}
			wantCloses := 1
			if stage == "socket6" {
				wantCloses = 0
			}
			if f.closes != wantCloses || (stage == "close") != (len(f.targets) == 1) {
				t.Fatalf("setup failure reached connect or wrong close count: %+v", f)
			}
			if (wantCloses == 1) != errors.Is(err, f.closeError) {
				t.Fatalf("lost or fabricated close error: %v", err)
			}
		})
	}
}

func TestPlannedSourceBoundProbeObservedIdentityRefused(t *testing.T) {
	source := netip.MustParseAddr("2001:db8::1").As16()
	for _, tc := range []struct {
		name   string
		actual unix.Sockaddr
	}{
		{"nil", nil}, {"typed-nil", (*unix.SockaddrInet6)(nil)}, {"family", &unix.SockaddrInet4{Port: 32123}},
		{"source", &unix.SockaddrInet6{Addr: netip.MustParseAddr("2001:db8::2").As16(), Port: 32123}},
		{"zero-port", &unix.SockaddrInet6{Addr: source}}, {"large-port", &unix.SockaddrInet6{Addr: source, Port: 65536}},
		{"zone", &unix.SockaddrInet6{Addr: source, Port: 32123, ZoneId: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSourceProbeFake()
			f.named = tc.actual
			err := plannedProbeConnect(t.Context(), netip.MustParseAddrPort("[2001:4860:4860::8888]:443"), f.ops())
			if !errors.Is(err, errPlannedNetwork) || len(f.targets) != 0 || f.closes != 1 {
				t.Fatalf("bad bound identity accepted or wrong cleanup: err=%v fake=%+v", err, f)
			}
		})
	}
}

func sourceProbeTargets() LinuxIsolationProbes {
	return LinuxIsolationProbes{ProductionBootstrap: netip.MustParseAddrPort("1.1.1.1:443"), FallbackPublic: netip.MustParseAddrPort("8.8.8.8:443"), PublicIPv6: netip.MustParseAddrPort("[2001:4860:4860::8888]:443")}
}

func TestPlannedSourceBoundProbeValidationAndCancellation(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		f := newSourceProbeFake()
		if err := plannedUnreachableUsing(context.Background(), sourceProbeTargets(), f.ops()); !errors.Is(err, ErrLinuxOwnerBound) || len(f.trace) != 0 {
			t.Fatalf("missing deadline reached socket: %v %+v", err, f)
		}
	})
	t.Run("invalid-target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		probes := sourceProbeTargets()
		probes.PublicIPv6 = netip.MustParseAddrPort("[::ffff:8.8.8.8]:443")
		f := newSourceProbeFake()
		if err := plannedUnreachableUsing(ctx, probes, f.ops()); !errors.Is(err, ErrInvalidConfiguration) || len(f.trace) != 0 {
			t.Fatalf("unvalidated target reached socket: %v %+v", err, f)
		}
	})
	t.Run("already-cancelled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		cancel()
		f := newSourceProbeFake()
		if err := plannedUnreachableUsing(ctx, sourceProbeTargets(), f.ops()); !errors.Is(err, context.Canceled) || len(f.trace) != 0 {
			t.Fatalf("cancelled probe reached socket: %v %+v", err, f)
		}
	})
	t.Run("cancelled-during-source-observation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f := newSourceProbeFake()
		f.afterName = cancel
		err := plannedProbeConnect(ctx, sourceProbeTargets().PublicIPv6, f.ops())
		if !errors.Is(err, context.Canceled) || len(f.targets) != 0 || f.closes != 1 {
			t.Fatalf("cancelled source setup connected or lost close: %v %+v", err, f)
		}
	})
}

func TestPlannedSourceBoundProbeOriginalTargetOrder(t *testing.T) {
	t.Run("three-original-targets", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		f := newSourceProbeFake()
		probes := sourceProbeTargets()
		if err := plannedUnreachableUsing(ctx, probes, f.ops()); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(f.targets, []netip.AddrPort{probes.ProductionBootstrap, probes.FallbackPublic, probes.PublicIPv6}) || f.closes != 3 {
			t.Fatalf("changed original target order: %+v", f)
		}
	})
	t.Run("cancel-after-first-close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		f := newSourceProbeFake()
		f.afterClose = cancel
		err := plannedUnreachableUsing(ctx, sourceProbeTargets(), f.ops())
		if !errors.Is(err, context.Canceled) || len(f.targets) != 1 || f.closes != 1 {
			t.Fatalf("cancelled action probed later targets: %v %+v", err, f)
		}
	})
}
