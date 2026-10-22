package loadstand

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// RuntimeProcess is an opaque, comparable original lifecycle owner. It is not
// a PID, peer identity, role, service StartedAt, or serialized authority. All
// values minted here contain pointers; outside packages cannot mint owners.
// A live key proves adapter custody, not Linux kernel observation success.
type RuntimeProcess struct{ owner any }

func (p RuntimeProcess) Valid() bool { return p.owner != nil }

// All services directly hosted by this Go process share this one heap owner.
// A child adapter must instead retain its original Process/Fixture key.
var currentRuntimeProcess = new(byte)

func CurrentRuntimeProcess() RuntimeProcess {
	return RuntimeProcess{owner: currentRuntimeProcess}
}
func (p *Process) RuntimeProcess() RuntimeProcess {
	if p == nil {
		return RuntimeProcess{}
	}
	return RuntimeProcess{owner: p}
}

// ValidateMemberRuntime binds the runtime DTO to the independently retained
// successful service lifetime. StartedAt is NOT a physical process identity.
// Unsupported readings remain unavailable, never a measured zero or RSS.
func ValidateMemberRuntime(m domain.RuntimeMetrics, startedAt time.Time) error {
	if m.Version != 1 || startedAt.IsZero() || !m.StartedAt.Equal(startedAt) || m.ReadAt.IsZero() || m.ReadAt.Before(startedAt) {
		return ErrInvalidSample
	}
	if len(m.Unsupported) != 0 || m.HeapLiveBytes == nil || m.HeapObjectsBytes == nil || m.HeapAllocsBytes == nil || m.GoMemoryBytes == nil || m.GCCycles == nil || m.Goroutines == nil || m.GOMAXPROCS == nil || m.CPUTotalSeconds == nil || m.CPUUserSeconds == nil || m.CPUGCSeconds == nil || m.CPUIdleSeconds == nil {
		return ErrFamilyUnavailable
	}
	return nil
}
