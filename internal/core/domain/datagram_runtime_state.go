package domain

import (
	"errors"
	"time"
)

type DatagramRuntimeStateKind string

const (
	DatagramStateUnknown              DatagramRuntimeStateKind = "unknown"
	DatagramStateConfiguredOff        DatagramRuntimeStateKind = "configured_off"
	DatagramStateInitialized          DatagramRuntimeStateKind = "initialized"
	DatagramStateInitializationFailed DatagramRuntimeStateKind = "initialization_failed"
)

var ErrDatagramStateUnavailable = errors.New("datagram constructor state unavailable")
var ErrDatagramStateInvalid = errors.New("invalid datagram constructor state")

// DatagramRuntimeState reports immutable construction facts for one Service.
// It does not report Run readiness, serving health or executable provenance.
type DatagramRuntimeState struct {
	Version           uint8                    `json:"version"`
	Identity          PeerIdentity             `json:"identity"`
	StartedAt         time.Time                `json:"started_at"`
	ReadAt            time.Time                `json:"read_at"`
	ConfiguredEnabled bool                     `json:"configured_enabled"`
	State             DatagramRuntimeStateKind `json:"state"`
}

func (s DatagramRuntimeState) Validate() error {
	if s.Version != 1 || s.Identity.IsZero() || s.StartedAt.IsZero() || s.ReadAt.IsZero() || s.ReadAt.Before(s.StartedAt) {
		return ErrDatagramStateInvalid
	}
	switch s.State {
	case DatagramStateConfiguredOff:
		if s.ConfiguredEnabled {
			return ErrDatagramStateInvalid
		}
	case DatagramStateInitialized, DatagramStateInitializationFailed:
		if !s.ConfiguredEnabled {
			return ErrDatagramStateInvalid
		}
	case DatagramStateUnknown:
	default:
		return ErrDatagramStateInvalid
	}
	return nil
}
