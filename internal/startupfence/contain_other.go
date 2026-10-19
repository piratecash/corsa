//go:build !linux

package startupfence

import (
	"context"
	"github.com/piratecash/corsa/internal/core/domain"
	"os"
)

type containedChild struct {
	done chan struct{}
	err  error
}

func (*containedChild) exited() bool           { return true }
func (*containedChild) kill() error            { return ErrUnsupported }
func privatePair() (*os.File, *os.File, error) { return nil, nil, ErrUnsupported }
func startContainedChild(context.Context, ChildCommand, *os.File, *os.File) (*containedChild, error) {
	return nil, ErrUnsupported
}
func verifySupervisor(*os.File, string, domain.PeerIdentity) error { return ErrUnsupported }
func checkContainment(ChildCommand) error                          { return ErrUnsupported }
func lockBinding(domain.PeerIdentity, StoreID) (*os.File, error)   { return nil, ErrUnsupported }
