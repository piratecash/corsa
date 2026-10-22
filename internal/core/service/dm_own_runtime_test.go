package service

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"testing"
	"time"
)

func TestOwnDMDesktopTypedBeforeEscapeAndSameProber(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeLoopbackAddress(t)})
	s, port, err := node.NewInactiveOwnDMAdmissionCandidate(cfg, id, nil, node.OwnDMResidenceProfile{MaxPrestage: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	db := newTestStateDB(t, domain.PeerIdentityFromWire(id.Address))
	c, err := NewDesktopClientWithOwnDMProvenance(config.App{}, cfg, id, s, db, port)
	if err != nil {
		t.Fatal(err)
	}
	if !c.dm.ownDMPort.ValidFor(s, id) || c.prober.dmCrypto != c.dm || c.dm.ownDMObserver != nil || c.rpc.LocalNode() != s {
		t.Fatal("typed Desktop escaped original composition")
	}
	ordinary := NewDesktopClient(config.App{}, cfg, id, s, db)
	if ordinary.dm.ownDMPort.ValidFor(s, id) {
		t.Fatal("ordinary Desktop activated R2")
	}
}
func TestOwnDMDesktopRejectsZeroForeignOriginalBeforeRegistration(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	other, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeLoopbackAddress(t)})
	s, port := node.NewInactiveOwnDMProvenanceCandidate(cfg, id, nil)
	for _, tc := range []struct {
		id   *identity.Identity
		port node.OwnDMProvenancePort
	}{{id, node.OwnDMProvenancePort{}}, {other, port}} {
		c, err := NewDesktopClientWithOwnDMProvenance(config.App{}, cfg, tc.id, s, nil, tc.port)
		if c != nil || err == nil {
			t.Fatal("zero/foreign Desktop accepted")
		}
	}
}
