package sdk

import (
	"context"
	"encoding/json"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	corsanode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func ownDMRuntimeProfile() corsanode.OwnDMRuntimeProfile {
	return corsanode.OwnDMRuntimeProfile{Residence: corsanode.OwnDMResidenceProfile{MaxPrestage: 2 * time.Minute}, MaxActive: 16, MaxMembers: 16, MaxRegistryVisits: 32, Intake: findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}, Signing: findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4}}
}
func ownDMRuntimeConfig(t *testing.T, peers []string) Config {
	t.Helper()
	cfg := testRuntimeConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Node.ListenAddress = listener.Addr().String()
	_ = listener.Close()
	_, raw, err := net.SplitHostPort(cfg.Node.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	value, err := strconv.ParseUint(raw, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(value)
	cfg.Node.AdvertisePort = &port
	// A nil slice would make normalizeConfig restore the public default seed.
	cfg.Node.BootstrapPeers = make([]string, len(peers))
	copy(cfg.Node.BootstrapPeers, peers)
	cfg.Node.ProxyAddress = ""
	cfg.Node.EnableDatagramV1 = true
	cfg.Node.DownloadDir = filepath.Join(cfg.Node.ChatLogDir, "downloads")
	cfg.Node.AllowPrivatePeers = true
	cfg.Node.MaxOutgoingPeers = 1
	return cfg
}

// Inspect both configuration boundaries before any of the three runtimes exists.
// Fresh per-node stores prevent learned peers from another run becoming seeds.
func ownDMRuntimeAssertLoopbackConfig(t *testing.T, cfg Config, seeds []string) {
	t.Helper()
	checkAddress := func(address string) {
		host, raw, err := net.SplitHostPort(address)
		port, portErr := strconv.ParseUint(raw, 10, 16)
		if err != nil || portErr != nil || port == 0 || host != "127.0.0.1" {
			t.Fatal("fixture address is not explicit IPv4 loopback", address)
		}
	}
	checkAddress(cfg.Node.ListenAddress)
	for _, seed := range seeds {
		checkAddress(seed)
	}
	normalized := normalizeConfig(cfg)
	internal := cfg.internal()
	if !cfg.Node.EnableDatagramV1 || !normalized.Node.EnableDatagramV1 || !internal.Node.EnableDatagramV1 {
		t.Fatal("fixture did not explicitly request original datagram transport before construction")
	}
	if cfg.Node.BootstrapPeers == nil || normalized.Node.BootstrapPeers == nil ||
		len(normalized.Node.BootstrapPeers) != len(seeds) || len(internal.Node.BootstrapPeers) != len(seeds) {
		t.Fatal("fixture bootstrap default/fallback escaped exact seed list")
	}
	for i, seed := range seeds {
		if cfg.Node.BootstrapPeers[i] != seed || normalized.Node.BootstrapPeers[i] != seed || internal.Node.BootstrapPeers[i] != seed {
			t.Fatal("fixture bootstrap seed changed at SDK boundary")
		}
	}
	_, listenPort, _ := net.SplitHostPort(cfg.Node.ListenAddress)
	advertised, _ := strconv.ParseUint(listenPort, 10, 16)
	if internal.Node.ListenAddress != cfg.Node.ListenAddress || internal.Node.AdvertisePort == nil ||
		uint16(*internal.Node.AdvertisePort) != uint16(advertised) || internal.Node.ProxyAddress != "" ||
		cfg.Node.IdentityIntentsPath != nil || cfg.RPC.Enabled || !internal.Node.AllowPrivatePeers {
		t.Fatal("fixture listener/proxy/dependency configuration escaped loopback stand")
	}
	dir := cfg.Node.ChatLogDir
	if !filepath.IsAbs(dir) || cfg.Node.DownloadDir != filepath.Join(dir, "downloads") {
		t.Fatal("fixture storage is not in its fresh absolute data directory")
	}
	for _, state := range []string{cfg.Node.TrustStorePath, cfg.Node.PeersStatePath, cfg.Node.StateDBPath} {
		if filepath.Dir(state) != dir {
			t.Fatal("fixture state escaped its fresh data directory", state)
		}
		if _, err := os.Stat(state); !os.IsNotExist(err) {
			t.Fatal("fixture would load existing trust/peers/database state", state, err)
		}
	}
	if filepath.Dir(cfg.Node.IdentityPath) != dir {
		t.Fatal("fixture identity escaped its fresh data directory")
	}
}
func ownDMRuntimeWait(t *testing.T, why string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal(why)
}
func ownDMRuntimeContact(t *testing.T, r, peer *Runtime) {
	t.Helper()
	_, body := peer.nodeService.SelfIdentityRecord()
	reply := r.nodeService.HandleLocalFrame(protocol.Frame{Type: "import_contacts", Contacts: []protocol.ContactFrame{{Address: peer.Address(), PubKey: string(body.PubKey), BoxKey: string(body.BoxKey), BoxSig: string(body.BoxSig)}}})
	if reply.Type != "contacts_imported" {
		t.Fatal("ordinary contact import", reply.Type, reply.Code)
	}
}
func ownDMRuntimeMetadata(t *testing.T, cfg Config) map[string]domain.IdentityPinMetadata {
	t.Helper()
	path := filepath.Join(filepath.Dir(cfg.Node.PeersStatePath), "secure-sessions-"+config.PortSuffix(cfg.Node.ListenAddress)+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var image struct {
		Metadata map[string]domain.IdentityPinMetadata `json:"identity_metadata"`
	}
	if err := json.Unmarshal(raw, &image); err != nil {
		t.Fatal(err)
	}
	return image.Metadata
}
func TestOwnDMRuntimeOriginalSDKSQLiteAndNonContactHopR(t *testing.T) {
	// No clock/record/route injection. X is ordinary K; Y is a genuine non-K
	// next hop. The unchanged SDK send traverses the actual original Runtime.
	xCfg := ownDMRuntimeConfig(t, []string{})
	yCfg := ownDMRuntimeConfig(t, []string{xCfg.Node.ListenAddress})
	cCfg := ownDMRuntimeConfig(t, []string{yCfg.Node.ListenAddress})
	if xCfg.Node.ListenAddress == yCfg.Node.ListenAddress || xCfg.Node.ListenAddress == cCfg.Node.ListenAddress || yCfg.Node.ListenAddress == cCfg.Node.ListenAddress ||
		xCfg.Node.ChatLogDir == yCfg.Node.ChatLogDir || xCfg.Node.ChatLogDir == cCfg.Node.ChatLogDir || yCfg.Node.ChatLogDir == cCfg.Node.ChatLogDir {
		t.Fatal("fixture requires three distinct original loopback listeners and fresh stores")
	}
	ownDMRuntimeAssertLoopbackConfig(t, xCfg, []string{})
	ownDMRuntimeAssertLoopbackConfig(t, yCfg, []string{xCfg.Node.ListenAddress})
	ownDMRuntimeAssertLoopbackConfig(t, cCfg, []string{yCfg.Node.ListenAddress})
	x, err := New(xCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = x.Close() })
	if err := x.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	y, err := New(yCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = y.Close() })
	if err := y.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	c, err := newWithOwnDMRuntime(t.Context(), cCfg, ownDMRuntimeProfile())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Existing immutable constructor facts, not a fabricated record receipt.
	for _, runtime := range []*Runtime{x, y, c} {
		state, err := runtime.nodeService.DatagramRuntimeState(t.Context())
		if err != nil || !state.ConfiguredEnabled || state.State != domain.DatagramStateInitialized || state.Identity != domain.PeerIdentityFromWire(runtime.Address()) {
			t.Fatal("original Runtime datagram prerequisite missing", state, err)
		}
	}
	startup := time.Now()
	ownDMRuntimeContact(t, c, x)
	ownDMRuntimeContact(t, x, c)
	target := domain.PeerIdentityFromWire(x.Address())
	hop := domain.PeerIdentityFromWire(y.Address())
	ownDMRuntimeWait(t, "actual C→Y→X route not published", func() bool {
		r := c.nodeService.RoutingSnapshot().BestRoute(target)
		return r != nil && r.NextHop == hop && r.Hops == 2
	})
	yRecord, _ := y.nodeService.SelfIdentityRecord()
	yBody, err := protocol.VerifyIdentityRecord(yRecord, domain.NetworkID("gazeta-devnet"), hop)
	if err != nil {
		t.Fatal(err)
	}
	if version, present := yBody.MinSession.Version(); !present || version != domain.SessionVersionSecure {
		t.Fatal("real Y record did not declare min2")
	}
	// The actual datagram plane enables its initial authenticated signed push.
	// This wider key view alone is not a signed-record receipt: Hello can also
	// supply keys. The later R assertion still requires the strict worker to
	// find and verify the genuine current signed min2 record in the trust store.
	ownDMRuntimeWait(t, "genuine Y key material missing", func() bool {
		// FetchContacts is trusted-only; observing non-K Y uses the existing
		// read-only wider local view, without importing Y or minting a proof.
		reply := c.nodeService.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
		if reply.Type != "contacts" {
			return false
		}
		for _, contact := range reply.Contacts {
			if contact.Address == hop.String() {
				return contact.PubKey == string(yBody.PubKey) && contact.BoxKey == string(yBody.BoxKey) && contact.BoxSig == string(yBody.BoxSig)
			}
		}
		return false
	})
	trusted := c.nodeService.HandleLocalFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	for _, contact := range trusted.Contacts {
		if contact.Address == hop.String() {
			t.Fatal("Y became K before own send")
		}
	}
	if stats := c.nodeService.SecureSessionStoreStats(); stats.RelationAllocations != 0 || stats.RelationConfirmed != 0 {
		t.Fatal("R appeared before any own send", stats)
	}
	// Real fixed startup window, no private clock; use the test deadline as the
	// outer bound. It is part of this finite light integration, not a load run.
	if remaining := time.Minute + time.Second - time.Since(startup); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	sent, err := c.SendDirectMessage(t.Context(), x.Address(), "original SDK to R through genuine Y")
	if err != nil || sent == nil {
		t.Fatal("ordinary SDK echo", err)
	}
	ownDMRuntimeWait(t, "actual X SQLite delivery missing", func() bool {
		row, err := x.client.ChatlogGateway().Store().ReadLastEntry(t.Context(), "dm", domain.PeerIdentityFromWire(c.Address()))
		return err == nil && row != nil && row.ID == sent.ID
	})
	row, err := c.client.ChatlogGateway().Store().ReadLastEntry(t.Context(), "dm", target)
	if err != nil || row == nil || row.ID != sent.ID || row.RowID <= 0 || row.Body == "" || row.Body == sent.Body {
		t.Fatal("original sender SQLite/echo mismatch", err)
	}
	xrow, err := x.client.ChatlogGateway().Store().ReadLastEntry(t.Context(), "dm", domain.PeerIdentityFromWire(c.Address()))
	if err != nil || xrow == nil || xrow.Body != row.Body {
		t.Fatal("actual relayed ciphertext differs", err)
	}
	id, err := identity.Load(cCfg.Node.IdentityPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := directmsg.VerifyEnvelope(c.Address(), identity.PublicKeyBase64(id.PublicKey), x.Address(), row.Body); err != nil {
		t.Fatal("SQLite ciphertext original signature", err)
	}
	ownDMRuntimeWait(t, "original own writer ACK never earned non-K Y R", func() bool {
		stats := c.nodeService.SecureSessionStoreStats()
		return stats.RelationAllocations == 1 && stats.RelationConfirmed == 1
	})
	metadata := ownDMRuntimeMetadata(t, cCfg)
	if m := metadata[hop.String()]; m.Contact.State != domain.IdentityPinAllocationAbsent || m.Relation.Origin != domain.IdentityPinOriginProvenNextHop || m.Relation.State != domain.IdentityPinAllocationEligible {
		t.Fatal("Y R original provenance/class wrong", m)
	}
	if metadata[target.String()].Relation.State != domain.IdentityPinAllocationAbsent {
		t.Fatal("X K was falsely counted as outgoing R")
	}
	if err := c.Close(); err != nil {
		t.Fatal("original Runtime Close", err)
	}
	if err := c.Wait(); err != nil {
		t.Fatal("original Runtime Wait", err)
	}
	if _, err := c.FinalTransport(); err != nil {
		t.Fatal("original managed transport did not join", err)
	}
	if _, err := c.database.Executor().ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("original SQLite remained open after Close")
	}
}
func TestOwnDMRuntimeCloseBeforeStartAndConstructorFailure(t *testing.T) {
	cfg := testRuntimeConfig(t)
	r, err := newWithOwnDMRuntime(t.Context(), cfg, ownDMRuntimeProfile())
	if err != nil {
		t.Fatal(err)
	}
	if r.cancelConstruction == nil || r.started {
		t.Fatal("candidate lost original prepared lifetime")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.ValueOf(r.nodeService).Elem().FieldByName("backgroundClosed").Bool() {
		t.Fatal("beforeStart Close left original admission open")
	}
	if err := r.Close(); err != nil {
		t.Fatal("idempotent Close", err)
	}
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("closed original restarted")
	}
	if _, err := r.database.Executor().ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("never-started SQLite remained open")
	}
	for _, kind := range []string{"profile", "rpc"} {
		t.Run(kind, func(t *testing.T) {
			cfg := testRuntimeConfig(t)
			p := ownDMRuntimeProfile()
			if kind == "profile" {
				p.Residence.MaxPrestage = 0
			} else {
				cfg.RPC.Enabled = true
				cfg.RPC.Username = "operator"
				cfg.RPC.Password = ""
			}
			got, err := newWithOwnDMRuntime(t.Context(), cfg, p)
			if got != nil || err == nil {
				t.Fatal("failed constructor escaped Runtime")
			}
			if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); err == nil {
				t.Fatal("failed paired constructor leaked SQLite WAL")
			}
		})
	}
}

func TestOwnDMRuntimeOrdinaryNilProfileClosesUnstartedOriginalGate(t *testing.T) {
	r, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.ValueOf(r.nodeService).Elem().FieldByName("backgroundClosed").Bool() {
		t.Fatal("normal never-started Runtime left admission open")
	}
	if _, err := r.database.Executor().ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("normal never-started Runtime left SQLite open")
	}
}
