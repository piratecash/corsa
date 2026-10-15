package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dtype_set_guard_test.go pins the EXACT set of datagram types this build
// handles and declares. The set is a wire statement: every neighbour reads it
// from hello/welcome and every holder of our identity record reads it from
// there, and a sender routes a type only to a node that declared it. Growing
// or shrinking it changes what the network may send us, so it is not an
// implementation detail that may drift with a refactor.
//
// The names are spelled out literally rather than through the domain
// constants on purpose: renaming a constant's VALUE is exactly the kind of
// change this guard exists to catch.

// releasedDTypes is the complete set of datagram types this build ships.
var releasedDTypes = []domain.DType{
	"dm_control",
	"get_identity",
	"post_identity",
	"push_identity",
}

// dtypeSetChangeRule is attached to every failure so whoever trips the guard
// learns why it exists instead of just updating the list.
const dtypeSetChangeRule = "the set of released datagram types is fixed: adding or removing a dtype is " +
	"not allowed until the protocol owner has decided on it, and a new dtype is declared only in the " +
	"pull request that releases it — together with this guard, docs/protocol/datagram.md and " +
	"docs/protocol/identity-lookup.md"

// TestDatagramTypeRegistryHoldsExactlyTheReleasedSet guards the production
// registry as NewService assembles it — not a hand-built copy — so a new
// register call anywhere on the construction path turns it red.
func TestDatagramTypeRegistryHoldsExactlyTheReleasedSet(t *testing.T) {
	t.Parallel()

	for _, nodeType := range []domain.NodeType{config.NodeTypeFull, config.NodeTypeClient} {
		t.Run(string(nodeType), func(t *testing.T) {
			t.Parallel()

			svc := newDatagramLayerServiceOfType(t, nodeType)
			requireExactDTypeSet(t, "type registry", svc.datagramLayer().types.DTypes())
		})
	}
}

// TestNodeDeclaresExactlyTheReleasedDTypeSet guards what the node SENDS:
// the dtypes field of its own hello and welcome, and the dtypes list inside
// the signed body of its own identity record. It inspects only outgoing
// artifacts; how a peer's declaration is read is deliberately untouched.
func TestNodeDeclaresExactlyTheReleasedDTypeSet(t *testing.T) {
	t.Parallel()

	for _, nodeType := range []domain.NodeType{config.NodeTypeFull, config.NodeTypeClient} {
		t.Run(string(nodeType), func(t *testing.T) {
			t.Parallel()

			svc := newDatagramLayerServiceOfType(t, nodeType)
			requireExactDTypeSet(t, "hello dtypes", declaredFrameDTypes(t, svc.nodeHelloFrame()))
			requireExactDTypeSet(t, "welcome dtypes", declaredFrameDTypes(t, svc.welcomeFrame("challenge", "")))
			record, _ := svc.SelfIdentityRecord()
			requireExactDTypeSet(t, "identity record dtypes", signedRecordBodyDTypes(t, record))
		})
	}
}

// TestEnsureSelfIdentityRecordRefusesTooManyDTypes: a declared set larger
// than a record may carry is an issue-time error, and nothing reaches the
// trust store — a record peers would read as "no type at all" is never
// persisted, hence never published.
func TestEnsureSelfIdentityRecordRefusesTooManyDTypes(t *testing.T) {
	store, _ := newRecordTestStore(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	names := make([]domain.DType, domain.MaxIdentityRecordDTypes+1)
	for i := range names {
		names[i] = domain.DType(fmt.Sprintf("t%d", i))
	}
	spec := selfRecordSpec{
		network: testRecordStoreNetwork,
		dm:      true,
		dtypes:  domain.ExplicitDTypes(names),
	}

	_, body, err := ensureSelfIdentityRecord(store, owner, spec, time.Unix(1780000000, 0))
	if !errors.Is(err, protocol.ErrIdentityRecordTooManyDTypes) {
		t.Fatalf("err = %v (issued dtypes %s/%d), want ErrIdentityRecordTooManyDTypes",
			err, body.DTypes.Declaration(), body.DTypes.Len())
	}
	ownerID, err := domain.ParsePeerIdentity(owner.Address)
	if err != nil {
		t.Fatalf("parse owner address: %v", err)
	}
	if _, _, stored := store.recordFor(spec.network, ownerID); stored {
		t.Fatal("a refused self record reached the trust store")
	}
}

func requireExactDTypeSet(t *testing.T, where string, got []domain.DType) {
	t.Helper()
	sorted := slices.Clone(got)
	slices.Sort(sorted)
	if !slices.Equal(sorted, releasedDTypes) {
		t.Fatalf("%s = %v, want exactly %v: %s", where, sorted, releasedDTypes, dtypeSetChangeRule)
	}
}

// declaredFrameDTypes reads the dtypes field the way it travels: the frame is
// rendered to its wire line and parsed back, so a field the encoder drops is
// seen as missing rather than read from the struct.
func declaredFrameDTypes(t *testing.T, frame protocol.Frame) []domain.DType {
	t.Helper()
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal %s frame: %v", frame.Type, err)
	}
	var wire struct {
		DTypes *[]domain.DType `json:"dtypes"`
	}
	if err := json.Unmarshal([]byte(line), &wire); err != nil {
		t.Fatalf("decode %s frame line: %v", frame.Type, err)
	}
	if wire.DTypes == nil {
		t.Fatalf("%s frame carries no dtypes field: %s", frame.Type, dtypeSetChangeRule)
	}
	return *wire.DTypes
}

// signedRecordBodyDTypes reads the dtypes list from the SIGNED body bytes —
// what a receiver verifies — rather than from the parsed projection, which
// would read an over-long list as absent and hide what was actually signed.
func signedRecordBodyDTypes(t *testing.T, record protocol.SignedIdentityRecord) []domain.DType {
	t.Helper()
	var body struct {
		DTypes *[]domain.DType `json:"dtypes"`
	}
	if err := json.Unmarshal(record.Body, &body); err != nil {
		t.Fatalf("decode self record body: %v", err)
	}
	if body.DTypes == nil {
		t.Fatalf("self record carries no dtypes field: %s", dtypeSetChangeRule)
	}
	return *body.DTypes
}
