package node

import (
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_handshake_test.go covers §9 lines 1125–1129 and the raw
// advertised-capability contract of §2.2:
//
//   - the `dtypes` field IS the set: an absent one names no type, so the peer
//     is an endpoint for none;
//   - an explicitly empty field means the EMPTY set: the envelope is
//     understood, no type is handled. That is what a node with an empty type
//     registry declares, and it advertises mesh_datagram_v1 all the same;
//   - duplicates collapse; the field is a set, not a list;
//   - a bounds breach drops the field to its ABSENT form — which names no type
//     either — WITHOUT tearing the handshake down;
//   - a capability name outside the compile-time set survives in the raw set,
//     while the typed set stays exactly what it was;
//   - a bounds breach empties the WHOLE raw set, keeps the session and leaves
//     the typed capabilities untouched.

// ---------------------------------------------------------------------------
// Local advertisement (§6)
// ---------------------------------------------------------------------------

// TestLocalDatagramAdvertise_SilentWithoutTheFlag pins the operator opt-out.
// The flag ships ON (config.enableDatagramV1FromEnv, CORSA_ENABLE_DATAGRAM_V1),
// so this is the state of a node whose operator turned the plane OFF: it
// advertises neither capability, and no peer ever sends it a datagram.
func TestLocalDatagramAdvertise_SilentWithoutTheFlag(t *testing.T) {
	t.Parallel()

	svc := &Service{cfg: config.Node{Type: config.NodeTypeFull}}
	if got := svc.localDatagramAdvertise(); got.Endpoint || got.Transit {
		t.Fatalf("localDatagramAdvertise() = %+v with the flag unset, want both false", got)
	}
	caps := localCapabilities(true, svc.localDatagramAdvertise())
	for _, capability := range caps {
		if capability == domain.CapMeshDatagramV1 || capability == domain.CapMeshDatagramTransitV1 {
			t.Fatalf("localCapabilities advertised %q with the datagram flag off", capability)
		}
	}
}

// TestLocalDatagramAdvertise_TransitOnlyOnFullNode pins the §6 split: the
// transit capability is a promise to carry OTHER people's datagrams, and a
// client node does not forward. Advertising it there would strand frames on a
// node that never relays them.
func TestLocalDatagramAdvertise_TransitOnlyOnFullNode(t *testing.T) {
	t.Parallel()

	full := newDatagramServiceWithTypes(t, config.NodeTypeFull)
	if got := full.localDatagramAdvertise(); !got.Endpoint || !got.Transit {
		t.Fatalf("full node advertise = %+v, want endpoint and transit", got)
	}
	client := newDatagramServiceWithTypes(t, config.NodeTypeClient)
	got := client.localDatagramAdvertise()
	if !got.Endpoint {
		t.Fatal("a client node must still advertise mesh_datagram_v1: it has to be able to receive what is addressed to it")
	}
	if got.Transit {
		t.Fatal("a client node must never advertise mesh_datagram_transit_v1: it does not forward")
	}

	caps := localCapabilities(false, client.localDatagramAdvertise())
	if !capsContain(caps, domain.CapMeshDatagramV1) {
		t.Fatalf("client capabilities %v missing mesh_datagram_v1", caps)
	}
	if capsContain(caps, domain.CapMeshDatagramTransitV1) {
		t.Fatalf("client capabilities %v advertise transit", caps)
	}
	// The plane must not ride on the routing/relay capabilities (§6).
	for _, capability := range localCapabilities(false, datagramAdvertise{Endpoint: true, Transit: true}) {
		if capability == domain.CapMeshRoutingV3 {
			t.Fatal("the datagram advertise pulled in a routing capability: the two planes must stay independent")
		}
	}
}

// TestNodeWithAnEmptyRegistryAdvertisesTheEnvelopeAndDeclaresTheEmptySet is
// the amendment of §6 / §6.1 on the emit side.
//
// mesh_datagram_v1 states exactly one thing (§6): this node understands the
// envelope and accepts a datagram addressed to it instead of answering
// unknown_command and closing. That is true with an empty registry, so it is
// advertised with an empty registry — and `dtypes` makes the OTHER statement,
// honestly, as an explicitly EMPTY set (§6.1): no handler for any type at all.
//
// Withholding the capability instead was not conservative but fatal: the
// candidate filter demands mesh_datagram_v1 from EVERY candidate, transit
// included (§2.2 rule 2), so a network of PR-0 nodes could carry nothing at
// all.
func TestNodeWithAnEmptyRegistryAdvertisesTheEnvelopeAndDeclaresTheEmptySet(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must build the layer")
	}
	if len(svc.datagramLayer().types.DTypes()) != 0 {
		t.Fatal("PR-0 ships an empty registry; this test is about exactly that state")
	}

	advertise := svc.localDatagramAdvertise()
	if !advertise.Endpoint {
		t.Fatal("a node with the layer wired must advertise mesh_datagram_v1: it does understand the envelope, and without the name no peer may even relay through it")
	}
	if got := svc.localDatagramDTypes(); len(got) != 0 {
		t.Fatalf("localDatagramDTypes = %v, want the (empty) registry contents", got)
	}
	field := svc.localDTypeStrings(advertise)
	if field == nil {
		t.Fatal("an empty registry emitted no dtypes field: an endpoint states its set, and \"said it handles nothing\" is a different fact about a peer than \"said nothing\"")
	}
	if len(*field) != 0 {
		t.Fatalf("declared dtypes = %v, want the explicitly empty set", *field)
	}
	// The capability really reaches the wire list.
	if !capsContain(localCapabilities(false, advertise), domain.CapMeshDatagramV1) {
		t.Fatal("mesh_datagram_v1 missing from the capability list of a node that speaks the envelope")
	}
	// And a neighbour reads that field as "no types".
	declared := datagramDeclaredDTypes(declarationsFromHandshake(protocol.Frame{DTypes: field}))
	for _, dtype := range fixtureDatagramTypes() {
		if declared.Supports(dtype) {
			t.Fatalf("a peer read the explicitly empty set as supporting %q", dtype)
		}
	}

	// A registry with types in it emits them, in full — an endpoint never
	// omits the field, whatever the set happens to be.
	registerFixtureDatagramTypes(t, svc)
	registerDatagramType(t, svc, domain.DType("file_transfer"), domain.DatagramModeRouted)
	advertise = svc.localDatagramAdvertise()
	if !advertise.Endpoint {
		t.Fatal("a node carrying a type kit must advertise mesh_datagram_v1")
	}
	names := svc.localDTypeStrings(advertise)
	if names == nil {
		t.Fatal("an endpoint emitted an absent field: the peer would learn about none of its types")
	}
	if len(*names) != len(fixtureDatagramTypes())+1 {
		t.Fatalf("declared %v, want every registered type", *names)
	}
	if !containsString(*names, "file_transfer") {
		t.Fatalf("declared %v, missing the type the node really handles", *names)
	}
}

// newDatagramServiceWithTypes builds a node with the plane on AND a kit of
// types registered — the state in which its `dtypes` declaration is non-empty.
func newDatagramServiceWithTypes(t *testing.T, nodeType domain.NodeType) *Service {
	t.Helper()
	svc := newDatagramLayerServiceOfType(t, nodeType)
	registerFixtureDatagramTypes(t, svc)
	return svc
}

// fixtureDatagramTypeModes is the kit the fixtures register. It is a FIXTURE
// and nothing more: the production registry is empty, and no set of names is
// implied by anything on the wire — a peer is an endpoint for exactly what it
// listed (§6.1). The four names are kept because they cover all three modes
// plus the request/response pairing the registry validates.
var fixtureDatagramTypeModes = map[domain.DType]domain.DatagramMode{
	"get_identity":    domain.DatagramModeRequest,
	"post_identity":   domain.DatagramModeResponse,
	"cached_identity": domain.DatagramModeResponse,
	"push_identity":   domain.DatagramModeRouted,
}

// fixtureDatagramTypes lists the kit in a stable order.
func fixtureDatagramTypes() []domain.DType {
	out := make([]domain.DType, 0, len(fixtureDatagramTypeModes))
	for dtype := range fixtureDatagramTypeModes {
		out = append(out, dtype)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// registerFixtureDatagramTypes registers the kit with inert handlers, which is
// what makes a fixture node declare a non-empty `dtypes` set rather than an
// explicitly empty one.
func registerFixtureDatagramTypes(t *testing.T, svc *Service) {
	t.Helper()
	for _, dtype := range fixtureDatagramTypes() {
		if _, already := svc.datagramLayer().types.Lookup(dtype); already {
			continue
		}
		registerDatagramType(t, svc, dtype, fixtureDatagramTypeModes[dtype], fixtureAnswersTo(dtype)...)
	}
}

// fixtureAnswersTo pairs the two response types of the kit with the requests
// they answer; the registry refuses a `response` type that names none.
func fixtureAnswersTo(dtype domain.DType) []domain.DType {
	switch dtype {
	case "cached_identity", "post_identity":
		return []domain.DType{"get_identity"}
	default:
		return nil
	}
}

func registerDatagramType(
	t *testing.T,
	svc *Service,
	dtype domain.DType,
	mode domain.DatagramMode,
	answersTo ...domain.DType,
) {
	t.Helper()
	err := svc.datagramLayer().types.Register(datagram.TypeRegistration{
		DType:     dtype,
		Modes:     []domain.DatagramMode{mode},
		AnswersTo: answersTo,
		Classes:   []domain.DatagramClass{domain.DatagramClassControl},
		Payload:   datagram.PayloadSchema{Name: dtype.String(), Version: 1},
		// The baseline kit authenticates its sender inside the payload (§7), so
		// these types are served on every direction. Stating it is what keeps
		// them reachable on a node that only dials out.
		SenderProof: datagram.SenderProvenInPayload,
		Handler: datagram.HandlerFunc(func(
			context.Context, datagram.DeliveryContext, []byte,
		) datagram.HandlerResult {
			return datagram.AcceptDelivery()
		}),
	})
	if err != nil {
		t.Fatalf("register %q: %v", dtype, err)
	}
}

func containsString(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

// TestDatagramCapabilitiesParse pins the wire names and their presence in
// ParseCapability — without it the negotiated set would silently drop them
// and every gate would read "peer does not support datagrams".
func TestDatagramCapabilitiesParse(t *testing.T) {
	t.Parallel()

	for _, want := range []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1} {
		got, ok := domain.ParseCapability(want.String())
		if !ok || got != want {
			t.Fatalf("ParseCapability(%q) = (%q, %v), want (%q, true)", want, got, ok, want)
		}
	}
	if domain.CapMeshDatagramV1.String() != "mesh_datagram_v1" ||
		domain.CapMeshDatagramTransitV1.String() != "mesh_datagram_transit_v1" {
		t.Fatal("the wire names of the datagram capabilities changed — every peer on the network disagrees now")
	}
}

// ---------------------------------------------------------------------------
// dtypes wire contract (§6.1, §9 lines 1125–1129)
// ---------------------------------------------------------------------------

// TestHandshakeDTypes_AbsentDeclaresNothing is one half of the closed
// contract: a missing field names no type, so the peer is an endpoint for
// none. The wire still tells the absent field from an explicitly empty one —
// that distinction is real and parsed — but the LAYER reads both the same way,
// because §6.1 makes unproven support equal to no support.
//
// This replaces the draft rule under which an absent field meant a set of four
// types: every v27 peer advertising mesh_datagram_v1 was then an endpoint for
// handlers no build implements.
func TestHandshakeDTypes_AbsentDeclaresNothing(t *testing.T) {
	t.Parallel()

	declarations := declarationsFromHandshake(protocol.Frame{Type: "welcome"})
	if got := declarations.DeclaredDTypes.Declaration(); got != domain.DTypeDeclarationAbsent {
		t.Fatalf("DeclaredDTypes = %s, want %s", got, domain.DTypeDeclarationAbsent)
	}
	declared := datagramDeclaredDTypes(declarations)
	for _, dtype := range fixtureDatagramTypes() {
		if declared.Supports(dtype) {
			t.Fatalf("%q read as supported from a field that was never sent", dtype)
		}
	}
	if declared.Supports(domain.DType("chunk_response")) {
		t.Fatal("an undeclared type must NOT be assumed supported: unproven support equals no support")
	}
}

// TestHandshakeDTypes_EmptyMeansNoTypes is the other half, and the one the
// amendment added. An explicitly empty array is NOT the absent field: it says
// the peer speaks the envelope and has a handler for nothing — the statement
// a node with an empty type registry has to make, and the one that used to be
// inexpressible, forcing such a node to withhold mesh_datagram_v1 instead.
func TestHandshakeDTypes_EmptyMeansNoTypes(t *testing.T) {
	t.Parallel()

	declarations := declarationsFromHandshake(protocol.Frame{Type: "welcome", DTypes: &[]string{}})
	if got := declarations.DeclaredDTypes.Declaration(); got != domain.DTypeDeclarationExplicit {
		t.Fatalf("DeclaredDTypes = %s, want %s: an empty array is a statement, not a missing field",
			got, domain.DTypeDeclarationExplicit)
	}
	declared := datagramDeclaredDTypes(declarations)
	for _, dtype := range fixtureDatagramTypes() {
		if declared.Supports(dtype) {
			t.Fatalf("%q read as supported: an empty set promises no handler at all", dtype)
		}
	}
	if declared.Supports(domain.DType("chunk_response")) {
		t.Fatal("an undeclared type read as supported from an empty set")
	}
}

// TestHandshakeDTypes_DuplicatesCollapse pins the set semantics: order does
// not matter and a repeat is not an error, because two implementations that
// disagreed on this would disagree on whether a handshake is valid at all.
func TestHandshakeDTypes_DuplicatesCollapse(t *testing.T) {
	t.Parallel()

	declarations := declarationsFromHandshake(protocol.Frame{
		DTypes: &[]string{"chunk_response", "get_identity", "chunk_response", "get_identity"},
	})
	if got := declarations.DeclaredDTypes.Len(); got != 2 {
		t.Fatalf("DeclaredDTypes = %v, want 2 distinct names", declarations.DeclaredDTypes.Types())
	}
	declared := datagramDeclaredDTypes(declarations)
	for _, dtype := range []domain.DType{"chunk_response", "get_identity"} {
		if !declared.Supports(dtype) {
			t.Fatalf("declared type %q must be supported", dtype)
		}
	}
	if declared.Supports(domain.DType("push_identity")) {
		t.Fatal("a name the list never carried must not be supported")
	}
}

// TestHandshakeDTypes_BoundsBreachDeclaresNothing covers the two bounds and
// the reaction §6.1 fixes for both: the field is ignored WHOLE — read as
// absent, hence as no type at all — and the handshake is NOT torn down.
// Refusing a connection over an extensible field would contradict the point of
// the layer.
func TestHandshakeDTypes_BoundsBreachDeclaresNothing(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, domain.MaxDTypesPerNode+1)
	for i := range tooMany {
		tooMany[i] = "type_" + strings.Repeat("a", i%8+1)
	}
	cases := map[string][]string{
		"too_many_names": tooMany,
		"name_too_long":  {"get_identity", strings.Repeat("z", domain.MaxDTypeLen+1)},
		"illegal_syntax": {"get_identity", "Chunk-Response"},
	}
	for name, field := range cases {
		t.Run(name, func(t *testing.T) {
			declarations := declarationsFromHandshake(protocol.Frame{DTypes: &field})
			if got := declarations.DeclaredDTypes.Declaration(); got != domain.DTypeDeclarationAbsent {
				t.Fatalf("a bounds breach must drop the WHOLE field to %s; got %s with %v",
					domain.DTypeDeclarationAbsent, got, declarations.DeclaredDTypes.Types())
			}
			declared := datagramDeclaredDTypes(declarations)
			for _, dtype := range fixtureDatagramTypes() {
				if declared.Supports(dtype) {
					t.Fatalf("after a bounds breach the set must be empty; %q survived", dtype)
				}
			}
			// The valid name that shared the list is dropped WITH the field:
			// half-honouring a breached list is the ambiguity the closed
			// contract exists to remove.
			if declared.Supports(domain.DType("chunk_response")) {
				t.Fatal("a name from a breached list must not survive")
			}
		})
	}
}

// TestLocalDTypeStrings_AnEndpointAlwaysEmitsItsSet pins the emit side: an
// endpoint sends the field for EVERY set it can have, because the field IS the
// set (§6.1) and there is no set whose omission says the same thing. A node
// that does not speak the envelope sends nothing, which is the one absent
// form left.
//
// The mutation this kills: reintroducing an "omit the field when the set
// equals X" shortcut — the peer would then learn about none of these types
// while being told the node handles some other set entirely.
func TestLocalDTypeStrings_AnEndpointAlwaysEmitsItsSet(t *testing.T) {
	t.Parallel()

	svc := newDatagramServiceWithTypes(t, config.NodeTypeFull)
	declared := svc.localDatagramDTypes()
	if len(declared) != len(fixtureDatagramTypes()) {
		t.Fatalf("localDatagramDTypes = %v, want the registered kit", declared)
	}

	field := svc.localDTypeStrings(datagramAdvertise{Endpoint: true})
	if field == nil {
		t.Fatal("an endpoint emitted an absent field")
	}
	if len(*field) != len(declared) {
		t.Fatalf("declared %v, want every registered type %v", *field, declared)
	}
	for _, dtype := range declared {
		if !containsString(*field, dtype.String()) {
			t.Fatalf("declared %v, missing the registered type %q", *field, dtype)
		}
	}

	if got := svc.localDTypeStrings(datagramAdvertise{}); got != nil {
		t.Fatalf("a node that does not speak the envelope declared %v", *got)
	}
}

// TestHandshakeFramesCarryDTypes walks the emit side through the real frame
// builders. With the plane off the field must be absent on BOTH frames — a
// node that does not speak the envelope declares nothing, and its handshake
// stays wire-identical to the legacy one.
func TestHandshakeFramesCarryDTypes(t *testing.T) {
	svc, _, _ := newDatagramInboundFixture(t)

	welcome := svc.welcomeFrame("challenge", "10.0.0.1")
	if welcome.DTypes != nil {
		t.Fatalf("welcome.dtypes = %v with the plane off, want absent", *welcome.DTypes)
	}
	line, err := protocol.MarshalFrameLine(welcome)
	if err != nil {
		t.Fatalf("MarshalFrameLine(welcome): %v", err)
	}
	if strings.Contains(line, `"dtypes"`) {
		t.Fatalf("welcome line carries a dtypes field with the plane off: %s", line)
	}

	hello := svc.nodeHelloJSONLine()
	if strings.Contains(hello, `"dtypes"`) {
		t.Fatalf("hello line carries a dtypes field with the plane off: %s", hello)
	}
}

// TestHandshakeFramesCarryTheEmptySetOfAnEmptyRegistry is the wire half of the
// amendment: PR-0's own state — the plane on, the registry empty — has to
// reach the peer as `"dtypes": []` on BOTH handshake frames.
//
// It goes through MarshalFrameLine rather than through the struct, because
// this is exactly where the old shape lost the statement: a []string with
// omitempty serialises the empty set and the absent field to the same bytes.
func TestHandshakeFramesCarryTheEmptySetOfAnEmptyRegistry(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	if len(svc.localDatagramDTypes()) != 0 {
		t.Fatal("the fixture must ship the empty PR-0 registry")
	}

	welcome := svc.welcomeFrame("challenge", "10.0.0.1")
	line, err := protocol.MarshalFrameLine(welcome)
	if err != nil {
		t.Fatalf("MarshalFrameLine(welcome): %v", err)
	}
	hello := svc.nodeHelloJSONLine()
	for name, frameLine := range map[string]string{"welcome": line, "hello": hello} {
		if !strings.Contains(frameLine, `"dtypes":[]`) {
			t.Fatalf("%s line does not carry the explicitly empty set: %s", name, frameLine)
		}
		parsed, err := protocol.ParseFrameLine(strings.TrimSpace(frameLine))
		if err != nil {
			t.Fatalf("ParseFrameLine(%s): %v", name, err)
		}
		if got := declarationsFromHandshake(parsed).DeclaredDTypes.Declaration(); got != domain.DTypeDeclarationExplicit {
			t.Fatalf("%s: the receiver read the empty array as %s, not as a field that was sent", name, got)
		}
		declared := datagramDeclaredDTypes(declarationsFromHandshake(parsed))
		for _, dtype := range fixtureDatagramTypes() {
			if declared.Supports(dtype) {
				t.Fatalf("%s: the receiver read the empty array as supporting %q", name, dtype)
			}
		}
	}
}

// TestDTypesWireContract covers the four wire cases of §6.1 end to end —
// emitted bytes, parsed frame, session declaration — because the contract is
// closed and every one of the four is a different statement:
//
//   - absent: the field was never sent, so no type is named;
//   - empty: the field was sent and names no type — a different STATEMENT
//     from the absent one, read by the layer the same way;
//   - duplicates: a set, so they collapse without being an error;
//   - a bounds breach: the WHOLE field drops back to absent, and the
//     handshake survives.
func TestDTypesWireContract(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, domain.MaxDTypesPerNode+1)
	for i := range tooMany {
		tooMany[i] = "type_" + strings.Repeat("a", i%8+1)
	}

	cases := map[string]struct {
		field           *[]string
		wantOnWire      string
		wantDeclaration domain.DTypeDeclaration
		wantSupported   []domain.DType
		wantRefused     []domain.DType
	}{
		"absent": {
			field:           nil,
			wantOnWire:      "",
			wantDeclaration: domain.DTypeDeclarationAbsent,
			wantRefused:     []domain.DType{"get_identity", "push_identity", "chunk_response"},
		},
		"empty": {
			field:           &[]string{},
			wantOnWire:      `"dtypes":[]`,
			wantDeclaration: domain.DTypeDeclarationExplicit,
			wantRefused:     []domain.DType{"get_identity", "chunk_response"},
		},
		"duplicates collapse": {
			field:           &[]string{"chunk_response", "chunk_response"},
			wantOnWire:      `"dtypes":["chunk_response","chunk_response"]`,
			wantDeclaration: domain.DTypeDeclarationExplicit,
			wantSupported:   []domain.DType{"chunk_response"},
			wantRefused:     []domain.DType{"get_identity"},
		},
		"bounds breach": {
			field: &tooMany,
			// The breached field IS on the wire — a hostile or buggy peer
			// sent it. What the contract fixes is the READING of it.
			wantOnWire:      `"dtypes":["type_a"`,
			wantDeclaration: domain.DTypeDeclarationAbsent,
			wantRefused:     []domain.DType{"type_a", "get_identity"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			emitted, err := protocol.MarshalFrameLine(protocol.Frame{Type: "welcome", DTypes: tc.field})
			if err != nil {
				t.Fatalf("MarshalFrameLine: %v", err)
			}
			if tc.wantOnWire == "" && strings.Contains(emitted, `"dtypes"`) {
				t.Fatalf("the field reached the wire although it is absent: %s", emitted)
			}
			if tc.wantOnWire != "" && !strings.Contains(emitted, tc.wantOnWire) {
				t.Fatalf("wire form = %s, want it to contain %s", emitted, tc.wantOnWire)
			}

			parsed, err := protocol.ParseFrameLine(strings.TrimSpace(emitted))
			if err != nil {
				t.Fatalf("ParseFrameLine: %v — a bounds breach must never fail the handshake", err)
			}
			declarations := declarationsFromHandshake(parsed)
			if got := declarations.DeclaredDTypes.Declaration(); got != tc.wantDeclaration {
				t.Fatalf("Declaration() = %s, want %s", got, tc.wantDeclaration)
			}
			if got := declarations.DeclaredDTypes.Len(); got > 1 {
				t.Fatalf("declared set holds %d names: duplicates did not collapse", got)
			}
			declared := datagramDeclaredDTypes(declarations)
			for _, dtype := range tc.wantSupported {
				if !declared.Supports(dtype) {
					t.Fatalf("%q must be supported", dtype)
				}
			}
			for _, dtype := range tc.wantRefused {
				if declared.Supports(dtype) {
					t.Fatalf("%q must not be supported", dtype)
				}
			}
		})
	}

	// A peer may also send `null`, which our own serializer never produces.
	// It reads as ABSENT — the conservative reading, and the only one that
	// does not turn a malformed field into "no handlers".
	nulled, err := protocol.ParseFrameLine(`{"type":"welcome","dtypes":null}`)
	if err != nil {
		t.Fatalf(`ParseFrameLine with "dtypes":null: %v`, err)
	}
	if got := declarationsFromHandshake(nulled).DeclaredDTypes.Declaration(); got != domain.DTypeDeclarationAbsent {
		t.Fatalf(`"dtypes":null read as %s, want an absent field`, got)
	}
	if datagramDeclaredDTypes(declarationsFromHandshake(nulled)).Supports(domain.DType("get_identity")) {
		t.Fatal(`"dtypes":null must name no type at all`)
	}
}

// ---------------------------------------------------------------------------
// Raw advertised capability set (§2.2, §9 line 1126)
// ---------------------------------------------------------------------------

// TestRawCapabilitySet_KeepsUnknownName is the reason the raw set exists at
// all: intersectCapabilities drops every name this build does not know, so a
// question about a name released next year would have nothing to match
// against. The raw set keeps it and the typed set does not change.
func TestRawCapabilitySet_KeepsUnknownName(t *testing.T) {
	t.Parallel()

	const futureName = "mesh_datagram_durable_v1"
	welcome := protocol.Frame{
		Type:         "welcome",
		Capabilities: []string{domain.CapMeshRelayV1.String(), futureName},
	}

	declarations := declarationsFromHandshake(welcome)
	advertised := datagramAdvertisedCapabilities(declarations)
	if !advertised.Has(domain.CapabilityName(futureName)) {
		t.Fatalf("the raw set lost %q — a name this build never heard of must survive it", futureName)
	}
	if advertised.Has(domain.CapabilityName("mesh_datagram_absent_v1")) {
		t.Fatal("the raw set invented a name nobody advertised")
	}

	// The typed set is untouched: the unknown name is not in it, and the
	// known one still is.
	typed := intersectCapabilities(localCapabilities(false, datagramAdvertise{}), welcome.Capabilities)
	if !capsContain(typed, domain.CapMeshRelayV1) {
		t.Fatalf("typed set %v lost mesh_relay_v1", typed)
	}
	for _, capability := range typed {
		if capability.String() == futureName {
			t.Fatalf("typed set %v absorbed an unknown name — dispatch must keep running on compile-time capabilities only", typed)
		}
	}
}

// TestRawCapabilitySet_BoundsBreachEmptiesTheWholeSet pins the all-or-nothing
// reaction of §2.2. "Drop one name" and "drop the set" behave differently in
// mixed implementations, so the spec picks one: the whole set goes, the
// session survives, and the typed capabilities are not touched.
func TestRawCapabilitySet_BoundsBreachEmptiesTheWholeSet(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, domain.MaxRawCapabilityNames+1)
	for i := range tooMany {
		tooMany[i] = "cap_" + strings.Repeat("x", i%7+1)
	}
	cases := map[string][]string{
		"too_many_names": append([]string{domain.CapMeshRelayV1.String()}, tooMany...),
		"name_too_long":  {domain.CapMeshRelayV1.String(), strings.Repeat("q", domain.MaxCapabilityNameLen+1)},
		"illegal_syntax": {domain.CapMeshRelayV1.String(), "Mesh-Relay-V1"},
	}
	for name, advertisedNames := range cases {
		t.Run(name, func(t *testing.T) {
			welcome := protocol.Frame{Type: "welcome", Capabilities: advertisedNames}
			declarations := declarationsFromHandshake(welcome)
			if declarations.AdvertisedNames != nil {
				t.Fatalf("a bounds breach must empty the WHOLE raw set; got %v", declarations.AdvertisedNames)
			}
			// The valid name that shared the breached list is gone from the
			// layer's view too — the projection carries the verdict, it does
			// not re-derive a milder one.
			advertised := datagramAdvertisedCapabilities(declarations)
			if advertised.Has(domain.CapabilityName(domain.CapMeshRelayV1.String())) {
				t.Fatal("a valid raw name survived a bounds breach in the layer's set")
			}
			// The typed set is unaffected: the session keeps every
			// capability it legitimately negotiated.
			typed := intersectCapabilities(localCapabilities(false, datagramAdvertise{}), advertisedNames)
			if !capsContain(typed, domain.CapMeshRelayV1) {
				t.Fatalf("typed set %v lost mesh_relay_v1 to a raw-set bounds breach", typed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Storage: NetCore and the peerSession mirror
// ---------------------------------------------------------------------------

// TestRememberConnPeerAddr_StoresDeclarationsOnNetCore walks the inbound
// handshake into storage. NetCore is the single source of truth for live
// connection state, so the raw set and the dtype set have to land THERE and
// not in a second map keyed by the same connection.
func TestRememberConnPeerAddr_StoresDeclarationsOnNetCore(t *testing.T) {
	svc, _, connID := newDatagramInboundFixture(t)

	hello := protocol.Frame{
		Type:         "hello",
		Address:      datagramTestDstHex,
		Version:      config.ProtocolVersion,
		Capabilities: []string{domain.CapMeshRelayV1.String(), "mesh_datagram_durable_v1"},
		DTypes:       &[]string{"chunk_response", "chunk_response"},
	}
	svc.rememberConnPeerAddr(connID, hello, "10.0.0.77:64646")

	declarations := svc.connDeclarations(connID)
	if len(declarations.AdvertisedNames) != 2 {
		t.Fatalf("NetCore raw set = %v, want both advertised names", declarations.AdvertisedNames)
	}
	if declarations.DeclaredDTypes.Len() != 1 {
		t.Fatalf("NetCore dtype set = %v, want one name after the duplicate collapsed", declarations.DeclaredDTypes.Types())
	}
	if !datagramAdvertisedCapabilities(declarations).Has("mesh_datagram_durable_v1") {
		t.Fatal("the unknown capability name did not survive into NetCore storage")
	}
	if !datagramDeclaredDTypes(declarations).Supports(domain.DType("chunk_response")) {
		t.Fatal("the declared dtype did not survive into NetCore storage")
	}
	// The typed set travelled on the same ApplyOpts call and is unchanged.
	if !svc.connHasCapability(connID, domain.CapMeshRelayV1) {
		t.Fatal("the typed capability set was lost while folding in the raw declarations")
	}
}

// TestApplyWelcomeMetadata_MirrorsDeclarationsOnSession is the outbound
// counterpart: the dispatcher there is addressed by PeerAddress and has no
// ConnID, so the session carries a mirror — written from the SAME welcome
// frame as the typed set, in one pass, so the two views cannot disagree.
func TestApplyWelcomeMetadata_MirrorsDeclarationsOnSession(t *testing.T) {
	t.Parallel()

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(netcore.ConnID(4242), serverPipe, netcore.Outbound, netcore.Options{})
	t.Cleanup(pc.Close)

	session := &peerSession{address: domain.PeerAddress("addr-peer"), connID: domain.ConnID(7601), netCore: pc}
	welcome := protocol.Frame{
		Type:         "welcome",
		Address:      datagramTestDstHex,
		Capabilities: []string{domain.CapMeshRelayV1.String(), "mesh_datagram_durable_v1"},
		DTypes:       &[]string{"chunk_response"},
	}

	applyWelcomeMetadata(session, welcome, false, datagramAdvertise{Endpoint: true, Transit: true})

	if !datagramAdvertisedCapabilities(session.declarations).Has("mesh_datagram_durable_v1") {
		t.Fatalf("session mirror lost the raw name: %v", session.declarations.AdvertisedNames)
	}
	if !datagramDeclaredDTypes(session.declarations).Supports(domain.DType("chunk_response")) {
		t.Fatalf("session mirror lost the declared dtype: %v", session.declarations.DeclaredDTypes)
	}
	// The NetCore behind the session sees exactly the same thing.
	stored := pc.Declarations()
	if len(stored.AdvertisedNames) != len(session.declarations.AdvertisedNames) ||
		stored.DeclaredDTypes.Len() != session.declarations.DeclaredDTypes.Len() {
		t.Fatalf("NetCore and the session mirror disagree: %+v vs %+v", stored, session.declarations)
	}
	// The mirror is a copy: mutating what the accessor returned must not
	// reach into stored state.
	stored.AdvertisedNames[0] = "tampered"
	if pc.Declarations().AdvertisedNames[0] == "tampered" {
		t.Fatal("Declarations() handed out NetCore-owned storage")
	}
}

// TestSessionDeclarations_ReadsTheRegisteredSession pins the accessor the
// outbound side uses: it resolves a session by peer address under peerMu and
// hands back a copy, so a caller cannot reach into session state and a
// missing session degrades to the absent-field value rather than panicking.
func TestSessionDeclarations_ReadsTheRegisteredSession(t *testing.T) {
	svc, address, session := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
	applyWelcomeMetadata(session, protocol.Frame{
		Type:         "welcome",
		Address:      datagramTestDstHex,
		Capabilities: []string{"mesh_datagram_durable_v1"},
		DTypes:       &[]string{"chunk_response"},
	}, false, datagramAdvertise{Endpoint: true})

	got := svc.sessionDeclarations(address)
	if !datagramAdvertisedCapabilities(got).Has("mesh_datagram_durable_v1") {
		t.Fatalf("sessionDeclarations lost the raw name: %v", got.AdvertisedNames)
	}
	if !datagramDeclaredDTypes(got).Supports(domain.DType("chunk_response")) {
		t.Fatalf("sessionDeclarations lost the declared dtype: %v", got.DeclaredDTypes)
	}
	unknown := svc.sessionDeclarations(domain.PeerAddress("addr-never-dialled"))
	if unknown.AdvertisedNames != nil || unknown.DeclaredDTypes.Declaration() != domain.DTypeDeclarationAbsent {
		t.Fatalf("an unknown address must yield the absent-field value; got %+v", unknown)
	}
}

// TestDeclarationsNotProvidedDoNotClobber pins why Options.Declarations is a
// pointer: a validated-to-nil verdict must be applicable. An update that does
// NOT carry declarations leaves the stored ones alone; an update that carries
// an emptied set really empties it.
func TestDeclarationsNotProvidedDoNotClobber(t *testing.T) {
	t.Parallel()

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(netcore.ConnID(4243), serverPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)

	pc.SetDeclarations(netcore.HandshakeDeclarations{
		AdvertisedNames: []domain.CapabilityName{"mesh_datagram_durable_v1"},
		DeclaredDTypes:  domain.ExplicitDTypes([]domain.DType{"chunk_response"}),
	})

	pc.ApplyOpts(netcore.Options{Address: domain.PeerAddress("addr")})
	if len(pc.Declarations().AdvertisedNames) != 1 {
		t.Fatal("an unrelated ApplyOpts wiped the stored declarations")
	}

	emptied := netcore.HandshakeDeclarations{}
	pc.ApplyOpts(netcore.Options{Declarations: &emptied})
	if got := pc.Declarations(); got.AdvertisedNames != nil ||
		got.DeclaredDTypes.Declaration() != domain.DTypeDeclarationAbsent {
		t.Fatalf("a bounds-breach verdict did not reach storage: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Mixed network (§9 line 1129)
// ---------------------------------------------------------------------------

// TestMixedNetwork_LegacyPeerIsNotADatagramCandidate is the send-side half of
// the mixed-network rule, expressed at the only level M9a owns: a peer whose
// handshake carried no datagram capability fails the candidate filter, so no
// datagram is ever addressed to it. A legacy node has no such command and
// would answer unknown_command and close.
func TestMixedNetwork_LegacyPeerIsNotADatagramCandidate(t *testing.T) {
	t.Parallel()

	frame := newNodeDatagram(t, nil)

	legacy := datagramAdvertisedCapabilities(declarationsFromHandshake(protocol.Frame{
		Capabilities: []string{domain.CapMeshRelayV1.String(), domain.CapMeshRoutingV1.String()},
	}))
	decision := datagram.AdmitCandidate(frame, domain.PeerIdentityFromWire("aa"+strings.Repeat("b", 38)), legacy)
	if decision.Admitted() {
		t.Fatal("a peer without mesh_datagram_v1 was admitted as a datagram candidate")
	}
	if decision.Outcome() != datagram.CandidateMissingEndpoint {
		t.Fatalf("outcome = %v, want missing_endpoint_capability", decision.Outcome())
	}

	// An endpoint-only client is admitted as the LAST HOP but never as
	// transit — the §6 split, read off a real handshake.
	endpointOnly := datagramAdvertisedCapabilities(declarationsFromHandshake(protocol.Frame{
		Capabilities: []string{domain.CapMeshDatagramV1.String(), "mesh_datagram_durable_v1"},
	}))
	if got := datagram.AdmitCandidate(frame, frame.Dst, endpointOnly); !got.Admitted() {
		t.Fatalf("the destination itself was refused as the last hop: %v", got.Outcome())
	}
	transitCandidate := domain.PeerIdentityFromWire("cc" + strings.Repeat("d", 38))
	if got := datagram.AdmitCandidate(frame, transitCandidate, endpointOnly); got.Outcome() != datagram.CandidateMissingTransit {
		t.Fatalf("an endpoint-only peer was judged %v as transit, want missing_transit_capability", got.Outcome())
	}
}
