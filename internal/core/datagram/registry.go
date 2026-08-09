package datagram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// registry.go is the type registry of §7 — knowledge of the ENDPOINT, never a
// condition of forwarding. Transit does not look into it at all, which is what
// makes "new protocols without a network upgrade" true rather than
// aspirational: an unknown dtype is relayed untouched, and at dst == self it is
// a silent drop with a metric — the connection stays up and no ban is charged.
//
// Two things are deliberately NOT stored here:
//
//   - whether auth is mandatory. It follows from `mode` (§2.1) and is
//     identical for every type; storing it per type would let the registry
//     contradict the header, and transit — which has no registry — could not
//     enforce a per-type rule anyway (§3.5);
//   - the signature profile. That is picked by `av`, and the wire admits
//     exactly the one this build implements — a per-type opinion about it
//     would be a second, contradictable source.
//
// Reference: docs/refactoring/datagram-transport.md §3.6, §4.1, §6.1, §7.

// Registration failures are sentinels: the wiring code reacts to the reason,
// never to error text.
var (
	// ErrTypeAlreadyRegistered marks a second registration of one dtype. Two
	// owners of one name is a silent split of the contract.
	ErrTypeAlreadyRegistered = errors.New("datagram: dtype already registered")

	// ErrTypeRegistrationInvalid marks a registration that could never
	// receive a lawful frame: no modes, no classes, no handler, or a
	// mode/class pair the §2.1 matrix forbids.
	ErrTypeRegistrationInvalid = errors.New("datagram: invalid type registration")

	// ErrTypePairingRequired marks a `response` type that declared no request
	// dtypes. Without the pairing set an answer of another protocol would be
	// free to take somebody else's single claimed slot (§4.2).
	ErrTypePairingRequired = errors.New("datagram: response type must declare the requests it answers")

	// ErrTypePairingForbidden marks pairing declared by a type that has no
	// `response` mode: §7 defines the property for response types only, and a
	// pairing nobody checks is a contract two implementations would read
	// differently.
	ErrTypePairingForbidden = errors.New("datagram: only a response type may declare paired requests")

	// ErrTypeMixesAuthenticatedPlanes marks a type that declared both the
	// authenticated plane (routed) and an unauthenticated one
	// (request/response). §3.6 states the rule without exceptions: one and the
	// same MEANING must not be accepted both authenticated and unauthenticated
	// — and one dtype is one meaning, so a type allowing itself both would open
	// the demotion path with its own hands. A protocol that genuinely needs
	// both planes needs two dtypes.
	ErrTypeMixesAuthenticatedPlanes = errors.New("datagram: a type must not accept both the authenticated and an unauthenticated plane")
)

// ---------------------------------------------------------------------------
// The header an endpoint sees
// ---------------------------------------------------------------------------

// The context carries NO delivery marker. The hook and the handler run only on
// local delivery — transit calls neither of them (§4.1) — so a field able to
// state exactly one value stated nothing, while remaining the seam through
// which transit hooks would come back. The day transit needs a hook, the
// context grows the field together with the caller that fills it.

// DeliveryHeader is the header as an endpoint and the authorization hook see
// it — for ALL THREE modes, unlike Header (header.go) which exists only on the
// signed routed plane.
//
// The accessors encode §2.1.1 rather than describing it:
//
//   - SignedSrc exists only for `routed`, where src is a verified signer;
//   - Label exists only for `request`/`response`, where the same 20 bytes are
//     a one-shot tag, and it has its own type so it cannot be mistaken for a
//     peer;
//   - Subject exists only for `response`, where src names WHO WAS ASKED, not
//     who answered.
//
// This is what makes "the hook may not build a decision on header.src in the
// unauthenticated modes" (§7) a property of the type: in those modes there is
// no accessor that returns src as a sender at all.
type DeliveryHeader struct {
	mode        domain.DatagramMode
	class       domain.DatagramClass
	dtype       domain.DType
	routePolicy domain.RoutePolicy
	signedSrc   domain.PeerIdentity
	destination domain.PeerIdentity
	subject     domain.PeerIdentity
	label       Label
	ttl         uint8
}

// NewDeliveryHeader projects a validated frame onto the endpoint view.
func NewDeliveryHeader(frame protocol.DatagramFrame) (DeliveryHeader, error) {
	if err := frame.Validate(); err != nil {
		return DeliveryHeader{}, err
	}
	header := DeliveryHeader{
		mode:        frame.Mode,
		class:       frame.Class,
		dtype:       frame.DType,
		routePolicy: frame.RoutePolicy,
		ttl:         frame.TTL,
	}
	switch frame.Mode {
	case domain.DatagramModeRouted:
		header.signedSrc = frame.Src
		header.destination = frame.Dst
	case domain.DatagramModeRequest:
		header.label = NewLabel(frame.Src)
		header.destination = frame.Dst
	case domain.DatagramModeResponse:
		header.label = NewLabel(frame.Dst)
		header.subject = frame.Src
	default:
		return DeliveryHeader{}, fmt.Errorf("%w: mode %q", protocol.ErrDatagramModeMatrix, frame.Mode.String())
	}
	return header, nil
}

// Mode returns the routing plane of the frame.
func (h DeliveryHeader) Mode() domain.DatagramMode { return h.mode }

// Class returns the traffic class.
func (h DeliveryHeader) Class() domain.DatagramClass { return h.class }

// DType returns the protocol name carried in payload.
func (h DeliveryHeader) DType() domain.DType { return h.dtype }

// TTL returns the hop budget as it arrived.
func (h DeliveryHeader) TTL() uint8 { return h.ttl }

// RoutePolicy returns the candidate policy; absent in `response`.
func (h DeliveryHeader) RoutePolicy() domain.RoutePolicy { return h.routePolicy }

// SignedSrc returns the VERIFIED signer of a routed frame. The bool is false
// in request and response, where src is not authenticated at all.
func (h DeliveryHeader) SignedSrc() (domain.PeerIdentity, bool) {
	if h.mode != domain.DatagramModeRouted {
		return domain.PeerIdentity{}, false
	}
	return h.signedSrc, true
}

// Destination returns the address the frame is routed to. The bool is false
// for a response, whose dst is an echoed label and not an address.
func (h DeliveryHeader) Destination() (domain.PeerIdentity, bool) {
	if h.mode == domain.DatagramModeResponse {
		return domain.PeerIdentity{}, false
	}
	return h.destination, true
}

// Label returns the one-shot tag of the request/response exchange. The bool
// is false on the routed plane, which has no reverse state.
func (h DeliveryHeader) Label() (Label, bool) {
	if h.mode == domain.DatagramModeRouted {
		return Label{}, false
	}
	return h.label, true
}

// Subject returns who the question was addressed to, as echoed in the src of
// a response. It is a logical subject and NOT the sender (§2.1.1).
func (h DeliveryHeader) Subject() (domain.PeerIdentity, bool) {
	if h.mode != domain.DatagramModeResponse {
		return domain.PeerIdentity{}, false
	}
	return h.subject, true
}

// ---------------------------------------------------------------------------
// The context the hook and the handler share
// ---------------------------------------------------------------------------

// DeliveryContextOpts collects the §7 context. An opts struct because the
// three identity-shaped fields are not interchangeable and a positional swap
// of incoming_peer and local_identity would compile.
type DeliveryContextOpts struct {
	Header DeliveryHeader
	// IncomingPeer is where the frame entered this node from: the CHANNEL, the
	// name the previous hop presents, and the level of proof behind that name —
	// or the local marker for a frame created here.
	//
	// It is NOT "the authenticated neighbour", which is what this field used to
	// call itself. Only one of the two directions authenticates: the handshake
	// proves the INITIATOR's identity to the RESPONDER, so on a session THIS node dialled
	// the name is the remote's own claim. A rule like push_identity's ("accept
	// only if the session identity equals record.address inside the payload")
	// is implementable exactly where IngressPeer.Identity answers — which is
	// exactly where the proof exists — and a type that depends on it says so in
	// its registration (SenderProofPolicy), rather than hoping the direction is
	// the right one.
	IncomingPeer IngressPeer
	// LocalIdentity is our own address.
	LocalIdentity domain.PeerIdentity
}

// DeliveryContext is the read-only context handed to the authorization hook
// and — by the same value, as §7 requires — to the type handler.
type DeliveryContext struct {
	header        DeliveryHeader
	incomingPeer  IngressPeer
	localIdentity domain.PeerIdentity
}

// NewDeliveryContext validates what a hook may rely on.
func NewDeliveryContext(opts DeliveryContextOpts) (DeliveryContext, error) {
	if opts.IncomingPeer.IsZero() {
		return DeliveryContext{}, errors.New("datagram: delivery context requires an incoming peer (local or remote)")
	}
	if opts.LocalIdentity.IsZero() {
		return DeliveryContext{}, errors.New("datagram: delivery context requires the local identity")
	}
	return DeliveryContext{
		header:        opts.Header,
		incomingPeer:  opts.IncomingPeer,
		localIdentity: opts.LocalIdentity,
	}, nil
}

// Header returns the header view.
func (c DeliveryContext) Header() DeliveryHeader { return c.header }

// IncomingPeer returns where the frame entered this node from.
//
// The value carries its own level of proof: Identity() answers only where this
// node has been shown who the neighbour is, and PresentedIdentity() returns the
// claim together with that level. A hook that wants "who sent this" and gets
// false has its answer — it is on a direction that proved nothing.
func (c DeliveryContext) IncomingPeer() IngressPeer { return c.incomingPeer }

// LocalIdentity returns our own address.
func (c DeliveryContext) LocalIdentity() domain.PeerIdentity { return c.localIdentity }

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

// HandlerOutcome is the three-way result of a type handler (§7). The three
// differ by the FATE OF THE REPLAY KEY, which is why they are three and not a
// bool with an error.
type HandlerOutcome uint8

const (
	// HandlerOutcomeUnset is the zero value; the layer treats it as a
	// failure, because "accepted by omission" must never be inferrable.
	HandlerOutcomeUnset HandlerOutcome = iota
	// HandlerAccepted means the frame was consumed. The key is committed as
	// `delivered`.
	HandlerAccepted
	// HandlerRejected is a deliberate PERMANENT refusal ("I do not want this
	// payload"): the key is committed as `rejected`, so a repeat is dropped
	// by the early Has without paying for cryptography or the handler again.
	// A refusal that might succeed later MUST be returned as failed.
	HandlerRejected
	// HandlerFailed is a fault after which a repeat makes sense: the key is
	// released.
	HandlerFailed
)

var handlerOutcomeNames = map[HandlerOutcome]string{
	HandlerOutcomeUnset: "unset",
	HandlerAccepted:     "accepted",
	HandlerRejected:     "rejected",
	HandlerFailed:       "failed",
}

// String returns the metric label of the outcome.
func (o HandlerOutcome) String() string { return enumName(handlerOutcomeNames, o) }

// HandlerResponse is the answer a `request` handler produced. It exists only
// together with HandlerAccepted (§4.1): answering on a refusal would disguise
// the refusal as success, and a "negative answer", where a type needs one, is
// application content inside an accepted answer, not a transport branch.
type HandlerResponse struct {
	dtype   domain.DType
	payload []byte
}

// DType returns the dtype of the answer.
func (r HandlerResponse) DType() domain.DType { return r.dtype }

// Payload returns the answer bytes.
func (r HandlerResponse) Payload() []byte { return r.payload }

// HandlerResult is what a handler returns.
type HandlerResult struct {
	err      error
	response *HandlerResponse
	outcome  HandlerOutcome
}

// AcceptDelivery reports a consumed frame with no answer.
func AcceptDelivery() HandlerResult { return HandlerResult{outcome: HandlerAccepted} }

// AcceptWithAnswer reports a consumed request together with its answer.
func AcceptWithAnswer(dtype domain.DType, payload []byte) HandlerResult {
	return HandlerResult{
		outcome:  HandlerAccepted,
		response: &HandlerResponse{dtype: dtype, payload: append([]byte(nil), payload...)},
	}
}

// RejectDelivery reports a permanent refusal.
func RejectDelivery(err error) HandlerResult {
	return HandlerResult{outcome: HandlerRejected, err: err}
}

// FailDelivery reports a fault worth retrying.
func FailDelivery(err error) HandlerResult {
	return HandlerResult{outcome: HandlerFailed, err: err}
}

// Outcome reports the variant.
func (r HandlerResult) Outcome() HandlerOutcome { return r.outcome }

// Response returns the produced answer. The bool is false unless the handler
// accepted AND produced one.
func (r HandlerResult) Response() (HandlerResponse, bool) {
	if r.outcome != HandlerAccepted || r.response == nil {
		return HandlerResponse{}, false
	}
	return *r.response, true
}

// Err returns the cause of a refusal or a fault, for logs.
func (r HandlerResult) Err() error { return r.err }

// Handler is the endpoint function of a type: the terminal receiver at
// dst == self. It receives incoming_peer through the same context as the
// authorization hook (§7).
//
// It MUST be idempotent: the layer promises ZERO OR MORE deliveries — neither
// at least once nor exactly once (§4.5). Zero, because a frame is dropped
// outright when no route is found, when an admission budget refuses it, when a
// queue is full or when a writer fails, and nothing here resends it. More than
// one, because a repeat arrives after a lost Commit, after a restart, and after
// an honest loop. Retransmission and acknowledgement belong to the two
// endpoints of the type, never to this layer.
type Handler interface {
	Handle(ctx context.Context, delivery DeliveryContext, payload []byte) HandlerResult
}

// HandlerFunc adapts a plain function to Handler.
type HandlerFunc func(ctx context.Context, delivery DeliveryContext, payload []byte) HandlerResult

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, delivery DeliveryContext, payload []byte) HandlerResult {
	return f(ctx, delivery, payload)
}

// ---------------------------------------------------------------------------
// Payload schema
// ---------------------------------------------------------------------------

// PayloadSchema names the format and the version INSIDE the payload — never
// in the type name (§7). Growing a schema means ADDITIVE fields only, and the
// receiver IGNORES unknown ones; a change of meaning is a new dtype.
//
// This is the exact opposite of the header, where an unknown field is a
// reject (§3.4), and the asymmetry is deliberate: the header is a closed
// contract every transit node enforces, while the payload belongs to two
// endpoints that may ship at different times.
type PayloadSchema struct {
	// Name is the schema identifier, free-form and meaningful to the type.
	Name string
	// Version is the schema version inside the payload.
	Version uint32
}

// Decode unmarshals a JSON payload into a value, IGNORING unknown fields.
//
// It is a method of the schema, not a free function, so the contract lives
// where the schema is declared: encoding/json ignores unknown fields by
// default, and this call site is the promise that the layer will never
// tighten it — the header parser's requireKnownFields is the tightened
// variant and it is a different contract, on different data.
func (s PayloadSchema) Decode(payload []byte, into any) error {
	if err := json.Unmarshal(payload, into); err != nil {
		return fmt.Errorf("datagram: decode payload schema %q v%d: %w", s.Name, s.Version, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// SenderProofPolicy is a type's DECLARATION of whether its local delivery
// depends on this node knowing who the neighbour that handed the frame over is.
//
// It replaces an inference — "the type registered an Authorizer, so it must
// depend on the neighbour's identity" — that was wrong in both directions and
// silently so. §7 describes a sender authenticated by a signature INSIDE the
// payload; such a type has an Authorizer that never reads IncomingPeer and must
// keep working on every session this node dialled, which the inference took it
// off. And a type with NO Authorizer may build its handler on the neighbour's
// name just as easily, which the inference let through.
//
// The two members are ordered so the ZERO VALUE is the strict one, and the
// choice is deliberate rather than incidental. A type that forgot to declare
// gets the failure that is visible and recoverable — it is refused on dialled
// sessions, with a named drop reason and a metric — instead of the failure that
// is silent and unrecoverable: a hook admitting a stranger under a borrowed
// fingerprint. Availability lost by omission is a bug report; authenticity lost
// by omission is a breach nobody files.
type SenderProofPolicy uint8

const (
	// RequiresProvenPeer is the ZERO VALUE: the type's local delivery may only
	// run where this node has been SHOWN who the neighbour is (an accepted
	// connection, where connauth verified a signature over a challenge this node
	// generated). On every other direction the frame is refused above the §7
	// seams with DropUnprovenSender.
	RequiresProvenPeer SenderProofPolicy = iota
	// SenderProvenInPayload states that the type carries its own proof of the
	// sender inside the payload and reads nothing about the neighbour from the
	// transport. Such a type is served on every direction — which is the path
	// §7 describes and the one a client node, whose traffic is almost all
	// dialled, depends on.
	//
	// Declaring it is a statement about the TYPE's handler and hooks, not about
	// this frame: a type that declares it and then reads IncomingPeer.Identity
	// simply gets `false` there, because the accessor answers only where the
	// proof exists.
	SenderProvenInPayload
)

var senderProofPolicyNames = map[SenderProofPolicy]string{
	RequiresProvenPeer:    "requires_proven_peer",
	SenderProvenInPayload: "sender_proven_in_payload",
}

// String returns the log label of the policy.
func (p SenderProofPolicy) String() string { return enumName(senderProofPolicyNames, p) }

// TypeRegistration is one dtype's entry (§7).
type TypeRegistration struct {
	// Handler is the terminal receiver. Mandatory: a type nobody can receive
	// has nothing to register.
	Handler Handler
	// Authorizer is optional; a type without one is authorized trivially
	// (§7).
	Authorizer Authorizer
	// DType is the name.
	DType domain.DType
	// Modes are the modes the type sends in and accepts on reception. §3.6
	// forbids one MEANING in both an authenticated and an unauthenticated
	// mode: a type that allows itself both routed and request for one action
	// opens the demotion path with its own hands.
	Modes []domain.DatagramMode
	// Classes is a SET: one type may legitimately use several (§7). Declaring
	// a whole protocol `bulk` would queue its own progress-control frames
	// behind its own large answers.
	Classes []domain.DatagramClass
	// AnswersTo is the pairing set — only for types with a `response` mode:
	// the request dtypes this type answers, checked against the dtype in the
	// reverse record BEFORE the claim (§4.2).
	AnswersTo []domain.DType
	// Payload is the schema descriptor.
	Payload PayloadSchema
	// SenderProof declares whether local delivery of this type needs a proven
	// neighbour. The zero value is the strict end; see SenderProofPolicy for why
	// that direction and not the other.
	SenderProof SenderProofPolicy
}

// RegisteredType is one entry as the pipeline reads it: values and closed
// sets, with no way to widen a mode or a class after registration.
type RegisteredType struct {
	handler     Handler
	authorizer  Authorizer
	modes       map[domain.DatagramMode]struct{}
	classes     map[domain.DatagramClass]struct{}
	answersTo   map[domain.DType]struct{}
	dtype       domain.DType
	payload     PayloadSchema
	senderProof SenderProofPolicy
}

// DType returns the registered name.
func (t RegisteredType) DType() domain.DType { return t.dtype }

// AllowsMode reports whether the type accepts this mode. A receipt arriving
// as `request` is refused here, BEFORE the handler — which is what makes the
// demotion of §3.6 harmless.
func (t RegisteredType) AllowsMode(mode domain.DatagramMode) bool {
	_, ok := t.modes[mode]
	return ok
}

// AllowsClass reports whether the type accepts this traffic class.
func (t RegisteredType) AllowsClass(class domain.DatagramClass) bool {
	_, ok := t.classes[class]
	return ok
}

// Handler returns the terminal receiver.
func (t RegisteredType) Handler() Handler { return t.handler }

// Authorizer returns the authorization hook. The bool is false for a type
// without one; §7 authorizes such types trivially with `accept`.
func (t RegisteredType) Authorizer() (Authorizer, bool) {
	if t.authorizer == nil {
		return nil, false
	}
	return t.authorizer, true
}

// AnswersRequest reports whether this response type answers that request
// dtype. It is only ever asked of a node that KNOWS the answer type: a node
// that does not know it performs no pairing check and forwards as before —
// demanding knowledge of future pairs from an old transit is impossible,
// which is why the check is typed rather than transport-level (§4.2).
func (t RegisteredType) AnswersRequest(request domain.DType) bool {
	_, ok := t.answersTo[request]
	return ok
}

// Schema returns the payload schema descriptor.
func (t RegisteredType) Schema() PayloadSchema { return t.payload }

// SenderProof returns the type's declared dependency on a proven neighbour.
func (t RegisteredType) SenderProof() SenderProofPolicy { return t.senderProof }

// RequiresProvenPeer reports whether the conveyor must refuse this type's local
// delivery on a direction that proved nothing about the neighbour. It is the ONE
// reading of the declaration, so the gate and the registration cannot come to
// disagree about what "declared nothing" means.
func (t RegisteredType) RequiresProvenPeer() bool {
	return t.senderProof == RequiresProvenPeer
}

// typeEntries is one immutable generation of the registry.
type typeEntries map[domain.DType]RegisteredType

// TypeRegistry maps dtypes to their entries. Constructed and explicitly
// populated — no package-level singleton and no init-time side effects — so a
// test builds exactly the registry it reasons about.
//
// Concurrency: Lookup and DTypes sit on the RECEIVE path of every datagram and
// run on the goroutine of each session, while Register runs at wiring time. The
// map is therefore a copy-on-write snapshot behind an atomic.Pointer
// (docs/locking.md): readers take no lock at all, writers serialise on mu and
// publish a whole new generation.
// A plain map would be a data race the moment anything registers a type lazily
// or behind a feature flag, and an RWMutex would put a writer-priority queue in
// front of the hottest read in the layer.
type TypeRegistry struct {
	entries atomic.Pointer[typeEntries]
	mu      sync.Mutex
}

// NewTypeRegistry builds an empty registry.
func NewTypeRegistry() *TypeRegistry {
	registry := &TypeRegistry{}
	registry.entries.Store(&typeEntries{})
	return registry
}

// current reads the live generation. It is the only read path, so no caller
// can reach a map a writer is replacing.
func (r *TypeRegistry) current() typeEntries { return *r.entries.Load() }

// Register adds one type after validating everything that CAN be validated
// statically.
func (r *TypeRegistry) Register(registration TypeRegistration) error {
	dtype, err := domain.ParseDType(registration.DType.String())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrTypeRegistrationInvalid, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.current()
	if _, exists := current[dtype]; exists {
		return fmt.Errorf("%w: %q", ErrTypeAlreadyRegistered, dtype.String())
	}
	// The AUTHORIZER is a SAFETY seam: absent means every frame of this type is
	// authorized, so a value that is present-but-unusable must not be quietly
	// read as absent. Registering none stays lawful — it just has to be said by
	// leaving the field empty rather than by filling it with a typed nil.
	if registration.Authorizer != nil && isNilValue(registration.Authorizer) {
		return fmt.Errorf(
			"datagram: %q was given an unusable authorizer; leave the field empty to run without one",
			registration.DType.String())
	}
	if isNilValue(registration.Handler) {
		return fmt.Errorf("%w: %q has no handler", ErrTypeRegistrationInvalid, dtype.String())
	}
	modes, err := validateTypeModes(dtype, registration)
	if err != nil {
		return err
	}
	classes, err := validateTypeClasses(dtype, registration)
	if err != nil {
		return err
	}
	if err := validateModeClassMatrix(dtype, modes, classes); err != nil {
		return err
	}
	answersTo, err := validatePairing(dtype, modes, registration.AnswersTo)
	if err != nil {
		return err
	}
	senderProof, err := validateSenderProof(dtype, registration.SenderProof)
	if err != nil {
		return err
	}
	// Copy-on-write: registration is rare and happens at wiring time, while
	// the read path runs per frame. Publishing a whole new generation keeps
	// readers lock-free and makes a half-inserted entry unobservable.
	next := make(typeEntries, len(current)+1)
	for registered, entry := range current {
		next[registered] = entry
	}
	next[dtype] = RegisteredType{
		handler:     registration.Handler,
		authorizer:  registration.Authorizer,
		modes:       modes,
		classes:     classes,
		answersTo:   answersTo,
		dtype:       dtype,
		payload:     registration.Payload,
		senderProof: senderProof,
	}
	r.entries.Store(&next)
	return nil
}

// Lookup resolves a dtype. The bool is false for a type this node does not
// implement — which at dst == self is a SILENT drop with a metric, reached
// before the authorization hook and before any replay slot is taken (§7).
func (r *TypeRegistry) Lookup(dtype domain.DType) (RegisteredType, bool) {
	entry, ok := r.current()[dtype]
	return entry, ok
}

// DTypes returns the registered names in a stable order — the set advertised
// in the handshake (§6.1), where order is not significant but determinism
// keeps the frame byte-stable.
func (r *TypeRegistry) DTypes() []domain.DType {
	entries := r.current()
	out := make([]domain.DType, 0, len(entries))
	for dtype := range entries {
		out = append(out, dtype)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ---------------------------------------------------------------------------
// Registration validation
// ---------------------------------------------------------------------------

func validateTypeModes(dtype domain.DType, registration TypeRegistration) (map[domain.DatagramMode]struct{}, error) {
	if len(registration.Modes) == 0 {
		return nil, fmt.Errorf("%w: %q declares no modes", ErrTypeRegistrationInvalid, dtype.String())
	}
	modes := make(map[domain.DatagramMode]struct{}, len(registration.Modes))
	for _, mode := range registration.Modes {
		if !mode.Valid() {
			return nil, fmt.Errorf("%w: %q declares mode %q", ErrTypeRegistrationInvalid, dtype.String(), mode.String())
		}
		modes[mode] = struct{}{}
	}
	if err := refuseMixedPlanes(dtype, modes); err != nil {
		return nil, err
	}
	return modes, nil
}

// refuseMixedPlanes enforces the type-level rule of §3.6 constructively.
//
// A hostile relay can strip the auth block off a signed routed datagram and
// pass it off as a request; the only reason that is harmless is that the
// destination's registry refuses the type in a mode it never declared. A type
// that declares BOTH planes removes exactly that protection for itself, and the
// registry is the one place able to notice it before the type ships.
func refuseMixedPlanes(dtype domain.DType, modes map[domain.DatagramMode]struct{}) error {
	if _, authenticated := modes[domain.DatagramModeRouted]; !authenticated {
		return nil
	}
	for _, unauthenticated := range []domain.DatagramMode{domain.DatagramModeRequest, domain.DatagramModeResponse} {
		if _, declared := modes[unauthenticated]; declared {
			return fmt.Errorf("%w: %q declares both %q and %q",
				ErrTypeMixesAuthenticatedPlanes, dtype.String(),
				domain.DatagramModeRouted.String(), unauthenticated.String())
		}
	}
	return nil
}

func validateTypeClasses(dtype domain.DType, registration TypeRegistration) (map[domain.DatagramClass]struct{}, error) {
	if len(registration.Classes) == 0 {
		return nil, fmt.Errorf("%w: %q declares no classes", ErrTypeRegistrationInvalid, dtype.String())
	}
	classes := make(map[domain.DatagramClass]struct{}, len(registration.Classes))
	for _, class := range registration.Classes {
		if !class.Valid() {
			return nil, fmt.Errorf("%w: %q declares class %q", ErrTypeRegistrationInvalid, dtype.String(), class.String())
		}
		classes[class] = struct{}{}
	}
	return classes, nil
}

// validateModeClassMatrix refuses a registration no lawful frame could ever
// satisfy: a mode of which NONE of the declared classes is admitted by §2.1 —
// `request` with `bulk` alone being the canonical case. The matrix stays the
// single source of truth; the registry only refuses to contradict it.
func validateModeClassMatrix(
	dtype domain.DType,
	modes map[domain.DatagramMode]struct{},
	classes map[domain.DatagramClass]struct{},
) error {
	for mode := range modes {
		rule, ok := domain.DatagramModeRuleFor(mode)
		if !ok {
			return fmt.Errorf("%w: %q declares mode %q", ErrTypeRegistrationInvalid, dtype.String(), mode.String())
		}
		admitted := false
		for class := range classes {
			if rule.AllowsClass(class) {
				admitted = true
				break
			}
		}
		if !admitted {
			return fmt.Errorf("%w: %q declares mode %q with no class the mode matrix admits",
				ErrTypeRegistrationInvalid, dtype.String(), mode.String())
		}
	}
	return nil
}

// validateSenderProof refuses a policy outside the closed set.
//
// It refuses rather than clamping to the strict end, and the difference matters
// at exactly one moment: a value out of range is a build that thinks it declared
// something. Silently reading it as `requires proven peer` would be the safe
// BEHAVIOUR and the wrong REPORT — the registration would go on being wrong,
// invisibly, until the day the clamp moved.
func validateSenderProof(dtype domain.DType, policy SenderProofPolicy) (SenderProofPolicy, error) {
	if _, named := senderProofPolicyNames[policy]; !named {
		return 0, fmt.Errorf("%w: %q declares sender-proof policy %d",
			ErrTypeRegistrationInvalid, dtype.String(), policy)
	}
	return policy, nil
}

func validatePairing(
	dtype domain.DType,
	modes map[domain.DatagramMode]struct{},
	answersTo []domain.DType,
) (map[domain.DType]struct{}, error) {
	_, isResponse := modes[domain.DatagramModeResponse]
	if !isResponse {
		if len(answersTo) > 0 {
			return nil, fmt.Errorf("%w: %q", ErrTypePairingForbidden, dtype.String())
		}
		return nil, nil
	}
	if len(answersTo) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrTypePairingRequired, dtype.String())
	}
	pairs := make(map[domain.DType]struct{}, len(answersTo))
	for _, request := range answersTo {
		parsed, err := domain.ParseDType(request.String())
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w", ErrTypeRegistrationInvalid, dtype.String(), err)
		}
		pairs[parsed] = struct{}{}
	}
	return pairs, nil
}
