package rpc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_binding_commands.go registers the operator's actions on the endpoint
// bindings of the v2 downgrade protection (docs/rpc/secure_session.md): the
// list, the addressed removal of an ordinary binding and the two-step
// demotion of selected protected bindings.
//
// All three are TransportLoopbackOnly. The bindings are the node's graph of
// endpoints and the identities proven on them, and two of the commands lower
// the downgrade protection of the endpoints they name: neither belongs to a
// caller on another host, nor to another local process on a listener without
// credentials.
//
// The handlers parse and validate the arguments into domain types and encode
// the answer; every decision — which binding an address names, what may be
// removed or demoted, whether a confirmation matches — is the node's. A
// node's refusal reaches the caller as a validation error, recognised by
// errors.Is, never by its text.

// DefaultEndpointBindingPage is the page size of listEndpointBindings when the
// caller names none.
const DefaultEndpointBindingPage = 200

// maxEndpointArgLength bounds an endpoint argument: a 253-byte name, a port
// and IPv6 brackets fit with room, and nothing longer names a binding.
const maxEndpointArgLength = 512

// EndpointBindingDTO is one binding of listEndpointBindings.
type EndpointBindingDTO struct {
	Endpoint    domain.PeerAddress            `json:"endpoint"`
	KeyForm     domain.EndpointBindingKeyForm `json:"key_form"`
	Transport   *domain.EndpointTransport     `json:"transport,omitempty"`
	SourceGroup domain.SourceGroup            `json:"source_group"`
	Tier        domain.EndpointBindingTier    `json:"tier"`
	Pool        *domain.ProtectedBindingPool  `json:"pool,omitempty"`
	ProtectedBy *domain.PeerIdentity          `json:"protected_by,omitempty"`
	Identity    domain.PeerIdentity           `json:"identity"`
	LastV2      time.Time                     `json:"last_v2"`
	ExpiresAt   *time.Time                    `json:"expires_at,omitempty"`
	FirstAt     *time.Time                    `json:"first_at,omitempty"`
	DerivedFrom *domain.PeerAddress           `json:"derived_from,omitempty"`
	DemotedAt   *time.Time                    `json:"demoted_at,omitempty"`
}

// EndpointBindingPageDTO is the answer of listEndpointBindings.
type EndpointBindingPageDTO struct {
	Bindings  []EndpointBindingDTO `json:"bindings"`
	Total     int                  `json:"total"`
	NextAfter *domain.PeerAddress  `json:"next_after,omitempty"`
}

// EndpointBindingRemovalDTO is the answer of removeEndpointBinding.
type EndpointBindingRemovalDTO struct {
	OperationID domain.EndpointBindingOperationID `json:"operation_id"`
	Endpoint    domain.PeerAddress                `json:"endpoint"`
	KeyForm     domain.EndpointBindingKeyForm     `json:"key_form"`
}

// EndpointBindingDemotionItemDTO is one endpoint of a demotion.
type EndpointBindingDemotionItemDTO struct {
	Endpoint           domain.PeerAddress            `json:"endpoint"`
	KeyForm            domain.EndpointBindingKeyForm `json:"key_form"`
	Pool               domain.ProtectedBindingPool   `json:"pool"`
	ExpiresAt          time.Time                     `json:"expires_at"`
	IdentityPinRemains bool                          `json:"identity_pin_remains"`
}

// EndpointBindingDemotionDTO is the answer of demoteEndpointBindings: the dry
// run (Confirmed false, with the token that confirms it) or the written
// demotion (Confirmed true, with its operation id).
type EndpointBindingDemotionDTO struct {
	Confirmed    bool                                  `json:"confirmed"`
	Endpoints    []EndpointBindingDemotionItemDTO      `json:"endpoints"`
	Consequence  string                                `json:"consequence"`
	ConfirmToken *domain.EndpointBindingSelectionToken `json:"confirm_token,omitempty"`
	OperationID  *domain.EndpointBindingOperationID    `json:"operation_id,omitempty"`
}

// EndpointBindingListRequest is the query of Client.ListEndpointBindings;
// empty fields are not sent.
type EndpointBindingListRequest struct {
	Identity string
	Endpoint string
	After    string
	Limit    int
}

// RegisterEndpointBindingCommands registers the operator's binding commands.
// A nil provider registers them as unavailable — with their policy, so a
// remote caller is refused by the gate before a 503 tells it anything.
func RegisterEndpointBindingCommands(t *CommandTable, ep EndpointBindingProvider) {
	listInfo := CommandInfo{
		Name:        "listEndpointBindings",
		Description: "List the endpoint bindings of the v2 downgrade protection a page at a time (filters: identity, endpoint; paging: after, limit)",
		Category:    "secure_session",
		Usage:       "[identity=<id>] [endpoint=<host:port>] [after=<host:port>] [limit=<1-1000>]",
		Transport:   TransportLoopbackOnly,
	}
	removeInfo := CommandInfo{
		Name:        "removeEndpointBinding",
		Description: "Remove one ordinary endpoint binding by its address; a protected binding is refused",
		Category:    "secure_session",
		Usage:       "<host:port>",
		Transport:   TransportLoopbackOnly,
	}
	demoteInfo := CommandInfo{
		Name:        "demoteEndpointBindings",
		Description: "Move selected protected endpoint bindings to general for 30 days: without confirm a dry run that shows the endpoints, the consequence and a confirm token; with the token, the demotion",
		Category:    "secure_session",
		Usage:       "<host:port>... | endpoints=<host:port,...> confirm=<token>",
		Transport:   TransportLoopbackOnly,
	}
	if ep == nil {
		t.RegisterUnavailable(listInfo)
		t.RegisterUnavailable(removeInfo)
		t.RegisterUnavailable(demoteInfo)
		return
	}
	t.Register(listInfo, listEndpointBindingsHandler(ep))
	t.Register(removeInfo, removeEndpointBindingHandler(ep))
	t.Register(demoteInfo, demoteEndpointBindingsHandler(ep))
}

func listEndpointBindingsHandler(ep EndpointBindingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		query, err := endpointBindingQueryArg(req.Args)
		if err != nil {
			return validationError(err)
		}
		page, err := ep.ListEndpointBindings(requestContext(req), query)
		if err != nil {
			return endpointBindingFailure("list endpoint bindings", err)
		}
		return jsonResponse(pageDTO(page))
	}
}

func removeEndpointBindingHandler(ep EndpointBindingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		endpoint, err := requiredEndpointArg(req.Args, "endpoint")
		if err != nil {
			return validationError(err)
		}
		removal, err := ep.RemoveEndpointBinding(requestContext(req), endpoint)
		if err != nil {
			return endpointBindingFailure("remove endpoint binding", err)
		}
		return jsonResponse(EndpointBindingRemovalDTO{OperationID: removal.OperationID, Endpoint: removal.Endpoint, KeyForm: removal.KeyForm})
	}
}

func demoteEndpointBindingsHandler(ep EndpointBindingProvider) CommandHandler {
	return func(req CommandRequest) CommandResponse {
		if r, done := ctxDone(req); done {
			return r
		}
		endpoints, err := endpointListArg(req.Args, "endpoints")
		if err != nil {
			return validationError(err)
		}
		confirm, err := optionalStringArg(req.Args, "confirm")
		if err != nil {
			return validationError(err)
		}
		if confirm == nil {
			plan, err := ep.PlanEndpointBindingDemotion(requestContext(req), endpoints)
			if err != nil {
				return endpointBindingFailure("plan endpoint binding demotion", err)
			}
			token := plan.Token
			return jsonResponse(EndpointBindingDemotionDTO{Endpoints: demotionItemDTOs(plan.Items), Consequence: plan.Consequence, ConfirmToken: &token})
		}
		result, err := ep.DemoteEndpointBindings(requestContext(req), endpoints, domain.EndpointBindingSelectionToken(*confirm))
		if err != nil {
			return endpointBindingFailure("demote endpoint bindings", err)
		}
		op := result.OperationID
		return jsonResponse(EndpointBindingDemotionDTO{Confirmed: true, Endpoints: demotionItemDTOs(result.Items), Consequence: result.Consequence, OperationID: &op})
	}
}

// endpointBindingRefusals are the node's refusals of what the caller named:
// the request is wrong for the store as it is, not the node broken.
var endpointBindingRefusals = []error{
	domain.ErrEndpointBindingNotFound,
	domain.ErrEndpointBindingProtected,
	domain.ErrEndpointBindingNotProtected,
	domain.ErrEndpointBindingSelectionEmpty,
	domain.ErrEndpointBindingSelectionTooLarge,
	domain.ErrEndpointBindingSelectionDuplicate,
	domain.ErrEndpointBindingSelectionChanged,
	domain.ErrEndpointBindingPageLimit,
}

// endpointBindingFailure maps a node error to its kind: a refusal of the
// request is a validation error, a store the node cannot act on is
// unavailable, anything else — a failed write — is internal.
func endpointBindingFailure(action string, err error) CommandResponse {
	wrapped := fmt.Errorf("%s: %w", action, err)
	if errors.Is(err, domain.ErrEndpointBindingStoreUnavailable) {
		return unavailableError(wrapped)
	}
	for _, refusal := range endpointBindingRefusals {
		if errors.Is(err, refusal) {
			return validationError(wrapped)
		}
	}
	return internalError(wrapped)
}

// endpointBindingQueryArg parses the list's filters and page.
func endpointBindingQueryArg(args map[string]interface{}) (domain.EndpointBindingQuery, error) {
	query := domain.EndpointBindingQuery{Limit: DefaultEndpointBindingPage}
	identity, err := optionalStringArg(args, "identity")
	if err != nil {
		return query, err
	}
	if identity != nil {
		id, err := domain.ParsePeerIdentity(*identity)
		if err != nil {
			return query, fmt.Errorf("identity: %w", err)
		}
		query.Identity = &id
	}
	if query.Endpoint, err = optionalEndpointArg(args, "endpoint"); err != nil {
		return query, err
	}
	if query.After, err = optionalEndpointArg(args, "after"); err != nil {
		return query, err
	}
	limit, present, err := optionalPositiveIntArg(args, "limit")
	if err != nil {
		return query, err
	}
	if present {
		if limit > domain.MaxEndpointBindingPage {
			return query, fmt.Errorf("limit must be 1..%d, got %d", domain.MaxEndpointBindingPage, limit)
		}
		query.Limit = limit
	}
	return query, nil
}

// optionalStringArg is the trimmed string under key; nil when absent or
// empty. A value of another type is an error, not an absence.
func optionalStringArg(args map[string]interface{}, key string) (*string, error) {
	raw, present := args[key]
	if !present || raw == nil {
		return nil, nil
	}
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("%s must be a string", key)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil
	}
	return &text, nil
}

// optionalEndpointArg is the endpoint under key; nil when absent.
func optionalEndpointArg(args map[string]interface{}, key string) (*domain.PeerAddress, error) {
	text, err := optionalStringArg(args, key)
	if err != nil || text == nil {
		return nil, err
	}
	endpoint, err := endpointArg(key, *text)
	if err != nil {
		return nil, err
	}
	return &endpoint, nil
}

// requiredEndpointArg is the one endpoint under key.
func requiredEndpointArg(args map[string]interface{}, key string) (domain.PeerAddress, error) {
	endpoint, err := optionalEndpointArg(args, key)
	if err != nil {
		return "", err
	}
	if endpoint == nil {
		return "", fmt.Errorf("%s is required: one host:port", key)
	}
	return *endpoint, nil
}

// endpointListArg is the endpoints under key: a JSON array of strings, or —
// the console's key=value form — one comma-separated string.
func endpointListArg(args map[string]interface{}, key string) ([]domain.PeerAddress, error) {
	var items []string
	switch raw := args[key].(type) {
	case nil:
	case string:
		items = strings.Split(raw, ",")
	case []interface{}:
		for i, item := range raw {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s[%d] must be a string", key, i)
			}
			items = append(items, text)
		}
	default:
		return nil, fmt.Errorf("%s must be an array of host:port strings", key)
	}
	endpoints := make([]domain.PeerAddress, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		endpoint, err := endpointArg(key, item)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s is required: name every endpoint", key)
	}
	return endpoints, nil
}

// endpointArg checks the shape of one endpoint argument; which binding it
// names is the node's to decide.
func endpointArg(key, text string) (domain.PeerAddress, error) {
	if len(text) > maxEndpointArgLength {
		return "", fmt.Errorf("%s longer than %d bytes", key, maxEndpointArgLength)
	}
	return domain.PeerAddress(text), nil
}

// optionalPositiveIntArg is the positive integer under key — a JSON number or
// the console's text. present is false when the key is absent.
func optionalPositiveIntArg(args map[string]interface{}, key string) (int, bool, error) {
	raw, present := args[key]
	if !present || raw == nil {
		return 0, false, nil
	}
	value, ok := numericArg(args, key)
	if !ok {
		return 0, true, fmt.Errorf("%s must be a positive integer, got %v", key, raw)
	}
	return value, true, nil
}

func pageDTO(page domain.EndpointBindingPage) EndpointBindingPageDTO {
	bindings := make([]EndpointBindingDTO, 0, len(page.Bindings))
	for _, view := range page.Bindings {
		bindings = append(bindings, EndpointBindingDTO{
			Endpoint:    view.Endpoint,
			KeyForm:     view.KeyForm,
			Transport:   view.Transport,
			SourceGroup: view.SourceGroup,
			Tier:        view.Tier,
			Pool:        view.Pool,
			ProtectedBy: view.ProtectedBy,
			Identity:    view.Identity,
			LastV2:      view.LastV2,
			ExpiresAt:   view.ExpiresAt,
			FirstAt:     view.FirstAt,
			DerivedFrom: view.DerivedFrom,
			DemotedAt:   view.DemotedAt,
		})
	}
	return EndpointBindingPageDTO{Bindings: bindings, Total: page.Total, NextAfter: page.NextAfter}
}

func demotionItemDTOs(items []domain.EndpointBindingDemotionItem) []EndpointBindingDemotionItemDTO {
	dtos := make([]EndpointBindingDemotionItemDTO, 0, len(items))
	for _, item := range items {
		dtos = append(dtos, EndpointBindingDemotionItemDTO{
			Endpoint:           item.Endpoint,
			KeyForm:            item.KeyForm,
			Pool:               item.Pool,
			ExpiresAt:          item.ExpiresAt,
			IdentityPinRemains: item.IdentityPinRemains,
		})
	}
	return dtos
}

// --- client ---

// ListEndpointBindings runs listEndpointBindings. The RPC listener must be
// reached over loopback with credentials.
func (c *Client) ListEndpointBindings(req EndpointBindingListRequest) (EndpointBindingPageDTO, error) {
	args := map[string]interface{}{}
	for key, value := range map[string]string{"identity": req.Identity, "endpoint": req.Endpoint, "after": req.After} {
		if value != "" {
			args[key] = value
		}
	}
	if req.Limit > 0 {
		args["limit"] = req.Limit
	}
	var page EndpointBindingPageDTO
	return page, c.execInto("listEndpointBindings", args, &page)
}

// RemoveEndpointBinding runs removeEndpointBinding for one endpoint.
func (c *Client) RemoveEndpointBinding(endpoint string) (EndpointBindingRemovalDTO, error) {
	var removal EndpointBindingRemovalDTO
	return removal, c.execInto("removeEndpointBinding", map[string]interface{}{"endpoint": endpoint}, &removal)
}

// DemoteEndpointBindings runs demoteEndpointBindings: the dry run when
// confirm is empty, the demotion with the dry run's token otherwise.
func (c *Client) DemoteEndpointBindings(endpoints []string, confirm string) (EndpointBindingDemotionDTO, error) {
	args := map[string]interface{}{"endpoints": endpoints}
	if confirm != "" {
		args["confirm"] = confirm
	}
	var demotion EndpointBindingDemotionDTO
	return demotion, c.execInto("demoteEndpointBindings", args, &demotion)
}
