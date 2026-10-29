package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// identity_state_inventory_test.go is the inventory of the state this package
// keeps about other nodes by identity (docs/protocol/session_v2.md,
// "Downgrade protection"): which records may hold an identity a legacy (v1)
// session merely claimed, and which only an identity a v2 session proved, a
// signature bound, or this node named itself. A record that trusts a legacy
// claim is where a v1 peer can act in another node's name, so whoever adds one
// has to decide it — and says so here — instead of inheriting the answer of
// whatever field it was copied from.
//
// What the guard finds, syntactically, in the package's non-test files:
//
//   - every field of Service whose type holds a map, whatever its key: an
//     identity spelled as a string is invisible to a syntactic check, and
//     Service still keeps several (pubKeys, subs, …);
//   - every field of any struct type of the package holding a map whose key
//     or value names an identity — domain.PeerIdentity under whatever name the
//     file imports domain, or a type of this package that carries one
//     (penaltySubject, endpointBinding, …), at any depth of nested maps,
//     through pointers, slices, arrays, anonymous structs, named map types of
//     the package (embedded ones too, under the type's name), and
//     instantiations of its generic types with such a type argument
//     (tokenBucketSet[penaltySubject]);
//   - the maps outside Service known to key an identity by a string, listed
//     by hand in stringKeyedIdentityFields.
//
// identity_state_inventory_probe_test.go feeds each of these shapes to the
// finder as a synthetic source.
//
// What it does not find, honestly:
//
//   - a NEW map outside Service keyed by an identity spelled as a string —
//     only the hand list covers such maps, so new code keys identities by
//     domain.PeerIdentity, which the finder sees and the compiler checks;
//   - an identity inside a type of another package (a protocol frame with a
//     string recipient, a routing type), and state kept in other packages
//     (routing.Table, the datagram admission);
//   - containers other than maps: a slice or list of identities, sync.Map,
//     a channel;
//   - a generic type of another package instantiated with an identity.
//
// Every field found must be classified below, and every classified field must
// still exist: an entry that matches nothing proves nothing.

// identityKeyTrust is what stands behind the identity a record holds.
type identityKeyTrust int

const (
	// identityFromV2Proof: written only from a v2 proof (sessionv2.
	// ProvenSession) of the identity — a legacy claim cannot create it.
	identityFromV2Proof identityKeyTrust = iota + 1
	// identityByOrigin: keyed by penaltySubject — the proven identity of a v2
	// session, or what this node observed about a legacy connection (the
	// dialled address, the source IP, the connection), never the identity a
	// legacy session names. These are the records split by origin.
	identityByOrigin
	// identityUnprovenClaim: may hold an identity nothing proved — the claim
	// of a legacy session, or unsigned content. The reason names how the
	// record goes when the identity later proves v2, or that it does not.
	identityUnprovenClaim
	// identitySelfAuthenticated: the identity is bound to the content by a
	// signature or a key fingerprint checked before the write, whoever
	// relayed it and over whichever session.
	identitySelfAuthenticated
	// identityNamedLocally: the identity is one this node chose — the
	// recipient of its own send, a contact the user added, its own lookup.
	identityNamedLocally
	// identityNotApplicable: no remote node's identity is a key or a value —
	// addresses, IPs, message ids, connections, topics, own data.
	identityNotApplicable
)

func (k identityKeyTrust) valid() bool {
	return k >= identityFromV2Proof && k <= identityNotApplicable
}

type identityStateEntry struct {
	trust  identityKeyTrust
	reason string
}

// stringKeyedIdentityFields are maps outside Service keyed by an identity
// spelled as a string, which no syntactic rule tells from any other string.
// They are listed by hand: the finder reports each while it still holds a
// map, so each is classified like the rest, and a renamed one turns stale.
var stringKeyedIdentityFields = map[string]bool{
	"nonDMKeySyncLimiter.hops":      true,
	"boundedKnownIdentities.nodes":  true,
	"boundedKnownIdentities.pinned": true,
	"trustStore.contacts":           true,
	"trustStore.conflicts":          true,
	"trustStore.records":            true,
	"sessionRecordCache.entries":    true,
}

// identityStateInventory is the classification, by Type.field (a field of an
// anonymous struct inside a type is Type.outer.inner).
var identityStateInventory = map[string]identityStateEntry{
	// Service: penalties and budgets, split by origin.
	"Service.peerQuarantine":        {identityByOrigin, "route quarantine per penaltySubject"},
	"Service.peerDisconnectHistory": {identityByOrigin, "flap history per penaltySubject"},
	"Service.peerAnnounceHistory":   {identityByOrigin, "announce history per penaltySubject"},
	"Service.lastResyncAccepted":    {identityByOrigin, "request_resync debounce per penaltySubject"},

	// Service: what a session names, proven or not.
	"Service.sessions":              {identityUnprovenClaim, "dialled session's peerIdentity comes from the welcome, unproven on v1; closeLegacyClaimsOf closes it at the proof and teardown removes it"},
	"Service.conns":                 {identityUnprovenClaim, "the NetCore of an accepted v1 conn carries the hello identity; closed by closeLegacyClaimsOf at the proof, unregistered on teardown"},
	"Service.identitySessions":      {identityUnprovenClaim, "counted from session.peerIdentity on v1 and v2 alike; decremented at the teardown closeLegacyClaimsOf causes"},
	"Service.identityRelaySessions": {identityUnprovenClaim, "as identitySessions, for relay-capable sessions"},
	"Service.pendingWithdrawals":    {identityUnprovenClaim, "armed by the close of a session's last relay connection, v1 included; cancelled when the identity re-establishes"},
	"Service.subs":                  {identityUnprovenClaim, "keyed by hello.Address on v1 auth_session and v2 alike; removed on inbound teardown after closeLegacyClaimsOf"},
	"Service.observedAddrs":         {identityUnprovenClaim, "keyed by the welcome or session identity; removed on teardown and by eviction"},
	"Service.peerIDs":               {identityUnprovenClaim, "address → identity with its origin: a v1 hello/welcome or peer exchange claims it, a v2 session proves it; a claim never replaces a v2 record, and the claims of an identity are forgotten at its proof (forgetClaimedAddressesOf)"},
	"Service.persistedMeta":         {identityUnprovenClaim, "peers.json metadata whose Identity and IdentityOrigin follow peerIDs; a claimed identity is cleared at the proof"},
	"Service.recipientPassState":    {identityUnprovenClaim, "mostly own recipients, but wakeOverdueForReturningPeer notes a session's peerIdentity, v1 included; rebuilt on the next retry pass"},
	"Service.receipts":              {identityUnprovenClaim, "keyed by the recipient named in unsigned receipt content; dropped when the recipient's last subscriber goes"},
	"Service.awaitingSeenAck":       {identityUnprovenClaim, "own seen receipts, but an unsigned inbound receipt naming this node as sender also lands here"},
	"Service.senderKeySyncInFlight": {identityUnprovenClaim, "keyed by the sender a DM names before its key is known — unverified; transient single-flight"},
	"Service.senderKeySyncLastRun":  {identityUnprovenClaim, "same unverified sender as senderKeySyncInFlight; pruned by age"},
	"Service.senderKeySyncHopInFlight": {identityUnprovenClaim,
		"previous hop by session identity or by peerIDs, v1 included; released when the pass ends"},

	// Inactive K3 advertisement observations preserve raw claims, not proof.
	"Service.k3AdvertisedClaims": {identityUnprovenClaim, "raw original inbound Hello/outbound Welcome identity, v1 included; proof mirroring only marks conflict and never upgrades the claim; removed with its original registered/session owner"},
	"k3AdvertisedSource.peer":    {identityUnprovenClaim, "bounded value projection of those raw claims and Core metadata, v1 included; replaced on original owner mutations and cleared on a source fault, never proof authority"},
	"Service.k3AdvertisedGenerations": {identityNotApplicable,
		"direction/ConnID to original observation generation, with no remote identity in key or value"},

	// The registered K3 population admits only the original v2 proof binding.
	"k3RegisteredSource.owners": {identityFromV2Proof, "only k3RegisteredInbound Auth.ProvenIdentity or k3RegisteredOutbound session.provenIdentity, with matching original Core/ConnID/session; removed on owner replacement/removal and cleared on source fault"},
	"k3RegisteredSource.refs":   {identityFromV2Proof, "refcounts updated solely from those validated registered bindings; decremented/deleted with their original owner and cleared on source fault"},

	// Service: identities bound by a signature or a fingerprint.
	"Service.pubKeys":          {identitySelfAuthenticated, "every writer checks the key's fingerprint or a signed record"},
	"Service.boxKeys":          {identitySelfAuthenticated, "every writer checks identity.VerifyBoxKeyBinding"},
	"Service.boxSigs":          {identitySelfAuthenticated, "the box binding signature is verified before it is stored"},
	"Service.relayDeliveredTo": {identitySelfAuthenticated, "written from ack_delete only after its payload verified against the fingerprint-checked key"},

	// Service: identities this node named.
	"Service.awaitingDelivered":         {identityNamedLocally, "message id → this node's own outgoing DM; optional fixture-only R2a stamp is exact local owner plus original X observation and actual Y proof, never inferred by ordinary/reseed input"},
	"Service.presenceResolutionAskedAt": {identityNamedLocally, "trusted contacts presence resolves"},

	// Service: no remote identity.
	"Service.bannedIPSet":             {identityNotApplicable, "IP → ban entry with addresses"},
	"Service.bans":                    {identityNotApplicable, "IP → ban score"},
	"Service.cancelledDeliveries":     {identityNotApplicable, "message id → time"},
	"Service.connIDByNetConn":         {identityNotApplicable, "net.Conn → ConnID"},
	"Service.dialOrigin":              {identityNotApplicable, "dial address → primary address"},
	"Service.events":                  {identityNotApplicable, "local subscriber channels"},
	"Service.frozenDeliveries":        {identityNotApplicable, "message ids"},
	"Service.health":                  {identityNotApplicable, "address → health, which holds no identity"},
	"Service.inboundByIP":             {identityNotApplicable, "IP → inbound count"},
	"Service.inboundHealthRefs":       {identityNotApplicable, "address → inbound count"},
	"Service.lastTrafficSnap":         {identityNotApplicable, "address → byte counters"},
	"Service.markedNeverEmitted":      {identityNotApplicable, "message ids"},
	"Service.metaOrphanFirstSeen":     {identityNotApplicable, "address → time"},
	"Service.notices":                 {identityNotApplicable, "ciphertext id → notice"},
	"Service.observedIPHistoryByPeer": {identityNotApplicable, "address → observed IPs"},
	"Service.outbound":                {identityNotApplicable, "message id → delivery state; the recipient in it is the message's addressee, not a peer attribution"},
	"Service.peerBuilds":              {identityNotApplicable, "address → build"},
	"Service.peerTypes":               {identityNotApplicable, "address → node type"},
	"Service.peerVersions":            {identityNotApplicable, "address → version"},
	"Service.pending":                 {identityNotApplicable, "address → queued frames"},
	"Service.pendingKeys":             {identityNotApplicable, "dedup of queued frames by address and frame"},
	"Service.pendingUIEvents":         {identityNotApplicable, "message id → UI event"},
	"Service.reachableGroups":         {identityNotApplicable, "network groups computed at startup"},
	"Service.setupFailures":           {identityNotApplicable, "dialled address → consecutive setup failures; never keyed by a claimed identity"},
	"Service.relayRetry":              {identityNotApplicable, "dedup key of a relay or receipt retry → attempts"},
	"Service.remoteBannedIPs":         {identityNotApplicable, "IP → remote ban"},
	"Service.remoteIPBanOffenders":    {identityNotApplicable, "IP → offending addresses"},
	"Service.topics":                  {identityNotApplicable, "topic name → envelopes"},
	"Service.upstream":                {identityNotApplicable, "address set"},

	// The v2 store: proven identities only.
	"secureSessionStore.identities":       {identitySelfAuthenticated, "genuine session publication or historical own file; inactive record-only constructor additionally verifies the signed subject and exact target proof, with no live producer"},
	"secureSessionStore.protectedByGiver": {identityFromV2Proof, "the identity a dial proved and that gave the protection"},
	"secureSessionFile.IdentityMetadata":  {identityByOrigin, "derived snapshot of single pin entries: explicit unclassified migration or verified local provenance, never a remote class claim"},
	"secureSessionFile.Identities":        {identitySelfAuthenticated, "the persisted form of secureSessionStore.identities; format does not assert a live session"},
	"provenIdentityPenalties.entries":     {identityFromV2Proof, "charged only from the proven identity of a loopback v2 session (chargeLoopbackViolation reads it from penaltySubjectOfCore); a legacy connection is condemned, never charged"},

	// The routing input of legacy connections.
	"unprovenRoutingWriters.at":       {identityUnprovenClaim, "noted for an unproven sender's claimed identity; taken at the proof by forgetUnprovenRoutingResidue"},
	"unprovenRoutingWriters.inflight": {identityUnprovenClaim, "legacy inputs in flight per claimed identity; the proof waits for them to drain"},
	"unprovenRoutingWriters.drained":  {identityFromV2Proof, "waited on only by onIdentityProvenV2"},
	"unprovenRoutingWriters.owed":     {identityFromV2Proof, "set only by onIdentityProvenV2 when its context ends"},

	"secureSessionStore.endpoints":    {identityFromV2Proof, "last_identity and protected_by of a binding are written only from a proven outbound v2 session"},
	"secureSessionStore.nonCanonical": {identityFromV2Proof, "bindings an earlier build wrote from v2 sessions, kept as read from the store's own file"},
	"secureSessionFile.Endpoints":     {identityFromV2Proof, "the persisted form of the store's bindings"},

	// Other state of the package.
	"liveKindBucket.identities":                {identityUnprovenClaim, "rebuilt on every walk from session and conn identities, the legacy buckets from claims"},
	"presenceProjectionState.reachable":        {identityUnprovenClaim, "from the routing snapshot, legacy routes included; the proof's purge drops it from the next snapshot"},
	"presenceProjector.records":                {identityUnprovenClaim, "contacts only, but session open/close evidence comes from v1 sessions too; not dropped at the proof"},
	"versionPolicyState.incompatibleReporters": {identityUnprovenClaim, "reporter is peerIDs of the address, a v1 claim; not dropped at the proof"},
	"queryRateLimit.emits":                     {identityUnprovenClaim, "our own route queries, but also targets named by unsigned transit relay frames — a budget, no attribution"},
	"queryRateLimit.inFlight":                  {identityUnprovenClaim, "as queryRateLimit.emits"},
	"pushIdentityHandler.lastAcceptedAt":       {identitySelfAuthenticated, "keyed by the signed source after the record verified"},
	"pushIdentityHandler.violationAt":          {identitySelfAuthenticated, "as lastAcceptedAt"},
	"dmControlSender.refusedAt":                {identitySelfAuthenticated, "from a pair-sealed dm_control refusal of its signed source"},
	"dmControlSender.pending":                  {identitySelfAuthenticated, "our recipients, and the signed source of a sealed command answer"},
	"dmControlSender.forgot":                   {identityNamedLocally, "a contact the user removed"},
	"dmControlSender.framesOut":                {identityNamedLocally, "recipients of our own batches"},
	"dmControlSender.inflight":                 {identityNamedLocally, "our own batches in flight"},
	"dmControlSender.pauseGen":                 {identityNamedLocally, "peers of a locally deleted message"},
	"dmControlSender.paused":                   {identityNamedLocally, "as pauseGen"},
	"dmControlSender.sentAt":                   {identityNamedLocally, "recipients our frames reached"},
	"dmControlSender.typeRefusalShown":         {identityNamedLocally, "a UI flag for our recipient"},
	"identityIntentStore.rows":                 {identityNamedLocally, "resolutions this node started"},
	"identityResolver.resolutions":             {identityNamedLocally, "resolutions this node started"},
	"identityResolver.cooldownUntil":           {identityNamedLocally, "targets of our own resolutions"},
	"identityResolver.attempts":                {identityNamedLocally, "our attempt label → our own lookup target"},
	"presenceProber.nextDue":                   {identityNamedLocally, "trusted contacts"},
	"presenceProber.attempts":                  {identityNamedLocally, "our probe label → a trusted contact"},
	"announceRateLimiter.buckets":              {identityByOrigin, "announce-plane token buckets per penaltySubject"},
	"relayStateStore.states":                   {identityUnprovenClaim, "the recipient comes from an unsigned relay frame; the attempt is charged by the penaltySubject of ForwardedVia"},
	"probeRegistry.pending":                    {identityUnprovenClaim, "the probe's uplink is the direct peer's session identity, a v1 claim included; dropped on ack or timeout"},
	"nonDMKeySyncLimiter.hops":                 {identityUnprovenClaim, "previous hop by session identity, v1 included, as senderKeySyncHopInFlight; windowed"},
	"boundedKnownIdentities.nodes":             {identityUnprovenClaim, "identities this node has heard of — DM recipients, contacts, handshake peers v1 included; membership grants nothing"},
	"boundedKnownIdentities.pinned":            {identityNamedLocally, "trusted contacts, mirrored from the trust store"},
	"trustStore.contacts":                      {identitySelfAuthenticated, "every contact passes identity.VerifyBoxKeyBinding before it is stored"},
	"trustFile.Contacts":                       {identitySelfAuthenticated, "persistent contact keys are fingerprint/box-binding checked; optional local intent additionally requires protected-file audit and explicit local origin, while transient v2 proof is never serialized"},
	"trustSnapshot.contacts":                   {identitySelfAuthenticated, "copy of validated contact rows; local intent retains its exact local generation and only captures an opaque v2 proof after mandatory pin publication"},
	"trustStore.conflicts":                     {identitySelfAuthenticated, "a contact address whose new keys contradict the verified pinned ones"},
	"trustStore.records":                       {identitySelfAuthenticated, "signed identity records, verified before they merge"},
	"sessionRecordCache.entries":               {identitySelfAuthenticated, "signed identity records of non-contacts, verified before they merge"},
	"getIdentityHandler.seenRequesterAttempts": {identityNotApplicable, "keyed by the datagram attempt label, written after the requester verified"},
}

// TestIdentityStateInventoryClassifiesEveryIdentityKeyedField fails on a
// field the guard finds that is not classified, on an entry for a field that
// no longer exists, and on an entry without a valid class or a reason.
func TestIdentityStateInventoryClassifiesEveryIdentityKeyedField(t *testing.T) {
	t.Parallel()

	found := collectIdentityStateFields(t)
	var unclassified []string
	for field := range found {
		if _, ok := identityStateInventory[field]; !ok {
			unclassified = append(unclassified, field)
		}
	}
	sort.Strings(unclassified)
	if len(unclassified) > 0 {
		t.Errorf("%d fields keep state by identity without a classification; decide for each whether it may hold an identity a legacy session merely claims, and add it to identityStateInventory:\n%s",
			len(unclassified), strings.Join(unclassified, "\n"))
	}

	var stale, incomplete []string
	for field, entry := range identityStateInventory {
		if !found[field] {
			stale = append(stale, field)
		}
		if !entry.trust.valid() || strings.TrimSpace(entry.reason) == "" {
			incomplete = append(incomplete, field)
		}
	}
	sort.Strings(stale)
	sort.Strings(incomplete)
	if len(stale) > 0 {
		t.Errorf("classified fields the guard no longer finds (renamed, removed, or no longer keyed by identity): %s", strings.Join(stale, ", "))
	}
	if len(incomplete) > 0 {
		t.Errorf("entries without a class or a reason: %s", strings.Join(incomplete, ", "))
	}
}

// collectIdentityStateFields parses the package's non-test files and returns
// the fields the rules of the file comment find.
func collectIdentityStateFields(t *testing.T) map[string]bool {
	t.Helper()
	return findIdentityStateFields(parsePackageFiles(t))
}

func parsePackageFiles(t *testing.T) []*ast.File {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
	}
	return files
}

// domainImportPath is the package whose PeerIdentity is the identity.
const domainImportPath = "github.com/piratecash/corsa/internal/core/domain"

// packageType is one type declared in the package: its definition and the
// names its file imports the domain package under.
type packageType struct {
	definition  ast.Expr
	domainNames map[string]bool
}

// identityTypeIndex is what the finder knows about the package's types: each
// declared type, and which of them carry an identity.
type identityTypeIndex struct {
	types   map[string]packageType
	bearing map[string]bool
}

// findIdentityStateFields returns the fields of files the rules find, plus the
// fields of stringKeyedIdentityFields that still hold a map.
func findIdentityStateFields(files []*ast.File) map[string]bool {
	index := indexPackageTypes(files)
	found := make(map[string]bool)
	for typeName, declared := range index.types {
		st, ok := declared.definition.(*ast.StructType)
		if !ok {
			continue
		}
		walkStructFields(typeName, st, func(path string, fieldType ast.Expr) {
			holdsIdentityMap := index.mapsIdentity(fieldType, declared.domainNames, map[string]bool{})
			holdsAnyMap := index.holdsMap(fieldType, map[string]bool{})
			if holdsIdentityMap || (typeName == "Service" && holdsAnyMap) || (stringKeyedIdentityFields[path] && holdsAnyMap) {
				found[path] = true
			}
		})
	}
	return found
}

func indexPackageTypes(files []*ast.File) identityTypeIndex {
	index := identityTypeIndex{types: make(map[string]packageType), bearing: make(map[string]bool)}
	for _, file := range files {
		domainNames := domainImportNames(file)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				if typeSpec, ok := spec.(*ast.TypeSpec); ok {
					index.types[typeSpec.Name.Name] = packageType{definition: typeSpec.Type, domainNames: domainNames}
				}
			}
		}
	}
	for grew := true; grew; {
		grew = false
		for name, declared := range index.types {
			if index.bearing[name] || !index.carriesIdentity(declared.definition, declared.domainNames) {
				continue
			}
			index.bearing[name] = true
			grew = true
		}
	}
	return index
}

// domainImportNames are the names file refers to the domain package by: its
// own name, or the alias of its import.
func domainImportNames(file *ast.File) map[string]bool {
	names := make(map[string]bool)
	for _, spec := range file.Imports {
		if strings.Trim(spec.Path.Value, "`\"") != domainImportPath {
			continue
		}
		name := "domain"
		if spec.Name != nil {
			name = spec.Name.Name
		}
		names[name] = true
	}
	return names
}

// carriesIdentity reports whether a type definition holds an identity: a
// struct through any field, any other type through what it is made of.
func (x identityTypeIndex) carriesIdentity(definition ast.Expr, domainNames map[string]bool) bool {
	st, ok := definition.(*ast.StructType)
	if !ok {
		return x.namesIdentity(definition, domainNames)
	}
	for _, field := range st.Fields.List {
		if x.namesIdentity(field.Type, domainNames) {
			return true
		}
	}
	return false
}

// namesIdentity reports whether expr is domain.PeerIdentity (under whatever
// name the file imports domain), a type of the package that carries one, an
// anonymous struct that does, or an instantiation with such a type argument
// — through pointers, slices, arrays and parentheses.
func (x identityTypeIndex) namesIdentity(expr ast.Expr, domainNames map[string]bool) bool {
	switch node := expr.(type) {
	case *ast.SelectorExpr:
		pkg, ok := node.X.(*ast.Ident)
		return ok && domainNames[pkg.Name] && node.Sel.Name == "PeerIdentity"
	case *ast.Ident:
		return x.bearing[node.Name]
	case *ast.StarExpr:
		return x.namesIdentity(node.X, domainNames)
	case *ast.ArrayType:
		return x.namesIdentity(node.Elt, domainNames)
	case *ast.ParenExpr:
		return x.namesIdentity(node.X, domainNames)
	case *ast.StructType:
		return x.carriesIdentity(node, domainNames)
	case *ast.IndexExpr, *ast.IndexListExpr:
		base, args := instantiation(node)
		return x.namesIdentity(base, domainNames) || x.anyNamesIdentity(args, domainNames)
	}
	return false
}

func (x identityTypeIndex) anyNamesIdentity(exprs []ast.Expr, domainNames map[string]bool) bool {
	for _, expr := range exprs {
		if x.namesIdentity(expr, domainNames) {
			return true
		}
	}
	return false
}

// instantiation splits a generic instantiation into its type and arguments.
func instantiation(expr ast.Expr) (ast.Expr, []ast.Expr) {
	switch node := expr.(type) {
	case *ast.IndexExpr:
		return node.X, []ast.Expr{node.Index}
	case *ast.IndexListExpr:
		return node.X, node.Indices
	}
	return expr, nil
}

// holdsMap reports whether expr is a map; a pointer, slice or array of one; a
// named non-struct type of the package that is one; or an instantiation of a
// generic type of the package that keeps one. visiting guards against a
// named type defined through itself.
func (x identityTypeIndex) holdsMap(expr ast.Expr, visiting map[string]bool) bool {
	switch node := expr.(type) {
	case *ast.MapType:
		return true
	case *ast.StarExpr:
		return x.holdsMap(node.X, visiting)
	case *ast.ArrayType:
		return x.holdsMap(node.Elt, visiting)
	case *ast.ParenExpr:
		return x.holdsMap(node.X, visiting)
	case *ast.Ident:
		declared, ok := x.types[node.Name]
		if _, isStruct := declared.definition.(*ast.StructType); !ok || isStruct || visiting[node.Name] {
			return false
		}
		visiting[node.Name] = true
		return x.holdsMap(declared.definition, visiting)
	case *ast.IndexExpr, *ast.IndexListExpr:
		base, _ := instantiation(node)
		return x.genericKeepsMap(base, visiting)
	}
	return false
}

// genericKeepsMap reports whether the generic type base names keeps a map —
// as itself or in a field.
func (x identityTypeIndex) genericKeepsMap(base ast.Expr, visiting map[string]bool) bool {
	ident, ok := base.(*ast.Ident)
	if !ok {
		return false
	}
	declared, ok := x.types[ident.Name]
	if !ok || visiting[ident.Name] {
		return false
	}
	visiting[ident.Name] = true
	st, isStruct := declared.definition.(*ast.StructType)
	if !isStruct {
		return x.holdsMap(declared.definition, visiting)
	}
	for _, field := range st.Fields.List {
		if x.holdsMap(field.Type, visiting) {
			return true
		}
	}
	return false
}

// mapsIdentity reports whether expr holds a map whose key or value names an
// identity, at any depth of nested maps; a named non-struct type of the
// package is looked through; an instantiation of a generic type that keeps a
// map counts when a type argument names an identity — the argument is what
// keys or fills it.
func (x identityTypeIndex) mapsIdentity(expr ast.Expr, domainNames map[string]bool, visiting map[string]bool) bool {
	switch node := expr.(type) {
	case *ast.MapType:
		return x.namesIdentity(node.Key, domainNames) || x.namesIdentity(node.Value, domainNames) || x.mapsIdentity(node.Value, domainNames, visiting)
	case *ast.StarExpr:
		return x.mapsIdentity(node.X, domainNames, visiting)
	case *ast.ArrayType:
		return x.mapsIdentity(node.Elt, domainNames, visiting)
	case *ast.ParenExpr:
		return x.mapsIdentity(node.X, domainNames, visiting)
	case *ast.Ident:
		declared, ok := x.types[node.Name]
		if _, isStruct := declared.definition.(*ast.StructType); !ok || isStruct || visiting[node.Name] {
			return false
		}
		visiting[node.Name] = true
		return x.mapsIdentity(declared.definition, declared.domainNames, visiting)
	case *ast.IndexExpr, *ast.IndexListExpr:
		base, args := instantiation(node)
		return x.anyNamesIdentity(args, domainNames) && x.genericKeepsMap(base, map[string]bool{})
	}
	return false
}

// walkStructFields visits every field of st, descending into anonymous struct
// fields. An embedded field is visited under the name of its type, as Go
// names it; an embedded struct of the package is inventoried as its own type.
func walkStructFields(prefix string, st *ast.StructType, visit func(path string, fieldType ast.Expr)) {
	for _, field := range st.Fields.List {
		if len(field.Names) == 0 {
			visit(prefix+"."+embeddedName(field.Type), field.Type)
			continue
		}
		for _, name := range field.Names {
			path := prefix + "." + name.Name
			if inner, ok := field.Type.(*ast.StructType); ok {
				walkStructFields(path, inner, visit)
				continue
			}
			visit(path, field.Type)
		}
	}
}

// embeddedName is the name Go gives an embedded field: its type's name,
// through a pointer, a package qualifier or type arguments.
func embeddedName(expr ast.Expr) string {
	switch node := expr.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.StarExpr:
		return embeddedName(node.X)
	case *ast.SelectorExpr:
		return node.Sel.Name
	case *ast.IndexExpr, *ast.IndexListExpr:
		base, _ := instantiation(node)
		return embeddedName(base)
	}
	return "?"
}
