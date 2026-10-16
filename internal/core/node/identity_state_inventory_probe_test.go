package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// identity_state_inventory_probe_test.go feeds the inventory's finder
// synthetic sources, one per shape identity state can take, so a blind spot
// of the walk shows up as a probe it does not find rather than as a field
// nobody classified.

// identityStateProbes are the shapes the finder must reach, each a source
// file and the field it must report.
var identityStateProbes = map[string]struct {
	source string
	field  string
}{
	"a map keyed by the identity": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type probe struct{ m map[domain.PeerIdentity]int }`,
		field: "probe.m",
	},
	"a map whose value is a struct carrying the identity": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type owner struct{ id domain.PeerIdentity }
type probe struct{ m map[string]owner }`,
		field: "probe.m",
	},
	"a generic set instantiated with an identity-bearing key": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type subject struct{ id domain.PeerIdentity }
type set[K comparable] struct{ entries map[K]int }
type probe struct{ s *set[subject] }`,
		field: "probe.s",
	},
	"a named map type": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type byIdentity map[domain.PeerIdentity]int
type probe struct{ m byIdentity }`,
		field: "probe.m",
	},
	"an embedded named map type": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type byID map[domain.PeerIdentity]int
type probe struct{ byID }`,
		field: "probe.byID",
	},
	"a map whose value is an anonymous struct carrying the identity": {
		source: `package node
import "github.com/piratecash/corsa/internal/core/domain"
type probe struct{ m map[string]struct{ id domain.PeerIdentity } }`,
		field: "probe.m",
	},
	"the domain package imported under an alias": {
		source: `package node
import d "github.com/piratecash/corsa/internal/core/domain"
type probe struct{ m map[d.PeerIdentity]int }`,
		field: "probe.m",
	},
}

func TestIdentityStateInventoryFindsEveryShapeOfIdentityState(t *testing.T) {
	t.Parallel()
	for name, probe := range identityStateProbes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			found := findIdentityStateFields([]*ast.File{parseProbe(t, probe.source)})
			if !found[probe.field] {
				t.Fatalf("the finder misses %s; found %v", probe.field, found)
			}
		})
	}
}

// A map that holds no identity outside Service, and a struct with an
// identity but no map, are not identity state: the finder must not flood the
// inventory with them.
func TestIdentityStateInventoryIgnoresStateWithoutIdentity(t *testing.T) {
	t.Parallel()
	found := findIdentityStateFields([]*ast.File{parseProbe(t, `package node
import "github.com/piratecash/corsa/internal/core/domain"
type counters struct{ m map[string]int }
type single struct{ id domain.PeerIdentity }
type other struct{ m map[string]single2 }
type single2 struct{ n int }`)})
	if len(found) != 0 {
		t.Fatalf("the finder reports state without identity: %v", found)
	}
}

func parseProbe(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "probe.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse the probe: %v", err)
	}
	return file
}
