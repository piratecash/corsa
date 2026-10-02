package runjournal

import (
	"errors"
	"regexp"
	"testing"
)

func TestConfigIDIsDerivedFromEveryField(t *testing.T) {
	base := gridKey("grid", "64")
	variants := map[string]ConfigKey{
		"base":        base,
		"measurement": {Measurement: "other", Label: base.Label, Params: base.Params},
		"label":       {Measurement: base.Measurement, Label: "control", Params: base.Params},
		"value":       gridKey("grid", "65"),
		"name": {Measurement: base.Measurement, Label: base.Label, Params: []Param{
			{Name: "peers", Value: "64"}, {Name: "churn", Value: "quiet"}, {Name: "seed", Value: "7"},
		}},
		"added": {Measurement: base.Measurement, Label: base.Label, Params: append(append([]Param(nil),
			base.Params...), Param{Name: "hubs", Value: "2"})},
		"removed": {Measurement: base.Measurement, Label: base.Label, Params: base.Params[:2]},
		"empty value": {Measurement: base.Measurement, Label: base.Label, Params: append(append([]Param(nil),
			base.Params...), Param{Name: "hubs", Value: ""})},
	}

	seen := map[ConfigID]string{}
	for name, key := range variants {
		id := key.ID()
		if previous, clash := seen[id]; clash {
			t.Fatalf("%q and %q share identifier %s: a parameter change went unnoticed", previous, name, id)
		}
		seen[id] = name
	}
}

func TestConfigIDHasCanonicalForm(t *testing.T) {
	id := gridKey("grid", "64").ID()
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(string(id)) {
		t.Fatalf("identifier %q is not 16 lowercase hex characters", id)
	}
	// Pinned: the identifier names files already on disk, so a change of the
	// canonical form must be a deliberate format change, not a side effect.
	const pinned ConfigID = "245c5d8aaa54d670"
	if id != pinned {
		t.Fatalf("identifier of the reference key is %s, pinned %s", id, pinned)
	}
}

func TestConfigIDIgnoresDeclarationOrder(t *testing.T) {
	key := gridKey("grid", "64")
	reordered := ConfigKey{Measurement: key.Measurement, Label: key.Label, Params: []Param{
		key.Params[2], key.Params[0], key.Params[1],
	}}
	if key.ID() != reordered.ID() {
		t.Fatalf("the same parameter set in another order gives %s and %s", key.ID(), reordered.ID())
	}
}

func TestConfigIDHasNoAmbiguousBoundaries(t *testing.T) {
	pairs := [][2]ConfigKey{
		{
			{Measurement: "m", Params: []Param{{Name: "a", Value: "b=c"}}},
			{Measurement: "m", Params: []Param{{Name: "a=b", Value: "c"}}},
		},
		{
			{Measurement: "m", Params: []Param{{Name: "a", Value: ""}, {Name: "b", Value: "x"}}},
			{Measurement: "m", Params: []Param{{Name: "c", Value: ""}, {Name: "b", Value: "x"}}},
		},
		{
			{Measurement: "ab", Label: "c"},
			{Measurement: "a", Label: "bc"},
		},
		{
			{Measurement: "m", Params: []Param{{Name: "a", Value: "1\nk1:b\nv1:2"}}},
			{Measurement: "m", Params: []Param{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}},
		},
	}
	for index, pair := range pairs {
		if pair[0].ID() == pair[1].ID() {
			t.Fatalf("pair %d: two different keys share identifier %s", index, pair[0].ID())
		}
	}
}

func TestValidateRefusesKeysThatCannotIdentifyARun(t *testing.T) {
	cases := map[string]ConfigKey{
		"no measurement": {Label: "x"},
		"nameless param": {Measurement: "m", Params: []Param{{Name: "", Value: "1"}}},
		"duplicate name": {Measurement: "m", Params: []Param{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}},
	}
	for name, key := range cases {
		if err := key.Validate(); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("%s: Validate = %v, want ErrInvalidKey", name, err)
		}
	}
	if err := gridKey("grid", "64").Validate(); err != nil {
		t.Fatalf("a complete key is refused: %v", err)
	}
}

func TestRequireDistinctRefusesSharedIdentifiers(t *testing.T) {
	keys := []ConfigKey{gridKey("grid", "64"), gridKey("grid", "128"), gridKey("grid", "64")}
	if err := RequireDistinct(keys); !errors.Is(err, ErrDuplicateConfig) {
		t.Fatalf("RequireDistinct = %v, want ErrDuplicateConfig", err)
	}
	if err := RequireDistinct(keys[:2]); err != nil {
		t.Fatalf("distinct keys refused: %v", err)
	}
}
