package desktop

import (
	"path/filepath"
	"reflect"
	"testing"
)

// Save used to marshal a hand-written mirror of the field list, so a field
// added to Preferences alone would load and then never be written back — and
// nothing would say so. This walks every exported field, sets it, and reads the
// file back: a new field that Save drops fails here rather than silently
// resetting the user's choice on the next start.
func TestEveryPreferenceSurvivesASaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json.desktop.json")

	prefs := &Preferences{path: path}
	value := reflect.ValueOf(prefs).Elem()
	populated := 0
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.CanSet() {
			continue // path, which is deliberately not persisted
		}
		populated++
		switch field.Kind() {
		case reflect.String:
			field.SetString("ru")
		case reflect.Bool:
			field.SetBool(true)
		case reflect.Slice:
			field.Set(reflect.ValueOf([]string{"🔥"}))
		case reflect.Map:
			field.Set(reflect.ValueOf(map[string]string{"peer": "alias"}))
		default:
			t.Fatalf("field %s has kind %s, which this test does not know how to populate",
				value.Type().Field(i).Name, field.Kind())
		}
	}
	if populated == 0 {
		t.Fatal("no settable fields found; this guard is inspecting the wrong type")
	}

	if err := prefs.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadPreferences(path)
	if err != nil {
		t.Fatalf("LoadPreferences: %v", err)
	}

	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		field := value.Field(i)
		if !field.CanSet() {
			continue
		}
		got := reflect.ValueOf(loaded).Elem().Field(i)
		if !reflect.DeepEqual(got.Interface(), field.Interface()) {
			t.Errorf("%s = %v after a save/load round trip, want %v", name, got.Interface(), field.Interface())
		}
	}
}

// The release check is opt-in, and "opt-in" has to mean the file with nothing
// in it — not just the file the user last wrote.
func TestReleaseCheckIsOffInAFreshProfile(t *testing.T) {
	prefs, err := LoadPreferences(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("LoadPreferences on a missing file: %v", err)
	}
	if prefs.CheckGitHubReleases {
		t.Fatal("a profile that has never been written consents to the GitHub release check")
	}
}
