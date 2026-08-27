package deeplink_test

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/deeplink"
)

// registration_test.go guards the OTHER half of a deep link: the code in
// this package can route a corsa: URI perfectly and never see one,
// because the operating system only hands over links for a scheme the
// application has DECLARED. Every platform declares it in a file of its
// own, none of them in Go, and none of them fails a build when the
// declaration is missing — which is exactly how all four shipped without
// it. These tests are that missing build failure.

func repoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{"..", "..", ".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

// TestMacOSBundleDeclaresTheScheme: LaunchServices routes corsa: links
// to the bundle only if its Info.plist claims the scheme.
func TestMacOSBundleDeclaresTheScheme(t *testing.T) {
	plist := repoFile(t, "packaging", "macos", "Info.plist")

	_, schemes, found := strings.Cut(plist, "<key>CFBundleURLSchemes</key>")
	if !found {
		t.Fatal("Info.plist declares no CFBundleURLSchemes: macOS will never deliver a corsa: link")
	}
	array, _, _ := strings.Cut(schemes, "</array>")
	if !strings.Contains(array, "<string>"+deeplink.Scheme+"</string>") {
		t.Errorf("CFBundleURLSchemes does not list %q:\n%s", deeplink.Scheme, array)
	}
}

// TestLinuxDesktopEntryDeclaresTheScheme: xdg-open picks the handler by
// the x-scheme-handler MIME type, and passes the URI through %u.
func TestLinuxDesktopEntryDeclaresTheScheme(t *testing.T) {
	entry := repoFile(t, "packaging", "linux", "corsa.desktop")

	if !strings.Contains(entry, "MimeType="+"x-scheme-handler/"+deeplink.Scheme) {
		t.Errorf("corsa.desktop declares no x-scheme-handler/%s MIME type", deeplink.Scheme)
	}
	exec := ""
	for _, line := range strings.Split(entry, "\n") {
		if strings.HasPrefix(line, "Exec=") {
			exec = line
		}
	}
	if !strings.Contains(exec, "%u") {
		t.Errorf("Exec line %q takes no URI argument (%%u): the link would be dropped on launch", exec)
	}
}

// TestAndroidManifestDeclaresTheScheme: without a VIEW/BROWSABLE intent
// filter for the scheme, Android hands corsa: links to a browser.
func TestAndroidManifestDeclaresTheScheme(t *testing.T) {
	var manifest androidManifest
	raw := repoFile(t, "packaging", "android", "app", "src", "main", "AndroidManifest.xml")
	if err := xml.Unmarshal([]byte(raw), &manifest); err != nil {
		t.Fatalf("parse AndroidManifest.xml: %v", err)
	}

	for _, activity := range manifest.Application.Activities {
		for _, filter := range activity.IntentFilters {
			if !filter.handlesScheme(deeplink.Scheme) {
				continue
			}
			if !filter.hasAction("android.intent.action.VIEW") {
				t.Error("the corsa: intent filter does not act on VIEW")
			}
			if !filter.hasCategory("android.intent.category.BROWSABLE") {
				t.Error("the corsa: intent filter is not BROWSABLE: a link tapped in a browser will not reach it")
			}
			if !filter.hasCategory("android.intent.category.DEFAULT") {
				t.Error("the corsa: intent filter has no DEFAULT category: an implicit intent will not match it")
			}
			return
		}
	}
	t.Fatalf("no intent filter declares the %q scheme", deeplink.Scheme)
}

// TestWindowsBuildDeclaresTheScheme: on Windows the declaration is a
// linker flag — Gio registers the scheme in HKCU at startup and relays a
// second launch into the running instance only when it is set.
func TestWindowsBuildDeclaresTheScheme(t *testing.T) {
	makefile := repoFile(t, "Makefile")

	schemes := ""
	for _, line := range strings.Split(makefile, "\n") {
		if name, value, ok := strings.Cut(line, "?="); ok && strings.TrimSpace(name) == "APP_URL_SCHEMES" {
			schemes = strings.TrimSpace(value)
		}
	}
	if !containsScheme(schemes, deeplink.Scheme) {
		t.Errorf("APP_URL_SCHEMES is %q, which does not declare %q", schemes, deeplink.Scheme)
	}
	if !strings.Contains(makefile, "-X gioui.org/app.schemesURI=$(APP_URL_SCHEMES)") {
		t.Error("the Windows desktop link flags do not pass APP_URL_SCHEMES to gioui.org/app.schemesURI")
	}
	for _, target := range []string{"build-desktop-windows-amd64", "build-desktop-windows-arm64"} {
		if !strings.Contains(makefile, target) {
			t.Errorf("%s target is gone — the scheme flag may have gone with it", target)
		}
	}
}

func containsScheme(list, want string) bool {
	for _, scheme := range strings.Split(list, ",") {
		if strings.TrimSpace(scheme) == want {
			return true
		}
	}
	return false
}

// androidManifest is the sliver of the manifest this test judges: which
// activity accepts which URI scheme, and under which categories.
type androidManifest struct {
	Application struct {
		Activities []struct {
			IntentFilters []androidIntentFilter `xml:"intent-filter"`
		} `xml:"activity"`
	} `xml:"application"`
}

type androidNamed struct {
	Name string `xml:"name,attr"`
}

type androidIntentFilter struct {
	Actions    []androidNamed `xml:"action"`
	Categories []androidNamed `xml:"category"`
	Data       []struct {
		Scheme string `xml:"scheme,attr"`
	} `xml:"data"`
}

func (f androidIntentFilter) handlesScheme(scheme string) bool {
	for _, data := range f.Data {
		if data.Scheme == scheme {
			return true
		}
	}
	return false
}

func (f androidIntentFilter) hasAction(name string) bool { return named(f.Actions, name) }

func (f androidIntentFilter) hasCategory(name string) bool { return named(f.Categories, name) }

func named(items []androidNamed, want string) bool {
	for _, item := range items {
		if item.Name == want {
			return true
		}
	}
	return false
}
