package updatecheck

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// tagsPayload is the shape the endpoint actually returns, trimmed to the one
// field this package reads.
const tagsPayload = `[
  {"name": "v2.0.66"},
  {"name": "v2.3.69"},
  {"name": "v2.2.68"},
  {"name": "v1.0.64"}
]`

func newTagServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestLatestReleaseTakesTheHighestTagNotTheFirst(t *testing.T) {
	// The endpoint documents no ordering for tags, and the payload above is
	// deliberately out of order: reading entry zero would answer 2.0.66 and
	// tell a user on 2.3.69 that they are ahead of the project.
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(tagsPayload))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	latest, err := client.LatestRelease(context.Background())
	if err != nil {
		t.Fatalf("LatestRelease: unexpected error: %v", err)
	}
	if want := (domain.ReleaseVersion{Major: 2, Minor: 3, Build: 69}); latest != want {
		t.Fatalf("LatestRelease = %s, want %s", latest, want)
	}
}

func TestLatestReleaseSkipsNamesThatAreNotReleases(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"nightly"},{"name":"v2.3.70-rc1"},{"name":"v2.3.69"}]`))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	latest, err := client.LatestRelease(context.Background())
	if err != nil {
		t.Fatalf("LatestRelease: unexpected error: %v", err)
	}
	if want := (domain.ReleaseVersion{Major: 2, Minor: 3, Build: 69}); latest != want {
		t.Fatalf("LatestRelease = %s, want %s", latest, want)
	}
}

// One page is not the listing. The endpoint returns 30 tags by default and
// promises nothing about their order, so a release past the first page is a
// release this program would answer "no update" about — and the two
// assumptions compound, because the only reason to expect the newest tag on
// page one is the ordering that was never promised.
func TestLatestReleaseFollowsThePagingToTheLastPage(t *testing.T) {
	var server *httptest.Server
	var paths []string

	server = newTagServer(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		switch r.URL.Query().Get("page") {
		case "", "1":
			w.Header().Set("Link",
				`<`+server.URL+`/tags?per_page=100&page=2>; rel="next", <`+server.URL+`/tags?per_page=100&page=3>; rel="last"`)
			_, _ = w.Write([]byte(tagsPayload))
		case "2":
			w.Header().Set("Link", `<`+server.URL+`/tags?per_page=100&page=3>; rel="next"`)
			// The newest release, hiding where a single-page reader cannot see
			// it.
			_, _ = w.Write([]byte(`[{"name":"v2.3.100"},{"name":"v1.0.60"}]`))
		default:
			_, _ = w.Write([]byte(`[{"name":"v0.9.1"}]`))
		}
	})

	client := NewClient(ClientOpts{TagsURL: server.URL + "/tags"})
	latest, err := client.LatestRelease(context.Background())
	if err != nil {
		t.Fatalf("LatestRelease: unexpected error: %v", err)
	}
	if want := (domain.ReleaseVersion{Major: 2, Minor: 3, Build: 100}); latest != want {
		t.Fatalf("LatestRelease = %s, want %s — the pages past the first were not read", latest, want)
	}
	if len(paths) != 3 {
		t.Fatalf("the client fetched %d pages (%v), want all three", len(paths), paths)
	}
	// The first request already asks for the biggest page the endpoint serves,
	// so the ordinary repository is one round trip rather than three.
	if !strings.Contains(paths[0], "per_page=100") {
		t.Fatalf("the first request did not ask for a full page: %s", paths[0])
	}
}

// A Link header pointing at the page it came from would otherwise keep the
// checker fetching for as long as the process lives. The cap stops it — and
// reaching the cap is a FAILED check, not a quiet answer from a prefix.
func TestPagingIsBoundedAndReportsTruncation(t *testing.T) {
	var server *httptest.Server
	requests := 0

	server = newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Link", `<`+server.URL+`/tags?page=1>; rel="next"`)
		_, _ = w.Write([]byte(`[{"name":"v1.0.1"}]`))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL + "/tags"})
	_, err := client.LatestRelease(context.Background())
	if !errors.Is(err, ErrTagListingTruncated) {
		t.Fatalf("LatestRelease error = %v, want ErrTagListingTruncated", err)
	}
	if requests != maxTagPages {
		t.Fatalf("a self-referencing Link header produced %d requests, want the %d-page cap", requests, maxTagPages)
	}
}

// The maximum of a prefix is not the maximum. Ten pages of old tags and an
// eleventh carrying the newest release must not read as "you are up to date" —
// that is the one conclusion an incomplete listing cannot support, and it is
// the conclusion the user acts on by doing nothing.
func TestReachingThePageCapIsNotAnAnswer(t *testing.T) {
	var server *httptest.Server

	server = newTagServer(t, func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" {
			page = "1"
		}
		number, err := strconv.Atoi(page)
		if err != nil {
			t.Errorf("unexpected page parameter %q", page)
			return
		}
		if number <= maxTagPages {
			w.Header().Set("Link",
				fmt.Sprintf(`<%s/tags?per_page=100&page=%d>; rel="next"`, server.URL, number+1))
			_, _ = w.Write([]byte(`[{"name":"v2.3.69"}]`))
			return
		}
		// The page the cap never reaches.
		_, _ = w.Write([]byte(`[{"name":"v2.3.100"}]`))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL + "/tags"})
	latest, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatalf("LatestRelease returned %s from a truncated listing instead of failing", latest)
	}
	if !errors.Is(err, ErrTagListingTruncated) {
		t.Fatalf("LatestRelease error = %v, want ErrTagListingTruncated", err)
	}
}

// The next-page URL comes out of the response, so it is checked rather than
// trusted: a page that answers "continue over there" must not be able to move
// the request to another host. Same rule as the redirect refusal.
func TestPagingRefusesToLeaveTheHost(t *testing.T) {
	elsewhere := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"v9.9.9"}]`))
	})
	origin := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Link", `<`+elsewhere.URL+`/tags?page=2>; rel="next"`)
		_, _ = w.Write([]byte(tagsPayload))
	})

	client := NewClient(ClientOpts{TagsURL: origin.URL + "/tags"})
	_, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatal("LatestRelease: expected the cross-host paging to be refused")
	}
	if !strings.Contains(err.Error(), "refusing to follow tag paging") {
		t.Fatalf("error %v is not the paging refusal", err)
	}
}

func TestLinkRelNextReadsOnlyTheNextRelation(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
	}{
		{name: "empty", header: "", want: ""},
		{
			name:   "next among others",
			header: `<https://x/tags?page=3>; rel="last", <https://x/tags?page=2>; rel="next"`,
			want:   "https://x/tags?page=2",
		},
		{
			name:   "no next relation",
			header: `<https://x/tags?page=1>; rel="prev", <https://x/tags?page=9>; rel="last"`,
			want:   "",
		},
		{name: "unquoted rel", header: `<https://x/tags?page=2>; rel=next`, want: "https://x/tags?page=2"},
		{name: "malformed", header: `https://x/tags?page=2; rel="next"`, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := linkRelNext(tc.header); got != tc.want {
				t.Fatalf("linkRelNext(%q) = %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}

func TestLatestReleaseReportsNoReleaseTags(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"nightly"}]`))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	_, err := client.LatestRelease(context.Background())
	if !errors.Is(err, ErrNoReleaseTags) {
		t.Fatalf("LatestRelease error = %v, want ErrNoReleaseTags", err)
	}
}

func TestLatestReleaseSendsTheHeadersTheEndpointRequires(t *testing.T) {
	// The API answers 403 to a request without a User-Agent, so a missing
	// header would turn every check into a permanent failure.
	var agent, accept string
	server := newTagServer(t, func(w http.ResponseWriter, r *http.Request) {
		agent = r.Header.Get("User-Agent")
		accept = r.Header.Get("Accept")
		_, _ = w.Write([]byte(tagsPayload))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	if _, err := client.LatestRelease(context.Background()); err != nil {
		t.Fatalf("LatestRelease: unexpected error: %v", err)
	}
	if agent != userAgent {
		t.Fatalf("User-Agent = %q, want %q", agent, userAgent)
	}
	if accept != "application/vnd.github+json" {
		t.Fatalf("Accept = %q, want the GitHub JSON media type", accept)
	}
}

// The request exists to learn whether this build is old. Naming the build
// anywhere in the request would answer that question for the endpoint instead.
//
// The REQUEST is scanned, not the constant: asserting that `userAgent` has no
// digits proves nothing about a header built as userAgent+"/"+CorsaVersion.
func TestTheRequestDoesNotCarryTheRunningVersion(t *testing.T) {
	var captured *http.Request
	server := newTagServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured = r.Clone(r.Context())
		_, _ = w.Write([]byte(tagsPayload))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	if _, err := client.LatestRelease(context.Background()); err != nil {
		t.Fatalf("LatestRelease: unexpected error: %v", err)
	}
	if captured == nil {
		t.Fatal("the endpoint was never reached")
	}

	version := config.CorsaVersion
	if captured.URL.String() != "" && strings.Contains(captured.URL.String(), version) {
		t.Fatalf("the request line carries the running version %q: %s", version, captured.URL)
	}
	for name, values := range captured.Header {
		for _, value := range values {
			if strings.Contains(value, version) {
				t.Fatalf("header %s carries the running version %q: %s", name, version, value)
			}
			// The version's own components, in case a future build assembles
			// them rather than using CorsaVersion.
			if strings.ContainsAny(value, "0123456789") && name == "User-Agent" {
				t.Fatalf("User-Agent %q carries digits; it must not identify the running build", value)
			}
		}
	}
}

func TestLatestReleaseFailsOnNonOKStatus(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	_, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatal("LatestRelease: expected an error on a 403 response")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error %v does not name the status", err)
	}
}

func TestLatestReleaseFailsOnUndecodableBody(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html>not json</html>"))
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	if _, err := client.LatestRelease(context.Background()); err == nil {
		t.Fatal("LatestRelease: expected an error on a non-JSON body")
	}
}

func TestLatestReleaseRefusesARedirectOffTheHost(t *testing.T) {
	elsewhere := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(tagsPayload))
	})
	origin := newTagServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL, http.StatusFound)
	})

	client := NewClient(ClientOpts{TagsURL: origin.URL})
	_, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatal("LatestRelease: expected the cross-host redirect to be refused")
	}
	if !strings.Contains(err.Error(), "refusing redirect") {
		t.Fatalf("error %v is not the redirect refusal", err)
	}
}

func TestLatestReleaseHonoursContextCancellation(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(tagsPayload))
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := NewClient(ClientOpts{TagsURL: server.URL})
	if _, err := client.LatestRelease(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("LatestRelease error = %v, want context.Canceled", err)
	}
}

func TestRedactURLDropsTheQueryString(t *testing.T) {
	if got := redactURL("https://api.github.com/repos/x/y/tags?token=secret"); got != "https://api.github.com/repos/x/y/tags" {
		t.Fatalf("redactURL = %q, query string survived", got)
	}
}

// The ERROR the user is shown, not the redaction helper on its own.
//
// net/http hands back a *url.Error whose Error() prints the full request URL,
// query string and all. Wrapping it with %w put that straight back into the
// message — so the helper passed its own test while the string next to it
// leaked exactly what the helper exists to remove.
func TestATransportFailureDoesNotLeakTheRequestURL(t *testing.T) {
	// A port nothing is listening on: the request fails inside the transport,
	// which is the path that produces a *url.Error.
	client := NewClient(ClientOpts{TagsURL: "http://127.0.0.1:1/repos/x/y/tags?token=SECRET#frag"})

	_, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatal("LatestRelease: expected a transport failure")
	}
	for _, secret := range []string{"SECRET", "token=", "#frag", "frag"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the error leaks %q: %s", secret, err)
		}
	}
	// The host is the point of the message and stays.
	if !strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Fatalf("the error does not say which endpoint failed: %s", err)
	}
}

// Redaction must not break errors.Is: a cancelled request has to stay
// recognisable as one, or every caller is left comparing strings.
func TestRedactionKeepsTheCauseInspectable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := NewClient(ClientOpts{TagsURL: "http://127.0.0.1:1/repos/x/y/tags"})
	if _, err := client.LatestRelease(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("LatestRelease error = %v, want it to unwrap to context.Canceled", err)
	}
}

// A body larger than the cap must not be read into memory whole. The cap is
// what stops a hostile or broken endpoint from feeding the decoder until the
// process runs out of memory, and an untested cap is a hope.
func TestOversizedBodiesAreCappedNotSwallowed(t *testing.T) {
	server := newTagServer(t, func(w http.ResponseWriter, _ *http.Request) {
		// Valid JSON that never ends: the decoder would keep going forever
		// without the cap.
		_, _ = w.Write([]byte(`[{"name":"v2.3.69"}`))
		chunk := bytes.Repeat([]byte(`,{"name":"v0.0.1"}`), 4096)
		// Just past the cap, not a flood: the point is that the reader stops,
		// and writing megabytes to prove it only makes the suite slower.
		for written := 0; written < maxResponseBytes+len(chunk); written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	})

	client := NewClient(ClientOpts{TagsURL: server.URL})
	_, err := client.LatestRelease(context.Background())
	if err == nil {
		t.Fatal("LatestRelease: an unterminated body decoded successfully")
	}
	// Truncated JSON, not an out-of-memory death: the point is that the read
	// stopped.
	if !strings.Contains(err.Error(), "decode tag response") {
		t.Fatalf("error %v does not look like a truncated decode", err)
	}
}
