// Package updatecheck asks the project's GitHub repository which release is
// the newest, so the UI can offer an update without waiting for the peer-based
// signal to accumulate evidence.
//
// It is OPT-IN and UI-only. The messenger is anonymous: every other byte this
// process sends goes over the p2p transport, and this package is the one place
// that opens a direct connection to a third party, which shows that party the
// node operator's IP address. Nothing under internal/core/node, sdk or
// cmd/corsa-node imports it — a headless node has no user to ask for consent,
// so it has no business making the request. The consent lives in the desktop
// preferences and is off by default.
//
// It does not replace the peer-based version policy (internal/core/node,
// domain.VersionPolicySnapshot); it is a second, independent source for the
// same question, and the UI lights its badge when EITHER says an update
// exists.
package updatecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	// DefaultTagsURL is the tag listing for the published repository.
	DefaultTagsURL = "https://api.github.com/repos/piratecash/corsa/tags"
	// defaultTimeout bounds one check. Long enough for a slow link, short
	// enough that an hourly schedule can never queue two requests.
	defaultTimeout = 15 * time.Second
	// maxResponseBytes caps what is read from ONE page. A tag page is a few
	// kilobytes; the cap is what stops a hostile or broken endpoint from
	// feeding the decoder until the process runs out of memory.
	maxResponseBytes = 1 << 20
	// tagsPerPage is the page size asked for. 100 is the endpoint's maximum,
	// so a repository with a normal number of tags is one request.
	tagsPerPage = 100
	// maxTagPages bounds the paging walk. Without it an endpoint whose Link
	// header points at itself keeps the checker fetching forever; with it the
	// worst case is a bounded 100 * maxTagPages tag names in memory. Ten pages
	// is a thousand tags — orders of magnitude past this project's count, and
	// answering from the first thousand is still better than not answering.
	maxTagPages = 10
	// userAgent is sent because the GitHub API refuses requests without one.
	// It deliberately does NOT carry the running version: the whole point of
	// the request is to learn whether this build is old, and volunteering
	// which build it is would hand the endpoint a fact it was not asked for.
	userAgent = "corsa"
)

// ErrNoReleaseTags means the endpoint answered, but not one of the names it
// returned parses as a release version. Distinct from a transport failure:
// retrying will not help until someone publishes a tag.
var ErrNoReleaseTags = errors.New("no release tags found")

// ErrTagListingTruncated means the page cap was reached with the listing still
// continuing. The tags that were read are a PREFIX, and the maximum of a prefix
// is not the maximum — so the question was not answered, and saying nothing
// about it would answer it wrongly.
var ErrTagListingTruncated = errors.New("tag listing truncated")

// ReleaseSource answers which release is the newest. The Checker takes this
// rather than *Client so the schedule can be driven without a server.
type ReleaseSource interface {
	LatestRelease(ctx context.Context) (domain.ReleaseVersion, error)
}

// ReleaseSourceFunc adapts a plain function to ReleaseSource, the way
// http.HandlerFunc adapts one to http.Handler.
type ReleaseSourceFunc func(ctx context.Context) (domain.ReleaseVersion, error)

// LatestRelease calls f.
func (f ReleaseSourceFunc) LatestRelease(ctx context.Context) (domain.ReleaseVersion, error) {
	return f(ctx)
}

// ClientOpts carries everything about a Client that has a working default.
type ClientOpts struct {
	// TagsURL overrides the endpoint. Empty means DefaultTagsURL. Tests set
	// it to an httptest server.
	TagsURL string
	// HTTP overrides the transport. Nil means a client with defaultTimeout
	// and the standard transport — which honours the HTTPS_PROXY/ALL_PROXY
	// environment, so an operator who already routes this machine through a
	// proxy keeps that routing here too.
	HTTP *http.Client
}

// Client reads the repository's tag list over HTTPS.
type Client struct {
	http    *http.Client
	tagsURL string
}

// NewClient builds the tag reader. Every field has a default, so the opts
// struct is optional in practice and the zero value is the production
// configuration.
func NewClient(opts ClientOpts) *Client {
	httpClient := opts.HTTP
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: defaultTimeout,
			// A redirect off the host the request was aimed at is refused
			// rather than followed. Nothing secret travels here, but an
			// endpoint that answers "go ask someone else" is not something
			// this package should silently obey.
			CheckRedirect: refuseCrossHostRedirect,
		}
	}
	tagsURL := opts.TagsURL
	if tagsURL == "" {
		tagsURL = DefaultTagsURL
	}
	return &Client{http: httpClient, tagsURL: tagsURL}
}

// refuseCrossHostRedirect stops a redirect chain that leaves the host the
// request started on.
func refuseCrossHostRedirect(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	if !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
		return fmt.Errorf("refusing redirect from %s to %s", via[0].URL.Host, req.URL.Host)
	}
	if len(via) >= 5 {
		return errors.New("too many redirects")
	}
	return nil
}

// gitHubTag is the one field of the tag listing this package reads.
type gitHubTag struct {
	Name string `json:"name"`
}

// LatestRelease returns the highest release version among the published tags.
//
// The MAXIMUM, not the first entry: the endpoint documents no ordering for
// tags, so "the newest one is at the top" is an assumption that holds until
// someone re-tags an old commit. Names that are not release versions (release
// candidates, branch markers) are skipped rather than treated as failures —
// a repository is allowed to carry tags this program does not understand.
func (c *Client) LatestRelease(ctx context.Context) (domain.ReleaseVersion, error) {
	tags, err := c.fetchTags(ctx)
	if err != nil {
		return domain.ReleaseVersion{}, err
	}

	latest := domain.ReleaseVersion{}
	found := false
	for _, tag := range tags {
		version, err := domain.ParseReleaseVersion(tag.Name)
		if err != nil {
			continue
		}
		if !found || version.Newer(latest) {
			latest = version
			found = true
		}
	}
	if !found {
		return domain.ReleaseVersion{}, ErrNoReleaseTags
	}
	return latest, nil
}

// fetchTags reads the whole tag listing, following the endpoint's paging.
//
// One page is not the listing. The endpoint returns 30 tags by default and says
// nothing about their order, so a release sitting past the first page is a
// release this program would answer "no update" about — and the two assumptions
// compound: the only reason to believe the newest tag is on page one is the
// ordering the endpoint does not promise. per_page raises the page size and the
// Link header is followed for the rest.
func (c *Client) fetchTags(ctx context.Context) ([]gitHubTag, error) {
	next := withPerPage(c.tagsURL)

	var all []gitHubTag
	for page := 0; page < maxTagPages && next != ""; page++ {
		tags, link, err := c.fetchTagPage(ctx, next)
		if err != nil {
			return nil, err
		}
		all = append(all, tags...)
		// A short page is the last one, whatever the header says.
		if len(tags) == 0 {
			next = ""
			break
		}
		next, err = nextPageURL(next, link)
		if err != nil {
			return nil, err
		}
	}

	// The cap was reached with the listing still going. What was read is a
	// PREFIX of the tags, and the maximum of a prefix is not the maximum — so
	// this is a failed check, not a quiet "no update". Reporting success here
	// would tell a user on an old build that they are current, which is the
	// one conclusion an incomplete listing cannot support.
	if next != "" {
		return nil, fmt.Errorf("%w after %d pages", ErrTagListingTruncated, maxTagPages)
	}
	return all, nil
}

// withPerPage asks for the largest page the endpoint serves, so the common
// repository is one request rather than three.
func withPerPage(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	query := parsed.Query()
	if query.Get("per_page") == "" {
		query.Set("per_page", strconv.Itoa(tagsPerPage))
		parsed.RawQuery = query.Encode()
	}
	return parsed.String()
}

// nextPageURL reads rel="next" out of a Link header.
//
// The URL comes from the response, so it is checked rather than trusted: a page
// that answers "continue over there" must not be able to move the request to
// another host. Same reasoning as refuseCrossHostRedirect.
func nextPageURL(current, link string) (string, error) {
	target := linkRelNext(link)
	if target == "" {
		return "", nil
	}

	parsed, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse the next tag page: %w", err)
	}
	here, err := url.Parse(current)
	if err != nil {
		return "", fmt.Errorf("parse the current tag page: %w", err)
	}
	if !strings.EqualFold(parsed.Host, here.Host) {
		return "", fmt.Errorf("refusing to follow tag paging from %s to %s", here.Host, parsed.Host)
	}
	return parsed.String(), nil
}

// linkRelNext picks the rel="next" URL out of an RFC 8288 Link header.
func linkRelNext(header string) string {
	for _, part := range strings.Split(header, ",") {
		segments := strings.Split(part, ";")
		if len(segments) < 2 {
			continue
		}
		target := strings.TrimSpace(segments[0])
		if !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
			continue
		}
		for _, parameter := range segments[1:] {
			value := strings.ReplaceAll(strings.TrimSpace(parameter), `"`, "")
			if strings.EqualFold(value, "rel=next") {
				return strings.TrimSuffix(strings.TrimPrefix(target, "<"), ">")
			}
		}
	}
	return ""
}

// fetchTagPage reads one page and returns it with the page's Link header.
func (c *Client) fetchTagPage(ctx context.Context, pageURL string) ([]gitHubTag, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build tag request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", userAgent)

	response, err := c.http.Do(request)
	if err != nil {
		return nil, "", fmt.Errorf("fetch tags from %s: %w", redactURL(pageURL), redactTransportError(err))
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("fetch tags from %s: unexpected status %s", redactURL(pageURL), response.Status)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return nil, "", fmt.Errorf("read tag response: %w", err)
	}

	var tags []gitHubTag
	if err := json.Unmarshal(body, &tags); err != nil {
		return nil, "", fmt.Errorf("decode tag response: %w", err)
	}
	return tags, response.Header.Get("Link"), nil
}

// redactURL keeps out of the error the parts of a URL that could carry
// something the user did not choose to publish — query, fragment, userinfo. The
// host is the point of the message, so it stays.
func redactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "the release endpoint"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.User = nil
	return parsed.String()
}

// transportError is a *url.Error with the URL taken out.
//
// It exists because redactURL alone is not redaction: net/http returns a
// *url.Error whose Error() prints the FULL request URL, query string and all,
// and wrapping that with %w puts it straight back into the message this error
// becomes — which is shown on the Settings tab and written to the log. The
// error is named by its operation and its cause; which URL it was is already in
// the sentence around it, redacted.
type transportError struct {
	op  string
	err error
}

func (e *transportError) Error() string {
	if e.op == "" {
		return e.err.Error()
	}
	return e.op + ": " + e.err.Error()
}

// Unwrap keeps errors.Is working through the redaction — a cancelled request
// must still answer to context.Canceled.
func (e *transportError) Unwrap() error { return e.err }

func redactTransportError(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) || urlErr.Err == nil {
		return err
	}
	return &transportError{op: urlErr.Op, err: urlErr.Err}
}
