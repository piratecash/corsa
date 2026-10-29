package node

import (
	"errors"
	"fmt"
	"os"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// pin_store_refusal.go is the node's refusal to start over its v2 pin store
// (domain.ErrPinStoreRefused), worded for the operator who has to act on it:
// the file, what is wrong with it, and what to do — down to the user and
// group the process runs as, since the usual cause is a volume prepared for
// another one.
//
// Unlike every other error of the store, this one names paths. It is
// returned to the process that starts the node — its terminal and log, the
// desktop's notice — and nowhere else: it is not part of any diagnostics or
// RPC answer, and the store's own log line about it stays pathless.

// PinStoreRefusalError is why the node refuses to start over its pin store.
// errors.Is(err, domain.ErrPinStoreRefused) holds for every one; the
// location refusals also unwrap to the audit's *secretfile.UnsafeLocationError.
type PinStoreRefusalError struct {
	Reason domain.PinStoreRefusalReason
	// File is the file the refusal is about — the store or its marker — as
	// the node was configured to find it; "" for a secure session that could
	// not be built, which is about no file.
	File string
	// problem and fix are the two halves of the operator's message.
	problem string
	fix     string
	cause   error
}

func (e *PinStoreRefusalError) Error() string {
	subject := e.problem
	if e.File != "" {
		subject = e.File + ": " + e.problem
	}
	return fmt.Sprintf("%s (%s): %s. To fix: %s. The node changes no permissions itself, removes no pins and does not fall back to v1 or to a temporary store",
		domain.ErrPinStoreRefused, e.Reason, subject, e.fix)
}

// Is makes errors.Is(err, domain.ErrPinStoreRefused) hold for every refusal.
func (e *PinStoreRefusalError) Is(target error) bool { return target == domain.ErrPinStoreRefused }

func (e *PinStoreRefusalError) Unwrap() error { return e.cause }

// unsafeLocation is the audit's verdict behind a location refusal, if any.
func (e *PinStoreRefusalError) unsafeLocation() (*secretfile.UnsafeLocationError, bool) {
	var unsafe *secretfile.UnsafeLocationError
	found := errors.As(e.cause, &unsafe)
	return unsafe, found
}

// pinStoreFileRole names which of the two files a refusal is about.
type pinStoreFileRole string

const (
	pinStoreRoleStore  pinStoreFileRole = "the pin store"
	pinStoreRoleMarker pinStoreFileRole = "the pin store marker"
)

// lossConsequence is what starting anyway would cost; every refusal over a
// missing or unreadable store says it, so the way out is a choice made
// knowingly.
const lossConsequence = "starting on an empty store would let every peer that proved v2 be taken back to v1"

// unsafeLocationFixes is the fix for each violation the audit reports.
var unsafeLocationFixes = map[secretfile.LocationViolation]func(account string) string{
	secretfile.ViolationSymlink: func(string) string {
		return "put the file itself at this name, and the real directories on the way, instead of a link (on Windows any reparse point, a junction included) — the node does not follow a link to its pins"
	},
	secretfile.ViolationNotRegularFile: func(string) string {
		return "put the node's own regular file at this name — a directory or a device cannot hold pins"
	},
	secretfile.ViolationFileNotOwnerOnly: func(account string) string {
		return fmt.Sprintf("make the file mode 0600 and owned by the node's user, %s (chmod 0600; on Windows leave only that user, SYSTEM and Administrators in the file's permissions)", account)
	},
	secretfile.ViolationForeignOwner: func(account string) string {
		return fmt.Sprintf("chown the component to the node's user, %s, or run the node as the user that owns it; a directory may also be root's (on Windows the owner must be the node's user, SYSTEM or Administrators, a directory also TrustedInstaller)", account)
	},
	secretfile.ViolationWritableByOthers: func(account string) string {
		return fmt.Sprintf("remove group and world write from the directory (chmod go-w), so that only the node's user, %s, or root may write it; "+
			"a volume other users must write stays usable when it is sticky (chmod 1777) and the node's files live in a subdirectory owned by the node's user with mode 0700. "+
			"In Docker do not mount a 0777 volume: chown the volume to the container's user (the image runs as uid 10001), "+
			"or give that user its own 0700 subdirectory of a 1777 volume and point CORSA_DATA_DIR at it. "+
			"On Windows, remove the entries that let other accounts add files to the directory holding the file, or delete, rename, re-permission or take ownership of any directory on the way up to the drive's root (icacls); keeping the data under the user's profile does this", account)
	},
	secretfile.ViolationStickyChildNotOwned: func(account string) string {
		return fmt.Sprintf("inside a sticky shared directory such as /tmp, the next directory must belong to the node's user: create one owned by %s with mode 0700 and keep the node's files there", account)
	},
}

func refuseUnsafeLocation(role pinStoreFileRole, file string, unsafe *secretfile.UnsafeLocationError) *PinStoreRefusalError {
	owner := unsafe.Owner
	if owner == "" {
		owner = "not read"
	}
	fix := "make the file and every directory above it changeable only by the node's user or root"
	if fixFor, known := unsafeLocationFixes[unsafe.Violation]; known {
		fix = fixFor(processAccount())
	}
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalLocationUnsafe,
		File:   file,
		problem: fmt.Sprintf("%s is in an unsafe location — %s: %s (owner %s, mode %s) — so someone other than the node's user (%s) or root could replace what the node reads",
			role, unsafe.Component, unsafe.Violation, owner, unsafe.Mode, processAccount()),
		fix:   fix,
		cause: unsafe,
	}
}

func refuseUnverifiableLocation(role pinStoreFileRole, file string, cause error) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalLocationUnverifiable,
		File:   file,
		problem: fmt.Sprintf("the location of %s could not be inspected (%v), so it is not known that only the node's user (%s) or root can change it",
			role, cause, processAccount()),
		fix:   "let the node's user list every directory on the way to the file, and keep the node's files on a filesystem that reports owners and modes",
		cause: cause,
	}
}

// refuseStoreMissing is the loss the marker exists to catch. recorded is
// where the marker saw the store last; nil when the marker is damaged.
func refuseStoreMissing(store, marker string, recorded *string) *PinStoreRefusalError {
	last := "at a path the damaged marker no longer says"
	moveFix := "if the port, CORSA_SECURE_SESSION_STORE_PATH or the data directory changed, move the store kept under the old setting to " + store
	if recorded != nil {
		last = "last at " + *recorded
		moveFix = fmt.Sprintf("if the port, CORSA_SECURE_SESSION_STORE_PATH or the data directory changed, move %s to %s, or set CORSA_SECURE_SESSION_STORE_PATH to %s", *recorded, store, *recorded)
	}
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalStoreMissing,
		File:   store,
		problem: fmt.Sprintf("there is no pin store here, and the marker %s says this node kept one, %s; %s",
			marker, last, lossConsequence),
		fix: fmt.Sprintf("restore %s from a backup; %s. Only if the store is lost for good, remove the marker %s: the node then starts with no pins, and every peer may be taken back to v1 once more",
			store, moveFix, marker),
	}
}

// refuseStoreUnreadable is a store that exists and cannot be trusted. marker
// is "" for a node that keeps none.
func refuseStoreUnreadable(store, marker string, cause error) *PinStoreRefusalError {
	giveUp := "move it aside (keep it) and start again"
	if marker != "" {
		giveUp = fmt.Sprintf("move it aside (keep it) and remove the marker %s", marker)
	}
	return &PinStoreRefusalError{
		Reason:  domain.PinStoreRefusalStoreUnreadable,
		File:    store,
		problem: fmt.Sprintf("the pin store cannot be read (%v); %s", cause, lossConsequence),
		fix: fmt.Sprintf("restore the file from a backup, or run the newer build that wrote it. Only if it is lost for good, %s: the node then starts with no pins, and every peer may be taken back to v1 once more",
			giveUp),
		cause: cause,
	}
}

// refuseStoreUnconfirmed preserves a valid visible store whose persistence
// barrier failed. An I/O repair is required, never a pin reset or legacy retry.
func refuseStoreUnconfirmed(store string, cause error) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason:  domain.PinStoreRefusalLocationUnverifiable,
		File:    store,
		problem: fmt.Sprintf("the loaded pin store is visible but its persistence cannot be confirmed (%v)", cause),
		fix:     "keep the pin store and marker, repair the filesystem or mount so file and directory synchronization succeeds, then restart; do not delete protection to bypass this refusal",
		cause:   cause,
	}
}

func refuseMarkerUnwritable(marker string, cause error) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalMarkerUnwritable,
		File:   marker,
		problem: fmt.Sprintf("the marker that lets a later start tell a lost pin store from a first start cannot be written (%v), and a pin store exists",
			cause),
		fix: fmt.Sprintf("let the node's user, %s, create files in the directory holding the identity file",
			processAccount()),
		cause: cause,
	}
}

// refuseTwoStores is a store loaded at the path the node uses now while the
// path the marker recorded still holds another one: copied, not moved.
// Taking the new one silently could put a stale copy in charge.
func refuseTwoStores(current, recorded, marker string) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalTwoStores,
		File:   current,
		problem: fmt.Sprintf("the marker %s says this node kept its pin store at %s, and a store is still there besides this one; which of the two is current is not the node's to guess",
			marker, recorded),
		fix: fmt.Sprintf("keep the current store and move the other aside (keep it): to go on with %s, move %s away; to go on with %s, set CORSA_SECURE_SESSION_STORE_PATH to it (or restore the old port or data directory) and move %s away",
			current, recorded, recorded, current),
	}
}

// refuseStoreNotConfigured is a node given no store file at all — no peers
// file and no CORSA_SECURE_SESSION_STORE_PATH, so it would keep its pins in
// memory — under a marker that says it kept them on disk.
func refuseStoreNotConfigured(marker string, recorded *string) *PinStoreRefusalError {
	last := "at a path the damaged marker no longer says"
	if recorded != nil {
		last = "last at " + *recorded
	}
	return &PinStoreRefusalError{
		Reason: domain.PinStoreRefusalStoreMissing,
		File:   marker,
		problem: fmt.Sprintf("the node is configured without a pin store file (no peers file, no CORSA_SECURE_SESSION_STORE_PATH), and this marker says it kept one, %s; %s",
			last, lossConsequence),
		fix: fmt.Sprintf("configure the peers file or CORSA_SECURE_SESSION_STORE_PATH so that the node finds its store again. Only if the store is lost for good, remove the marker %s",
			marker),
	}
}

// refuseSecureSessionUnavailable is a build with the secure session whose v2
// state could not be built: on v1 alone not a single pin would be enforced.
func refuseSecureSessionUnavailable(cause error) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason:  domain.PinStoreRefusalSecureSessionUnavailable,
		problem: fmt.Sprintf("the secure session of this node could not be set up (%v), and running on v1 alone would enforce none of its pins", cause),
		fix:     "check that the identity file holds a complete key pair and that the build is not broken; report the cause above if it persists",
		cause:   cause,
	}
}

// refuseLegacyOnlyBuild is a build that speaks v1 only — a rollback to a
// build without the secure session — over a pin store or marker on disk.
func refuseLegacyOnlyBuild(file string) *PinStoreRefusalError {
	return &PinStoreRefusalError{
		Reason:  domain.PinStoreRefusalLegacyOnlyBuild,
		File:    file,
		problem: "this build speaks v1 only and cannot enforce the pins this node kept; " + lossConsequence,
		fix:     "run a build with the secure session. Only to give the pins up for good, move the pin store aside (keep it) and remove its marker",
	}
}

// processAccount names the account the node runs as, the way the fixes need
// it: the numeric user and group on POSIX (a container's user has no name on
// the host), the current user on Windows, where os.Geteuid is -1.
func processAccount() string {
	uid, gid := os.Geteuid(), os.Getegid()
	if uid < 0 {
		return "the current Windows user"
	}
	return fmt.Sprintf("uid %d, gid %d", uid, gid)
}
