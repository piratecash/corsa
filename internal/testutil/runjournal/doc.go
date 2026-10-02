// Package runjournal keeps the results of a long, interruptible measurement
// sweep on disk, so that a sweep can be cut into calls and the calls add up to
// one honest account of the whole set.
//
// Why it exists. A load stand run is minutes per configuration and hours per
// set, while one invocation of the environment that drives it is capped far
// below that. A sweep that only prints to the test log cannot be finished: every
// call restarts it and loses what the previous one measured. So each run is a
// file whose name is derived from its configuration, and a sweep looks at the
// directory to decide what is left.
//
// The four rules the journal enforces, each of which has already cost a sweep
// its results once:
//
//  1. A configuration's identifier is derived from ALL of its parameters, in a
//     canonical order, never from a loop position or a counter. A position
//     moves when the enumeration changes, and a resumed sweep then skips a
//     configuration it never ran.
//  2. A finished result is never overwritten. The refusal is the kernel's
//     no-replace link of a fully written file onto the result name, not a
//     check before a write: stat-then-write leaves a window in which two
//     writers both see no file, and a sequential test never sees it. A failed
//     attempt has a name of its own, so a retry can never replace a success.
//  3. A configuration is skipped only after its record has been VERIFIED:
//     checksum, every parameter, and the sources it was measured with. A record
//     that does not verify is a refusal, never a silent skip and never a silent
//     rerun.
//  4. The ledger reports its states apart — expected, completed, failed,
//     missing, quarantined — and describes the WHOLE enumeration as the
//     journal holds it, not the slice the current call happened to execute.
//
// Every record is also bound to the sources that produced it: a fingerprint is
// a sha256 manifest over a filtered set of files from one or more directories,
// derived from the files rather than typed in, and a copy of those exact bytes
// is kept beside the journal so a number can be read together with its code.
// A fingerprint fails closed: every root must exist as a real directory and
// match at least one file, and any symlink or other non-regular entry met on
// the way is refused whatever the filter says — a stamp that quietly covers
// less than it names is worse than no stamp.
//
// What the journal requires of its filesystem. Rule 2 rests on link(2) failing
// with EEXIST when the target name exists. A filesystem without hard links
// (FAT, several network and FUSE mounts), or one that "links" by replacing,
// cannot keep that promise, so KeepSources — and therefore Sweep, before its
// first Step — publishes a probe file twice and refuses with ErrNoHardLinks
// when the link itself is refused (EPERM, ENOTSUP, EXDEV, EMLINK) or the second
// link onto the taken name does not fail with EEXIST. Any other failure of the
// probe (a full disk, a cancel) is reported as itself.
//
// What a source stamp does and does not prove. The stamp says which files were
// in the tree when the driver called TakeSnapshot; it does not prove those
// files are what the running binary was compiled from. Sweep re-reads the tree
// when it ends and fails with ErrSourcesChanged if it differs — a heuristic
// that the tree stayed QUIET for the duration, nothing more. A result counts
// as done only once that check has vouched for it — and only the check of the
// sweep run that WROTE it: every call of Sweep draws a random run token, the
// result carries it, and the result is in doubt until a "verified" mark of
// that same run exists. Another executor's check, even over a directory with
// equal fingerprints, read another tree and vouches for nothing here. Any
// exit before the check — a cancel, a stand defect after
// which the context is gone, a killed process that runs no cleanup at all —
// leaves the result QUARANTINED, and so does a check that finds the sources
// changed. A quarantined result counts as neither done nor redoable — a
// resumed sweep fails with ErrQuarantined — until the operator records a
// decision (ReleaseQuarantine). Without this, restoring the tree would
// restore the stamp, and the resume would verify those results and skip them
// as done. A stand defect that leaves the context alive still runs the check,
// so the points measured before it are not handed to the operator. An edit undone before the
// end of a sweep still passes the check, and it cannot see the window
// between `go test` compiling the binary and the snapshot being taken, where
// an edit is stamped but was never compiled. A driver that needs the binding
// exact takes the snapshot before building, or records the binary's build ID
// as a parameter.
//
// Why it does not reuse internal/overlaysim. The overlay simulator carries the
// same rules in test-only files of its own package (runstore_test.go): they are
// not importable, they are tuned to that simulator's drivers, and that package
// is under a long-running measurement whose sources must not move. An
// independent implementation also means a defect in one journal is not
// silently shared by the other — the two are checked by separate tests.
//
// What the package does NOT do: it holds no threshold and never judges a
// measurement. A result that came out badly is a result and is recorded like
// any other; only a stand defect is a failure.
package runjournal
