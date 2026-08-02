//go:build android

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gioui.org/app"
	"git.wow.st/gmp/jni"

	"github.com/rs/zerolog/log"
)

// dataDirStrict makes anchorDataDir fail-closed on Android: if the
// no-backup directory cannot be resolved or created, the app refuses to
// start instead of silently falling back to a backup-included path. The
// data dir holds the identity private keys and the chat log — running
// with them exposed to Android Backup is a security-invariant violation,
// not a degraded mode.
const dataDirStrict = true

// platformDataDir returns the app's no-backup files directory
// (Context.getNoBackupFilesDir, API 21+).
//
// The Android wrapper disables backup at the application level, while the
// no_backup directory provides a second platform-enforced boundary for the
// identity private keys and chat log. It remains excluded from every backup
// path even if the manifest policy changes, protecting Android ≤30 and ≥31
// uniformly.
//
// The Makefile pins -minsdk 26 (ANDROID_MINSDK, guarded to never drop
// below 21), so getNoBackupFilesDir exists on every device that can
// install the APK. Errors are returned, not papered over: the caller is
// fail-closed (dataDirStrict) and refuses to run with a backup-exposed
// data dir.
func platformDataDir() (string, error) {
	var dir string
	err := jni.Do(jni.JVMFor(app.JavaVM()), func(env jni.Env) error {
		ctx := jni.Object(app.AppContext())
		ctxCls := jni.GetObjectClass(env, ctx)
		getDir := jni.GetMethodID(env, ctxCls, "getNoBackupFilesDir", "()Ljava/io/File;")
		fileObj, err := jni.CallObjectMethod(env, ctx, getDir)
		if err != nil {
			return err
		}
		if fileObj == 0 {
			return errors.New("getNoBackupFilesDir returned null")
		}
		fileCls := jni.GetObjectClass(env, fileObj)
		getPath := jni.GetMethodID(env, fileCls, "getAbsolutePath", "()Ljava/lang/String;")
		pathObj, err := jni.CallObjectMethod(env, fileObj, getPath)
		if err != nil {
			return err
		}
		if pathObj == 0 {
			return errors.New("getAbsolutePath returned null")
		}
		dir = jni.GoString(env, jni.String(uintptr(pathObj)))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("no-backup dir lookup: %w", err)
	}
	if dir == "" {
		return "", errors.New("no-backup dir lookup returned an empty path")
	}
	return dir, nil
}

// migrateLegacyDataDir moves the pre-no-backup data dir
// (filesDir/corsa, used by earlier builds) to target so existing
// installs keep their identity and chat history. Both dirs live on the
// same filesystem, so the rename is atomic: it either fully succeeds or
// leaves the legacy dir intact.
//
// TWO legacy layouts have to be considered, both under filesDir:
//
//   - "corsa" — written by builds whose anchorDataDir succeeded, i.e.
//     app.DataDir() + "/corsa";
//   - ".corsacore" — appdata.DefaultDir()'s own answer on Android. Gio
//     sets HOME=filesDir before calling main (os_android.go), so the
//     package's default branch resolves to $HOME/.corsacore — NOT the
//     relative ".corsa" its doc mentions (that path is test-only). Any
//     build where anchorDataDir fell back to the appdata defaults wrote
//     its identity there.
//
// Returns nil only when the world is in a known-good state: no legacy
// dir (fresh install or already migrated), or exactly one was moved.
// Every ambiguous state is an error and the caller (anchorDataDir,
// strict on Android) refuses to start:
//
//   - a Stat failure on any path is NOT treated as "absent" — a
//     permission/I/O error masking an existing legacy dir would make
//     the app open an empty target and mint a fresh identity;
//   - more than one candidate (either legacy layout alongside the
//     target, or both legacy layouts at once) is a conflict: several
//     candidate identities, and silently picking one could switch the
//     user's account. A human (or a future explicit recovery path) has
//     to decide which one wins.
//
// On any error the data on disk is untouched.
func migrateLegacyDataDir(target string) error {
	// A DataDir failure is ambiguous, not "nothing to migrate": it could
	// mask an existing legacy dir, and proceeding would mint a fresh
	// identity. Strict like everything else here — the caller exits.
	legacyBase, err := app.DataDir()
	if err != nil {
		return fmt.Errorf("legacy base dir lookup (app.DataDir): %w", err)
	}
	if legacyBase == "" {
		return errors.New("legacy base dir lookup returned an empty path")
	}

	var candidates []string
	for _, name := range []string{"corsa", ".corsacore"} {
		p := filepath.Join(legacyBase, name)
		if p == target {
			continue
		}
		switch _, err := os.Stat(p); {
		case err == nil:
			candidates = append(candidates, p)
		case errors.Is(err, os.ErrNotExist):
			// Not present — nothing to migrate from this layout.
		default:
			return fmt.Errorf("legacy data dir stat %s: %w", p, err)
		}
	}
	if len(candidates) == 0 {
		// Fresh install or migration already completed.
		return nil
	}
	if len(candidates) > 1 {
		return fmt.Errorf("multiple legacy data dirs exist (%s); refusing to guess which identity is current", strings.Join(candidates, ", "))
	}
	legacy := candidates[0]

	if _, err := os.Stat(target); err == nil {
		return fmt.Errorf("both legacy (%s) and no-backup (%s) data dirs exist; refusing to guess which identity is current", legacy, target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("target data dir stat %s: %w", target, err)
	}

	if err := os.Rename(legacy, target); err != nil {
		return fmt.Errorf("legacy data dir migration %s → %s: %w", legacy, target, err)
	}
	log.Info().Str("from", legacy).Str("to", target).Msg("data dir migrated to no-backup storage")
	return nil
}
