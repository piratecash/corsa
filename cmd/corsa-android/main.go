// Command corsa-android is the Android entry point for Corsa.
//
// It runs the same application as cmd/corsa-desktop — Gio renders the
// identical UI on Android. The one Android-specific step is anchoring
// the data directory: appdata's own default would land in
// $HOME/.corsacore (Gio sets HOME=filesDir), which Android Auto Backup
// copies — so all node-local state (identity, chat log, downloads, crash
// logs) is placed under the app's no-backup dir instead, before anything
// derives a path from appdata.DefaultDir. See datadir_android.go.
//
// KNOWN LIMITATION — foreground-only operation. The node lives inside
// the Activity process: the Android wrapper currently declares no service,
// push receiver or reconnect mechanism. Once the Activity leaves the
// foreground, Doze/App Standby throttle the network and the process may be
// killed. Messages are received only while the app is open. The protocol has
// no durable mailbox, and sender-side delivery retry gives up after roughly 3.5
// hours (internal/core/node/delivery_retry.go), so a client that stays
// closed longer than that misses those messages permanently. Tracked in
// docs/roadmap.md, "Android background delivery" (#iter-25).
package main

import (
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/app/desktop"
	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/crashlog"

	"github.com/rs/zerolog/log"
)

func main() {
	// Must happen before crashlog.Setup and desktop.Run — both derive
	// their paths from appdata.DefaultDir.
	anchorDataDir()

	crashlog.Setup()
	// recover() must be called DIRECTLY by the deferred function to stop
	// an unwinding panic — deferring Setup's cleanup closure (which
	// recovers inside a nested call) would always observe nil and never
	// write the promised crash-*.log for a panic in main.
	defer func() {
		if r := recover(); r != nil {
			crashlog.HandlePanicValue(r)
		}
	}()

	log.Info().Msg("corsa-android starting")

	if err := desktop.Run(); err != nil {
		log.Error().Err(err).Msg("corsa-android exited with error")
		os.Exit(1)
	}
}

// anchorDataDir points appdata at the platform data directory — on
// Android the app's no-backup dir (datadir_android.go: the identity keys
// and the chat log must not enter Android Auto Backup), elsewhere Gio's
// DataDir.
//
// Failure handling is platform-split via dataDirStrict. Android is
// FAIL-CLOSED: resolving or creating the no-backup dir is a security
// invariant (keys outside Android Backup) — on failure the process exits
// instead of silently running with a backup-exposed default path. Other
// platforms keep the lenient fallback to appdata's defaults.
//
// Calling Gio's DataDir/JavaVM here (before app.Main) is safe on
// Android: for gogio-built apps, Gio's Java_org_gioui_Gio_runGoMain sets
// dataPath — and $HOME/$XDG_CONFIG_HOME/$XDG_CACHE_HOME — BEFORE
// invoking Go main() (see gioui.org/app/os_android.go). The documented
// "DataDir panics if called before main" case is about package init().
// The recover below turns an unexpected panic (upstream lifecycle
// change, JNI exception) into the same strict/lenient policy instead of
// a crash with no explanation.
func anchorDataDir() {
	defer func() {
		if r := recover(); r != nil {
			if dataDirStrict {
				log.Error().Interface("panic", r).Msg("data dir resolution panicked; refusing to start with a backup-exposed data dir")
				os.Exit(1)
			}
			log.Error().Interface("panic", r).Msg("data dir resolution panicked; using appdata defaults")
		}
	}()

	dir, err := platformDataDir()
	if err != nil || dir == "" {
		if dataDirStrict {
			log.Error().Err(err).Msg("no-backup data dir unavailable; refusing to start (identity keys must not enter Android backups)")
			os.Exit(1)
		}
		log.Error().Err(err).Msg("platform data dir unavailable; using appdata defaults")
		return
	}

	corsaDir := filepath.Join(dir, "corsa")
	// Earlier builds kept data under filesDir/corsa — move it once so
	// existing installs keep their identity and history. The rename is
	// atomic (same filesystem): it either fully succeeds or leaves the
	// legacy dir intact. On failure the strict (Android) policy refuses
	// to start: anchoring the empty target would mint a fresh identity,
	// and running from the legacy dir would keep writing keys and
	// messages into backup-exposed storage — either way an invariant
	// breaks. The data is untouched, so the next launch simply retries.
	if err := migrateLegacyDataDir(corsaDir); err != nil {
		if dataDirStrict {
			log.Error().Err(err).Msg("data dir migration failed; refusing to start (legacy dir is backup-exposed, empty target would mint a new identity)")
			os.Exit(1)
		}
		log.Error().Err(err).Msg("data dir migration failed; using appdata defaults")
		return
	}
	if err := os.MkdirAll(corsaDir, 0o700); err != nil {
		if dataDirStrict {
			log.Error().Err(err).Str("dir", corsaDir).Msg("data dir create failed; refusing to start with a backup-exposed data dir")
			os.Exit(1)
		}
		log.Error().Err(err).Str("dir", corsaDir).Msg("data dir create failed; using appdata defaults")
		return
	}

	appdata.SetDir(corsaDir)
}
