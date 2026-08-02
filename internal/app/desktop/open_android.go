//go:build android

package desktop

import (
	"path/filepath"
	"strings"

	"gioui.org/app"
	"git.wow.st/gmp/jni"
	"github.com/rs/zerolog/log"
)

// FLAG_ACTIVITY_NEW_TASK — required when starting an activity from a
// non-activity context (we hold the application context).
const androidFlagActivityNewTask = 0x10000000

// isAndroid — see the twin declaration in open_default.go.
const isAndroid = true

// openBrowser opens url in the default browser via an ACTION_VIEW
// intent. Runs on its own goroutine (jni.Do attaches/detaches the
// thread); errors are logged, not surfaced — the same fire-and-forget
// contract as the exec-based desktop variant in open_default.go.
func openBrowser(url string) {
	go func() {
		err := jni.Do(jni.JVMFor(app.JavaVM()), func(env jni.Env) error {
			ctx := jni.Object(app.AppContext())
			loader := jni.ClassLoaderFor(env, ctx)

			uriCls, err := jni.LoadClass(env, loader, "android/net/Uri")
			if err != nil {
				return err
			}
			parse := jni.GetStaticMethodID(env, uriCls, "parse",
				"(Ljava/lang/String;)Landroid/net/Uri;")
			uri, err := jni.CallStaticObjectMethod(env, uriCls, parse,
				jni.Value(jni.JavaString(env, url)))
			if err != nil {
				return err
			}

			intentCls, err := jni.LoadClass(env, loader, "android/content/Intent")
			if err != nil {
				return err
			}
			ctor := jni.GetMethodID(env, intentCls, "<init>",
				"(Ljava/lang/String;Landroid/net/Uri;)V")
			intent, err := jni.NewObject(env, intentCls, ctor,
				jni.Value(jni.JavaString(env, "android.intent.action.VIEW")),
				jni.Value(uri))
			if err != nil {
				return err
			}

			addFlags := jni.GetMethodID(env, intentCls, "addFlags",
				"(I)Landroid/content/Intent;")
			if _, err := jni.CallObjectMethod(env, intent, addFlags,
				jni.Value(androidFlagActivityNewTask)); err != nil {
				return err
			}

			startActivity := jni.GetMethodID(env, jni.GetObjectClass(env, ctx),
				"startActivity", "(Landroid/content/Intent;)V")
			return jni.CallVoidMethod(env, ctx, startActivity, jni.Value(intent))
		})
		if err != nil {
			log.Warn().Err(err).Str("url", url).Msg("open browser intent failed")
		}
	}()
}

// exportFileName returns the file name to hand to the SAF exporter.
// Gio's exporter derives the picker MIME strictly from the extension via
// the SYSTEM registry (explorer_android.java →
// MimeTypeMap.getMimeTypeFromExtension), so the decision must consult
// that same registry — Go's mime db and the device's map disagree in
// both directions. If the device does not resolve the extension, a
// ".bin" suffix (application/octet-stream) is appended so the
// ACTION_CREATE_DOCUMENT intent always carries a valid type; the user
// can strip the suffix in the save dialog's name field.
func exportFileName(name string) string {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), ".")
	if ext != "" && androidMimeKnown(ext) {
		return name
	}
	return name + ".bin"
}

// androidMimeKnown reports whether the device MimeTypeMap resolves the
// extension (lowercase, no dot) to a MIME type. Any JNI failure counts
// as unknown — the caller then falls back to the ".bin" suffix, which is
// always resolvable.
func androidMimeKnown(ext string) bool {
	known := false
	err := jni.Do(jni.JVMFor(app.JavaVM()), func(env jni.Env) error {
		ctx := jni.Object(app.AppContext())
		loader := jni.ClassLoaderFor(env, ctx)
		mapCls, err := jni.LoadClass(env, loader, "android/webkit/MimeTypeMap")
		if err != nil {
			return err
		}
		getSingleton := jni.GetStaticMethodID(env, mapCls, "getSingleton",
			"()Landroid/webkit/MimeTypeMap;")
		mimeMap, err := jni.CallStaticObjectMethod(env, mapCls, getSingleton)
		if err != nil {
			return err
		}
		getMime := jni.GetMethodID(env, mapCls, "getMimeTypeFromExtension",
			"(Ljava/lang/String;)Ljava/lang/String;")
		mimeType, err := jni.CallObjectMethod(env, mimeMap, getMime,
			jni.Value(jni.JavaString(env, ext)))
		if err != nil {
			return err
		}
		known = mimeType != 0
		return nil
	})
	if err != nil {
		log.Warn().Err(err).Str("ext", ext).Msg("MimeTypeMap lookup failed")
		return false
	}
	return known
}

// openFile is a stub on Android: opening an app-private file in an
// external viewer requires a content:// URI from a FileProvider declared
// in the manifest, and gogio's generated manifest has none. The UI hides
// the affordances that would call this (layoutFileActionButtons); the
// in-app image preview still works. Revisit if/when packaging gains a
// FileProvider entry.
func openFile(path string) {
	log.Debug().Str("path", path).Msg("openFile is not supported on android")
}

// revealFileInDir is a stub on Android: there is no "reveal in file
// manager" concept for app-private storage. The UI hides the affordance
// (layoutFileActionButtons).
func revealFileInDir(path string) {
	log.Debug().Str("path", path).Msg("revealFileInDir is not supported on android")
}
