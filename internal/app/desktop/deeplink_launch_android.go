//go:build android

package desktop

import (
	"gioui.org/app"
	"git.wow.st/gmp/jni"
	"github.com/rs/zerolog/log"
)

// platformLaunchDeepLink reads the URI of the intent that STARTED this
// activity, and then CLEARS it.
//
// Gio does deliver intent data on its own (GioActivity.onNewIntent →
// app.URLEvent), and that path carries every link tapped while the app
// runs. The launch intent is the one it cannot be trusted with: Gio runs
// Go main in a goroutine and delivers the URL event only if the global
// event iterator is already installed, which on a cold start is a race
// against our own startup. getIntent() has no such window — it keeps
// answering with the launch intent for as long as the activity lives.
//
// Reading it makes the link arrive twice for ONE tap, so the read
// consumes it: setData(null) on the activity's own intent. The ordering
// that makes this work is Gio's, not a hope — GioActivity.onCreate calls
// onNewIntent(getIntent()) only AFTER `new GioView(this)` returns, and
// that constructor blocks inside onCreateView until this handler has
// run. By the time Gio looks at the intent, its data is gone and it
// delivers nothing. A duplicate would only be a second identical import
// with a second status line, never a wrong one — which is why this is a
// consumed intent rather than a flag that outlives the launch.
//
// The activity comes from the VIEW, not from app.AppContext(): Gio
// hands the APPLICATION context to JNI, and getIntent() is an Activity
// method.
// Reports read=false while there is no view to ask — a detached view
// event carries no handle — so the next attach tries again instead of
// losing the link.
func platformLaunchDeepLink(e app.ViewEvent) (uri string, read bool) {
	view, ok := e.(app.AndroidViewEvent)
	if !ok || view.View == 0 {
		return "", false
	}

	err := jni.Do(jni.JVMFor(app.JavaVM()), func(env jni.Env) error {
		viewObj := jni.Object(view.View)
		getContext := jni.GetMethodID(env, jni.GetObjectClass(env, viewObj),
			"getContext", "()Landroid/content/Context;")
		activity, err := jni.CallObjectMethod(env, viewObj, getContext)
		if err != nil {
			return err
		}
		if activity == 0 {
			return nil
		}

		activityCls := jni.GetObjectClass(env, activity)
		getIntent := jni.GetMethodID(env, activityCls, "getIntent", "()Landroid/content/Intent;")
		intent, err := jni.CallObjectMethod(env, activity, getIntent)
		if err != nil || intent == 0 {
			return err
		}

		getData := jni.GetMethodID(env, jni.GetObjectClass(env, intent),
			"getData", "()Landroid/net/Uri;")
		data, err := jni.CallObjectMethod(env, intent, getData)
		if err != nil || data == 0 {
			// A normal launcher tap: an intent with no data at all.
			return err
		}

		toString := jni.GetMethodID(env, jni.GetObjectClass(env, data),
			"toString", "()Ljava/lang/String;")
		text, err := jni.CallObjectMethod(env, data, toString)
		if err != nil || text == 0 {
			return err
		}
		uri = jni.GoString(env, jni.String(text))

		// Consume it, so Gio's own delivery of this same launch finds
		// nothing to deliver. A failure here costs one duplicate import,
		// so it is reported but does not discard the URI we just read.
		setData := jni.GetMethodID(env, jni.GetObjectClass(env, intent),
			"setData", "(Landroid/net/Uri;)Landroid/content/Intent;")
		if _, err := jni.CallObjectMethod(env, intent, setData, jni.Value(0)); err != nil {
			log.Warn().Err(err).Msg("clearing the launch intent failed; the link may arrive twice")
		}
		return nil
	})
	if err != nil {
		log.Warn().Err(err).Msg("reading the launch intent failed")
		return "", false
	}
	return uri, true
}
