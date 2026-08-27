# Android packaging

The Android application is assembled in two independent steps:

1. the official `gogio` command builds Corsa as an Android AAR with
   `-buildmode archive`;
2. the standard Android Gradle Plugin packages that AAR with the manifest and
   resources in this directory.

This keeps Android-specific resources out of `gogio`. In particular,
`app/src/main/res/mipmap-anydpi-v26/ic_launcher.xml` references a solid color
background and a separate transparent foreground PNG. The Gradle build copies
the source icon assets from `assets/icons/png` into generated resources, so
they are not duplicated here.

Use the Android targets in the repository `Makefile`; they supply all required
Gradle properties. Update the official packager with
`go get -tool gioui.org/cmd/gogio@latest`. Gradle and the Android Gradle Plugin
are pinned independently by `gradle/wrapper/gradle-wrapper.properties` and the
root `build.gradle`.

## JNI classes gogio does not package

`gogio -buildmode archive` compiles the Java of `gioui.org/app` alone. It
collects the `*.jar` files shipped by every other imported package — and then
uses them only in its `-buildmode exe` path, which this project does not take.
Anything else that calls into Java over JNI therefore reaches the APK only
because the Gradle module is given those jars.

Today that is `gioui.org/x/explorer`, the system file picker behind **Attach**
and **Save as…**. Without its classes `jni.LoadClass` fails inside the picker,
the call returns an error before any dialog appears, and the buttons do
nothing at all — which is exactly how it shipped until the jars were staged.

`make android-extra-jars` copies them out of the module cache (resolved with
`go list`, so they always match `go.mod`) into `dist/android-libs`, which the
Makefile passes as `-PcorsaExtraJarsDir`. Every Android target depends on it,
and the Gradle build fails with a pointer to that target when the directory is
empty rather than producing an app with dead buttons. A second package that
ships Java is one more word in `ANDROID_JAVA_PKGS`.

## Launch mode

`app/src/main/AndroidManifest.xml` deliberately differs from gogio's generated
manifest in one attribute: the activity is `singleTask`, not `singleInstance`.
A `singleInstance` activity is the only member of its task and everything it
starts opens in a task of its own — and an activity that cannot host another
in its task cannot receive that activity's result, so
`startActivityForResult` comes back as `RESULT_CANCELED` immediately. That is
the whole of the file picker, in both directions. `singleTask` keeps the same
"one instance, at the root of its task" behaviour and lets the picker answer.
