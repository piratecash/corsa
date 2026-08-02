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
