package deeplink

// FromArgs returns the first corsa: URI on a command line.
//
// Desktops that have no way to deliver into a running process start the
// program again with the URI appended to the Exec line of its
// .desktop entry (`Exec=corsa-desktop %u`), so the command line is where
// the link arrives there. Everything else on the line is left alone: the
// URI is recognised by its scheme, not by its position.
func FromArgs(args []string) (string, bool) {
	for _, arg := range args {
		if IsDeepLink(arg) {
			return arg, true
		}
	}
	return "", false
}
