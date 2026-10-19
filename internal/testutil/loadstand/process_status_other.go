//go:build !unix

package loadstand

import "os"

func ownedWaitSignal(*os.ProcessState) ProcessSignal { return 0 }
