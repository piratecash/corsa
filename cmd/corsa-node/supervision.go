package main

import (
	"github.com/piratecash/corsa/internal/startupfence"
	"path/filepath"
)

type candidateSupervision struct {
	enabled  bool
	launcher string
	store    startupfence.StoreID
}

func candidateSupervisionFromEnvironment(getenv func(string) string) (candidateSupervision, error) {
	candidate, fd, launcher, store := getenv("CORSA_STARTUP_FENCE_CANDIDATE"), getenv("CORSA_STARTUP_FENCE_FD"), getenv("CORSA_STARTUP_FENCE_LAUNCHER"), getenv("CORSA_STARTUP_FENCE_STORE")
	if candidate == "" && fd == "" && launcher == "" && store == "" {
		return candidateSupervision{}, nil
	}
	if candidate != "1" || fd != "3" || !filepath.IsAbs(launcher) || len(store) == 0 || len(store) > 128 {
		return candidateSupervision{}, startupfence.ErrUnavailable
	}
	return candidateSupervision{enabled: true, launcher: launcher, store: startupfence.StoreID(store)}, nil
}
