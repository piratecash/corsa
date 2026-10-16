//go:build sessiontraffic

package node

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func sessionTrafficEnv(dir string, overrides map[string]string) func(string) string {
	env := map[string]string{
		sessionTrafficEnvNeighbours: "8, 16,32",
		sessionTrafficEnvRepeats:    "3",
		sessionTrafficEnvWarmup:     "2m",
		sessionTrafficEnvWindow:     "10m",
		sessionTrafficEnvTick:       "30s",
		sessionTrafficEnvIdleSettle: "10s",
		sessionTrafficEnvIdleWindow: "30s",
		sessionTrafficEnvMode:       "measure",
		sessionTrafficEnvOut:        dir,
	}
	for name, value := range overrides {
		env[name] = value
	}
	return func(name string) string { return env[name] }
}

func TestSessionTrafficParamsReadEveryParameter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	got, err := readSessionTrafficParams(sessionTrafficEnv(dir, nil))
	if err != nil {
		t.Fatalf("readSessionTrafficParams: %v", err)
	}
	want := sessionTrafficParams{
		Neighbours: []sessionTrafficNeighbourCount{8, 16, 32},
		Repeats:    3,
		Warmup:     2 * time.Minute,
		Window:     10 * time.Minute,
		Tick:       30 * time.Second,
		IdleSettle: 10 * time.Second,
		IdleWindow: 30 * time.Second,
		Mode:       sessionTrafficModeMeasure,
		OutDir:     dir,
	}
	if !slices.Equal(got.Neighbours, want.Neighbours) {
		t.Errorf("Neighbours = %v, want %v", got.Neighbours, want.Neighbours)
	}
	got.Neighbours, want.Neighbours = nil, nil
	if got.Repeats != want.Repeats || got.Warmup != want.Warmup || got.Window != want.Window || got.Tick != want.Tick ||
		got.IdleSettle != want.IdleSettle || got.IdleWindow != want.IdleWindow || got.Mode != want.Mode || got.OutDir != want.OutDir {
		t.Errorf("params = %+v, want %+v", got, want)
	}
}

// A measurement parameter nobody wrote down is refused, not defaulted.
func TestSessionTrafficParamsRefuseWhatThePlanCannotName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cases := map[string]map[string]string{
		"missing window":       {sessionTrafficEnvWindow: ""},
		"zero repeats":         {sessionTrafficEnvRepeats: "0"},
		"bad neighbour count":  {sessionTrafficEnvNeighbours: "8,x"},
		"unknown mode":         {sessionTrafficEnvMode: "quick"},
		"relative out dir":     {sessionTrafficEnvOut: "records"},
		"missing out dir":      {sessionTrafficEnvOut: dir + "/absent"},
		"tick does not divide": {sessionTrafficEnvTick: "7m"},
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := readSessionTrafficParams(sessionTrafficEnv(dir, overrides))
			if !errors.Is(err, errSessionTrafficParams) {
				t.Fatalf("error = %v, want errSessionTrafficParams", err)
			}
		})
	}
}

func TestSessionTrafficTallyCountsFramesByDirectionAndType(t *testing.T) {
	t.Parallel()

	tally := newSessionTrafficFrameTally()
	for _, line := range []string{
		`2026-10-16T12:44:03.412Z out: {"type":"ping"}`,
		`2026-10-16T12:44:04.000Z out: {"type":"ping"}`,
		`2026-10-16T12:44:05.000Z in: {"type":"pong"}`,
		`2026-10-16T12:44:06.000Z out [write_failed]: {"type":"announce_routes","routes":[]}`,
		`2026-10-16T12:44:07.000Z in: <frame_too_large>`,
		`not a capture line`,
	} {
		tally.addLine(line)
	}
	// Direction first, then bytes, largest first.
	want := []sessionTrafficFrameRow{
		{Direction: domain.WireDirectionIn, Type: sessionTrafficUntypedFrame, Frames: 1, Bytes: loadStandByteCount(len(`<frame_too_large>`) + 1)},
		{Direction: domain.WireDirectionIn, Type: "pong", Frames: 1, Bytes: loadStandByteCount(len(`{"type":"pong"}`) + 1)},
		{Direction: domain.WireDirectionOut, Type: "announce_routes", Frames: 1, Bytes: loadStandByteCount(len(`{"type":"announce_routes","routes":[]}`) + 1)},
		{Direction: domain.WireDirectionOut, Type: "ping", Frames: 2, Bytes: loadStandByteCount(2 * (len(`{"type":"ping"}`) + 1))},
	}
	got := tally.rows()
	if !slices.Equal(got, want) {
		t.Errorf("rows =\n%+v\nwant\n%+v", got, want)
	}
}

func TestSessionTrafficTickMustDivideTheWindow(t *testing.T) {
	t.Parallel()

	if err := requireSessionTrafficTickFits(30*time.Second, 10*time.Minute); err != nil {
		t.Errorf("30s in 10m: %v", err)
	}
	if err := requireSessionTrafficTickFits(7*time.Minute, 10*time.Minute); err == nil {
		t.Error("7m in 10m accepted")
	}
}
