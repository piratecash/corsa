package desktop

import (
	"bytes"
	_ "embed"
	"sync"
	"time"

	"github.com/ebitengine/oto/v3"
	"github.com/hajimehoshi/go-mp3"
	"github.com/rs/zerolog/log"
)

//go:embed assets/new-message.mp3
var notifyMP3 []byte

var (
	otoCtx     *oto.Context
	otoOnce    sync.Once
	otoInitErr error
)

func initAudioContext() (*oto.Context, error) {
	otoOnce.Do(func() {
		op := &oto.NewContextOptions{
			SampleRate:   44100,
			ChannelCount: 2,
			Format:       oto.FormatSignedInt16LE,
		}
		var ready chan struct{}
		otoCtx, ready, otoInitErr = oto.NewContext(op)
		if otoInitErr == nil {
			<-ready
		}
	})
	return otoCtx, otoInitErr
}

// It is safe to call from any goroutine.  Callers should use `go systemBeep()`.
// Every call creates a new oto.Player on the shared audio context.  Multiple
// concurrent playbacks are allowed — the audio driver mixes them.  This means
// rapid-fire messages produce overlapping sounds ("drum roll") rather than
// being silently dropped.
func systemBeep() {
	ctx, err := initAudioContext()
	if err != nil {
		log.Warn().Err(err).Msg("audio init failed")
		return
	}

	decoder, err := mp3.NewDecoder(bytes.NewReader(notifyMP3))
	if err != nil {
		log.Warn().Err(err).Msg("mp3 decode failed")
		return
	}

	player := ctx.NewPlayer(decoder)
	// Note: as of oto v3.4, Player.Close() is deprecated and not needed.
	// The player is automatically cleaned up when it goes out of scope.

	player.Play()

	// Safety timeout: if the audio driver hangs and IsPlaying() never
	// returns false, bail out after 5 seconds so the goroutine doesn't
	// leak.
	deadline := time.After(5 * time.Second)
	for player.IsPlaying() {
		select {
		case <-deadline:
			log.Warn().Msg("systemBeep: playback exceeded 5s deadline, closing player")
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
