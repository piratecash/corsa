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

//go:embed assets/download-done.mp3
var downloadDoneMP3 []byte

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

// playEmbeddedMP3 decodes and plays an MP3 byte slice on the shared audio
// context. Logs are tagged with `cue` so the source of "audio init failed"
// or "mp3 decode failed" is unambiguous when more than one notification
// sound is wired up. Safe to call from any goroutine — every invocation
// allocates its own player and the audio driver mixes overlapping playbacks.
func playEmbeddedMP3(cue string, data []byte) {
	ctx, err := initAudioContext()
	if err != nil {
		log.Warn().Str("cue", cue).Err(err).Msg("audio init failed")
		return
	}

	decoder, err := mp3.NewDecoder(bytes.NewReader(data))
	if err != nil {
		log.Warn().Str("cue", cue).Err(err).Msg("mp3 decode failed")
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
			log.Warn().Str("cue", cue).Msg("audio playback exceeded 5s deadline, closing player")
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// systemBeep plays the new-message notification sound. Safe to call
// from any goroutine. Callers should use `go systemBeep()`. Multiple
// concurrent playbacks are allowed — the audio driver mixes them, so
// rapid-fire messages produce overlapping sounds ("drum roll") rather
// than being silently dropped.
func systemBeep() {
	playEmbeddedMP3("new-message", notifyMP3)
}

// playDownloadDone plays the download-finished notification sound after
// the receiver-side verification has stored the file durably. Wired
// from window.go's TopicFileDownloadCompleted subscription. Same
// concurrency contract as systemBeep — callers should run it on a
// dedicated goroutine.
func playDownloadDone() {
	playEmbeddedMP3("download-done", downloadDoneMP3)
}
