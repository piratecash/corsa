package desktop

import (
	"bytes"
	"io"
	_ "embed"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hajimehoshi/go-mp3"
	"github.com/ebitengine/oto/v3"
)

//go:embed assets/new-message.mp3
var notifyMP3 []byte

var (
	otoCtx     *oto.Context
	otoOnce    sync.Once
	otoInitErr error
	playing    atomic.Bool // prevents overlapping playback
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

// systemBeep plays the embedded mp3 notification sound.
// It is safe to call from any goroutine.  Playback is synchronous
// (blocks until the sound finishes), so callers should use `go systemBeep()`.
// Concurrent calls are coalesced: if a sound is already playing, the new
// call returns immediately rather than overlapping playback.
func systemBeep() {
	if !playing.CompareAndSwap(false, true) {
		return // already playing
	}
	defer playing.Store(false)

	ctx, err := initAudioContext()
	if err != nil {
		log.Printf("desktop: audio init failed: %v", err)
		return
	}

	decoder, err := mp3.NewDecoder(bytes.NewReader(notifyMP3))
	if err != nil {
		log.Printf("desktop: mp3 decode failed: %v", err)
		return
	}

	player := ctx.NewPlayer(decoder)

	player.Play()
	for player.IsPlaying() {
		time.Sleep(10 * time.Millisecond)
	}

	// Drain any remaining buffered data to avoid truncation.
	_, _ = io.ReadAll(decoder)
}
