package skeleton

import (
	"testing"
	"time"

	"github.com/jpillora/backoff"
)

// succeedsSoon keeps retrying the function until it returns nil, or 15 seconds
// elapse when the test fails.
func succeedsSoon(t *testing.T, f func() error) {
	max := 15 * time.Second
	end := time.NewTimer(max).C
	done := make(chan struct{})
	defer func() {
		done <- struct{}{}
	}()

	go func() {
		select {
		case <-end:
			t.Fatal("succeedsSoon timed out")
		case <-done:
		}
	}()

	b := &backoff.Backoff{
		Min:    1 * time.Millisecond,
		Max:    max,
		Factor: 2,
		Jitter: false,
	}
	for err := f(); err != nil; {
		time.Sleep(b.Duration())
	}
}
