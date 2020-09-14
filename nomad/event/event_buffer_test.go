package event

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBufferFuzz(t *testing.T) {
	nReaders := 1000
	nMessages := 1000

	b := newEventBuffer(1000, defaultTTL)

	// Start a write goroutine that will publish 10000 messages with sequential
	// indexes and some jitter in timing (to allow clients to "catch up" and block
	// waiting for updates).
	go func() {
		seed := time.Now().UnixNano()
		t.Logf("Using seed %d", seed)
		// z is a Zipfian distribution that gives us a number of milliseconds to
		// sleep which are mostly low - near zero but occasionally spike up to near
		// 100.
		z := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.5, 1.5, 50)

		for i := 0; i < nMessages; i++ {
			// Event content is arbitrary and not valid for our use of buffers in
			// streaming - here we only care about the semantics of the buffer.
			e := Event{
				Index: uint64(i), // Indexes should be contiguous
			}
			b.Append(uint64(i), []Event{e})
			// Sleep sometimes for a while to let some subscribers catch up
			wait := time.Duration(z.Uint64()) * time.Millisecond
			time.Sleep(wait)
		}
	}()

	// Run n subscribers following and verifying
	errCh := make(chan error, nReaders)

	// Load head here so all subscribers start from the same point or they might
	// not run until several appends have already happened.
	head := b.Head()

	for i := 0; i < nReaders; i++ {
		go func(i int) {
			expect := uint64(0)
			item := head
			var err error
			for {
				item, err = item.Next(context.Background(), nil)
				if err != nil {
					errCh <- fmt.Errorf("subscriber %05d failed getting next %d: %s", i,
						expect, err)
					return
				}
				if item.Events[0].Index != expect {
					errCh <- fmt.Errorf("subscriber %05d got bad event want=%d, got=%d", i,
						expect, item.Events[0].Index)
					return
				}
				expect++
				if expect == uint64(nMessages) {
					// Succeeded
					errCh <- nil
					return
				}
			}
		}(i)
	}

	// Wait for all readers to finish one way or other
	for i := 0; i < nReaders; i++ {
		err := <-errCh
		assert.NoError(t, err)
	}
}

func TestEventBuffer_Slow_Reader(t *testing.T) {

	b := newEventBuffer(10, defaultTTL)

	for i := 0; i < 10; i++ {
		e := Event{
			Index: uint64(i), // Indexes should be contiguous
		}
		b.Append(uint64(i), []Event{e})
	}

	head := b.Head()

	for i := 10; i < 15; i++ {
		e := Event{
			Index: uint64(i), // Indexes should be contiguous
		}
		b.Append(uint64(i), []Event{e})
	}

	// Ensure the slow reader errors to handle dropped events and
	// fetch latest head
	ev, err := head.Next(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, ev)

	newHead := b.Head()
	require.Equal(t, 4, int(newHead.Index))
}

func TestEventBuffer_Size(t *testing.T) {
	b := newEventBuffer(100, defaultTTL)

	for i := 0; i < 10; i++ {
		e := Event{
			Index: uint64(i), // Indexes should be contiguous
		}
		b.Append(uint64(i), []Event{e})
	}

	require.Equal(t, int64(10), b.Len())
}

// TestEventBuffer_Prune_AllOld tests the behavior when all items
// are past their TTL, the event buffer should prune down to the last message
// and hold onto the last item.
func TestEventBuffer_Prune_AllOld(t *testing.T) {
	b := newEventBuffer(100, 1*time.Second)

	for i := 0; i < 10; i++ {
		e := Event{
			Index: uint64(i), // Indexes should be contiguous
		}
		b.Append(uint64(i), []Event{e})
	}

	require.Equal(t, 10, int(b.Len()))

	time.Sleep(1 * time.Second)

	b.prune()

	require.Equal(t, 9, int(b.Head().Index))
	require.Equal(t, 0, b.Len())
}
