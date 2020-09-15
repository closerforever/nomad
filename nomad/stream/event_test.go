package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func testCfg() EventPublisherCfg {
	return EventPublisherCfg{
		EventBufferSize: 100,
	}
}

func TestEvents_Publisher_Publish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := NewEventPublisher(ctx, testCfg())

	for i := 0; i < 10; i++ {
		idx := uint64(i)
		e := Event{
			Index: idx,
		}
		pub.Publish(idx, []Event{e})
	}

	idx := uint64(10)
	var events []Event
	for i := 0; i < 5; i++ {
		idx := uint64(i)
		e := Event{
			Index: idx,
		}
		events = append(events, e)
	}
	pub.Publish(idx, events)

	require.Equal(t, 11, pub.events.Len())
}
