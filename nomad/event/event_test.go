package event

import (
	"testing"
)

func TestEvents_Publish(t *testing.T) {

	// pub, err := NewEventPublisher(EventPublisherCfg{EventCacheSize: 5})
	// require.NoError(t, err)

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// go pub.handleUpdates(ctx)

	// for i := 0; i < 25; i++ {
	// 	e := []Event{Event{Index: uint64(i)}}
	// 	pub.Publish(e)
	// }

	// require.Equal(t, pub.events.Len(), 5)
	// oldest, v, _ := pub.events.GetOldest()
}
