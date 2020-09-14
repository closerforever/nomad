package event

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

type Event struct {
	Topic   string
	Key     string
	Index   uint64
	Payload interface{}
}

type EventPublisherCfg struct {
	EventBufferSize int64
}

type EventPublisher struct {
	size int64

	lock sync.Mutex

	events *eventBuffer

	pruneTick time.Duration

	logger hclog.Logger

	// publishCh is used to send messages from an active txn to a goroutine which
	// publishes events, so that publishing can happen asynchronously from
	// the Commit call in the FSM hot path.
	publishCh chan changeEvents
}

func NewEventPublisher(ctx context.Context, cfg EventPublisherCfg) (*EventPublisher, error) {
	buffer := newEventBuffer(cfg.EventBufferSize, 1*time.Hour)
	e := &EventPublisher{
		events:    buffer,
		publishCh: make(chan changeEvents),
	}

	go e.handleUpdates(ctx)
	go e.periodicPrune(ctx)

	return e, nil
}

type changeEvents struct {
	index  uint64
	events []Event
}

func NewPublisher() *EventPublisher {
	return &EventPublisher{}
}

// Publish events to all subscribers of the event Topic.
func (e *EventPublisher) Publish(index uint64, events []Event) {
	if len(events) > 0 {
		e.publishCh <- changeEvents{index: index, events: events}
	}
}

func (e *EventPublisher) handleUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// TODO handle closing subscriptions
			// e.subscriptions.closeAll()
			return
		case update := <-e.publishCh:
			e.sendEvents(update)
		}
	}
}

func (e *EventPublisher) periodicPrune(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.pruneTick):
			e.lock.Lock()
			e.events.prune()
			e.lock.Unlock()
		}
	}
}

// sendEvents sends the given events to any applicable topic listeners, as well
// as any ACL update events to cause affected listeners to reset their stream.
func (e *EventPublisher) sendEvents(update changeEvents) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.events.Append(update.index, update.events)
}
