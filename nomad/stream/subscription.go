package stream

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/hashicorp/go-hclog"
)

const (
	// subscriptionStateOpen is the default state of a subscription. An open
	// subscription may receive new events.
	subscriptionStateOpen uint32 = 0

	// subscriptionStateClosed indicates that the subscription was closed, possibly
	// as a result of a change to an ACL token, and will not receive new events.
	// The subscriber must issue a new Subscribe request.
	subscriptionStateClosed uint32 = 1
)

// ErrSubscriptionClosed is a error signalling the subscription has been
// closed. The client should Unsubscribe, then re-Subscribe.
var ErrSubscriptionClosed = errors.New("subscription closed by server, client should resubscribe")

type Subscriber struct {
	logger hclog.Logger
}

type Subscription struct {
	// state is accessed atomically 0 means open, 1 means closed with reload
	state uint32

	req *SubscribeRequest

	// currentItem stores the current buffer item we are on. It
	// is mutated by calls to Next.
	currentItem *bufferItem

	// forceClosed is closed when forceClose is called. It is used by
	// EventPublisher to cancel Next().
	forceClosed chan struct{}
}

type SubscribeRequest struct {
	// topics []Topic

	topics map[Topic][]string
}

// type Topic struct {
// 	Type string
// 	Keys []string
// }

func (s *Subscription) Next(ctx context.Context) ([]Event, error) {
	if atomic.LoadUint32(&s.state) == subscriptionStateClosed {
		return nil, ErrSubscriptionClosed
	}

	for {
		next, err := s.currentItem.Next(ctx, s.forceClosed)
		switch {
		case err != nil && atomic.LoadUint32(&s.state) == subscriptionStateClosed:
			return nil, ErrSubscriptionClosed
		case err != nil:
			return nil, err
		}
		s.currentItem = next

		events := filter(s.req, next.Events)
		if len(events) == 0 {
			continue
		}
		return events, nil
	}
}

// filter events to only those that match a subscriptions topic/keys
func filter(req *SubscribeRequest, events []Event) []Event {
	if len(events) == 0 {
		return events
	}

	var count int
	for _, e := range events {
		if _, ok := req.topics[e.Topic]; ok {
			for _, k := range req.topics[e.Topic] {
				if e.Key == k || k == AllKeys {
					count++
				}
			}
		}
	}

	// Only allocate a new slice if some events need to be filtered out
	switch count {
	case 0:
		return nil
	case len(events):
		return events
	}

	// Return filtered events
	result := make([]Event, 0, count)
	for _, e := range events {
		if _, ok := req.topics[e.Topic]; ok {
			for _, k := range req.topics[e.Topic] {
				if e.Key == k || k == AllKeys {
					result = append(result, e)
				}
			}
		}
	}
	return result
}
