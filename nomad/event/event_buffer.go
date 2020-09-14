package event

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultTTL = 1 * time.Hour
)

// eventBuffer is a single-writer, multiple-reader, fixed length concurrent
// buffer of events that have been published. The buffer is
// the head and tail of an atomically updated single-linked list. Atomic
// accesses are usually to be suspected as premature optimization but this
// specific design has several important features that significantly simplify a
// lot of our PubSub machinery.
//
// eventBuffer is an adaptation of conuls agent/stream/event eventBuffer but
// has been updated to be a max length buffer to work for Nomad's usecase.
//
// The eventBuffer only tracks the most recent set of published events,
// up to the max configured size, older events are dropped from the buffer
// but will only be garbage collected once the slowest reader drops the item.
// Consumers are notified of new events by closing a channel on the previous head
// allowing efficient broadcast to many watchers without having to run multiple
// goroutines or deliver to O(N) separate channels.
//
// Because eventBuffer is a linked list with atomically updated pointers, readers don't
// have to take a lock and can consume at their own pace. Slow readers can eventually
// append
//
// A new buffer is constructed with a sentinel "empty" bufferItem that has a nil
// Events array. This enables subscribers to start watching for the next update
// immediately.
//
// The zero value eventBuffer is _not_ usable, as it has not been
// initialized with an empty bufferItem so can not be used to wait for the first
// published event. Call newEventBuffer to construct a new buffer.
//
// Calls to Append or AppendBuffer that mutate the head must be externally
// synchronized. This allows systems that already serialize writes to append
// without lock overhead (e.g. a snapshot goroutine appending thousands of
// events).
type eventBuffer struct {
	size *int64

	ctx context.Context

	head atomic.Value
	tail atomic.Value

	maxSize    int64
	maxItemTTL time.Duration
}

// newEventBuffer creates an eventBuffer ready for use.
func newEventBuffer(size int64, maxItemTTL time.Duration) *eventBuffer {
	zero := int64(0)
	b := &eventBuffer{maxSize: size, size: &zero}

	item := newBufferItem(0, nil)

	b.head.Store(item)
	b.tail.Store(item)

	return b
}

// Append a set of events from one raft operation to the buffer and notify
// watchers. After calling append, the caller must not make any further
// mutations to the events as they may have been exposed to subscribers in other
// goroutines. Append only supports a single concurrent caller and must be
// externally synchronized with other Append, AppendBuffer or AppendErr calls.
func (b *eventBuffer) Append(index uint64, events []Event) {
	b.appendItem(newBufferItem(index, events))
}

func (b *eventBuffer) appendItem(item *bufferItem) {
	// Store the next item to the old tail
	oldTail := b.Tail()
	oldTail.link.next.Store(item)

	// Update the tail to the new item
	b.tail.Store(item)

	// Increment the buffer size
	size := atomic.AddInt64(b.size, 1)

	// Check if we need to advance the head to keep the list
	// constrained to max size
	if size > b.maxSize {
		b.advanceHead()
	}

	// notify waiters next event is available
	close(oldTail.link.ch)

}

// advanceHead drops the current Head buffer item and notifies readers
// that the item should be discarded by closing droppedCh.
// Slow readers will prevent the old head from being GC'd until they
// discard it.
func (b *eventBuffer) advanceHead() {
	old := b.Head()
	next := old.link.next.Load()

	close(old.link.droppedCh)
	b.head.Store(next)
	atomic.AddInt64(b.size, -1)

}

// Head returns the current head of the buffer. It will always exist but it may
// be a "sentinel" empty item with a nil Events slice to allow consumers to
// watch for the next update. Consumers should always check for empty Events and
// treat them as no-ops. Will panic if eventBuffer was not initialized correctly
// with NewEventBuffer
func (b *eventBuffer) Head() *bufferItem {
	return b.head.Load().(*bufferItem)
}

// Tail returns the current tail of the buffer. It will always exist but it may
// be a "sentinel" empty item with a Nil Events slice to allow consumers to
// watch for the next update. Consumers should always check for empty Events and
// treat them as no-ops. Will panic if eventBuffer was not initialized correctly
// with NewEventBuffer
func (b *eventBuffer) Tail() *bufferItem {
	return b.tail.Load().(*bufferItem)
}

// Len returns the current length of the buffer
func (b *eventBuffer) Len() int {
	return int(atomic.LoadInt64(b.size))
}

func (b *eventBuffer) prune() {
	for {
		head := b.Head()
		if b.Len() == 0 {
			return
		}

		if time.Since(head.createdAt) > b.maxItemTTL {
			b.advanceHead()
		} else {
			return
		}
	}
}

// bufferItem represents a set of events published by a single raft operation.
// The first item returned by a newly constructed buffer will have nil Events.
// It is a sentinel value which is used to wait on the next events via Next.
//
// To iterate to the next event, a Next method may be called which may block if
// there is no next element yet.
//
// Holding a pointer to the item keeps all the events published since in memory
// so it's important that subscribers don't hold pointers to buffer items after
// they have been delivered except where it's intentional to maintain a cache or
// trailing store of events for performance reasons.
//
// Subscribers must not mutate the bufferItem or the Events or Encoded payloads
// inside as these are shared between all readers.
type bufferItem struct {
	// Events is the set of events published at one raft index. This may be nil as
	// a sentinel value to allow watching for the first event in a buffer. Callers
	// should check and skip nil Events at any point in the buffer. It will also
	// be nil if the producer appends an Error event because they can't complete
	// the request to populate the buffer. Err will be non-nil in this case.
	Events []Event

	Index uint64

	// Err is non-nil if the producer can't complete their task and terminates the
	// buffer. Subscribers should return the error to clients and cease attempting
	// to read from the buffer.
	Err error

	// link holds the next pointer and channel. This extra bit of indirection
	// allows us to splice buffers together at arbitrary points without including
	// events in one buffer just for the side-effect of watching for the next set.
	// The link may not be mutated once the event is appended to a buffer.
	link *bufferLink

	createdAt time.Time
}

type bufferLink struct {
	// next is an atomically updated pointer to the next event in the buffer. It
	// is written exactly once by the single published and will always be set if
	// ch is closed.
	next atomic.Value

	// ch is closed when the next event is published. It should never be mutated
	// (e.g. set to nil) as that is racey, but is closed once when the next event
	// is published. the next pointer will have been set by the time this is
	// closed.
	ch chan struct{}

	// droppedCh is closed when the event is dropped from the buffer due to
	// sizing constraints.
	droppedCh chan struct{}
}

// newBufferItem returns a blank buffer item with a link and chan ready to have
// the fields set and be appended to a buffer.
func newBufferItem(index uint64, events []Event) *bufferItem {
	return &bufferItem{
		link: &bufferLink{
			ch:        make(chan struct{}),
			droppedCh: make(chan struct{}),
		},
		Events:    events,
		Index:     index,
		createdAt: time.Now(),
	}
}

// Next return the next buffer item in the buffer. It may block until ctx is
// cancelled or until the next item is published.
func (i *bufferItem) Next(ctx context.Context, forceClose <-chan struct{}) (*bufferItem, error) {
	// See if there is already a next value, block if so. Note we don't rely on
	// state change (chan nil) as that's not threadsafe but detecting close is.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-forceClose:
		return nil, fmt.Errorf("subscription closed")
	case <-i.link.ch:
	}

	// Check if the reader is too slow and the event buffer as discarded the event
	select {
	case <-i.link.droppedCh:
		return nil, fmt.Errorf("event dropped from buffer")
	default:
	}

	// If channel closed, there must be a next item to read
	nextRaw := i.link.next.Load()
	if nextRaw == nil {
		// shouldn't be possible
		return nil, errors.New("invalid next item")
	}
	next := nextRaw.(*bufferItem)
	if next.Err != nil {
		return nil, next.Err
	}
	return next, nil
}

// NextNoBlock returns the next item in the buffer without blocking. If it
// reaches the most recent item it will return nil.
func (i *bufferItem) NextNoBlock() *bufferItem {
	nextRaw := i.link.next.Load()
	if nextRaw == nil {
		return nil
	}
	return nextRaw.(*bufferItem)
}

// NextLink returns either the next item in the buffer if there is one, or
// an empty item (that will be ignored by subscribers) that has a pointer to
// the same link as this bufferItem (but none of the bufferItem content).
// When the link.ch is closed, subscriptions will be notified of the next item.
func (i *bufferItem) NextLink() *bufferItem {
	next := i.NextNoBlock()
	if next == nil {
		// Return an empty item that can be followed to the next item published.
		return &bufferItem{link: i.link}
	}
	return next
}
