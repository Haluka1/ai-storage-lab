package cacheindex

import (
	"context"
	"errors"
	"sync"
)

var ErrEventBusClosed = errors.New("event bus closed")

type EventMessage struct {
	Event Event
	Ack   func(context.Context) error
}

type EventBus interface {
	Publish(context.Context, Event) error
	Subscribe(context.Context, string) (<-chan EventMessage, error)
	Close() error
}

type inMemorySubscriber struct {
	ch      chan EventMessage
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
	senders sync.WaitGroup
	once    sync.Once
}

func newInMemorySubscriber(bufferSize int) *inMemorySubscriber {
	return &inMemorySubscriber{
		ch:   make(chan EventMessage, bufferSize),
		done: make(chan struct{}),
	}
}

func (s *inMemorySubscriber) acquireSender() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.senders.Add(1)
	return true
}

func (s *inMemorySubscriber) close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.done)
		s.mu.Unlock()
		s.senders.Wait()
		close(s.ch)
	})
}

type InMemoryBus struct {
	mu         sync.RWMutex
	bufferSize int
	closed     bool
	subs       map[string]*inMemorySubscriber
}

func NewInMemoryBus(bufferSize int) *InMemoryBus {
	if bufferSize <= 0 {
		bufferSize = 16
	}
	return &InMemoryBus{
		bufferSize: bufferSize,
		subs:       make(map[string]*inMemorySubscriber),
	}
}

func (b *InMemoryBus) Publish(ctx context.Context, event Event) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrEventBusClosed
	}
	subs := make([]*inMemorySubscriber, 0, len(b.subs))
	for _, sub := range b.subs {
		subs = append(subs, sub)
	}
	b.mu.RUnlock()

	msg := EventMessage{Event: event, Ack: func(context.Context) error { return nil }}
	for _, sub := range subs {
		if !sub.acquireSender() {
			continue
		}
		select {
		case sub.ch <- msg:
			sub.senders.Done()
		case <-sub.done:
			sub.senders.Done()
		case <-ctx.Done():
			sub.senders.Done()
			return ctx.Err()
		}
	}
	return nil
}

func (b *InMemoryBus) Subscribe(ctx context.Context, name string) (<-chan EventMessage, error) {
	if name == "" {
		name = "default"
	}
	sub := newInMemorySubscriber(b.bufferSize)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		sub.close()
		return nil, ErrEventBusClosed
	}
	old := b.subs[name]
	b.subs[name] = sub
	b.mu.Unlock()
	if old != nil {
		old.close()
	}

	go func() {
		select {
		case <-ctx.Done():
			b.removeSubscriber(name, sub)
		case <-sub.done:
		}
	}()
	return sub.ch, nil
}

func (b *InMemoryBus) removeSubscriber(name string, sub *inMemorySubscriber) {
	b.mu.Lock()
	if b.subs[name] == sub {
		delete(b.subs, name)
	}
	b.mu.Unlock()
	sub.close()
}

func (b *InMemoryBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := make([]*inMemorySubscriber, 0, len(b.subs))
	for name, sub := range b.subs {
		delete(b.subs, name)
		subs = append(subs, sub)
	}
	b.mu.Unlock()
	for _, sub := range subs {
		sub.close()
	}
	return nil
}
