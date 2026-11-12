package main

import (
	"sync"
)

type Broker struct {
	mu          sync.Mutex
	subscribers map[int]chan string
	nextID      int
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[int]chan string),
	}
}

// Subscribe creates a new subscriber
func (b *Broker) Subscribe() (int, <-chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++

	ch := make(chan string, 10)
	b.subscribers[id] = ch

	return id, ch
}

// Publish sends message to all subscribers
func (b *Broker) Publish(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.subscribers {
		ch <- msg
	}
}

// Unsubscribe removes subscriber
func (b *Broker) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.subscribers[id]; ok {
		close(ch)
		delete(b.subscribers, id)
	}
}