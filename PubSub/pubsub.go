package main

import "sync"

type BasicBroker struct {
	mu          sync.Mutex
	subscribers []chan string
}

func NewBasicBroker() *BasicBroker {
	return &BasicBroker{}
}

// Subscribe creates a new subscriber
func (b *BasicBroker) Subscribe() <-chan string {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan string, 10)
	b.subscribers = append(b.subscribers, ch)

	return ch
}

// Publish sends message to all subscribers
func (b *BasicBroker) Publish(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.subscribers {
		ch <- msg
	}
}