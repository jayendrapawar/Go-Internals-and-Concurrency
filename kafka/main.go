
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Value string
}

type Handler func(context.Context, Message)

type Subscription struct {
	ID      string
	channel chan Message
	done    chan struct{}
}

type Topic struct {
	Name string

	mu          sync.Mutex
	subscribers map[string]*Subscription

	queue chan Message
	done  chan struct{}

	closed bool
	wg     sync.WaitGroup
}

type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	closed bool
}

var (
	ErrTopicClosed = errors.New("topic is closed")
	ErrBrokerClosed = errors.New("broker is closed")
	ErrAlreadySubscribed = errors.New("consumer already subscribed")
	ErrTopicNotFound = errors.New("topic not found")
	ErrSubscriptionNotFound = errors.New("subscription not found")
	ErrQueueFull = errors.New("topic queue is full")
)

// ---------------- BROKER ----------------

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBrokerClosed
	}

	if _, exists := b.topics[name]; exists {
		return nil
	}

	topic := NewTopic(name)
	b.topics[name] = topic

	return nil
}

func (b *Broker) GetTopic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	topic, exists := b.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return topic, nil
}

func (b *Broker) Shutdown() {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return
	}

	b.closed = true

	topics := make([]*Topic, 0, len(b.topics))

	for _, topic := range b.topics {
		topics = append(topics, topic)
	}

	b.topics = make(map[string]*Topic)

	b.mu.Unlock()

	for _, topic := range topics {
		topic.Close()
	}
}

// ---------------- TOPIC ----------------

func NewTopic(name string) *Topic {
	t := &Topic{
		Name:        name,
		subscribers: make(map[string]*Subscription),
		queue:       make(chan Message, 100),
		done:        make(chan struct{}),
	}

	t.wg.Add(1)
	go t.distribute()

	return t
}

// Subscribe creates a dedicated channel and consumer goroutine.
func (t *Topic) Subscribe(
	consumerID string,
	handler Handler,
) error {

	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return ErrTopicClosed
	}

	if _, exists := t.subscribers[consumerID]; exists {
		t.mu.Unlock()
		return ErrAlreadySubscribed
	}

	sub := &Subscription{
		ID:      consumerID,
		channel: make(chan Message, 100),
		done:    make(chan struct{}),
	}

	t.subscribers[consumerID] = sub

	// Add while holding the same lock used by Close.
	// Close cannot begin Wait before this Add completes.
	t.wg.Add(1)

	t.mu.Unlock()

	go func() {
		defer t.wg.Done()
		defer close(sub.done)

		for {
			select {
			case <-t.done:
				return

			case msg, ok := <-sub.channel:
				if !ok {
					return
				}

				handler(t.context(), msg)
			}
		}
	}()

	return nil
}

// Unsubscribe safely removes one consumer.
func (t *Topic) Unsubscribe(consumerID string) error {
	t.mu.Lock()

	sub, exists := t.subscribers[consumerID]
	if !exists {
		t.mu.Unlock()
		return ErrSubscriptionNotFound
	}

	delete(t.subscribers, consumerID)

	// Distributor also uses t.mu while sending.
	// Therefore no send can race with this close.
	close(sub.channel)

	t.mu.Unlock()

	// Wait for this consumer goroutine to exit.
	<-sub.done

	return nil
}

// Publish adds a message to the topic queue.
func (t *Topic) Publish(msg Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTopicClosed
	}

	// Non-blocking publish to avoid blocking producers.
	select {
	case t.queue <- msg:
		return nil

	default:
		return ErrQueueFull
	}
}

// distribute reads messages and broadcasts to subscribers.
func (t *Topic) distribute() {
	defer t.wg.Done()

	for {
		select {
		case <-t.done:
			return

		case msg := <-t.queue:
			t.mu.Lock()

			if t.closed {
				t.mu.Unlock()
				return
			}

			for id, sub := range t.subscribers {
				select {
				case sub.channel <- msg:
					// Successfully delivered.

				default:
					// Consumer is slow; drop this message.
					fmt.Printf(
						"Consumer %s missed message %s\n",
						id,
						msg.Value,
					)
				}
			}

			t.mu.Unlock()
		}
	}
}

// Close gracefully shuts down the topic.
func (t *Topic) Close() {
	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return
	}

	t.closed = true

	// Signal dispatcher and consumers.
	close(t.done)

	// Close all subscriber channels.
	for id, sub := range t.subscribers {
		close(sub.channel)
		delete(t.subscribers, id)
	}

	t.mu.Unlock()

	// Wait for all managed goroutines to exit.
	t.wg.Wait()
}

// context returns a context cancelled when the topic shuts down.
// Each handler can use it for cooperative cancellation.
func (t *Topic) context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	// This helper is not used for long-running handler lifetimes.
	// See the implementation note below.
	go func() {
		select {
		case <-t.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx
}

// ---------------- MAIN ----------------

func main() {
	broker := NewBroker()

	broker.CreateTopic("orders")
	broker.CreateTopic("payments")

	orders, _ := broker.GetTopic("orders")
	payments, _ := broker.GetTopic("payments")

	handler := func(ctx context.Context, msg Message) {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Println("Processed:", msg.Value)
		}
	}

	// Subscribe consumers.
	orders.Subscribe("consumer-1", handler)
	orders.Subscribe("consumer-2", handler)
	payments.Subscribe("consumer-1", handler)

	// Publish messages.
	orders.Publish(Message{Value: "Order Created"})
	payments.Publish(Message{Value: "Payment Received"})

	time.Sleep(100 * time.Millisecond)

	// Unsubscribe consumer-1 from orders only.
	fmt.Println("--- Unsubscribe consumer-1 ---")
	orders.Unsubscribe("consumer-1")

	orders.Publish(Message{Value: "Order Updated"})

	time.Sleep(100 * time.Millisecond)

	// Graceful shutdown.
	fmt.Println("--- Broker Shutdown ---")
	broker.Shutdown()

	fmt.Println("All broker topics shut down")
}
