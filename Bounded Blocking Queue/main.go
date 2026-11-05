package main 

// Implement a thread safe bounded blocking queue that has the following methods:

// BoundedBlockingQueue(int capacity) The constructor initializes the queue with a maximum capacity.
// void enqueue(int element) Adds an element to the front of the queue. If the queue is full, the calling thread is blocked until the queue is no longer full.
// int dequeue() Returns the element at the rear of the queue and removes it. If the queue is empty, the calling thread is blocked until the queue is no longer empty.
// int size() Returns the number of elements currently in the queue./

import (
	"fmt"
	"sync"
)

type queue struct {
	capacity int
	items    []int

	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond
}

func BoundedBlockingQueue(capacity int) *queue {
	q := &queue{
		capacity: capacity,
		items:    make([]int, 0, capacity),
	}

	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)

	return q
}

func (q *queue) enqueue(element int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Wait while queue is full.
	for len(q.items) == q.capacity {
		q.notFull.Wait()
	}

	q.items = append(q.items, element)

	// Notify a waiting consumer.
	q.notEmpty.Signal()
}

func (q *queue) dequeue() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Wait while queue is empty.
	for len(q.items) == 0 {
		q.notEmpty.Wait()
	}

	// Remove element from the front (FIFO).
	element := q.items[0]
	q.items = q.items[1:]

	// Notify a waiting producer.
	q.notFull.Signal()

	return element
}

func (q *queue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.items)
}

func main() {
	q := BoundedBlockingQueue(3)

	q.enqueue(2)
	q.enqueue(5)

	fmt.Println(q.dequeue()) // 2

	q.enqueue(7)

	fmt.Println(q.dequeue()) // 5
	fmt.Println(q.dequeue()) // 7

	fmt.Println(q.size()) // 0
}