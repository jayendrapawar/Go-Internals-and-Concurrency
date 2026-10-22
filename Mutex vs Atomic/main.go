package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	goroutines = 100
	increments = 1000
)

// Mutex vs Atomic:
// Aspect       | sync.Mutex             | sync/atomic
// Operation    | Lock → update → unlock | Atomic update
// Best for     | Shared/complex state   | Simple counters/flags
// Correctness  | Lock all accesses      | Use atomic accesses
// Performance  | Possible contention    | Often faster for counters
// Complexity   | Simple for state       | Careful atomic usage

// Mutex-based counter
type MutexCounter struct {
	mu    sync.Mutex
	value int
}

func (c *MutexCounter) Increment() {
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

func (c *MutexCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Atomic-based counter
type AtomicCounter struct {
	value atomic.Int64
}

func (c *AtomicCounter) Increment() {
	c.value.Add(1)
}

func (c *AtomicCounter) Value() int64 {
	return c.value.Load()
}

func main() {
	var wg sync.WaitGroup
	var mutexCounter MutexCounter
	var atomicCounter AtomicCounter

	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range increments {
				mutexCounter.Increment()
			}
		}()

		go func() {
			defer wg.Done()
			for range increments {
				atomicCounter.Increment()
			}
		}()
	}

	wg.Wait()

	expected := goroutines * increments
	fmt.Println("Expected:", expected)
	fmt.Println("Mutex:   ", mutexCounter.Value())
	fmt.Println("Atomic:  ", atomicCounter.Value())
}
