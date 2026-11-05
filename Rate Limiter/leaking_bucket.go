package main

import (
	"sync"
	"time"
)

type LeakyBucket struct {
	mu       sync.Mutex
	queue    []int
	capacity int
	rate     time.Duration
	lastLeak time.Time
}

func NewLeakyBucket(capacity int, leakRate int) *LeakyBucket {
	return &LeakyBucket{
		queue:    make([]int, 0, capacity),
		capacity: capacity,
		rate:     time.Second / time.Duration(leakRate),
		lastLeak: time.Now(),
	}
}

func (lb *LeakyBucket) Add(request int) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(lb.lastLeak)

	leaked := int(elapsed / lb.rate)

	if leaked > 0 {
		if leaked >= len(lb.queue) {
			lb.queue = lb.queue[:0]
		} else {
			lb.queue = lb.queue[leaked:]
		}

		lb.lastLeak = lb.lastLeak.Add(time.Duration(leaked) * lb.rate)
	}

	if len(lb.queue) >= lb.capacity {
		return false
	}

	lb.queue = append(lb.queue, request)
	return true
}