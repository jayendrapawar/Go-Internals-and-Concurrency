package main

import (
	"sync"
	"time"
)

type SlidingWindow struct {
	mu        sync.Mutex
	requests  []time.Time
	limit     int
	window    time.Duration
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		requests: make([]time.Time, 0),
		limit:    limit,
		window:   window,
	}
}

func (sw *SlidingWindow) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.window)

	i := 0
	for i < len(sw.requests) && !sw.requests[i].After(cutoff) {
		i++
	}

	sw.requests = sw.requests[i:]

	if len(sw.requests) >= sw.limit {
		return false
	}

	sw.requests = append(sw.requests, now)
	return true
}