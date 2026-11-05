package main

import (
	"sync"
	"time"
)

type FixedWindow struct {
	mu        sync.Mutex
	count     int
	limit     int
	window    time.Duration
	startTime time.Time
}

func NewFixedWindow(limit int, window time.Duration) *FixedWindow {
	return &FixedWindow{
		count:     0,
		limit:     limit,
		window:    window,
		startTime: time.Now(),
	}
}

func (fw *FixedWindow) Allow() bool {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	now := time.Now()

	if now.Sub(fw.startTime) >= fw.window {
		fw.count = 0
		fw.startTime = now
	}

	if fw.count >= fw.limit {
		return false
	}

	fw.count++
	return true
}