package main

import "sync"

// Get takes Lock because it increments frequency and moves the node.
// Peek is a read-only lookup and can use RLock.
type RWMutexLFU struct {
	mu    sync.RWMutex
	cache *LFU
}

func NewRWMutexLFU(capacity int) *RWMutexLFU {
	return &RWMutexLFU{
		cache: NewLFU(capacity),
	}
}

func (c *RWMutexLFU) Get(key int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Get(key)
}

func (c *RWMutexLFU) Peek(key int) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.cache.items[key]
	if !ok {
		return 0, false
	}
	return n.value, true
}

func (c *RWMutexLFU) Put(key, value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Put(key, value)
}
