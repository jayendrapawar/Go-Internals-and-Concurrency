package main

import "sync"

type MutexLFU struct {
	mu    sync.Mutex
	cache *LFU
}

func NewMutexLFU(capacity int) *MutexLFU {
	return &MutexLFU{
		cache: NewLFU(capacity),
	}
}

func (c *MutexLFU) Get(key int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Get(key)
}

func (c *MutexLFU) Put(key, value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Put(key, value)
}
