package main

import "sync"

// RWMutexLRU uses an RWMutex. Get needs Lock (not RLock), because it
// updates recency in the linked list. Peek is read-only and can use RLock.
type RWMutexLRU struct {
	mu    sync.RWMutex
	cache *NormalLRU
}

func NewRWMutexLRU(capacity int) *RWMutexLRU {
	return &RWMutexLRU{cache: NewNormalLRU(capacity)}
}

func (l *RWMutexLRU) Get(key int) (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cache.Get(key)
}

func (l *RWMutexLRU) Peek(key int) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	node, ok := l.cache.items[key]
	if !ok {
		return 0, false
	}
	return node.value, true
}

func (l *RWMutexLRU) Put(key, value int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cache.Put(key, value)
}
