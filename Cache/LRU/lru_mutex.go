package main

import "sync"

// MutexLRU protects every cache operation with one exclusive mutex.
type MutexLRU struct {
	mu    sync.Mutex
	cache *NormalLRU
}

func NewMutexLRU(capacity int) *MutexLRU {
	return &MutexLRU{cache: NewNormalLRU(capacity)}
}

func (l *MutexLRU) Get(key int) (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cache.Get(key)
}

func (l *MutexLRU) Put(key, value int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cache.Put(key, value)
}
