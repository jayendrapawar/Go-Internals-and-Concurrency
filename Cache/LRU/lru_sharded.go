package main

import "sync"

// ShardedLRU splits capacity across independent LRU shards.
// This can reduce lock contention, but eviction is per-shard rather than
// globally least-recently-used.
type ShardedLRU struct {
	shards []shard
}

type shard struct {
	mu    sync.Mutex
	cache *NormalLRU
}

func NewShardedLRU(totalCapacity, shardCount int) *ShardedLRU {
	if shardCount <= 0 {
		shardCount = 1
	}
	if totalCapacity < 0 {
		totalCapacity = 0
	}
	s := &ShardedLRU{shards: make([]shard, shardCount)}
	base, remainder := totalCapacity/shardCount, totalCapacity%shardCount
	for i := range s.shards {
		capacity := base
		if i < remainder {
			capacity++
		}
		s.shards[i].cache = NewNormalLRU(capacity)
	}
	return s
}

func (s *ShardedLRU) shardFor(key int) *shard {
	// Convert to uint so negative keys are handled consistently.
	index := uint(key) % uint(len(s.shards))
	return &s.shards[index]
}

func (s *ShardedLRU) Get(key int) (int, bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.cache.Get(key)
}

func (s *ShardedLRU) Put(key, value int) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.cache.Put(key, value)
}
