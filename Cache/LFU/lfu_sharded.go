package main

import "sync"

// ShardedLFU reduces lock contention. Eviction is LFU within each shard,
// not globally LFU across the entire cache.
type ShardedLFU struct{ 
	shards []lfuShard 
}

type lfuShard struct {
	mu    sync.Mutex
	cache *LFU
}

func NewShardedLFU(totalCapacity, shardCount int) *ShardedLFU {
	if shardCount <= 0 {
		shardCount = 1
	}
	if totalCapacity < 0 {
		totalCapacity = 0
	}
	s := &ShardedLFU{shards: make([]lfuShard, shardCount)}
	base, rem := totalCapacity/shardCount, totalCapacity%shardCount
	for i := range s.shards {
		cap := base
		if i < rem {
			cap++
		}
		s.shards[i].cache = NewLFU(cap)
	}
	return s
}

func (s *ShardedLFU) shardFor(key int) *lfuShard {
	return &s.shards[uint(key)%uint(len(s.shards))]
}

func (s *ShardedLFU) Get(key int) (int, bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.cache.Get(key)
}

func (s *ShardedLFU) Put(key, value int) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.cache.Put(key, value)
}
