package main

import (
	"fmt"
)

func main() {
	fmt.Println("1. Normal LRU")
	normal := NewNormalLRU(2)
	normal.Put(1, 10)
	normal.Put(2, 20)
	normal.Get(1)
	normal.Put(3, 30) // evicts key 2
	value, ok := normal.Get(2)
	fmt.Println("key 2:", value, ok)

	fmt.Println("\n2. Thread-safe LRU with Mutex")
	mutexCache := NewMutexLRU(2)
	mutexCache.Put(1, 100)
	value, ok = mutexCache.Get(1)
	fmt.Println("key 1:", value, ok)

	fmt.Println("\n3. Thread-safe LRU with RWMutex")
	rwCache := NewRWMutexLRU(2)
	rwCache.Put(1, 100)
	value, ok = rwCache.Get(1)
	fmt.Println("Get key 1:", value, ok)
	value, ok = rwCache.Peek(1)
	fmt.Println("Peek key 1:", value, ok)

	fmt.Println("\n4. Sharded concurrent LRU")
	sharded := NewShardedLRU(100, 8)
	for i := 0; i < 10; i++ {
		sharded.Put(i, i*10)
	}
	value, ok = sharded.Get(5)
	fmt.Println("key 5:", value, ok)
}
