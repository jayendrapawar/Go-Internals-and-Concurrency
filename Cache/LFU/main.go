package main

import (
	"fmt"
)

func main() {
	fmt.Println("1. Normal LFU")
	a := NewLFU(2)
	a.Put(1, 10)
	a.Put(2, 20)
	a.Get(1)
	a.Put(3, 30)
	evictedValue, evictedOK := a.Get(2)
	fmt.Println("key 2 (evicted):", evictedValue, evictedOK)

	fmt.Println("\n2. Mutex LFU")
	b := NewMutexLFU(2)
	b.Put(1, 100)
	value, ok := b.Get(1)
	fmt.Println("key 1:", value, ok)

	fmt.Println("\n3. RWMutex LFU")
	c := NewRWMutexLFU(2)
	c.Put(1, 100)
	value, ok = c.Get(1)
	fmt.Println("Get:", value, ok)
	peekValue, peekOK := c.Peek(1)
	fmt.Println("Peek:", peekValue, peekOK)

	fmt.Println("\n6. Sharded LFU")
	f := NewShardedLFU(100, 8)
	for i := 0; i < 10; i++ {
		f.Put(i, i*10)
	}
	value, ok = f.Get(5)
	fmt.Println("key 5:", value, ok)
}
