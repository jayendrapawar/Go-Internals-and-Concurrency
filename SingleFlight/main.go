// Implement a SingleFlight class/struct in Go that ensures
// duplicate concurrent calls for the same key execute
// an underlying function only once, and all callers
// receive the shared return value.

// purpose -  mechanism that prevents duplicate concurrent work for the same key
// example -  if N concurrent callers request the same missing resource, only 1
//            caller executes the expensive downstream task
//            (like a database query or external API call), while the other N-1
//            callers wait and share the exact same result

package main

import (
	"fmt"
	"sync"
	"time"
)

type Call struct {
	done   chan struct{}
	result interface{}
	err    error
}

type Singleflight struct {
	mu    sync.Mutex
	calls map[string]*Call
}

func NewSingleflight() *Singleflight {
	return &Singleflight{
		calls: make(map[string]*Call),
	}
}

func (sf *Singleflight) FetchUser(key string) (interface{}, error) {
	sf.mu.Lock()

	if call, exists := sf.calls[key]; exists {
		sf.mu.Unlock()
		<-call.done
		return call.result, call.err
	}

	call := &Call{
		done: make(chan struct{}),
	}
	sf.calls[key] = call
	sf.mu.Unlock()

	fmt.Println("Fetching from DB")
	time.Sleep(2 * time.Second)

	call.result = "Jayendra"

	sf.mu.Lock()
	delete(sf.calls, key)
	close(call.done)
	sf.mu.Unlock()

	return call.result, nil
}

func main() {
	sf := NewSingleflight()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			result, err := sf.FetchUser("user:123")
			fmt.Println(result, err)
		}()
	}

	wg.Wait()
}
