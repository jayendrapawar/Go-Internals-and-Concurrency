package main

import (
	"fmt"
	"sync"
)

// Problem Statement
// build a single output channel that multiplexes (combines) messages from
// N different input channels concurrently without data loss, deadlocks,
// or resource leaks

// Input: N separate incoming channels producing values asynchronously
// Output: A single merged output channel (chan T) that receives
//         all values produced across the N inputs


func Funnel(sources ...<-chan int) chan int {
	dest := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(len(sources))

	// Start a goroutine for each input channel
	for _, ch := range sources {
		go func(ch <-chan int) {
			defer wg.Done()
			for val := range ch {
				dest <- val
			}
		}(ch)
	}

	// Close the destination channel when all input channels are processed
	go func() {
		wg.Wait()
		close(dest)
	}()

	return dest
}

func main() {
	sources := make([]<-chan int, 0)

	// Create three source channels
	for i := 0; i < 3; i++ {
		ch := make(chan int)
		sources = append(sources, ch)
		go func(ch chan int) {
			defer close(ch)
			for j := 1; j <= 5; j++ {
				ch <- j
			}
		}(ch)
	}

	// Merge channels using Funnel
	dest := Funnel(sources...)
	for val := range dest {
		fmt.Printf("Received: %d\n", val)
	}
}
