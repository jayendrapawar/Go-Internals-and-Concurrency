// You are building a data ingestion service that needs to fetch data from a large list of URLs.
// Implement a function ProcessURLs that processes a slice of input
// strings concurrently, limiting the maximum number of simultaneous (semaphore)
// in-flight tasks to maxConcurrency.

// // 1. Bounded Parallelism WITH Bounded Blocking (Waits for a free slot)
// // 2. Bounded Parallelism WITHOUT Blocking (Drops/rejects if full)

package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	input := []string{
		"https://example.com",
		"https://example.org",
		"https://example.net",
		"https://example.edu",
	}

	ProcessURLsWithBlocking(input, 2)
	ProcessURLsWithoutBlocking(input, 2)
}

func ProcessURLsWithBlocking(urls []string, maxConcurrency int) {
    if maxConcurrency <= 0 {
        maxConcurrency = 1
    }

    sem := make(chan struct{}, maxConcurrency)
    var wg sync.WaitGroup

    for _, v := range urls {
        sem <- struct{}{} // Acquire slot
        wg.Add(1)

        go func(targetURL string) {
            defer wg.Done()
            defer func() { <-sem }() // Release slot

            fmt.Println("Accepted",targetURL, time.Now())
        }(v)
    }

    wg.Wait()
}

func ProcessURLsWithoutBlocking(urls []string, maxConcurrency int){
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	
	for _, v := range urls {
		
		select {
			case sem <-struct{}{}: // Acquire slot if available
				wg.Add(1)

				go func(targetURL string){
					defer wg.Done()
					fmt.Println("Accepted:", targetURL)
				}(v)

			default:
				fmt.Println("Rejected:", v)
		}
	}

	wg.Wait()
}