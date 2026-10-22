package main

import (
	"context"
	"fmt"
	"time"
)

// Key takeaway:
// Use a buffered channel (capacity 1) when a goroutine may send a result
// after the receiver has returned due to a context timeout.
// The buffered send won't block, preventing a goroutine leak.
//
// Unbuffered channel: ch := make(chan string)
// If no receiver exists, the sender blocks indefinitely.
//
// Buffered channel: ch := make(chan string, 1)
// The sender can send one result without a receiver.


func simulateWork(ctx context.Context) (string, error) {
	ch := make(chan string, 1) // Buffered channel to prevent goroutine leak

	go func() {
		// Simulate heavy lifting (DB query or API call)
		time.Sleep(2 * time.Second)
		ch <- "operation success"
	}()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel() // Always call cancel to release resources

	res, err := simulateWork(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %s\n", res)
}
