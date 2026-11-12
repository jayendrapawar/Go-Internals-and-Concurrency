package main

import (
	"fmt"
	"sync"
)

func main() {
	broker := NewBroker()

	id, ch := broker.Subscribe()

	var wg sync.WaitGroup
	wg.Add(1)

	// Subscriber goroutine
	go func() {
		defer wg.Done()

		for msg := range ch {
			fmt.Println("Received:", msg)
		}

		fmt.Println("Subscriber stopped")
	}()

	// Publish messages
	broker.Publish("Hello")
	broker.Publish("World")

	// Unsubscribe
	broker.Unsubscribe(id)

	// Wait for subscriber goroutine to exit
	wg.Wait()
}