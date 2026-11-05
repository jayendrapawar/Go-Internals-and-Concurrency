package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("---- Token Bucket ----")

	tb := NewTokenBucket(10, 5)
	for i := 0; i < 12; i++ {
		fmt.Println("Request:", i+1, "Allowed:", tb.Allow())
	}

	fmt.Println("\n---- Leaky Bucket ----")

	lb := NewLeakyBucket(5, 2)
	for i := 0; i < 7; i++ {
		fmt.Println("Request:", i+1, "Accepted:", lb.Add(i))
	}

	fmt.Println("\n---- Fixed Window ----")

	fw := NewFixedWindow(5, time.Second)
	for i := 0; i < 7; i++ {
		fmt.Println("Request:", i+1, "Allowed:", fw.Allow())
	}

	fmt.Println("\n---- Sliding Window ----")

	sw := NewSlidingWindow(5, time.Second)
	for i := 0; i < 7; i++ {
		fmt.Println("Request:", i+1, "Allowed:", sw.Allow())
	}
}