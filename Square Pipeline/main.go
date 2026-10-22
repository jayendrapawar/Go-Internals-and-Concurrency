package main

import (
	"context"
	"fmt"
)

// Problem Statement
// build a high-throughput data processing pipeline that takes a stream of integers,
// calculates the square of each number, and outputs the results

// 1. Generator: Produces a stream of integers and sends them down the pipeline.
// 2. Squarer: calculates their squares, and sends the results to the next stage.
// 3. Consumer: Receives the squared numbers and prints them or aggregates them

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numsIn := generator(ctx, 2, 3, 4, 5)
	squaresOut := squarer(ctx, numsIn)

	// Stage 3: Consumer
	for result := range squaresOut {
		fmt.Println(result)
	}
}

// Stage 1: Generator
// Converts a slice of ints to a channel. Closes the channel when done.
func generator(ctx context.Context, nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		// IMP KEYNOTE
		// Closing a channel does not delete its buffered values.
		// The receiver can still consume all values that were sent
		// before the channel was closed.
		for _, n := range nums {
			select {
			case <-ctx.Done():
				return // Exit early if context is cancelled (prevents leaks)
			case out <- n:
			}
		}
	}()
	return out
}

// Stage 2: Squarer
// Receives from input channel, squares the number, and sends to output channel.
func squarer(ctx context.Context, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			select {
			case <-ctx.Done():
				return // Exit early if context is cancelled
			case out <- n * n:
			}
		}
	}()
	return out
}
