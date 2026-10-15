package main

import (
	"fmt"
	"sync"
)

// channels
// 1. Single goroutine - controlled via Single buffered channel
// 2. Multiple goroutine - controlled via Single buffered channel

func main() {
	nums := []int{0, 1, 2}
	SingleGoroutine(nums)
	fmt.Println(" ")
	MultipleGoroutineUsingChannels(nums)
	fmt.Println(" ")
	MultipleGoroutineUsingMutex(nums)
	fmt.Println(" ")
	MultipleGoroutineDoesNotGuaranty(nums)

}

func SingleGoroutine(nums []int) {

	turn := make(chan int, len(nums))

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := range turn {
			printNum(i)
		}
	}()

	for i := range nums {
		turn <- i
	}

	close(turn)
	wg.Wait()
}

// Key Learnings 
// channel is only send/recive communication
// channel always hold value in FIFO order 
// goroutines are started concurrently, Go does not guarantee which one receives first.

func MultipleGoroutineDoesNotGuaranty(nums []int) {

	// Creates a slice that can hold 3 channels
	turns := make([]chan struct{}, len(nums))
	// turns
	// 	├── turns[0] → nil 
	// 	├── turns[1] → nil 
	// 	└── turns[2] → nil 

	// assign channels 
	for i := range  nums {
		turns[i] = make(chan struct{})
	}
	// turns
	// 	├── turns[0] → channel 
	// 	├── turns[1] → channel 
	// 	└── turns[2] → channel 


	var wg sync.WaitGroup
	wg.Add(len(nums))

	for _, i := range nums {
		go func(n int, turn chan struct{}) {
			defer wg.Done()
			<- turn // Wait for permission to print
			printNum(n)
		}(i, turns[i])
	}

	// channel is only send/recive communication
	//  // Release goroutines in sequence
	for i:= range nums {
		close(turns[i])
	}

	wg.Wait()
}

func MultipleGoroutineUsingChannels(nums []int) {
    turns := make([]chan int, len(nums))
	// done channel needed - channels are independent
    done := make(chan struct{})

    var wg sync.WaitGroup

    for i := range nums {
        turns[i] = make(chan int)
    }

    for _, i := range nums {
        wg.Add(1)

        go func(n int, turn chan int) {
            defer wg.Done()

            val := <-turn
            if val == n {
                printNum(n)
            }

            done <- struct{}{} // Send signal goroutine to completed work
        }(i, turns[i])
    }

    for i := range nums {
        turns[i] <- i
        <-done // Wait for this goroutine to finish printing.
    }

    wg.Wait()
}

func MultipleGoroutineUsingMutex(nums []int) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	next := 0

	for _, n := range nums {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			mu.Lock()
			defer mu.Unlock()

			// Wait until it's this number's turn.
			for n != next {
				cond.Wait() // goroutines sleep until it wakesup
			}

			fmt.Println(n)

			// Allow the next number to proceed.
			next++
			cond.Broadcast() // wakeup all pending goroutines
		}(n)
	}

	wg.Wait()
}

func printNum(i int) {
	switch i {
	case 0:
		fmt.Print("Zero")
	case 1:
		fmt.Print("One")
	case 2:
		fmt.Print("Two")
	}
}
