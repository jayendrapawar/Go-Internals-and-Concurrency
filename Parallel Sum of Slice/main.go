package main

import (
	"fmt"
	"sync"
)

// The Problem Statement
// Given a large slice of integers, write a function that
// splits the slice into smaller chunks, sums each chunk
// in a separate goroutine using sync.WaitGroup for
// synchronization, and returns the total sum of the slice.

// Approaches
// 1. Using only sync.WaitGroup (IMP)
// 2. Using only Channels
// 3. Using only Mutexes


func main(){
	n := []int{1,2,3,4}
	chunks := 2

	RunUsingWaitGroup(n, chunks)
	RunUsingChannels(n, chunks)
	RunUsingMutexes(n, chunks)
}

//--------------WAITGROUP-----------------------------------------------------------
type partitionIdx struct {
	chunk []int
}

func RunUsingWaitGroup(n []int, numWorkers int) {
	if len(n) == 0 || numWorkers <= 0 {
		return
	}

	if numWorkers > len(n) {
		numWorkers = len(n)
	}

	chunkSize := (len(n) + numWorkers - 1) / numWorkers
	ans := make([]int, numWorkers)

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			start := i * chunkSize
			end := start + chunkSize

			if end > len(n) {
				end = len(n)
			}

			part := partitionIdx{
				chunk: n[start:end],
			}

			ans[i] = AddSum(part)
		}(i)
	}

	wg.Wait()

	total := 0
	for _, sum := range ans {
		total += sum
	}

	fmt.Print(total)
}

func AddSum(pidx partitionIdx) int {
	sum := 0
	for _, v := range pidx.chunk {
		sum += v
	}
	return sum
}

//---------------CHANNELS-----------------------------------------------------------
func RunUsingChannels(n []int, chunks int){

}

//---------------MUTEXES-----------------------------------------------------------
func RunUsingMutexes(n []int, chunks int){

}
