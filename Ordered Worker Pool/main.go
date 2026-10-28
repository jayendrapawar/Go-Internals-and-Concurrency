package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	Index int
	Value int
}

type Result struct {
	Index int
	Value int
}

func process(value int) int {
	// Simulate different processing times.
	time.Sleep(time.Duration(6-value) * 100 * time.Millisecond)

	return value * 10
}

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		output := process(job.Value)

		results <- Result{
			Index: job.Index,
			Value: output,
		}
	}
}

func workerPool(inputs []int, numWorkers int) []int {
	if len(inputs) == 0 {
		return []int{}
	}

	if numWorkers <= 0 {
		panic("numWorkers must be greater than zero")
	}

	jobs := make(chan Job)
	resultsCh := make(chan Result)

	var wg sync.WaitGroup

	// Start the workers.
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(jobs, resultsCh, &wg)
	}

	// Send jobs to the workers.
	go func() {
		defer close(jobs)

		for i, value := range inputs {
			jobs <- Job{
				Index: i,
				Value: value,
			}
		}
	}()

	// Close resultsCh after every worker finishes.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results in their original positions.
	output := make([]int, len(inputs))

	for result := range resultsCh {
		output[result.Index] = result.Value
	}

	return output
}

func main() {
	inputs := []int{1, 2, 3, 4, 5}

	output := workerPool(inputs, 3)

	fmt.Println("Input: ", inputs)
	fmt.Println("Output:", output)
}