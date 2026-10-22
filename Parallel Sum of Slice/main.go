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


func main(){
	n := []int{1,2,3,4,5,6}
	chunks := 2

	OrderedSumOfSlice(n, chunks)
	UnOrderedSumOfSlice(n, chunks) // chunks == numWorkers
}

//---------------Ordered-----------------------------------------------------------
type partitionIdx struct {
	chunk []int
}

func OrderedSumOfSlice(n []int, numWorkers int) {
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

//---------------UnOrdered-----------------------------------------------------------
func UnOrderedSumOfSlice(n []int, chunks int){
	if len(n) == 0 || chunks <= 0 {
		return
	}
	
	cap := 0 
	if len(n)%2 == 0{
		cap = len(n)/2
	} else {
		cap = len(n)/2 + 1
	}

	sum := make([]int,0,cap) 
	var wg sync.WaitGroup

	for i:= 0; i<len(n); i += 2{
		wg.Add(1)
		go func(p int){
			defer wg.Done()
			if p+1 < len(n) {
				ans := n[p] + n[p+1]
				sum = append(sum, ans)
			} else {
			sum = append(sum, n[p])
			}
		}(i)
	}

	wg.Wait()

	ans := 0
	for _, n := range sum {
		ans += n
	}

	fmt.Println(ans)
}