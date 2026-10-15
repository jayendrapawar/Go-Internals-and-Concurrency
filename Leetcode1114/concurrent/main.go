package main

import (
	"fmt"
	"sync"
)

func main() {
	nums := []int{0, 1, 2}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := range nums {
		go func(n int) {
			defer wg.Done()
			printNum(n)
		}(i)
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