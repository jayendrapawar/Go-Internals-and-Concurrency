package main

import (
	"fmt"
	"sync"
)

func main() {
	k := 3
	
	PrintFooBarUnorderedLoop(k)
	fmt.Println(" ")
	PrintFooBarOrderedLoop(k)
	fmt.Println(" ")
	PrintFooBaroOrderedLoopUsingLoop(k)

}

func PrintFooBarOrderedLoop(k int){

	footurn := make(chan struct{})
	barturn := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func(){
		defer wg.Done()

		for i:=0; i<k; i++ {
			<- footurn
			PrintFoo()
			// send signal to Bar 
			barturn <- struct{}{}
		}
	}()

	go func(){
		defer wg.Done()

		for i:=0; i<k; i++ {
			<- barturn
			PrintBar()

			// Send signal to foo 
			if i < k-1 {
				footurn <- struct{}{}
			}
		}
	}()

	// Starting signal to foo
	footurn <- struct{}{}

	wg.Wait()

}

func PrintFooBaroOrderedLoopUsingLoop(k int){
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)

	fooTurn := true

	var wg sync.WaitGroup
	wg.Add(2)

	// Foo goroutine
	go func() {
		defer wg.Done()

		for i := 0; i < k; i++ {
			mu.Lock()

			for !fooTurn {
				cond.Wait()
			}

			fmt.Print("foo")
			fooTurn = false

			cond.Signal()
			mu.Unlock()
		}
	}()

	// Bar goroutine
	go func() {
		defer wg.Done()

		for i := 0; i < k; i++ {
			mu.Lock()

			for fooTurn {
				cond.Wait()
			}

			fmt.Print("Bar")
			fooTurn = true

			cond.Signal()
			mu.Unlock()
		}
	}()

	wg.Wait()
}

func PrintFooBarUnorderedLoop(k int){
	FooTurn := make(chan struct{})
	BarTurn := make(chan struct{})


	var wg sync.WaitGroup
	for i:=0; i<k; i++ {
		// fmt.Println(i) - verify UnorderedLoop
		wg.Add(2)
		go func(){
			defer wg.Done()
			<- FooTurn
			PrintFoo()
			
			// Signal Bar to execute.
			BarTurn <- struct{}{}
		}()
		go func(){
			defer wg.Done()
			<- BarTurn 
			PrintBar()
		}()

		// Start Foo's turn.
		FooTurn <- struct{}{}
		
	}
	wg.Wait()
}

func PrintFoo() {
	fmt.Printf("foo")
}

func PrintBar(){
	fmt.Printf("Bar")
}