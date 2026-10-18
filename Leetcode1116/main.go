
package main

import (
	"fmt"
	"sync"
)

func main(){
	RunUsingChannels()
	RunUsingMutexes()
}



// ---------------3 CHANNELS--------------------------------------------------------------
func RunUsingChannels() {
	n := 5

	zeroturn := make(chan struct{})
	oddturn := make(chan struct{})
	eventurn := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(3)


// No need to explicitly pass arguments because these variables
// are already available in the surrounding scope and aren't reassigned.
	go func() {
		defer wg.Done()
		printZero(n, zeroturn, oddturn, eventurn)
	}()

	go func() {
		defer wg.Done()
		printOdd(n, zeroturn, oddturn)
	}()

	go func() {
		defer wg.Done()
		printEven(n, zeroturn, eventurn)
	}()

	// Start only after the receiver goroutine exists.
	zeroturn <- struct{}{}

	wg.Wait()
	fmt.Println()
}

func printZero(n int, Zturn, Oturn, Eturn chan struct{}) {
	for i := 1; i <= n; i++ {
		<-Zturn
		fmt.Print(0)

		if i%2 == 0 {
			Eturn <- struct{}{}
		} else {
			Oturn <- struct{}{}
		}
	}
}

func printOdd(n int, Zturn, Oturn chan struct{}) {
	for i := 1; i <= n; i += 2 {
		<-Oturn
		fmt.Print(i)

		if i < n {
			Zturn <- struct{}{}
		}
	}
}

func printEven(n int, Zturn, Eturn chan struct{}) {
	for i := 2; i <= n; i += 2 {
		<-Eturn
		fmt.Print(i)

		if i < n {
			Zturn <- struct{}{}
		}
	}
}



// ------------------MUTEXES-----------------------------------------------------------
func RunUsingMutexes() {
	n := 5

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	turn := 0 // 0 = zero, 1 = odd, 2 = even

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		for i := 1; i <= n; i++ {
			mu.Lock()

			for turn != 0 {
				cond.Wait()
			}

			fmt.Print(0)

			if i%2 == 1 {
				turn = 1
			} else {
				turn = 2
			}

			cond.Broadcast()
			mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()

		for i := 1; i <= n; i += 2 {
			mu.Lock()

			for turn != 1 {
				cond.Wait()
			}

			fmt.Print(i)
			turn = 0

			cond.Broadcast()
			mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()

		for i := 2; i <= n; i += 2 {
			mu.Lock()

			for turn != 2 {
				cond.Wait()
			}

			fmt.Print(i)
			turn = 0

			cond.Broadcast()
			mu.Unlock()
		}
	}()

	wg.Wait()
	fmt.Println()
}
