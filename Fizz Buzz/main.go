package main

import (
	"fmt"
)

// `number` as a coordinator

type FizzBuzz struct {
	fizzSignal     chan struct{}
	buzzSignal     chan struct{}
	fizzbuzzSignal chan struct{}
	numSignal      chan struct{}
	done           chan struct{}
	n              int
}

func NewFizzBuzz(n int) *FizzBuzz {
	fb := &FizzBuzz{
		fizzSignal:     make(chan struct{}, 1),
		buzzSignal:     make(chan struct{}, 1),
		fizzbuzzSignal: make(chan struct{}, 1),
		numSignal:      make(chan struct{}, 1),
		done:           make(chan struct{}),
		n:              n,
	}
	fb.numSignal <- struct{}{}
	return fb
}

func (fb *FizzBuzz) fizz() {
	for _ = range fb.fizzSignal {
		fmt.Println("fizz")
		fb.numSignal <- struct{}{}
	}
}

func (fb *FizzBuzz) buzz() {
	for _ = range fb.buzzSignal {
		fmt.Println("buzz")
		fb.numSignal <- struct{}{}
	}
}

func (fb *FizzBuzz) fizzbuzz() {
	for _ = range fb.fizzbuzzSignal {
		fmt.Println("fizzbuzz")
		fb.numSignal <- struct{}{}
	}
}

func (fb *FizzBuzz) number() {
	for i := 1; i <= fb.n+1; i++ {
		<-fb.numSignal
		switch {
		case i == fb.n+1:
			fb.done <- struct{}{}
		case i%3 == 0 && i%5 == 0:
			fb.fizzbuzzSignal <- struct{}{}
		case i%3 == 0:
			fb.fizzSignal <- struct{}{}
		case i%5 == 0:
			fb.buzzSignal <- struct{}{}
		default:
			fmt.Println(i)
			go func() {
				fb.numSignal <- struct{}{}
			}()
		}
	}
}

func main() {
	fb := NewFizzBuzz(20)

	go func() {
		fb.fizz()
	}()

	go func() {
		fb.buzz()
	}()

	go func() {
		fb.fizzbuzz()
	}()

	go func() {
		fb.number()
	}()

	<-fb.done
}