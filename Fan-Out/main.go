package main

import (
    "fmt"
    "sync"
)

func fanOut(in <-chan int, n int) []<-chan int {
    outs := make([]<-chan int, n)
    for i := 0; i < n; i++ {
        ch := make(chan int)
        outs[i] = ch
        go func() {
            defer close(ch)
            for v := range in {
                ch <- v * 2
            }
        }()
    }
    return outs
}

func generateInput() <-chan int {
    in := make(chan int)
    go func() {
        defer close(in)
        for i := 1; i <= 5; i++ {
            in <- i
        }
    }()
    return in
}

func main() {
    input := generateInput()
    workers := fanOut(input, 3)
    var wg sync.WaitGroup
    wg.Add(len(workers))
    for i, ch := range workers {
        go func(id int, c <-chan int) {
            defer wg.Done()
            for v := range c {
                fmt.Printf("Worker %d: %d\n", id, v)
            }
        }(i, ch)
    }
    wg.Wait()
}