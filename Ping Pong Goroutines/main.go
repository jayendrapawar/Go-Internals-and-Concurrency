package main

import (
	"fmt"
	"sync"
)


func main() {
	SinglePingPong()
	ContinuousPingPong()
}

func SinglePingPong(){
	pingCh := make(chan string)
	pongCh := make(chan string)


	var wg sync.WaitGroup
	wg.Add(2)

	//Ping
	go func() {
		defer wg.Done()
		pingCh <- "Ping"
		PongMsg := <-pongCh
		fmt.Println(PongMsg)
	}()

	//Pong
	go func() {
		defer wg.Done()
		PingMsg := <-pingCh
		fmt.Println(PingMsg)
		pongCh <- "Pong"

	}()

	wg.Wait()
}

func ContinuousPingPong(){
	pingCh := make(chan string)
	pongCh := make(chan string)

	var wg sync.WaitGroup
	wg.Add(2)

	// Ping goroutine
	go func() {
		defer wg.Done()

		// recursive call
		var ping func()
		ping = func() {
			pingCh <- "Ping"
			fmt.Println(<-pongCh)
			ping()
		}

		ping()
	}()

	// Pong goroutine
	go func() {
		defer wg.Done()

		// recursive call
		var pong func()
		pong = func() {
			fmt.Println(<-pingCh)
			pongCh <- "Pong"
			pong()
		}

		pong()
	}()

	wg.Wait()
}