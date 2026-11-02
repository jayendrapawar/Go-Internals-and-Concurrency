package main

import (
	"fmt"
	"sync"
)

// Approach 1: Coordinator — form maximum molecules and report leftovers.

func h2OCoordinator(s string) {
	hchan := make(chan struct{}, len(s))
	ochan := make(chan struct{}, len(s))

	var wg sync.WaitGroup

	// Launch independent atom producers.
	for _, ch := range s {
		switch ch {
		case 'H':
			wg.Add(1)
			go func() {
				defer wg.Done()
				hchan <- struct{}{}
			}()
		case 'O':
			wg.Add(1)
			go func() {
				defer wg.Done()
				ochan <- struct{}{}
			}()
		}
	}

	// Close channels after all producers finish.
	go func() {
		wg.Wait()
		close(hchan)
		close(ochan)
	}()

	h, o, molecules := 0, 0, 0

	// Collect atoms until both channels are drained.
	for hchan != nil || ochan != nil {
		select {
		case _, ok := <-hchan:
			if !ok {
				hchan = nil
				continue
			}
			h++
		case _, ok := <-ochan:
			if !ok {
				ochan = nil
				continue
			}
			o++
		}

		// Form molecules whenever enough atoms are available.
		for h >= 2 && o >= 1 {
			fmt.Println("HHO")
			h -= 2
			o--
			molecules++
		}
	}

	fmt.Printf("Molecules: %d, Unused H: %d, Unused O: %d\n",
		molecules, h, o)
}

// Approach 2: Semaphore + barrier — synchronize complete groups.

type H2OSem struct {
	hSlots  chan struct{}
	oSlots  chan struct{}
	arrived chan struct{}
	next    chan struct{}
}

func NewH2OSem(molecules int) *H2OSem {
	h := &H2OSem{
		hSlots:  make(chan struct{}, 2),
		oSlots:  make(chan struct{}, 1),
		arrived: make(chan struct{}),
		next:    make(chan struct{}, 3),
	}

	// Allow two hydrogen atoms and one oxygen atom.
	h.hSlots <- struct{}{}
	h.hSlots <- struct{}{}
	h.oSlots <- struct{}{}

	// Wait for each group to finish before releasing the next.
	go func() {
		for i := 0; i < molecules; i++ {
			for j := 0; j < 3; j++ {
				<-h.arrived
			}

			if i+1 < molecules {
				h.hSlots <- struct{}{}
				h.hSlots <- struct{}{}
				h.oSlots <- struct{}{}
			}

			for j := 0; j < 3; j++ {
				h.next <- struct{}{}
			}
		}
	}()

	return h
}

func (h *H2OSem) Hydrogen(release func()) {
	<-h.hSlots
	release()
	h.arrived <- struct{}{}
	<-h.next
}

func (h *H2OSem) Oxygen(release func()) {
	<-h.oSlots
	release()
	h.arrived <- struct{}{}
	<-h.next
}

func h2OSemaphore(s string) {
	hCount, oCount := 0, 0

	for _, ch := range s {
		switch ch {
		case 'H':
			hCount++
		case 'O':
			oCount++
		}
	}

	molecules := min(hCount/2, oCount)
	h := NewH2OSem(molecules)

	var wg sync.WaitGroup

	// Launch only atoms needed for complete molecules.
	for i := 0; i < 2*molecules; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Hydrogen(func() { fmt.Print("H") })
		}()
	}

	for i := 0; i < molecules; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Oxygen(func() { fmt.Print("O") })
		}()
	}

	wg.Wait()
	fmt.Println()
	fmt.Printf("Molecules: %d, Unused H: %d, Unused O: %d\n",
		molecules, hCount-2*molecules, oCount-molecules)
}

// Approach 3: Mutex + condition variable — reusable barrier.

type H2OCond struct {
	mu         sync.Mutex
	cond       *sync.Cond
	hCount     int
	oCount     int
	done       int
	generation int
}

func NewH2OCond() *H2OCond {
	h := &H2OCond{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *H2OCond) Hydrogen(release func()) {
	h.form(true, release)
}

func (h *H2OCond) Oxygen(release func()) {
	h.form(false, release)
}

func (h *H2OCond) form(isHydrogen bool, release func()) {
	h.mu.Lock()

	// Wait if this atom type has filled its slots for this group.
	for (isHydrogen && h.hCount == 2) ||
		(!isHydrogen && h.oCount == 1) {
		h.cond.Wait()
	}

	gen := h.generation

	// Register this atom in the current group.
	if isHydrogen {
		h.hCount++
	} else {
		h.oCount++
	}

	h.mu.Unlock()

	release()

	h.mu.Lock()
	h.done++

	// Reset the group and wake waiters after all three callbacks finish.
	if h.done == 3 {
		h.hCount = 0
		h.oCount = 0
		h.done = 0
		h.generation++
		h.cond.Broadcast()
	} else {
		for h.generation == gen {
			h.cond.Wait()
		}
	}

	h.mu.Unlock()
}

func h2OCondition(s string) {
	hCount, oCount := 0, 0

	for _, ch := range s {
		switch ch {
		case 'H':
			hCount++
		case 'O':
			oCount++
		}
	}

	// Start only complete groups.
	molecules := min(hCount/2, oCount)
	h := NewH2OCond()

	var wg sync.WaitGroup

	for i := 0; i < molecules; i++ {
		wg.Add(3)

		go func() {
			defer wg.Done()
			h.Hydrogen(func() { fmt.Print("H") })
		}()

		go func() {
			defer wg.Done()
			h.Hydrogen(func() { fmt.Print("H") })
		}()

		go func() {
			defer wg.Done()
			h.Oxygen(func() { fmt.Print("O") })
		}()
	}

	wg.Wait()
	fmt.Println()
	fmt.Printf("Molecules: %d, Unused H: %d, Unused O: %d\n",
		molecules, hCount-2*molecules, oCount-molecules)
}

func main() {
	input := "OOOHHHOOHHHHHH"

	fmt.Println("Approach 1: Coordinator")
	h2OCoordinator(input)

	fmt.Println("\nApproach 2: Semaphore + barrier")
	h2OSemaphore(input)

	fmt.Println("\nApproach 3: Mutex + condition variable")
	h2OCondition(input)
}
