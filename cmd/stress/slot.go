package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Slot struct {
	mu       sync.Mutex
	id       uint32
	manager  *Manager
	isReady  bool
	isClosed bool
	wg       sync.WaitGroup
}

func NewSlot(slotId uint32, mgr *Manager) *Slot {
	s := &Slot{
		id:      slotId,
		manager: mgr,
	}

	fmt.Printf("NewSlot %v\n", slotId)
	s.wg.Add(1)
	go s.run()
	return s
}

func (s *Slot) ID() uint32 {
	return s.id
}

func (s *Slot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return
	}

	s.isClosed = true

	s.wg.Wait()

	fmt.Printf("CloseSlot: %v\n", s.ID())
}

func (s *Slot) IsReady() bool {
	return s.isReady
}

func (s *Slot) run() {
	defer s.wg.Done()
	time.Sleep(15 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return
	}

	s.isReady = true

	if _, err := s.manager.manager.RequestAddSlot(context.Background(), s.ID()); err != nil {
		fmt.Printf("Unable to add slot %d: %v\n", s.ID(), err)
	} else {
		fmt.Printf("Added running slot %d\n", s.ID())
	}
}
