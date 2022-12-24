package fixtures

import (
	"sync"
	"time"
)

var (
	SLOT_STATE_STARTED = "start"
	SLOT_STATE_READY   = "ready"
	SLOT_STATE_STOPPED = "stopped"
)

type NotifySlotStateChangeFunc func(slot Slot, state string)

type Slot interface {
	ID() uint32
	IsReady() bool
	Close()
}

type slot struct {
	Slot

	id                uint32
	startDuration     time.Duration
	mu                sync.Mutex
	wg                sync.WaitGroup
	stopSignalChannel chan struct{}
	isReady           bool
	isRunning         bool
	notifyStateChange NotifySlotStateChangeFunc
}

func NewSlot(id uint32, startDuration time.Duration, notifyStateChange NotifySlotStateChangeFunc) Slot {
	s := &slot{
		id:                id,
		startDuration:     startDuration,
		stopSignalChannel: make(chan struct{}),
		isRunning:         true,
		notifyStateChange: notifyStateChange,
	}

	s.wg.Add(1)
	go func(s *slot) {
		if s.notifyStateChange != nil {
			s.notifyStateChange(s, SLOT_STATE_STARTED)
		}

		// Startup
		startTicker := time.NewTicker(s.startDuration)
		select {
		case <-startTicker.C:
			startTicker.Stop()
			// Started

		case <-s.stopSignalChannel:
			s.wg.Done()
			return
		}

		// Run
		s.isReady = true

		if s.notifyStateChange != nil {
			s.notifyStateChange(s, SLOT_STATE_READY)
		}

		// Wait until closed
		<-s.stopSignalChannel
		s.wg.Done()

		if s.notifyStateChange != nil {
			s.notifyStateChange(s, SLOT_STATE_STOPPED)
		}
	}(s)

	return s
}

func (s *slot) ID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.id
}

func (s *slot) IsReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.isReady
}

func (s *slot) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		s.stopSignalChannel <- struct{}{}
		s.wg.Wait()

		s.isRunning = false
		s.isReady = false
	}
}
