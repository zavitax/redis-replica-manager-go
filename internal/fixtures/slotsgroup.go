package fixtures

import (
	"math/rand"
	"sync"
	"time"
)

type SlotsGroup interface {
	AddSlot(slotId uint32) (Slot, bool)
	RemoveSlot(slotId uint32) bool
	IsSlotReady(slotId uint32) bool
	IsSlotRunning(slotId uint32) bool
	Close()
}

type group struct {
	SlotsGroup

	mu                    sync.Mutex
	slots                 map[uint32]Slot
	startupDuration       time.Duration
	startupDurationJitter time.Duration
	notifyStateChange     NotifySlotStateChangeFunc
}

func NewSlotsGroup(startupDuration, startupDurationJitter time.Duration, notifyStateChange NotifySlotStateChangeFunc) SlotsGroup {
	g := &group{
		slots:                 make(map[uint32]Slot, 0),
		startupDuration:       startupDuration,
		startupDurationJitter: startupDurationJitter,
		notifyStateChange:     notifyStateChange,
	}

	return g
}

func (g *group) AddSlot(slotId uint32) (Slot, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.slots[slotId] != nil {
		return g.slots[slotId], false
	}

	durSec := int(g.startupDuration.Seconds())
	jitSec := int(g.startupDurationJitter.Seconds())

	startupDuration := time.Second * time.Duration(durSec+rand.Intn(int(jitSec)))

	g.slots[slotId] = NewSlot(slotId, startupDuration, g.notifyStateChange)

	return g.slots[slotId], true
}

func (g *group) RemoveSlot(slotId uint32) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.slots[slotId] == nil {
		return false
	}

	g.slots[slotId].Close()
	delete(g.slots, slotId)

	return true
}

func (g *group) IsSlotReady(slotId uint32) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.slots[slotId] == nil {
		return false
	}

	return g.slots[slotId].IsReady()
}

func (g *group) IsSlotRunning(slotId uint32) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.slots[slotId] == nil {
		return false
	}

	return true
}

func (g *group) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, slot := range g.slots {
		slot.Close()
	}

	g.slots = make(map[uint32]Slot)
}
