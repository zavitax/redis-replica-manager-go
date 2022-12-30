package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	redisreplicamanager "github.com/zavitax/redis-replica-manager-go"
)

type Config struct {
	redisOpts         *redis.Options
	MinimumSitesCount int
	SlotReplicaCount  int
}

type Manager struct {
	SiteID  string
	slots   map[uint32]*Slot
	slotsMx sync.RWMutex

	client   redisreplicamanager.ReplicaManagerClient
	balancer redisreplicamanager.ReplicaBalancer
	manager  redisreplicamanager.LocalSiteManager

	slotShouldBe map[uint32]bool
}

func NewManager(ctx context.Context, cfg Config) *Manager {
	m := &Manager{
		SiteID:       hex.EncodeToString(Must(RandomBytes(16))),
		slots:        make(map[uint32]*Slot),
		slotShouldBe: make(map[uint32]bool),
	}

	m.client = Must(redisreplicamanager.NewRedisReplicaManagerClient(ctx, &redisreplicamanager.ReplicaManagerOptions{
		RedisOptions:   cfg.redisOpts,
		RedisKeyPrefix: "{test-replica-manager}",
		SiteID:         m.SiteID,
		SiteTimeout:    time.Minute,
	}))

	m.balancer = Must(redisreplicamanager.NewReplicaBalancer(ctx, &redisreplicamanager.ReplicaBalancerOptions{
		TotalSlotsCount:   200,
		MinimumSitesCount: cfg.MinimumSitesCount,
		SlotReplicaCount:  cfg.SlotReplicaCount,
	}))

	m.manager = Must(redisreplicamanager.NewLocalSiteManager(ctx, &redisreplicamanager.ClusterNodeManagerOptions{
		ReplicaManagerClient: m.client,
		ReplicaBalancer:      m.balancer,
		RefreshInterval:      15 * time.Second,

		NotifyMissingSlotsHandler:        m.onNotifyMissingSlots,
		NotifyRedundantSlotsHandler:      m.onNotifyRedundantSlots,
		NotifyPrimarySlotsChangedHandler: m.onNotifyPrimarySlotsChanged,
	}))
	return m
}

func (m *Manager) GetDiff() (missing, redundant []uint32) {
	m.slotsMx.RLock()
	defer m.slotsMx.RUnlock()

	redis := *Must(m.manager.GetSlotIdentifiers(context.Background()))
	for _, slot := range redis {
		if _, ok := m.slots[slot]; !ok {
			missing = append(missing, slot)
		}
	}

	for slot := range m.slots {
		if !Contains(redis, slot) {
			redundant = append(redundant, slot)
		}
	}
	return missing, redundant
}

func (m *Manager) Slots() []uint32 {
	m.slotsMx.RLock()
	defer m.slotsMx.RUnlock()
	slots := make([]uint32, 0, len(m.slots))
	for slot := range m.slots {
		slots = append(slots, slot)
	}
	return slots
}

func (m *Manager) SlotsShouldBe() map[uint32]bool {
	m.slotsMx.RLock()
	defer m.slotsMx.RUnlock()
	return m.slotShouldBe
}

func (m *Manager) Close() error {
	return m.manager.Close()
}

func (m *Manager) onNotifyMissingSlots(ctx context.Context, manager redisreplicamanager.LocalSiteManager, slots *[]uint32) error {
	m.slotsMx.Lock()
	defer m.slotsMx.Unlock()

	if m.manager == nil {
		return nil
	}

	sort.Slice(*slots, func(i, j int) bool { return (*slots)[i] < (*slots)[j] })

	fmt.Printf("Missing slots: %v: %v\n", len(*slots), slots)

	for _, slotId := range *slots {
		m.slotShouldBe[slotId] = true

		if m.slots[slotId] == nil {
			m.slots[slotId] = NewSlot(slotId, m)
		} else {
			/*if m.slots[slotId].IsReady() {
				if _, err := manager.RequestAddSlot(context.Background(), slotId); err != nil {
					fmt.Printf("Unable to add slot %d\n", slotId)
				} else {
					fmt.Printf("Added ready slot %d\n", slotId)
				}
			}*/
		}
	}
	return nil
}

func (m *Manager) onNotifyRedundantSlots(ctx context.Context, manager redisreplicamanager.LocalSiteManager, slots *[]uint32) error {
	m.slotsMx.Lock()
	defer m.slotsMx.Unlock()

	sort.Slice(*slots, func(i, j int) bool { return (*slots)[i] < (*slots)[j] })

	fmt.Printf("Redundant slots: %v: %v\n", len(*slots), slots)

	for _, slotId := range *slots {
		m.slotShouldBe[slotId] = false

		ok, err := manager.RequestRemoveSlot(ctx, slotId)
		if !ok || err != nil {
			fmt.Printf("Unable to remove slot %d: %v\n", slotId, err)
			continue
		} else {
			fmt.Printf("Removed slot %d\n", slotId)
		}

		slot, ok := m.slots[slotId]
		if !ok {
			continue
		}
		slot.Close()
		delete(m.slots, slotId)
	}
	return nil
}

func (m *Manager) onNotifyPrimarySlotsChanged(ctx context.Context, manager redisreplicamanager.LocalSiteManager) error {
	return nil
}
