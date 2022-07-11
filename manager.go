package redisReplicaManager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yourbasic/bit"
)

type RouteTableEntry struct {
	SlotID  uint32
	ShardID uint32
	SiteID  string
	Role    string
}

type ClusterLocalNodeManager interface {
	RequestAddSlot(ctx context.Context, slotId uint32) (bool, error)
	RequestRemoveSlot(ctx context.Context, slotId uint32) (bool, error)

	GetSlotIdentifiers(ctx context.Context) (*[]uint32, error)

	GetSlotForObject(objectId string) uint32

	GetSlotShardsRouteTable(ctx context.Context, slotId uint32) *[]*RouteTableEntry
	GetSlotMasterShardRoute(ctx context.Context, slotId uint32) *RouteTableEntry

	IsSlotMaster(ctx context.Context, slotId uint32) (bool, error)
	GetAllSlotsLocalNodeIsMasterFor(ctx context.Context) (*[]uint32, error)

	Close() error
}

type nodeManager struct {
	ClusterLocalNodeManager

	mu sync.RWMutex

	opts *ClusterNodeManagerOptions

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	slots           map[uint32]bool
	masterSlots     *bit.Set
	slotsRoutingMap *map[uint32][]*RouteTableEntry

	siteId string
}

func (c *nodeManager) formatSlotId(slotId uint32) string {
	return fmt.Sprintf("slot-%v", slotId)
}

func (c *nodeManager) parseSlotId(slotId string) (uint32, error) {
	result := int(0)
	if _, err := fmt.Sscanf(slotId, "slot-%d", &result); err != nil {
		return 0, err
	} else {
		return uint32(result), nil
	}
}

func NewClusterLocalNodeManager(ctx context.Context, opts *ClusterNodeManagerOptions) (ClusterLocalNodeManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	slotsRoutingMap := make(map[uint32][]*RouteTableEntry)

	c := &nodeManager{
		opts:            opts,
		slots:           make(map[uint32]bool),
		slotsRoutingMap: &slotsRoutingMap,
		masterSlots:     bit.New(),
		siteId:          opts.ReplicaManagerClient.GetSiteID(),
	}

	c._housekeep(ctx)

	c.housekeep_context, c.housekeep_cancelFunc = context.WithCancel(ctx)
	go (func() {
		housekeepingInterval := time.Duration(c.opts.RefreshInterval)

		ticker := time.NewTicker(housekeepingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.housekeep_context.Done():
				return
			case packet := <-c.opts.ReplicaManagerClient.Channel():
				c._inspectReplicaManagerClientPacket(c.housekeep_context, packet)
			case <-ticker.C:
				c._housekeep(c.housekeep_context)
			}
		}
	})()

	return c, nil
}

func (c *nodeManager) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	c.masterSlots = bit.New()
	c.slots = make(map[uint32]bool)

	return c.opts.ReplicaManagerClient.Close()
}

func (c *nodeManager) GetSlotForObject(objectId string) uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.opts.ReplicaBalancer.GetSlotForObject(objectId)
}

func (c *nodeManager) GetSlotShardsRouteTable(ctx context.Context, slotId uint32) *[]*RouteTableEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.slotsRoutingMap == nil {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}
	}

	if c.slotsRoutingMap == nil {
		return nil
	}

	shards := (*c.slotsRoutingMap)[slotId]

	if len(shards) < 1 {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}

		shards = (*c.slotsRoutingMap)[slotId]
	}

	return &shards
}

func (c *nodeManager) GetSlotMasterShardRoute(ctx context.Context, slotId uint32) *RouteTableEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.slotsRoutingMap == nil {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}
	}

	if c.slotsRoutingMap == nil {
		return nil
	}

	shards := (*c.slotsRoutingMap)[slotId]

	for _, shard := range shards {
		if shard.Role == ROLE_MASTER {
			return shard
		}
	}

	if err := c._evalSlotsRouting(ctx); err != nil {
		return nil
	}

	for _, shard := range shards {
		if shard.Role == ROLE_MASTER {
			return shard
		}
	}

	return nil
}

func (c *nodeManager) GetAllSlotsLocalNodeIsMasterFor(ctx context.Context) (*[]uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := []uint32{}

	c.masterSlots.Visit(func(n int) bool {
		result = append(result, uint32(n))

		return false
	})

	return &result, nil
}

func (c *nodeManager) IsSlotMaster(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.masterSlots.Contains(int(slotId)), nil
}

func (c *nodeManager) GetSlotIdentifiers(ctx context.Context) (*[]uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]uint32, len(c.slots))

	index := 0
	for slotId, _ := range c.slots {
		result[index] = slotId
		index++
	}

	return &result, nil
}

func (c *nodeManager) RequestAddSlot(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.slots[slotId] {
		if err := c.opts.ReplicaManagerClient.AddSlot(ctx, c.formatSlotId(slotId)); err != nil {
			return false, err
		}

		c.slots[slotId] = true
	}

	return true, nil
}

func (c *nodeManager) RequestRemoveSlot(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots[slotId] {
		if err := c.opts.ReplicaManagerClient.RemoveSlot(
			ctx,
			c.formatSlotId(slotId),
			int(c.opts.ReplicaBalancer.GetSlotReplicaCount()),
			"moved"); err != nil {
			// Minimum replica count is not satisfied, can't remove this slot yet
			return false, err
		}

		delete(c.slots, slotId)
	}

	return true, nil
}

func (c *nodeManager) _housekeep(ctx context.Context) error {
	knownSites := &[]*RedisReplicaManagerSite{}

	if knownSitesResponse, err := c.opts.ReplicaManagerClient.GetAllKnownSites(ctx); err != nil {
		return err
	} else {
		knownSites = knownSitesResponse
	}

	c.mu.RLock()

	// Step 1: make sure we are using an up-to-date list of shards

	knownShards := c.opts.ReplicaBalancer.GetShardIdentifiers()
	knownShardsMap := make(map[uint32]bool)
	for _, shardId := range *knownShards {
		knownShardsMap[shardId] = true
	}

	wantShardsMap := make(map[uint32]bool)
	for _, site := range *knownSites {
		wantShardsMap[site.ShardID] = true
		if !knownShardsMap[site.ShardID] {
			// We don't have this shard
			c.opts.ReplicaBalancer.AddShard(ctx, site.ShardID)
		}
	}

	for shardId, _ := range knownShardsMap {
		if !wantShardsMap[shardId] {
			// We think this shard exists, but it no longer does
			c.opts.ReplicaBalancer.RemoveShard(ctx, shardId)
		}
	}

	// Step 2: make sure slots are correctly distributed between shards

	wantSlots := c.opts.ReplicaBalancer.GetTargetSlotsForShard(ctx, c.opts.ReplicaManagerClient.GetShardID())
	wantSlotsMap := make(map[uint32]bool)

	missingSlots := []uint32{}
	redundantSlots := []uint32{}

	for _, slotId := range *wantSlots {
		wantSlotsMap[slotId] = true

		if !c.slots[slotId] {
			// Missing
			missingSlots = append(missingSlots, slotId)
		}
	}

	for slotId, _ := range c.slots {
		if !wantSlotsMap[slotId] {
			// Redundant
			redundantSlots = append(redundantSlots, slotId)
		}
	}

	c.mu.RUnlock()

	if len(redundantSlots) > 0 && c.opts.NotifyRedundantSlotsHandler != nil {
		c.opts.NotifyRedundantSlotsHandler(ctx, c, &redundantSlots)
	}

	if len(missingSlots) > 0 && c.opts.NotifyMissingSlotsHandler != nil {
		c.opts.NotifyMissingSlotsHandler(ctx, c, &missingSlots)
	}

	if err := c._evalMasterSlots(ctx); err != nil {
		// TODO: Log
	}

	c.mu.RLock()
	if err := c._evalSlotsRouting(ctx); err != nil {
		// TODO: Log
	}
	c.mu.RUnlock()

	return nil
}

func (c *nodeManager) _inspectReplicaManagerClientPacket(ctx context.Context, packet *RedisReplicaManagerUpdate) error {
	if packet.Event == "slot_master_change" {
		if slotId, err := c.parseSlotId(packet.SlotID); err != nil {
			return err
		} else {
			c.mu.Lock()
			hasChange := false
			hasSlot := c.masterSlots.Contains(int(slotId))

			if !hasSlot && c.opts.ReplicaManagerClient.GetSiteID() == packet.SiteID {
				c.masterSlots = c.masterSlots.Add(int(slotId))

				hasChange = true
			} else if hasSlot && c.opts.ReplicaManagerClient.GetSiteID() != packet.SiteID {
				c.masterSlots = c.masterSlots.Delete(int(slotId))

				hasChange = true
			}
			c.mu.Unlock()

			if hasChange {
				if c.opts.NotifyMasterSlotsChangedHandler != nil {
					c.opts.NotifyMasterSlotsChangedHandler(ctx, c)
				}
			}
		}
	}

	c.mu.Lock()
	c.slotsRoutingMap = nil
	c.mu.Unlock()

	return nil
}

func (c *nodeManager) _evalMasterSlots(ctx context.Context) error {
	if slots, err := c.opts.ReplicaManagerClient.GetSlots(ctx); err != nil {
		return err
	} else {
		c.mu.Lock()

		hasChange := false

		for _, slot := range *slots {
			isMaster := slot.Role == ROLE_MASTER
			slotId, err := c.parseSlotId(slot.SlotID)

			if err != nil {
				// TODO: Log error
				continue
			}

			hasSlot := c.masterSlots.Contains(int(slotId))

			if !hasSlot && isMaster {
				c.masterSlots = c.masterSlots.Add(int(slotId))

				hasChange = true
			} else if hasSlot && !isMaster {
				c.masterSlots = c.masterSlots.Delete(int(slotId))

				hasChange = true
			}
		}

		c.mu.Unlock()

		if hasChange {
			if c.opts.NotifyMasterSlotsChangedHandler != nil {
				c.opts.NotifyMasterSlotsChangedHandler(ctx, c)
			}
		}
	}

	return nil
}

func (c *nodeManager) _evalSlotsRouting(ctx context.Context) error {
	if sites, err := c.opts.ReplicaManagerClient.GetAllKnownSites(ctx); err != nil {
		return err
	} else {
		sitesToShardsMap := make(map[string]uint32)

		for _, site := range *sites {
			sitesToShardsMap[site.SiteID] = site.ShardID
		}

		if routes, err := c.opts.ReplicaManagerClient.GetSlotsRouting(ctx); err != nil {
			return err
		} else {
			routeTable := make(map[uint32][]*RouteTableEntry)

			for _, route := range *routes {
				if slotId, err := c.parseSlotId(route.SlotID); err != nil {
					// TODO: Log
				} else {
					if shardId, ok := sitesToShardsMap[route.SiteID]; ok {
						if _, ok := routeTable[slotId]; !ok {
							routeTable[slotId] = []*RouteTableEntry{}
						}

						routeTable[slotId] = append(routeTable[slotId], &RouteTableEntry{
							SiteID:  route.SiteID,
							ShardID: shardId,
							SlotID:  slotId,
							Role:    route.Role,
						})
					}
				}
			}

			c.slotsRoutingMap = &routeTable
		}
	}

	return nil
}
