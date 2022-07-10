package redisReplicaManager

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ClusterLocalNodeManager interface {
	RequestAddSlot(ctx context.Context, slotId uint32) (bool, error)
	RequestRemoveSlot(ctx context.Context, slotId uint32) (bool, error)

	GetSlotIdentifiers(ctx context.Context) (*[]uint32, error)
}

type nodeManager struct {
	ClusterLocalNodeManager

	mu sync.RWMutex

	opts *ClusterNodeManagerOptions

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	slots map[uint32]bool

	siteId string
}

func NewClusterLocalNodeManager(ctx context.Context, opts *ClusterNodeManagerOptions) (ClusterLocalNodeManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	c := &nodeManager{
		opts:   opts,
		slots:  make(map[uint32]bool),
		siteId: opts.ReplicaManagerClient.GetSiteID(),
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
			case <-ticker.C:
				c._housekeep(c.housekeep_context)
			}
		}
	})()

	return c, nil
}

func (c *nodeManager) slotId(slotId uint32) string {
	return fmt.Sprintf("slot-%v", slotId)
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
		if err := c.opts.ReplicaManagerClient.AddSlot(ctx, c.slotId(slotId)); err != nil {
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
			c.slotId(slotId),
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

	return nil
}
