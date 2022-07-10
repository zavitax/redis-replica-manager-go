package redisReplicaManager

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

type ReplicaBalancer interface {
	AddShard(ctx context.Context, shardId uint32) error
	RemoveShard(ctx context.Context, shardId uint32) error

	GetTargetSlotsForShard(ctx context.Context, shardId uint32) *[]uint32
	GetSlotShards(ctx context.Context, slotId uint32) *[]uint32

	GetTotalShardsCount() uint32
	GetTotalSlotsCount() uint32
	GetSlotReplicaCount() uint32
}

type shardSlotBalancer struct {
	ReplicaBalancer

	mu   sync.RWMutex
	opts *ReplicaRouterOptions

	shards           map[uint32]bool
	totalShardsCount uint32
}

func NewReplicaBalancer(ctx context.Context, opts *ReplicaRouterOptions) (ReplicaBalancer, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	c := &shardSlotBalancer{
		opts:   opts,
		shards: make(map[uint32]bool),
	}

	return c, nil
}

func (c *shardSlotBalancer) GetSlotReplicaCount() uint32 {
	return uint32(c.opts.SlotReplicaCount)
}

func (c *shardSlotBalancer) GetTotalSlotsCount() uint32 {
	return uint32(c.opts.TotalSlotsCount)
}

func (c *shardSlotBalancer) GetTotalShardsCount() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.totalShardsCount
}

func (c *shardSlotBalancer) AddShard(ctx context.Context, shardId uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.shards[shardId] {
		return fmt.Errorf("Duplicate shard: %v", shardId)
	}

	c.shards[shardId] = true

	c.totalShardsCount = uint32(0)

	for shardId, _ := range c.shards {
		if shardId >= c.totalShardsCount {
			c.totalShardsCount = shardId + 1
		}
	}

	return nil
}

func (c *shardSlotBalancer) RemoveShard(ctx context.Context, shardId uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.shards[shardId] {
		return fmt.Errorf("Unknown shard: %v", shardId)
	}

	delete(c.shards, shardId)

	c.totalShardsCount = uint32(0)

	for shardId, _ := range c.shards {
		if shardId >= c.totalShardsCount {
			c.totalShardsCount = shardId + 1
		}
	}

	return nil
}

func (c *shardSlotBalancer) GetSlotShards(ctx context.Context, slotId uint32) *[]uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.totalShardsCount < uint32(c.opts.MinimumShardCount) {
		// Minimum shard count condition is not satisfied - cluster is not large enough for expected load
		return &[]uint32{}
	}

	if slotId >= uint32(c.opts.TotalSlotsCount) {
		// Slot out of bounds
		return &[]uint32{}
	}

	matrix := c.getShardSlotsMatrix()

	slotShardsMap := c.getSlotShards(matrix)

	shards := (*slotShardsMap)[slotId]

	return shards
}

func (c *shardSlotBalancer) GetTargetSlotsForShard(ctx context.Context, shardId uint32) *[]uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.totalShardsCount < uint32(c.opts.MinimumShardCount) {
		// Minimum shard count condition is not satisfied - cluster is not large enough for expected load
		return &[]uint32{}
	}

	matrix := c.getShardSlotsMatrix()

	slots := c.getShardSlots(matrix, shardId)

	return slots
}

func (c *shardSlotBalancer) getSlotShards(matrix *[]*[]uint32) *map[uint32]*[]uint32 {
	result := make(map[uint32]*[]uint32)

	for slotId, shards := range *matrix {
		result[uint32(slotId)] = shards
	}

	return &result
}

func (c *shardSlotBalancer) getShardSlots(matrix *[]*[]uint32, shardId uint32) *[]uint32 {
	result := []uint32{}

	for slotId, shards := range *matrix {
		for index := 0; index < len(*shards) && index < c.opts.SlotReplicaCount; index++ {
			if (*shards)[index] == shardId {
				result = append(result, uint32(slotId))
			}
		}
	}

	return &result
}

type slotShardHash struct {
	hash    uint32
	shardId uint32
}

func (c *shardSlotBalancer) getShardSlotsMatrix() *[]*[]uint32 {
	result := make([]*[]uint32, c.opts.TotalSlotsCount)

	for slotId := uint32(0); slotId < uint32(c.opts.TotalSlotsCount); slotId++ {
		var list []*slotShardHash

		for shardId, _ := range c.shards {
			list = append(list, &slotShardHash{
				hash:    calcShardSlotHash(shardId, slotId, uint32(c.opts.TotalSlotsCount)),
				shardId: shardId,
			})
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].hash < list[j].hash
		})

		shards := make([]uint32, len(list))

		for index, shard := range list {
			shards[index] = shard.shardId
		}

		result[slotId] = &shards
	}

	return &result
}

func calcShardSlotHash(shardId uint32, slotId uint32, totalSlots uint32) uint32 {
	hasher := md5.New()
	hasher.Write([]byte(fmt.Sprintf("%v:%v:%v", shardId, slotId, totalSlots)))

	hex := hex.EncodeToString(hasher.Sum(nil))
	id, _ := strconv.ParseInt(hex[:4], 16, 0)

	return uint32(id + 1)
}
