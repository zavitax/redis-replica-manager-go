package redisReplicaManager_test

import (
	"context"
	"testing"

	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

func TestBalancer(t *testing.T) {
	ctx := context.Background()

	totalReplicas := 2
	totalShards := 8 * totalReplicas
	totalSlots := 256

	balancer, _ := redisReplicaManager.NewReplicaBalancer(ctx, &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   totalSlots,
		MinimumShardCount: totalShards / totalReplicas,
		SlotReplicaCount:  totalReplicas,
	})

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		balancer.AddShard(ctx, shardId)
	}

	expectedSlots := totalReplicas * totalSlots
	foundSlots := 0

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		slots := balancer.GetTargetSlotsForShard(ctx, shardId)

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.RemoveShard(ctx, 2)
	balancer.RemoveShard(ctx, 3)

	foundSlots = 0

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		slots := balancer.GetTargetSlotsForShard(ctx, shardId)

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddShard(ctx, 2)
	balancer.AddShard(ctx, 3)

	balancer.RemoveShard(ctx, 5)

	foundSlots = 0

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		slots := balancer.GetTargetSlotsForShard(ctx, shardId)

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddShard(ctx, 5)

	foundSlots = 0

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		slots := balancer.GetTargetSlotsForShard(ctx, shardId)

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddShard(ctx, 16)
	balancer.AddShard(ctx, 17)
	balancer.AddShard(ctx, 18)
	balancer.AddShard(ctx, 19)
	balancer.AddShard(ctx, 20)
	balancer.AddShard(ctx, 21)

	foundSlots = 0

	for shardId := uint32(0); shardId < uint32(totalShards); shardId++ {
		slots := balancer.GetTargetSlotsForShard(ctx, shardId)

		foundSlots += len(*slots)
	}

	if foundSlots < totalSlots {
		t.Errorf("Expected at least %d slots, got %d slots", totalSlots, foundSlots)
	}
}
