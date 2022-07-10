package main

import (
	"context"
	"fmt"

	"github.com/texttheater/golang-levenshtein/levenshtein"

	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

func printShards(ctx context.Context, router redisReplicaManager.ReplicaBalancer, prev [][]uint32) [][]uint32 {
	totalShards := router.GetTotalShardsCount()

	result := make([][]uint32, totalShards)

	slotsMap := make(map[uint32]bool)

	for shardId := uint32(0); shardId < totalShards; shardId++ {
		slots := router.GetTargetSlotsForShard(ctx, shardId)

		distance := 0

		if prev != nil {
			source := []rune{}
			dest := make([]rune, len(*slots))

			if uint32(len(prev)) > shardId {
				source = make([]rune, len(prev[shardId]))

				for index, slot := range prev[shardId] {
					source[index] = rune(slot)
				}
			}

			for index, slot := range *slots {
				dest[index] = rune(slot)
			}

			distance = levenshtein.DistanceForStrings(source, dest, levenshtein.DefaultOptions)
		}

		fmt.Printf("Shard %v -> Slots: %v -> changes: %v\n", shardId, len(*slots), distance)

		for _, slot := range *slots {
			slotsMap[slot] = true
		}

		result[shardId] = *slots
	}

	fmt.Printf("  -> unique slots: %v\n", len(slotsMap))

	return result
}

func main() {
	// zerolog.SetGlobalLevel(zerolog.Disabled)

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

	fmt.Printf("All shards\n")
	prev := printShards(ctx, balancer, nil)

	fmt.Printf("Removed some shards from middle\n")

	balancer.RemoveShard(ctx, 2)
	balancer.RemoveShard(ctx, 3)

	prev = printShards(ctx, balancer, prev)

	fmt.Printf("Removed some shards from end\n")

	balancer.AddShard(ctx, 2)
	balancer.AddShard(ctx, 3)

	balancer.RemoveShard(ctx, 5)

	prev = printShards(ctx, balancer, prev)

	fmt.Printf("Returned some shards to end\n")

	balancer.AddShard(ctx, 5)

	prev = printShards(ctx, balancer, prev)

	fmt.Printf("Added shards after end\n")

	balancer.AddShard(ctx, 16)
	balancer.AddShard(ctx, 17)
	balancer.AddShard(ctx, 18)
	balancer.AddShard(ctx, 19)
	balancer.AddShard(ctx, 20)
	balancer.AddShard(ctx, 21)

	prev = printShards(ctx, balancer, prev)
}
