package redisReplicaManager_test

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

func createLocalClusterNodeManagerForBenchmark(ctx context.Context, testId string, siteId string) redisReplicaManager.ClusterLocalNodeManager {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	setup()

	options1 := createReplicaManagerOptions("BenchmarkManager", "site1")

	options1.ShardID = 0

	var client1 redisReplicaManager.ReplicaManagerClient

	client1, _ = createReplicaManagerClient(options1)

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumShardCount: 1,
	}

	var balancer1 redisReplicaManager.ReplicaBalancer

	balancer1, _ = redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	manager1, _ := redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client1,
		ReplicaBalancer:      balancer1,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			return nil
		},
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager) error {
			return nil
		},
	})

	return manager1
}

func BenchmarkManager_GetSlotIdentifiers(b *testing.B) {
	ctx := context.Background()

	manager := createLocalClusterNodeManagerForBenchmark(ctx, "BenchmarkManager::GetSlotIdentifiers", "site1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetSlotIdentifiers(ctx)
	}

	manager.Close()
}

func BenchmarkManager_GetSlotShardsRouteTable(b *testing.B) {
	ctx := context.Background()

	manager := createLocalClusterNodeManagerForBenchmark(ctx, "BenchmarkManager::GetSlotShardsRouteTable", "site1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetSlotShardsRouteTable(ctx, 1)
	}

	manager.Close()
}

func BenchmarkManager_GetSlotPrimaryShardRoute(b *testing.B) {
	ctx := context.Background()

	manager := createLocalClusterNodeManagerForBenchmark(ctx, "BenchmarkManager::GetSlotPrimaryShardRoute", "site1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetSlotPrimaryShardRoute(ctx, 1)
	}

	manager.Close()
}

func BenchmarkManager_GetSlotForObject(b *testing.B) {
	ctx := context.Background()

	manager := createLocalClusterNodeManagerForBenchmark(ctx, "BenchmarkManager::GetSlotForObject", "site1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetSlotForObject("abcdefg")
	}

	manager.Close()
}
