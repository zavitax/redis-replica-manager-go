package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

var redisOptions = &redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
}

func setup() {
	redis := redis.NewClient(redisOptions)
	redis.Do(context.Background(), "FLUSHDB").Result()
	redis.Close()
}

func createReplicaManagerOptions(
	testId string,
	siteId string,
) *redisReplicaManager.ReplicaManagerOptions {
	result := &redisReplicaManager.ReplicaManagerOptions{
		RedisOptions:   redisOptions,
		SiteTimeout:    time.Second * 5,
		RedisKeyPrefix: fmt.Sprintf("{test-redis-replica-manager}::%v", testId),
		SiteID:         siteId,
	}

	return result
}

func createReplicaManagerClient(options *redisReplicaManager.ReplicaManagerOptions) (redisReplicaManager.ReplicaManagerClient, error) {
	// setup(options)

	return redisReplicaManager.NewRedisReplicaManagerClient(context.TODO(), options)
}

func main() {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	setup()

	options1 := createReplicaManagerOptions("main", "site1")
	options2 := createReplicaManagerOptions("main", "site2")

	options1.ShardID = 0
	options2.ShardID = 1

	client1, _ := createReplicaManagerClient(options1)
	client2, _ := createReplicaManagerClient(options2)

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumShardCount: 1,
	}

	balancer1, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)
	balancer2, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	manager1, _ := redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client1,
		ReplicaBalancer:      balancer1,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m1: missing slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m1: redundant slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
					fmt.Printf("m1: allowed to remove slot: %v\n", allowed)
				}
			}

			return nil
		},
		NotifyMasterSlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager) error {
			slots, _ := manager.GetAllSlotsLocalNodeIsMasterFor(ctx)

			fmt.Printf("m1: master slots changed: %v\n", len(*slots))

			return nil
		},
	})

	manager2, _ := redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client2,
		ReplicaBalancer:      balancer2,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m2: missing slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m2: redundant slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
					fmt.Printf("m2: allowed to remove slot: %v\n", allowed)
				}
			}

			return nil
		},
		NotifyMasterSlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager) error {
			slots, _ := manager.GetAllSlotsLocalNodeIsMasterFor(ctx)

			fmt.Printf("m2: master slots changed: %v\n", len(*slots))

			return nil
		},
	})

	slots1, _ := manager1.GetSlotIdentifiers(ctx)
	slots2, _ := manager2.GetSlotIdentifiers(ctx)

	fmt.Printf("manager1: %v\n", len(*slots1))
	fmt.Printf("manager2: %v\n", len(*slots2))
	fmt.Printf("sum: %v\n", len(*slots1)+len(*slots2))

	// time.Sleep(time.Second)

	fmt.Printf("m1: shards for slot 1: %v\n", manager1.GetSlotShardsRouteTable(ctx, 1))
	fmt.Printf("m2: shards for slot 1: %v\n", manager2.GetSlotShardsRouteTable(ctx, 1))

	fmt.Printf("m1: shards for slot 497: %v\n", manager1.GetSlotShardsRouteTable(ctx, 497))
	fmt.Printf("m2: shards for slot 497: %v\n", manager2.GetSlotShardsRouteTable(ctx, 497))

	fmt.Printf("m1: master shard for slot 1: %v\n", manager1.GetSlotMasterShardRoute(ctx, 1))
	fmt.Printf("m2: master shard for slot 1: %v\n", manager2.GetSlotMasterShardRoute(ctx, 1))

	fmt.Printf("m1: master shard for slot 497: %v\n", manager1.GetSlotMasterShardRoute(ctx, 497))
	fmt.Printf("m2: master shard for slot 497: %v\n", manager2.GetSlotMasterShardRoute(ctx, 497))

	fmt.Printf("m1: slot for object abcdefg: %v\n", manager1.GetSlotForObject("abcdefg"))
	fmt.Printf("m2: slot for object abcdefg: %v\n", manager2.GetSlotForObject("abcdefg"))

	time.Sleep(time.Second)

	manager1.Close()
	manager2.Close()
}
