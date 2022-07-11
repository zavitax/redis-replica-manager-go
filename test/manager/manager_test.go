package redisReplicaManager_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
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

func TestManager(t *testing.T) {
	// zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	setup()

	options1 := createReplicaManagerOptions("TestManager", "site1")
	options2 := createReplicaManagerOptions("TestManager", "site2")

	options1.ShardID = 0
	options2.ShardID = 1

	var err error

	var client1 redisReplicaManager.ReplicaManagerClient
	var client2 redisReplicaManager.ReplicaManagerClient

	client1, err = createReplicaManagerClient(options1)

	if err != nil {
		t.Error(err)
	}

	client2, err = createReplicaManagerClient(options2)

	if err != nil {
		t.Error(err)
	}

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumShardCount: 1,
	}

	var balancer1 redisReplicaManager.ReplicaBalancer
	var balancer2 redisReplicaManager.ReplicaBalancer

	balancer1, err = redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	if err != nil {
		t.Error(err)
	}

	balancer2, err = redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	if err != nil {
		t.Error(err)
	}

	var manager1 redisReplicaManager.ClusterLocalNodeManager
	var manager2 redisReplicaManager.ClusterLocalNodeManager

	manager1, err = redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
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
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager) error {
			slots, _ := manager.GetAllSlotsLocalNodeIsPrimaryFor(ctx)

			fmt.Printf("m1: primary slots changed: %v\n", len(*slots))

			return nil
		},
	})

	if err != nil {
		t.Error(err)
	}

	manager2, err = redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
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
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager) error {
			slots, _ := manager.GetAllSlotsLocalNodeIsPrimaryFor(ctx)

			fmt.Printf("m2: primary slots changed: %v\n", len(*slots))

			return nil
		},
	})

	if err != nil {
		t.Error(err)
	}

	slots1, err := manager1.GetSlotIdentifiers(ctx)

	if err != nil {
		t.Error(err)
	}

	slots2, err := manager2.GetSlotIdentifiers(ctx)

	if err != nil {
		t.Error(err)
	}

	fmt.Printf("manager1: %v\n", len(*slots1))
	fmt.Printf("manager2: %v\n", len(*slots2))
	fmt.Printf("sum: %v\n", len(*slots1)+len(*slots2))

	fmt.Printf("m1: shards for slot 1: %v\n", manager1.GetSlotShardsRouteTable(ctx, 1))
	fmt.Printf("m2: shards for slot 1: %v\n", manager2.GetSlotShardsRouteTable(ctx, 1))

	fmt.Printf("m1: shards for slot 497: %v\n", manager1.GetSlotShardsRouteTable(ctx, 497))
	fmt.Printf("m2: shards for slot 497: %v\n", manager2.GetSlotShardsRouteTable(ctx, 497))

	fmt.Printf("m1: primary shard for slot 1: %v\n", manager1.GetSlotPrimaryShardRoute(ctx, 1))
	fmt.Printf("m2: primary shard for slot 1: %v\n", manager2.GetSlotPrimaryShardRoute(ctx, 1))

	fmt.Printf("m1: primary shard for slot 497: %v\n", manager1.GetSlotPrimaryShardRoute(ctx, 497))
	fmt.Printf("m2: primary shard for slot 497: %v\n", manager2.GetSlotPrimaryShardRoute(ctx, 497))

	fmt.Printf("m1: slot for object abcdefg: %v\n", manager1.GetSlotForObject("abcdefg"))
	fmt.Printf("m2: slot for object abcdefg: %v\n", manager2.GetSlotForObject("abcdefg"))

	manager1.Close()
	manager2.Close()
}
