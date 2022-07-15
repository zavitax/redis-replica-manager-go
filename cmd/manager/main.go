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

	client1, _ := createReplicaManagerClient(options1)
	client2, _ := createReplicaManagerClient(options2)

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumSitesCount: 1,
	}

	balancer1, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)
	balancer2, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	manager1, _ := redisReplicaManager.NewLocalSiteManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client1,
		ReplicaBalancer:      balancer1,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m1: missing slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m1: redundant slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
					fmt.Printf("m1: allowed to remove slot: %v\n", allowed)
				}
			}

			return nil
		},
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager) error {
			slots, _ := manager.GetAllSlotsLocalSiteIsPrimaryFor(ctx)

			fmt.Printf("m1: primary slots changed: %v\n", len(*slots))

			return nil
		},
	})

	manager2, _ := redisReplicaManager.NewLocalSiteManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client2,
		ReplicaBalancer:      balancer2,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m2: missing slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m2: redundant slots: %v\n", len(*slots))

			for _, slotId := range *slots {
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
					fmt.Printf("m2: allowed to remove slot: %v\n", allowed)
				}
			}

			return nil
		},
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager) error {
			slots, _ := manager.GetAllSlotsLocalSiteIsPrimaryFor(ctx)

			fmt.Printf("m2: primary slots changed: %v\n", len(*slots))

			return nil
		},
	})

	slots1, _ := manager1.GetSlotIdentifiers(ctx)
	slots2, _ := manager2.GetSlotIdentifiers(ctx)

	fmt.Printf("manager1: %v\n", len(*slots1))
	fmt.Printf("manager2: %v\n", len(*slots2))
	fmt.Printf("sum: %v\n", len(*slots1)+len(*slots2))

	fmt.Printf("m1: sites for slot 1: %v\n", manager1.GetSlotRouteTable(ctx, 1))
	fmt.Printf("m2: sites for slot 1: %v\n", manager2.GetSlotRouteTable(ctx, 1))

	fmt.Printf("m1: sites for slot 497: %v\n", manager1.GetSlotRouteTable(ctx, 497))
	fmt.Printf("m2: sites for slot 497: %v\n", manager2.GetSlotRouteTable(ctx, 497))

	fmt.Printf("m1: primary site for slot 1: %v\n", manager1.GetSlotPrimarySiteRoute(ctx, 1))
	fmt.Printf("m2: primary site for slot 1: %v\n", manager2.GetSlotPrimarySiteRoute(ctx, 1))

	fmt.Printf("m1: primary site for slot 497: %v\n", manager1.GetSlotPrimarySiteRoute(ctx, 497))
	fmt.Printf("m2: primary site for slot 497: %v\n", manager2.GetSlotPrimarySiteRoute(ctx, 497))

	fmt.Printf("m1: slot for object abcdefg: %v\n", manager1.GetSlotForObject("abcdefg"))
	fmt.Printf("m2: slot for object abcdefg: %v\n", manager2.GetSlotForObject("abcdefg"))

	manager1.Close()
	manager2.Close()
}
