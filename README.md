# redis-replica-manager

Group membership, sharding, replication and request routing manager relying on Redis for coordination.

# TL;DR

This library allows building distributed applications where partitioning of work is important, replication is desired and the cost of moving a work partition from shard to shard is high.

It achieves it's goal by partitioning work into _slots_ and assigning each shard (cluster node) a set of _slots_ to be responsible for.

A _slot_ can be assigned to more than one shard (replication scenario), yet only one shard will be designated a _master_ for each _slot_.

The differentiation between a _master_ and a _normal_ shard role for a _slot_ is determined by the application.

Assignment of _slots_ to _shards_ is determined by Rendezvous hashing (see below for more details).

# Guarantees

## High Availability

The library allows replication of _slots_ to more than one _shard_.

A _shard_ is not allowed to relinquish control of a _slot_ before the _slot_ has been migrated to enough _replicas_. he only exception to this rule is when a _shard_ is determined to be faulting.

## Minimum Partition Migration

Removal of a _shard_ from the cluster, results in redistribution of the _slots_ which that _shard_ was responsible for among other _shards_.

When a _shard_ is added to the cluster, _slots_ which this _shard_ is now responsible for will move from other shards to the newly available _shard_.

## Application Control

Migration of _slots_ between _shards_ is _requested_ by the library, and _executed_ by the application.

No assumptions are made about whether migration has completed or not by the library.

The application determines when migration has completed, and notifies the library of that fact.

## Faulty Shards Detection

A heartbeat and a watchdog timer guarantee that faulting shards are removed from the cluster.

## Dynamic Routing

The library maintains a routing table that maps _slots_ to a list of _shards_ which is used to route requests for _slots_ to the right _shard_.

# Rendezvous hashing

Rendezvous or highest random weight (HRW) hashing is an algorithm that allows clients to achieve distributed agreement on a set of _k_ options out of a possible set of _n_ options. A typical application is when clients need to agree on which sites (or proxies) objects are assigned to.

Rendezvous hashing is both much simpler and more general than consistent hashing, which becomes a special case (for _k_=1) of rendezvous hashing.

More information:
https://en.wikipedia.org/wiki/Rendezvous_hashing

# Example

```go
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
	return redisReplicaManager.NewRedisReplicaManagerClient(context.TODO(), options)
}

func main() {
	ctx := context.Background()

	options1 := createReplicaManagerOptions("main", "site1")

	options1.ShardID = 0

	client1, _ := createReplicaManagerClient(options1)

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumShardCount: 1,
	}

	balancer1, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	manager1, _ := redisReplicaManager.NewClusterLocalNodeManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client1,
		ReplicaBalancer:      balancer1,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m1: missing slots to be added to local shard: %v\n", len(*slots))

			for _, slotId := range *slots {
        // Notify the cluster that the slot was added to the local shard.
        // This should happen only when the slot migration has fully and successfully completed.
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.ClusterLocalNodeManager, slots *[]uint32) error {
			fmt.Printf("m1: redundant slots to be removed from local shard: %v\n", len(*slots))

			for _, slotId := range *slots {
        // Ask the cluster manager if we are allower to remove a redundant slot
        // (if it satisfies minimum replica count on other shards)
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
          // Slot has been approved for removal by the cluater (and has bee removed from the routing table)
					fmt.Printf("m1: allowed to remove slot from local shard: %v\n", allowed)
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

	slots1, _ := manager1.GetSlotIdentifiers(ctx)

	fmt.Printf("manager1 slots count: %v\n", len(*slots1))

	fmt.Printf("m1: shards for slot 1: %v\n", manager1.GetSlotShardsRouteTable(ctx, 1))
	fmt.Printf("m1: shards for slot 497: %v\n", manager1.GetSlotShardsRouteTable(ctx, 497))

	fmt.Printf("m1: master shard for slot 1: %v\n", manager1.GetSlotMasterShardRoute(ctx, 1))
	fmt.Printf("m1: master shard for slot 497: %v\n", manager1.GetSlotMasterShardRoute(ctx, 497))

	manager1.Close()
}
```
