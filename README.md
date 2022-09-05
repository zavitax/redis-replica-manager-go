# redis-replica-manager

Group membership, sharding, replication and request routing manager relying on Redis for coordination.

# TL;DR

This library allows building distributed applications where partitioning of work is important, replication is desired and the cost of moving a work partition from site to site is high.

It achieves it's goal by partitioning work into _slots_ and assigning each site (cluster node) a set of _slots_ to be responsible for.

A _slot_ can be assigned to more than one site (replication scenario), yet only one site will be designated a _primary_ for each _slot_.

The differentiation between a _primary_ and a _secondary_ site role for a _slot_ is determined by the application.

Assignment of _slots_ to _sites_ is determined by Rendezvous hashing (see below for more details).

# Guarantees

## High Availability

The library allows replication of _slots_ to more than one _site_.

A _site_ is not allowed to relinquish responsibility of a _slot_ before the _slot_ has been migrated to enough _replicas_. The only exception to this rule is when a _site_ is determined to be faulting.

## Minimum Partition Migration

Removal of a _site_ from the cluster, results in redistribution of the _slots_ which that _site_ was responsible for among other _sites_.

When a _site_ is added to the cluster, _slots_ which this _site_ is now responsible for will move from other sites to the newly available _site_.

## Application Control

Migration of _slots_ between _sites_ is _requested_ by the library, and _executed_ by the application.

No assumptions are made about whether migration has completed or not by the library.

The application determines when migration has completed, and notifies the library of that fact.

## Faulty Sites Detection

A heartbeat and a watchdog timer guarantee that faulting sites are removed from the cluster.

## Faulty Slot Handler Detection

If the application detects that a handler for a _slot_ has failed, it should signal the cluster of that fact by calling `LocalSiteManager.RemoveFailedSlot(context.Background(), slotId)`. This will immediately and unconditionally remove the slot from the local site, trigger failover to one of the secondary replicas, and eventually trigger a retry to add the slot to the local site.

## Dynamic Routing

The library maintains a routing table that maps _slots_ to a list of _sites_ which is used to route requests for _slots_ to the right _site_.

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

	client1, _ := createReplicaManagerClient(options1)

	balancerOptions := &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   512,
		SlotReplicaCount:  1,
		MinimumSitesCount: 1,
	}

	balancer1, _ := redisReplicaManager.NewReplicaBalancer(ctx, balancerOptions)

	manager1, _ := redisReplicaManager.NewLocalSiteManager(ctx, &redisReplicaManager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client1,
		ReplicaBalancer:      balancer1,
		RefreshInterval:      time.Second * 15,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m1: missing slots to be added to local site: %v\n", len(*slots))

			for _, slotId := range *slots {
				// Perform necessary operations to be able to fully serve requests for `slotId`
			
				// Notify the cluster that the slot was added to the local site.
				// This should happen only when the slot is completely ready to be served.
				// Calling `manager.RequestAddSlot()` tells the site manager "I am now ready to serve all requests for `slotId`".
				manager.RequestAddSlot(ctx, slotId)
			}

			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager, slots *[]uint32) error {
			fmt.Printf("m1: redundant slots to be removed from local site: %v\n", len(*slots))

			for _, slotId := range *slots {
				// Ask the cluster manager if we are allowed to remove a redundant slot
				// (if it satisfies minimum replica count on other sites)
				if allowed, _ := manager.RequestRemoveSlot(ctx, slotId); allowed {
					// Slot has been approved for removal by the cluster (and has been removed from the routing table)

					// Only after the cluster approved our request to remove the slot,
					// we can release resources which were allocated to serve requests for `slotId`
					fmt.Printf("m1: allowed to remove slot from local site: %v\n", allowed)
				}
			}

			return nil
		},
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager redisReplicaManager.LocalSiteManager) error {
			slots, _ := manager.GetAllSlotsLocalNodeIsPrimaryFor(ctx)

			fmt.Printf("m1: primary slots changed: %v\n", len(*slots))

			return nil
		},
	})

	slots1, _ := manager1.GetSlotIdentifiers(ctx)

	fmt.Printf("manager1 slots count: %v\n", len(*slots1))

	fmt.Printf("m1: sites for slot 1: %v\n", manager1.GetSlotRouteTable(ctx, 1))
	fmt.Printf("m1: sites for slot 497: %v\n", manager1.GetSlotRouteTable(ctx, 497))

	fmt.Printf("m1: primary site for slot 1: %v\n", manager1.GetSlotPrimarySiteRoute(ctx, 1))
	fmt.Printf("m1: primary site for slot 497: %v\n", manager1.GetSlotPrimarySiteRoute(ctx, 497))

	fmt.Printf("m1: slot for object abcdefg: %v\n", manager1.GetSlotForObject("abcdefg"))

	manager1.Close()
}
```
