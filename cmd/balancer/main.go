package main

import (
	"context"
	"fmt"

	"github.com/texttheater/golang-levenshtein/levenshtein"

	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

func printSites(ctx context.Context, router redisReplicaManager.ReplicaBalancer, prev map[string][]uint32) map[string][]uint32 {
	totalSites := router.GetTotalSitesCount()

	result := make(map[string][]uint32, totalSites)

	slotsMap := make(map[uint32]bool)

	for _, siteId := range *router.GetSites() {
		slots := router.GetTargetSlotsForSite(ctx, siteId)

		distance := 0

		if prev != nil {
			source := []rune{}
			dest := make([]rune, len(*slots))

			if len(prev[siteId]) > 0 {
				source = make([]rune, len(prev[siteId]))

				for index, slot := range prev[siteId] {
					source[index] = rune(slot)
				}
			}

			for index, slot := range *slots {
				dest[index] = rune(slot)
			}

			distance = levenshtein.DistanceForStrings(source, dest, levenshtein.DefaultOptions)
		}

		fmt.Printf("Site %v -> Slots: %v -> changes: %v\n", siteId, len(*slots), distance)

		for _, slot := range *slots {
			slotsMap[slot] = true
		}

		result[siteId] = *slots
	}

	fmt.Printf("  -> unique slots: %v\n", len(slotsMap))

	return result
}

func main() {
	// zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	totalReplicas := 2
	totalSites := 8 * totalReplicas
	totalSlots := 256

	balancer, _ := redisReplicaManager.NewReplicaBalancer(ctx, &redisReplicaManager.ReplicaBalancerOptions{
		TotalSlotsCount:   totalSlots,
		MinimumSitesCount: totalSites / totalReplicas,
		SlotReplicaCount:  totalReplicas,
	})

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		balancer.AddSite(ctx, fmt.Sprintf("shard-%v", siteId))
	}

	fmt.Printf("All sites\n")
	prev := printSites(ctx, balancer, nil)

	fmt.Printf("Removed some sites from middle\n")

	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 2))
	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 3))

	prev = printSites(ctx, balancer, prev)

	fmt.Printf("Removed some sites from end\n")

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 2))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 3))

	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 5))

	prev = printSites(ctx, balancer, prev)

	fmt.Printf("Returned some sites to end\n")

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 5))

	prev = printSites(ctx, balancer, prev)

	fmt.Printf("Added sites after end\n")

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 16))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 17))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 18))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 19))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 20))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 21))

	prev = printSites(ctx, balancer, prev)
}
