package redisReplicaManager_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	redisReplicaManager "github.com/zavitax/redis-replica-manager-go"
)

func TestBalancer(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

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

	expectedSlots := totalReplicas * totalSlots
	foundSlots := 0

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		slots := balancer.GetTargetSlotsForSite(ctx, fmt.Sprintf("shard-%v", siteId))

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 2))
	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 3))

	foundSlots = 0

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		slots := balancer.GetTargetSlotsForSite(ctx, fmt.Sprintf("shard-%v", siteId))

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 2))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 3))

	balancer.RemoveSite(ctx, fmt.Sprintf("shard-%v", 5))

	foundSlots = 0

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		slots := balancer.GetTargetSlotsForSite(ctx, fmt.Sprintf("shard-%v", siteId))

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 5))

	foundSlots = 0

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		slots := balancer.GetTargetSlotsForSite(ctx, fmt.Sprintf("shard-%v", siteId))

		foundSlots += len(*slots)
	}

	if foundSlots != expectedSlots {
		t.Errorf("Expected %d slots, got %d slots", expectedSlots, foundSlots)
	}

	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 16))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 17))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 18))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 19))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 20))
	balancer.AddSite(ctx, fmt.Sprintf("shard-%v", 21))

	foundSlots = 0

	for siteId := uint32(0); siteId < uint32(totalSites); siteId++ {
		slots := balancer.GetTargetSlotsForSite(ctx, fmt.Sprintf("shard-%v", siteId))

		foundSlots += len(*slots)
	}

	if foundSlots < totalSlots {
		t.Errorf("Expected at least %d slots, got %d slots", totalSlots, foundSlots)
	}
}
