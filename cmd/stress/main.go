package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
)

const RedisURL = "redis://127.0.0.1:6379"

func main() {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	defer cancel()

	redisOpts := Must(redis.ParseURL(RedisURL))
	manager := NewManager(ctx, Config{
		redisOpts:         redisOpts,
		MinimumSitesCount: 1,
		SlotReplicaCount:  2,
	})

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := manager.Close(); err != nil {
				panic(err)
			}
			return
		case <-ticker.C:
			redisSlots, _ := manager.manager.GetSlotIdentifiers(context.Background())
			if m_diff, r_diff := manager.GetDiff(); len(m_diff)+len(r_diff) > 0 {
				slotsShouldBe := manager.SlotsShouldBe()

				fmt.Printf("%s: s: %v | rs: %v | local_m: %v | local_r: %v\n", manager.SiteID, len(manager.Slots()), len(*redisSlots), formatSlots(m_diff, slotsShouldBe), formatSlots(r_diff, slotsShouldBe))
				continue
			}
			fmt.Printf("%s: s: %v | rs: %v | State is valid\n", manager.SiteID, len(manager.Slots()), len(*redisSlots))
		}
	}
}

func formatSlots(slots []uint32, slotsShouldBe map[uint32]bool) []string {
	result := []string{}

	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	for _, slotId := range slots {
		if slotsShouldBe[slotId] {
			result = append(result, fmt.Sprintf("%v+", slotId))
		} else {
			result = append(result, fmt.Sprintf("%v-", slotId))
		}
	}

	return result
}

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func Contains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func RandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}
