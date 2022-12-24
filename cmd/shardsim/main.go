package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	replicamanager "github.com/zavitax/redis-replica-manager-go"
	"github.com/zavitax/redis-replica-manager-go/internal/fixtures"
)

var (
	redisOptions = &redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	}

	// manager replicamanager.LocalSiteManager
)

func createClient(siteId string) (replicamanager.ReplicaManagerClient, error) {
	return replicamanager.NewRedisReplicaManagerClient(context.Background(), &replicamanager.ReplicaManagerOptions{
		RedisOptions:   redisOptions,
		SiteTimeout:    time.Second * 60,
		RedisKeyPrefix: fmt.Sprintf("shardmanager::shardsim"),
		SiteID:         siteId,
	})
}

func listenForSignals(cancel context.CancelFunc) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sc
	cancel()
}

func main() {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	//ctx, _ := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	go listenForSignals(cancel)

	var shardId string
	if len(os.Args) > 1 {
		shardId = os.Args[1]
	}

	client, err := createClient(shardId)
	if err != nil {
		panic(err)
	}

	balancer, _ := replicamanager.NewReplicaBalancer(context.Background(), &replicamanager.ReplicaBalancerOptions{
		TotalSlotsCount:   20,
		SlotReplicaCount:  2,
		MinimumSitesCount: 1,
	})

	type stateStruct struct {
		manager replicamanager.LocalSiteManager
		group   fixtures.SlotsGroup
	}

	state := &stateStruct{}

	state.group = fixtures.NewSlotsGroup(time.Second*30, time.Second*5, func(slot fixtures.Slot, status string) {
		// Slot ready
		fmt.Println("Slot ", slot.ID(), " is ", status)

		switch status {
		case fixtures.SLOT_STATE_STARTED:
		case fixtures.SLOT_STATE_READY:
			success, err := state.manager.RequestAddSlot(ctx, slot.ID())
			fmt.Printf("manager.RequestAddSlot: slot: %v; success: %v; err: %v\n", slot, success, err)
		case fixtures.SLOT_STATE_STOPPED:
			//success, err := state.manager.RequestRemoveSlot(ctx, slot.ID())
			//fmt.Printf("manager.RequestRemoveSlot: slot: %v; success: %v; err: %v\n", slot, success, err)
		}
	})

	state.manager, _ = replicamanager.NewLocalSiteManager(context.Background(), &replicamanager.ClusterNodeManagerOptions{
		ReplicaManagerClient: client,
		ReplicaBalancer:      balancer,
		RefreshInterval:      time.Second * 10,
		NotifyMissingSlotsHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager, slots *[]uint32) error {
			fmt.Println("missing ", len(*slots), " slots:", *slots)
			for _, slot := range *slots {
				//success, err := manager.RequestAddSlot(ctx, slot)
				//fmt.Printf("manager.RequestAddSlot: slot: %v; success: %v; err: %v\n", slot, success, err)

				s, added := state.group.AddSlot(slot)
				if !added {
					// Slot is already in group
					fmt.Println("Missing Slot is already in group: ", slot, " isReady: ", s.IsReady())

					if s.IsReady() {
						success, err := manager.RequestAddSlot(ctx, slot)
						fmt.Printf("manager.RequestAddSlot: slot: %v; success: %v; err: %v\n", slot, success, err)
					}
				}
			}
			return nil
		},
		NotifyRedundantSlotsHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager, slots *[]uint32) error {
			fmt.Println("redundant ", len(*slots), " slots:", *slots)
			for _, slot := range *slots {
				success, err := manager.RequestRemoveSlot(ctx, slot)
				fmt.Printf("manager.RequestRemoveSlot: slot: %v; success: %v; err: %v\n", slot, success, err)

				if success {
					state.group.RemoveSlot(slot)
				}
			}
			return nil
		},
		NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager) error {
			// slots, _ := manager.GetAllSlotsLocalSiteIsPrimaryFor(ctx)
			// fmt.Println("primary slots:", slots)
			return nil
		},
	})

	fmt.Println(state.manager.GetSlotIdentifiers(context.Background()))

	go func() {
		for {
			if slots, err := state.manager.GetSlotIdentifiers(context.Background()); err == nil && len(*slots) > 0 {
				fmt.Printf("slots: [%v]\n", slots)
			}

			time.Sleep(time.Second * 1)
		}
	}()

	/*
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(5+rand.Intn(10)))

				if slots, err := state.manager.GetSlotIdentifiers(context.Background()); err == nil && len(*slots) > 0 {
					index := rand.Intn(len(*slots))

					slotId := (*slots)[index]

					fmt.Printf("Failing slotId %v\n", slotId)
					state.manager.RemoveFailedSlot(context.Background(), slotId)
				}
			}
		}()*/

	<-ctx.Done()
	fmt.Println("manager.Close()")
	state.group.Close()
	state.manager.Close()
	fmt.Println("All done.")
}
