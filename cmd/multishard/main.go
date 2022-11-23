package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	replicamanager "github.com/zavitax/redis-replica-manager-go"
)

var (
	redisOptions = &redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	}

	manager replicamanager.LocalSiteManager
)

func createClient(siteId string) (replicamanager.ReplicaManagerClient, error) {
	return replicamanager.NewRedisReplicaManagerClient(context.Background(), &replicamanager.ReplicaManagerOptions{
		RedisOptions:   redisOptions,
		SiteTimeout:    time.Second * 5,
		RedisKeyPrefix: fmt.Sprintf("shardmanager::multishard"),
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
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	//ctx, _ := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	go listenForSignals(cancel)

	var shardCount int = 1
	if len(os.Args) > 1 {
		shardCount, _ = strconv.Atoi(os.Args[1])
	}
	var minShardCount int = shardCount
	if len(os.Args) > 2 {
		minShardCount, _ = strconv.Atoi(os.Args[2])
	}

	wg := sync.WaitGroup{}

	for shardId := 1; shardId <= shardCount; shardId++ {
		wg.Add(1)

		go func(shardId int) {
			client, err := createClient(fmt.Sprintf("shard-%d", shardId))
			if err != nil {
				panic(err)
			}

			balancer, err := replicamanager.NewReplicaBalancer(context.Background(), &replicamanager.ReplicaBalancerOptions{
				TotalSlotsCount:   10,
				SlotReplicaCount:  2,
				MinimumSitesCount: minShardCount,
			})

			if err != nil {
				panic(err)
			}

			manager, err = replicamanager.NewLocalSiteManager(context.Background(), &replicamanager.ClusterNodeManagerOptions{
				ReplicaManagerClient: client,
				ReplicaBalancer:      balancer,
				RefreshInterval:      time.Second * 15,
				NotifyMissingSlotsHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager, slots *[]uint32) error {
					fmt.Println("missing slots:", *slots)
					for _, slot := range *slots {
						success, err := manager.RequestAddSlot(ctx, slot)

						fmt.Printf("manager.RequestAddSlot: slot: %v; success: %v; err: %v\n", slot, success, err)
					}
					return nil
				},
				NotifyRedundantSlotsHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager, slots *[]uint32) error {
					fmt.Println("redundant slots:", *slots)
					for _, slot := range *slots {
						success, err := manager.RequestRemoveSlot(ctx, slot)

						fmt.Printf("manager.RequestRemoveSlot: slot: %v; success: %v; err: %v\n", slot, success, err)
					}
					return nil
				},
				NotifyPrimarySlotsChangedHandler: func(ctx context.Context, manager replicamanager.LocalSiteManager) error {
					slots, _ := manager.GetAllSlotsLocalSiteIsPrimaryFor(ctx)
					fmt.Println("primary slots:", slots)
					return nil
				},
			})

			if err != nil {
				panic(err)
			}

			/*
				fmt.Println(manager.GetSlotIdentifiers(context.Background()))

				go func() {
					for {
						time.Sleep(time.Second * time.Duration(5+rand.Intn(10)))

						if slots, err := manager.GetSlotIdentifiers(context.Background()); err == nil && len(*slots) > 0 {
							index := rand.Intn(len(*slots))

							slotId := (*slots)[index]

							fmt.Printf("Failing slotId %v\n", slotId)
							manager.RemoveFailedSlot(context.Background(), slotId)
						}
					}
				}()*/

			<-ctx.Done()

			log.Info().Int("ShardID", shardId).Msg("Shutting down")
			manager.Close()
			log.Info().Int("ShardID", shardId).Msg("Shutdown")

			wg.Done()
		}(shardId)
	}

	<-ctx.Done()
	wg.Wait()
	log.Info().Msg("Program shutdown")
}
