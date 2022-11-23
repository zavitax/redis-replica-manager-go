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
		RedisKeyPrefix: fmt.Sprintf("shardmanager::test"),
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

	client1, err := createClient(fmt.Sprintf("shard-%d", 1))
	if err != nil {
		panic(err)
	}
	defer client1.Close()

	client2, err := createClient(fmt.Sprintf("shard-%d", 2))
	if err != nil {
		panic(err)
	}
	defer client2.Close()

	client1.AddSlot(ctx, "slot-1")
	client1.AddSlot(ctx, "slot-1")
	client1.AddSlot(ctx, "slot-2")

	client2.AddSlot(ctx, "slot-1")
	client2.AddSlot(ctx, "slot-2")

	client2.RemoveFailedSlot(ctx, "slot-2", 2)
	client2.AddSlot(ctx, "slot-2")

	client1.RemoveFailedSlot(ctx, "slot-2", 2)
	client1.AddSlot(ctx, "slot-2")

	client1.RemoveFailedSlot(ctx, "slot-2", 2)
	client2.RemoveFailedSlot(ctx, "slot-2", 2)

	time.Sleep(time.Second * 10)

	client2.AddSlot(ctx, "slot-2")
	client1.AddSlot(ctx, "slot-2")

	<-ctx.Done()
}
