package main

import (
	"context"
	"fmt"
	"sync"
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

func createReplicaManagerClient(options *redisReplicaManager.ReplicaManagerOptions) (redisReplicaManager.RedisReplicaManagerClient, error) {
	// setup(options)

	return redisReplicaManager.NewRedisReplicaManagerClient(context.TODO(), options)
}

func main() {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	setup()

	options1 := createReplicaManagerOptions("main", "site1")
	options2 := createReplicaManagerOptions("main", "site2")

	options2.ManualHeartbeat = true
	options2.SiteTimeout = time.Second * 3
	options1.SiteTimeout = time.Second * 3

	var mutex sync.Mutex

	eventCount := make(map[string]int)

	updateNotificationHandler := func(ctx context.Context, msg *redisReplicaManager.RedisReplicaManagerUpdate) error {
		mutex.Lock()
		defer mutex.Unlock()

		fmt.Printf("******** UpdateNotificationHandler: %v\n", msg)

		if curr, ok := eventCount[msg.Event]; ok {
			eventCount[msg.Event] = curr + 1
		} else {
			eventCount[msg.Event] = 1
		}

		eventKey := fmt.Sprintf("%s:%s", msg.Event, msg.Role)
		if curr, ok := eventCount[eventKey]; ok {
			eventCount[eventKey] = curr + 1
		} else {
			eventCount[eventKey] = 1
		}

		reasonKey := fmt.Sprintf("%s:%s", msg.Event, msg.Reason)
		if curr, ok := eventCount[reasonKey]; ok {
			eventCount[reasonKey] = curr + 1
		} else {
			eventCount[reasonKey] = 1
		}

		return nil
	}

	options1.UpdateNotificationHandler = updateNotificationHandler
	//options2.UpdateNotificationHandler = updateNotificationHandler

	client1, _ := createReplicaManagerClient(options1)
	client2, _ := createReplicaManagerClient(options2)

	client1.AddSlot(ctx, "slot1")
	client1.AddSlot(ctx, "slot2")
	client1.AddSlot(ctx, "slot3")

	client2.AddSlot(ctx, "slot1")
	client2.AddSlot(ctx, "slot2")

	expectedTimedOutSlots := 2

	for i := 0; i < 20 && eventCount["slot_site_removed:timeout"] != expectedTimedOutSlots; i++ {
		time.Sleep(time.Second / 4)
		fmt.Printf("eventCount: %v\n", eventCount["slot_site_removed:timeout"])
	}

	client1.Close()
	client2.Close()
}
