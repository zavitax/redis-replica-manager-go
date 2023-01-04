package redisReplicaManager_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	//"github.com/rs/zerolog"

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

func TestConnectDisconnect(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	client, err := createReplicaManagerClient(createReplicaManagerOptions("TestConnectDisconnect", "site1"))

	if err != nil {
		t.Error(err)
		return
	}

	client.Close()
}

func TestPrimaryChange(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	setup()

	options1 := createReplicaManagerOptions("TestPrimaryChange", "site1")
	options2 := createReplicaManagerOptions("TestPrimaryChange", "site2")

	var mutex sync.Mutex

	eventCount := make(map[string]int)

	updateNotificationHandler := func(ctx context.Context, msg *redisReplicaManager.RedisReplicaManagerUpdate) error {
		mutex.Lock()
		defer mutex.Unlock()

		if curr, ok := eventCount[msg.Event]; ok {
			eventCount[msg.Event] = curr + 1
		} else {
			eventCount[msg.Event] = 1
		}

		removeKey := fmt.Sprintf("%s:%s", msg.Event, msg.Role)
		if curr, ok := eventCount[removeKey]; ok {
			eventCount[removeKey] = curr + 1
		} else {
			eventCount[removeKey] = 1
		}

		return nil
	}

	options2.UpdateNotificationHandler = updateNotificationHandler

	client1, _ := createReplicaManagerClient(options1)
	client2, _ := createReplicaManagerClient(options2)

	client1.AddSlot(ctx, "slot1")
	client1.AddSlot(ctx, "slot2")
	client1.AddSlot(ctx, "slot3")

	client2.AddSlot(ctx, "slot1")
	client2.AddSlot(ctx, "slot2")

	client1.Close()

	mutex.Lock()
	curr := eventCount["slot_primary_change"]
	expected := 4
	mutex.Unlock()

	if curr != expected {
		t.Errorf("eventCount: %v, expected: %v", curr, expected)
	}

	client2.Close()
}

func TestGetAllKnownSites(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	ctx := context.Background()

	setup()

	options1 := createReplicaManagerOptions("TestSiteTimeout", "site1")
	options2 := createReplicaManagerOptions("TestSiteTimeout", "site2")

	client1, _ := createReplicaManagerClient(options1)
	client2, _ := createReplicaManagerClient(options2)

	if sites, err := client1.GetLiveSites(ctx); err != nil {
		t.Error(err)
	} else {
		curr := len(*sites)
		expected := 2

		if curr != expected {
			t.Errorf("expected %v, got %v", expected, curr)
		}
	}

	client1.Close()
	client2.Close()
}
