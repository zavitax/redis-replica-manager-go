package redisReplicaManager_test

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
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
