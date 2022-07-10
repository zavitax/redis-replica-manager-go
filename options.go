package redisReplicaManager

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type ReplicaManagerOptions struct {
	RedisOptions              *redis.Options
	SiteID                    string
	SiteTimeout               time.Duration
	RedisKeyPrefix            string
	UpdateNotificationHandler RedisReplicaManagerUpdateFunc
	ManualHeartbeat           bool
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o *ReplicaManagerOptions) Validate() error {
	if o == nil {
		return validationError
	}

	if o.RedisOptions == nil {
		return validationError
	}

	if o.SiteTimeout < time.Second {
		return validationError
	}

	if len(o.RedisKeyPrefix) < 1 {
		return validationError
	}

	if len(o.SiteID) < 1 {
		return validationError
	}

	return nil
}
