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

type ReplicaRouterOptions struct {
	TotalSlotsCount      int
	InitialSitesCount    int
	MinimumReplicaCount  int
	ReplicaManagerClient *RedisReplicaManagerClient
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

func (o *ReplicaRouterOptions) Validate() error {
	if o == nil {
		return validationError
	}

	if o.TotalSlotsCount < 1 || o.TotalSlotsCount > 16384 {
		return validationError
	}

	if o.InitialSitesCount < 1 {
		return validationError
	}

	if o.MinimumReplicaCount < 1 {
		return validationError
	}

	if o.ReplicaManagerClient == nil {
		return validationError
	}

	return nil
}
