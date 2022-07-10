package redisReplicaManager

import (
	"context"
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

type ReplicaManagerClientProviderOptions struct {
	SiteID                    string
	UpdateNotificationHandler RedisReplicaManagerUpdateFunc
}

type ReplicaManagerClientProviderFunc func(ctx context.Context, opts *ReplicaManagerClientProviderOptions) (ReplicaManagerClient, error)

type ReplicaRouterOptions struct {
	TotalSlotsCount   int
	SlotReplicaCount  int
	MinimumShardCount int
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

	if o.MinimumShardCount < 1 {
		return validationError
	}

	if o.SlotReplicaCount < 1 {
		return validationError
	}

	return nil
}
