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
	ShardID                   uint32
	SiteTimeout               time.Duration
	RedisKeyPrefix            string
	UpdateNotificationHandler RedisReplicaManagerUpdateFunc
	ManualHeartbeat           bool
}

type ReplicaManagerClientProviderOptions struct {
	SiteID                    string
	ShardID                   uint32
	UpdateNotificationHandler RedisReplicaManagerUpdateFunc
}

type ReplicaManagerClientProviderFunc func(ctx context.Context, opts *ReplicaManagerClientProviderOptions) (ReplicaManagerClient, error)
type ReplicaBalancerOptions struct {
	TotalSlotsCount   int
	SlotReplicaCount  int
	MinimumShardCount int
}

type ClusterNodeManagerOptions struct {
	ReplicaManagerClient ReplicaManagerClient
	ReplicaBalancer      ReplicaBalancer

	RefreshInterval time.Duration

	NotifyMissingSlotsHandler        func(ctx context.Context, manager ClusterLocalNodeManager, slots *[]uint32) error
	NotifyRedundantSlotsHandler      func(ctx context.Context, manager ClusterLocalNodeManager, slots *[]uint32) error
	NotifyPrimarySlotsChangedHandler func(ctx context.Context, manager ClusterLocalNodeManager) error
}

var validationError = fmt.Errorf("All Options values must be correctly specified")

func (o *ClusterNodeManagerOptions) Validate() error {
	if o == nil {
		return validationError
	}

	if o.ReplicaManagerClient == nil {
		return validationError
	}

	if o.ReplicaBalancer == nil {
		return validationError
	}

	if o.NotifyMissingSlotsHandler == nil {
		return validationError
	}

	if o.NotifyRedundantSlotsHandler == nil {
		return validationError
	}

	if o.RefreshInterval < time.Second {
		return validationError
	}

	return nil
}

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

func (o *ReplicaBalancerOptions) Validate() error {
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
