package redisReplicaManager

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"

	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

func currentTimestamp() int64 {
	return time.Now().UTC().UnixMilli()
}

const (
	ROLE_MASTER = "master"
	ROLE_SLAVE  = "normal"
)

type ReplicaManagerClient interface {
	AddSlot(ctx context.Context, slotId string) error
	RemoveSlot(ctx context.Context, slotId string, minReplicaCount int, reason string) error

	GetSlot(ctx context.Context, slotId string) (*RedisReplicaManagerSiteSlot, error)
	GetSlots(ctx context.Context) (*[]*RedisReplicaManagerSiteSlot, error)

	GetSiteID() string
	GetShardID() uint32

	GetAllKnownSites(ctx context.Context) (*[]*RedisReplicaManagerSite, error)

	Channel() <-chan *RedisReplicaManagerUpdate

	Close() error
}

type RedisReplicaManagerSiteSlot struct {
	SiteID string
	SlotID string
	Role   string
}

type RedisReplicaManagerSite struct {
	SiteID  string
	ShardID uint32
}

type RedisReplicaManagerUpdate struct {
	Event  string `json:"event"`
	SlotID string `json:"slot,omitempty"`
	SiteID string `json:"site"`
	Reason string `json:"reason"`
	Role   string `json:"role,omitempty"`
}

type RedisReplicaManagerUpdateFunc func(ctx context.Context, update *RedisReplicaManagerUpdate) error

type redisReplicaManagerClient struct {
	ReplicaManagerClient

	mu sync.RWMutex

	options          *ReplicaManagerOptions
	redis            *redis.Client
	redis_subscriber *redis.Client

	redis_subscriber_handle      *redis.PubSub
	redis_subscriber_context     context.Context
	redis_subscriber_cancel_func context.CancelFunc

	subscriber_channel chan *RedisReplicaManagerUpdate

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	callAddSlotSite                *redisLuaScriptUtils.CompiledRedisScript
	callConditionalRemoveSlotSite  *redisLuaScriptUtils.CompiledRedisScript
	callUpdateSiteTimestampSnippet *redisLuaScriptUtils.CompiledRedisScript
	callGetTimedOutSites           *redisLuaScriptUtils.CompiledRedisScript
	callGetSiteSlotInfo            *redisLuaScriptUtils.CompiledRedisScript
	callGetSiteSlots               *redisLuaScriptUtils.CompiledRedisScript
	callGetAllSiteIDs              *redisLuaScriptUtils.CompiledRedisScript

	keyPubsubChannel string

	redisKeys []*redisLuaScriptUtils.RedisKey
}

func NewRedisReplicaManagerClient(ctx context.Context, options *ReplicaManagerOptions) (ReplicaManagerClient, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	c := &redisReplicaManagerClient{}

	c.options = options

	c.keyPubsubChannel = fmt.Sprintf("%s::global::pubsub", c.options.RedisKeyPrefix)

	c.redisKeys = []*redisLuaScriptUtils.RedisKey{
		redisLuaScriptUtils.NewStaticKey("keyPubsubChannel", c.keyPubsubChannel),
		redisLuaScriptUtils.NewStaticKey("keySiteSlotsHash", fmt.Sprintf("%s::global::sites-slots", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keySitesTimestamps", fmt.Sprintf("%s::global::sites-timestamps", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewStaticKey("keySitesShardIdentifiers", fmt.Sprintf("%s::global::sites-shards-identifiers", c.options.RedisKeyPrefix)),
		redisLuaScriptUtils.NewDynamicKey("keySlotSitesRolesHash", func(args *redisLuaScriptUtils.RedisScriptArguments) string {
			return fmt.Sprintf("%s::slot::sites::%s", c.options.RedisKeyPrefix, (*args)["argSlotID"].(string))
		}),
	}

	var err error

	if c.callAddSlotSite, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptAddSlotSite,
			scriptUpdateSiteSlotChangeSnippet,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callConditionalRemoveSlotSite, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptConditionalRemoveSlotSite,
			scriptUpdateSiteSlotChangeSnippet,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callUpdateSiteTimestampSnippet, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptUpdateSiteTimestampSnippet,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callGetTimedOutSites, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptGetTimedOutSites,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callGetSiteSlotInfo, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptGetSiteSlotInfo,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callGetSiteSlots, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptGetSiteSlots,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	if c.callGetAllSiteIDs, err = redisLuaScriptUtils.CompileRedisScripts(
		[]*redisLuaScriptUtils.RedisScript{
			scriptGetAllSiteIDs,
		}, c.redisKeys); err != nil {
		return nil, err
	}

	c.redis = redis.NewClient(c.options.RedisOptions)

	c.redis_subscriber = redis.NewClient(c.options.RedisOptions)
	c.redis_subscriber_context, c.redis_subscriber_cancel_func = context.WithCancel(ctx)
	c.redis_subscriber_handle = c.redis_subscriber.Subscribe(c.redis_subscriber_context, c.keyPubsubChannel)

	go (func(subscriber_channel <-chan *redis.Message) {
		// Listen for client timeout messages
		for msg := range subscriber_channel {
			var packet RedisReplicaManagerUpdate

			if err := json.Unmarshal([]byte(msg.Payload), &packet); err != nil {
				log.Warn().
					Str("payload", msg.Payload).
					Err(err).
					Msg("Failed parsing cluster update notification message")
			} else {
				log.Trace().
					Str("event", packet.Event).
					Str("SiteID", packet.SiteID).
					Str("SlotID", packet.SlotID).
					Str("reason", packet.Reason).
					Msg("Received cluster update notification message")

				if c.options.UpdateNotificationHandler != nil {
					if err := c.options.UpdateNotificationHandler(c.redis_subscriber_context, &packet); err != nil {
						log.Warn().
							Str("payload", msg.Payload).
							Err(err).
							Msg("Failed handling cluster update notification message")
					}
				}

				if c.subscriber_channel != nil {
					c.subscriber_channel <- &packet
				}
			}
		}

		if c.subscriber_channel != nil {
			close(c.subscriber_channel)
			c.subscriber_channel = nil
		}
	})(c.redis_subscriber_handle.Channel())

	init_packet := RedisReplicaManagerUpdate{
		Event:  "site_init",
		SiteID: c.options.SiteID,
		SlotID: "",
		Role:   "",
	}
	if init_json, err := json.Marshal(init_packet); err != nil {
		c.redis_subscriber_cancel_func()
		c.redis_subscriber_handle.Close()

		c.redis.Close()
		c.redis_subscriber.Close()

		return nil, err
	} else {
		if err := c.redis.Publish(ctx, c.keyPubsubChannel, string(init_json)).Err(); err != nil {
			c.redis_subscriber_cancel_func()
			c.redis_subscriber_handle.Close()

			c.redis.Close()
			c.redis_subscriber.Close()

			return nil, err
		}
	}

	c._updateSiteTimestamp(ctx, c.options.SiteID)

	c.housekeep_context, c.housekeep_cancelFunc = context.WithCancel(ctx)
	go (func() {
		housekeepingInterval := time.Duration(c.options.SiteTimeout / 4)

		ticker := time.NewTicker(housekeepingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.housekeep_context.Done():
				return
			case <-ticker.C:
				c._housekeep(c.housekeep_context)
			}
		}
	})()

	return c, nil
}

func (c *redisReplicaManagerClient) GetAllKnownSites(ctx context.Context) (*[]*RedisReplicaManagerSite, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := make(redisLuaScriptUtils.RedisScriptArguments)
	if response, err := c.callGetAllSiteIDs.Run(ctx, c.redis, &args).Slice(); err != nil {
		return nil, err
	} else {
		result := response[0].([]interface{})

		sites := make([]*RedisReplicaManagerSite, len(result)/2)

		for index := 0; index < len(result); index += 2 {
			if shardId, err := strconv.ParseUint(result[index+1].(string), 10, 32); err != nil {
				return nil, err
			} else {
				sites[index/2] = &RedisReplicaManagerSite{
					SiteID:  result[index].(string),
					ShardID: uint32(shardId),
				}
			}
		}

		return &sites, nil
	}
}

func (c *redisReplicaManagerClient) GetSiteID() string {
	return c.options.SiteID
}

func (c *redisReplicaManagerClient) GetShardID() uint32 {
	return c.options.ShardID
}

func (c *redisReplicaManagerClient) AddSlot(ctx context.Context, slotId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c._addSiteSlot(ctx, c.options.SiteID, slotId)
}

func (c *redisReplicaManagerClient) RemoveSlot(ctx context.Context, slotId string, minReplicaCount int, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c._removeSiteSlot(ctx, c.options.SiteID, slotId, minReplicaCount, reason)
}

func (c *redisReplicaManagerClient) GetSlots(ctx context.Context) (*[]*RedisReplicaManagerSiteSlot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c._getSiteSlots(ctx, c.options.SiteID)
}

func (c *redisReplicaManagerClient) GetSlot(ctx context.Context, slotId string) (*RedisReplicaManagerSiteSlot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c._getSlot(ctx, c.options.SiteID, slotId)
}

func (c *redisReplicaManagerClient) Channel() <-chan *RedisReplicaManagerUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscriber_channel == nil {
		c.subscriber_channel = make(chan *RedisReplicaManagerUpdate, 1)
	}

	return c.subscriber_channel
}

func (c *redisReplicaManagerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	shutdown_packet := RedisReplicaManagerUpdate{
		Event:  "site_shutdown",
		SiteID: c.options.SiteID,
		SlotID: "",
		Role:   "",
		Reason: "shutdown",
	}
	shutdown_json, _ := json.Marshal(shutdown_packet)
	c.redis.Publish(context.Background(), c.keyPubsubChannel, string(shutdown_json)).Result()

	err0 := c._removeAllSiteSlots(context.Background(), c.options.SiteID, "shutdown")

	// time.Sleep(time.Second)

	c.redis_subscriber_cancel_func()
	c.redis_subscriber_handle.Close()

	err1 := c.redis_subscriber.Close()

	err2 := c.redis.Close()

	if err0 != nil {
		return err0
	}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}

	return nil
}

func (c *redisReplicaManagerClient) keyGlobalLock(tag string) string {
	return fmt.Sprintf("%s::lock::%s", c.options.RedisKeyPrefix, tag)
}

func (c *redisReplicaManagerClient) _lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "SET", c.keyGlobalLock(tag), "sys$global", "NX", "PX", timeout.Milliseconds()).Err()

	return err
}

func (c *redisReplicaManagerClient) _extend_lock(ctx context.Context, tag string, timeout time.Duration) error {
	err := c.redis.Do(ctx, "PEXPIRE", c.keyGlobalLock(tag), timeout.Milliseconds()).Err()

	return err
}

func (c *redisReplicaManagerClient) _housekeep(ctx context.Context) {
	if !c.options.ManualHeartbeat {
		c._updateSiteTimestamp(ctx, c.options.SiteID)
	}

	c._removeTimedOutSites(ctx)
}

func (c *redisReplicaManagerClient) _removeTimedOutSites(ctx context.Context) error {
	lockKey := "_removeTimedOutSites"
	lockTimeout := time.Duration(c.options.SiteTimeout / 2)

	if err := c._lock(ctx, lockKey, lockTimeout); err != nil {
		return err
	}

	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argOldestTimestamp"] = currentTimestamp() - c.options.SiteTimeout.Milliseconds()

	if response, err := c.callGetTimedOutSites.Run(ctx, c.redis, &args).Slice(); err != nil {
		return err
	} else {
		sites := response[0].([]interface{})

		for _, site := range sites {
			siteId := site.([]interface{})[0].(string)
			slots := site.([]interface{})[1]

			shutdown_packet := RedisReplicaManagerUpdate{
				Event:  "site_shutdown",
				SiteID: siteId,
				SlotID: "",
				Role:   "",
				Reason: "timeout",
			}
			shutdown_json, _ := json.Marshal(shutdown_packet)
			c.redis.Publish(ctx, c.keyPubsubChannel, string(shutdown_json)).Result()

			for _, slotId := range slots.([]interface{}) {
				c._removeSiteSlot(ctx, siteId, slotId.(string), 0, "timeout")
				c._extend_lock(ctx, lockKey, lockTimeout)
			}
		}
	}

	return nil
}

func (c *redisReplicaManagerClient) _removeSiteSlot(ctx context.Context, siteId string, slotId string, minReplicaCount int, reason string) error {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argSiteID"] = siteId
	args["argShardID"] = c.options.ShardID
	args["argSlotID"] = slotId
	args["argMinReplicaCount"] = minReplicaCount
	args["argReason"] = reason
	args["argCurrentTimestamp"] = currentTimestamp()

	if response, err := c.callConditionalRemoveSlotSite.Run(ctx, c.redis, &args).Slice(); err != nil {
		log.Error().
			Str("SiteID", siteId).
			Str("SlotID", slotId).
			Str("action", "request_remove_site_slot").
			Str("reason", reason).
			Err(err).
			Msg("Failed to requested slot to be removed from site")

		return err
	} else {
		result := response[0].([]interface{})

		removedSitesCount := result[0].(int64)
		newReplicaCount := result[1].(int64)
		newMasterSiteID := ""
		if len(result) > 2 && result[2] != nil {
			newMasterSiteID = result[2].(string)
		}

		log.Info().
			Str("SiteID", siteId).
			Str("SlotID", slotId).
			Int64("removedSitesCount", removedSitesCount).
			Int64("newReplicaCount", newReplicaCount).
			Str("newMasterSiteID", newMasterSiteID).
			Str("action", "request_remove_site_slot").
			Str("reason", reason).
			Msg("Requested slot to be removed from site")

		return nil
	}
}

func (c *redisReplicaManagerClient) _addSiteSlot(ctx context.Context, siteId string, slotId string) error {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argSiteID"] = siteId
	args["argShardID"] = c.options.ShardID
	args["argSlotID"] = slotId
	args["argCurrentTimestamp"] = currentTimestamp()

	if response, err := c.callAddSlotSite.Run(ctx, c.redis, &args).Slice(); err != nil {
		log.Error().
			Str("SiteID", siteId).
			Str("SlotID", slotId).
			Str("action", "request_add_site_slot").
			Err(err).
			Msg("Failed to requested slot to be added to site")

		return err
	} else {
		result := response[0].([]interface{})

		added := result[0].(int64)

		if added > 0 {
			newSiteRole := result[1].(string)
			newReplicaCount := result[2].(int64)

			log.Info().
				Str("SiteID", siteId).
				Str("SlotID", slotId).
				Str("newSiteRole", newSiteRole).
				Int64("newReplicaCount", newReplicaCount).
				Str("action", "request_add_site_slot").
				Msg("Requested slot to be added to site")
		} else {
			log.Warn().
				Str("SiteID", siteId).
				Str("SlotID", slotId).
				Str("action", "request_add_site_slot").
				Msg("Request slot to be added to site resulted in a NOOP")
		}

		return nil
	}
}

func (c *redisReplicaManagerClient) _updateSiteTimestamp(ctx context.Context, siteId string) error {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argSiteID"] = siteId
	args["argShardID"] = c.options.ShardID
	args["argCurrentTimestamp"] = currentTimestamp()

	if err := c.callUpdateSiteTimestampSnippet.Run(ctx, c.redis, &args).Err(); err != nil {
		log.Error().
			Str("SiteID", siteId).
			Str("action", "request_update_site_timestamp").
			Err(err).
			Msg("Failed updating site timestamp (failed sending heartbeat)")

		return err
	} else {
		log.Trace().
			Str("SiteID", siteId).
			Str("action", "request_update_site_timestamp").
			Err(err).
			Msg("Updated site timestamp (heartbeat)")

		return nil
	}
}

func (c *redisReplicaManagerClient) _getSlot(ctx context.Context, siteId string, slotId string) (*RedisReplicaManagerSiteSlot, error) {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argSiteID"] = siteId
	args["argShardID"] = c.options.ShardID
	args["argSlotID"] = slotId
	args["argCurrentTimestamp"] = currentTimestamp()

	if response, err := c.callGetSiteSlotInfo.Run(ctx, c.redis, &args).Slice(); err != nil {
		log.Error().
			Str("SiteID", siteId).
			Str("SlotID", slotId).
			Str("action", "request_site_slot_info").
			Err(err).
			Msg("Failed requesting site slot information")

		return nil, err
	} else {
		result := response[0].([]interface{})

		if len(result) == 0 {
			err = fmt.Errorf("Slot '%s' on site '%s' not found", slotId, siteId)

			log.Error().
				Str("SiteID", siteId).
				Str("SlotID", slotId).
				Str("action", "request_site_slot_info").
				Err(err).
				Msg("Failed requesting site slot information")

			return nil, err
		} else {
			return &RedisReplicaManagerSiteSlot{
				SiteID: siteId,
				SlotID: slotId,
				Role:   result[0].(string),
			}, nil
		}
	}
}

func (c *redisReplicaManagerClient) _getSiteSlots(ctx context.Context, siteId string) (*[]*RedisReplicaManagerSiteSlot, error) {
	args := make(redisLuaScriptUtils.RedisScriptArguments)
	args["argSiteID"] = siteId
	args["argShardID"] = c.options.ShardID

	if response, err := c.callGetSiteSlots.Run(ctx, c.redis, &args).Slice(); err != nil {
		log.Error().
			Str("SiteID", siteId).
			Str("action", "request_site_slots_info").
			Err(err).
			Msg("Failed requesting site slots information")

		return nil, err
	} else {
		result := response[0].([]interface{})

		slots := make([]*RedisReplicaManagerSiteSlot, len(result))

		for slotIndex, slot := range result {
			parts := slot.([]interface{})

			role := ROLE_SLAVE
			if parts[1] != nil {
				role = parts[1].(string)
			}

			slots[slotIndex] = &RedisReplicaManagerSiteSlot{
				SiteID: siteId,
				SlotID: parts[0].(string),
				Role:   role,
			}
		}

		return &slots, nil
	}
}

func (c *redisReplicaManagerClient) _removeAllSiteSlots(ctx context.Context, siteId string, reason string) error {
	if slots, err := c._getSiteSlots(ctx, siteId); err != nil {
		return err
	} else {
		for _, slot := range *slots {
			if removeErr := c._removeSiteSlot(ctx, slot.SiteID, slot.SlotID, 0, reason); removeErr != nil {
				log.Error().
					Str("SiteID", slot.SiteID).
					Str("SlotID", slot.SlotID).
					Str("role", slot.Role).
					Str("action", "request_remove_all_site_slots").
					Err(removeErr).
					Msg("Failed requesting removal of site slot during remove all site slots operation, this error should self correct by the site timeout watchdog")
			}
		}

		return nil
	}
}
