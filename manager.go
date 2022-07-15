package redisReplicaManager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/yourbasic/bit"
)

type RouteTableEntry struct {
	SlotID uint32
	SiteID string
	Role   string
}

type LocalSiteManager interface {
	RequestAddSlot(ctx context.Context, slotId uint32) (bool, error)
	RequestRemoveSlot(ctx context.Context, slotId uint32) (bool, error)

	GetSlotIdentifiers(ctx context.Context) (*[]uint32, error)

	GetSlotForObject(objectId string) uint32

	GetSlotRouteTable(ctx context.Context, slotId uint32) *[]*RouteTableEntry
	GetSlotPrimarySiteRoute(ctx context.Context, slotId uint32) *RouteTableEntry

	IsLocalSitePrimaryForSlot(ctx context.Context, slotId uint32) (bool, error)
	GetAllSlotsLocalSiteIsPrimaryFor(ctx context.Context) (*[]uint32, error)

	Close() error
}

type localSiteManager struct {
	LocalSiteManager

	mu sync.RWMutex

	opts *ClusterNodeManagerOptions

	housekeep_context    context.Context
	housekeep_cancelFunc context.CancelFunc

	slots           map[uint32]bool
	primarySlots    *bit.Set
	slotsRoutingMap *map[uint32][]*RouteTableEntry

	siteId string

	housekeep_done_channel chan bool
}

func (c *localSiteManager) formatSlotId(slotId uint32) string {
	return fmt.Sprintf("slot-%v", slotId)
}

func (c *localSiteManager) parseSlotId(slotId string) (uint32, error) {
	result := int(0)
	if _, err := fmt.Sscanf(slotId, "slot-%d", &result); err != nil {
		return 0, err
	} else {
		return uint32(result), nil
	}
}

func NewLocalSiteManager(ctx context.Context, opts *ClusterNodeManagerOptions) (LocalSiteManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	slotsRoutingMap := make(map[uint32][]*RouteTableEntry)

	c := &localSiteManager{
		opts:            opts,
		slots:           make(map[uint32]bool),
		slotsRoutingMap: &slotsRoutingMap,
		primarySlots:    bit.New(),
		siteId:          opts.ReplicaManagerClient.GetSiteID(),
	}

	c._housekeep(ctx)

	c.housekeep_done_channel = make(chan bool)
	c.housekeep_context, c.housekeep_cancelFunc = context.WithCancel(ctx)
	go (func(ctx context.Context) {
		housekeepingInterval := time.Duration(c.opts.RefreshInterval)

		ticker := time.NewTicker(housekeepingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.housekeep_context.Done():
				c.housekeep_done_channel <- true
				close(c.housekeep_done_channel)
				return
			case packet := <-c.opts.ReplicaManagerClient.Channel():
				c._inspectReplicaManagerClientPacket(ctx, packet)
			case <-ticker.C:
				c._housekeep(ctx)
			}
		}
	})(c.housekeep_context)

	return c, nil
}

func (c *localSiteManager) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.housekeep_cancelFunc()

	<-c.housekeep_done_channel

	c.primarySlots = bit.New()
	c.slots = make(map[uint32]bool)

	return c.opts.ReplicaManagerClient.Close()
}

func (c *localSiteManager) GetSlotForObject(objectId string) uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.opts.ReplicaBalancer.GetSlotForObject(objectId)
}

func (c *localSiteManager) GetSlotRouteTable(ctx context.Context, slotId uint32) *[]*RouteTableEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.slotsRoutingMap == nil {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}
	}

	if c.slotsRoutingMap == nil {
		return nil
	}

	sites := (*c.slotsRoutingMap)[slotId]

	if len(sites) < 1 {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}

		sites = (*c.slotsRoutingMap)[slotId]
	}

	return &sites
}

func (c *localSiteManager) GetSlotPrimarySiteRoute(ctx context.Context, slotId uint32) *RouteTableEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.slotsRoutingMap == nil {
		if err := c._evalSlotsRouting(ctx); err != nil {
			return nil
		}
	}

	if c.slotsRoutingMap == nil {
		return nil
	}

	sites := (*c.slotsRoutingMap)[slotId]

	for _, site := range sites {
		if site.Role == ROLE_PRIMARY {
			return site
		}
	}

	// Didn't find master: search again in case the routing table needs a refresh

	if err := c._evalSlotsRouting(ctx); err != nil {
		return nil
	}

	for _, site := range sites {
		if site.Role == ROLE_PRIMARY {
			return site
		}
	}

	return nil
}

func (c *localSiteManager) GetAllSlotsLocalSiteIsPrimaryFor(ctx context.Context) (*[]uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := []uint32{}

	c.primarySlots.Visit(func(n int) bool {
		result = append(result, uint32(n))

		return false
	})

	return &result, nil
}

func (c *localSiteManager) IsLocalSitePrimaryForSlot(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.primarySlots.Contains(int(slotId)), nil
}

func (c *localSiteManager) GetSlotIdentifiers(ctx context.Context) (*[]uint32, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]uint32, len(c.slots))

	index := 0
	for slotId, _ := range c.slots {
		result[index] = slotId
		index++
	}

	return &result, nil
}

func (c *localSiteManager) RequestAddSlot(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.slots[slotId] {
		if err := c.opts.ReplicaManagerClient.AddSlot(ctx, c.formatSlotId(slotId)); err != nil {
			return false, err
		}

		c.slots[slotId] = true
	}

	return true, nil
}

func (c *localSiteManager) RequestRemoveSlot(ctx context.Context, slotId uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.slots[slotId] {
		if err := c.opts.ReplicaManagerClient.RemoveSlot(
			ctx,
			c.formatSlotId(slotId),
			int(c.opts.ReplicaBalancer.GetSlotReplicaCount()),
			"moved"); err != nil {
			// Minimum replica count is not satisfied, can't remove this slot yet
			return false, err
		}

		delete(c.slots, slotId)
	}

	return true, nil
}

func (c *localSiteManager) _housekeep(ctx context.Context) error {
	liveSites := &[]*RedisReplicaManagerSite{}

	if liveSitesResponse, err := c.opts.ReplicaManagerClient.GetLiveSites(ctx); err != nil {
		return err
	} else {
		liveSites = liveSitesResponse
	}

	c.mu.RLock()

	// Step 1: make sure we are using an up-to-date list of sites

	knownSites := c.opts.ReplicaBalancer.GetSites()
	knownSitesMap := make(map[string]bool)
	for _, siteId := range *knownSites {
		knownSitesMap[siteId] = true
	}

	wantSitesMap := make(map[string]bool)
	for _, site := range *liveSites {
		wantSitesMap[site.SiteID] = true
		if !knownSitesMap[site.SiteID] {
			// We don't have this site
			c.opts.ReplicaBalancer.AddSite(ctx, site.SiteID)
		}
	}

	for siteId, _ := range knownSitesMap {
		if !wantSitesMap[siteId] {
			// We think this site exists, but it no longer does
			c.opts.ReplicaBalancer.RemoveSite(ctx, siteId)
		}
	}

	// Step 2: make sure slots are correctly distributed between sites

	wantSlots := c.opts.ReplicaBalancer.GetTargetSlotsForSite(ctx, c.opts.ReplicaManagerClient.GetSiteID())
	wantSlotsMap := make(map[uint32]bool)

	missingSlots := []uint32{}
	redundantSlots := []uint32{}

	for _, slotId := range *wantSlots {
		wantSlotsMap[slotId] = true

		if !c.slots[slotId] {
			// Missing
			missingSlots = append(missingSlots, slotId)
		}
	}

	for slotId, _ := range c.slots {
		if !wantSlotsMap[slotId] {
			// Redundant
			redundantSlots = append(redundantSlots, slotId)
		}
	}

	c.mu.RUnlock()

	if len(redundantSlots) > 0 && c.opts.NotifyRedundantSlotsHandler != nil {
		c.opts.NotifyRedundantSlotsHandler(ctx, c, &redundantSlots)
	}

	if len(missingSlots) > 0 && c.opts.NotifyMissingSlotsHandler != nil {
		c.opts.NotifyMissingSlotsHandler(ctx, c, &missingSlots)
	}

	if err := c._evalPrimarySlots(ctx); err != nil {
		log.Error().
			Str("action", "housekeep_eval_local_primary_slots").
			Err(err).
			Msg("Failed evaluating changes to list of slots local site is the primary site for")
	}

	c.mu.RLock()
	if err := c._evalSlotsRouting(ctx); err != nil {
		log.Error().
			Str("action", "housekeep_refresh_slots_routing_table").
			Err(err).
			Msg("Failed refreshing slots routing table")
	}
	c.mu.RUnlock()

	return nil
}

func (c *localSiteManager) _inspectReplicaManagerClientPacket(ctx context.Context, packet *RedisReplicaManagerUpdate) error {
	if packet.Event == "slot_primary_change" {
		if slotId, err := c.parseSlotId(packet.SlotID); err != nil {
			return err
		} else {
			c.mu.Lock()
			hasChange := false
			hasSlot := c.primarySlots.Contains(int(slotId))

			if !hasSlot && c.opts.ReplicaManagerClient.GetSiteID() == packet.SiteID {
				c.primarySlots = c.primarySlots.Add(int(slotId))

				hasChange = true
			} else if hasSlot && c.opts.ReplicaManagerClient.GetSiteID() != packet.SiteID {
				c.primarySlots = c.primarySlots.Delete(int(slotId))

				hasChange = true
			}
			c.mu.Unlock()

			if hasChange {
				if c.opts.NotifyPrimarySlotsChangedHandler != nil {
					c.opts.NotifyPrimarySlotsChangedHandler(ctx, c)
				}
			}
		}
	}

	c.mu.Lock()
	c.slotsRoutingMap = nil
	c.mu.Unlock()

	return nil
}

func (c *localSiteManager) _evalPrimarySlots(ctx context.Context) error {
	if slots, err := c.opts.ReplicaManagerClient.GetSlots(ctx); err != nil {
		return err
	} else {
		c.mu.Lock()

		hasChange := false

		for _, slot := range *slots {
			isPrimary := slot.Role == ROLE_PRIMARY
			slotId, err := c.parseSlotId(slot.SlotID)

			if err != nil {
				log.Warn().
					Str("SiteID", slot.SiteID).
					Str("SlotID", slot.SlotID).
					Str("action", "eval_local_primary_slots").
					Err(err).
					Msg("Failed parsing Slot ID")
				continue
			}

			hasSlot := c.primarySlots.Contains(int(slotId))

			if !hasSlot && isPrimary {
				c.primarySlots = c.primarySlots.Add(int(slotId))

				hasChange = true
			} else if hasSlot && !isPrimary {
				c.primarySlots = c.primarySlots.Delete(int(slotId))

				hasChange = true
			}
		}

		c.mu.Unlock()

		if hasChange {
			if c.opts.NotifyPrimarySlotsChangedHandler != nil {
				c.opts.NotifyPrimarySlotsChangedHandler(ctx, c)
			}
		}
	}

	return nil
}

func (c *localSiteManager) _evalSlotsRouting(ctx context.Context) error {
	if routes, err := c.opts.ReplicaManagerClient.GetSlotsRouting(ctx); err != nil {
		return err
	} else {
		routeTable := make(map[uint32][]*RouteTableEntry)

		for _, route := range *routes {
			if slotId, err := c.parseSlotId(route.SlotID); err != nil {
				log.Warn().
					Str("SiteID", route.SiteID).
					Str("SlotID", route.SlotID).
					Str("action", "eval_slot_routing").
					Err(err).
					Msg("Failed parsing Slot ID")
			} else {
				if _, ok := routeTable[slotId]; !ok {
					routeTable[slotId] = []*RouteTableEntry{}
				}

				routeTable[slotId] = append(routeTable[slotId], &RouteTableEntry{
					SiteID: route.SiteID,
					SlotID: slotId,
					Role:   route.Role,
				})
			}
		}

		c.slotsRoutingMap = &routeTable
	}

	return nil
}
