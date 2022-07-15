package redisReplicaManager

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

type ReplicaBalancer interface {
	AddSite(ctx context.Context, siteId string) error
	RemoveSite(ctx context.Context, siteId string) error

	GetSites() *[]string

	GetTargetSlotsForSite(ctx context.Context, siteId string) *[]uint32
	GetSlotSites(ctx context.Context, slotId uint32) *[]string

	GetTotalSitesCount() uint32
	GetTotalSlotsCount() uint32
	GetSlotReplicaCount() uint32

	GetSlotForObject(objectId string) uint32
}

type siteSlotsBalancer struct {
	ReplicaBalancer

	mu   sync.RWMutex
	opts *ReplicaBalancerOptions

	sites           map[string]bool
	totalSitesCount uint32

	sitesSlotsMatrixCache *[]*[]string
}

func NewReplicaBalancer(ctx context.Context, opts *ReplicaBalancerOptions) (ReplicaBalancer, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	c := &siteSlotsBalancer{
		opts:  opts,
		sites: make(map[string]bool),
	}

	return c, nil
}

func (c *siteSlotsBalancer) GetSlotForObject(objectId string) uint32 {
	hasher := md5.New()
	hasher.Write([]byte(objectId))

	hex := hex.EncodeToString(hasher.Sum(nil))
	id, _ := strconv.ParseInt(hex[:4], 16, 0)

	return uint32(id) % uint32(c.GetTotalSlotsCount())
}

func (c *siteSlotsBalancer) GetSites() *[]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, len(c.sites))
	index := 0

	for siteId, _ := range c.sites {
		result[index] = siteId
		index++
	}

	return &result
}

func (c *siteSlotsBalancer) GetSlotReplicaCount() uint32 {
	return uint32(c.opts.SlotReplicaCount)
}

func (c *siteSlotsBalancer) GetTotalSlotsCount() uint32 {
	return uint32(c.opts.TotalSlotsCount)
}

func (c *siteSlotsBalancer) GetTotalSitesCount() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.totalSitesCount
}

func (c *siteSlotsBalancer) AddSite(ctx context.Context, siteId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sites[siteId] {
		return fmt.Errorf("Duplicate site: %v", siteId)
	}

	c.sitesSlotsMatrixCache = nil

	c.sites[siteId] = true

	c.totalSitesCount = uint32(len(c.sites))

	return nil
}

func (c *siteSlotsBalancer) RemoveSite(ctx context.Context, siteId string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.sites[siteId] {
		return fmt.Errorf("Unknown site: %v", siteId)
	}

	c.sitesSlotsMatrixCache = nil

	delete(c.sites, siteId)

	c.totalSitesCount = uint32(len(c.sites))

	return nil
}

func (c *siteSlotsBalancer) GetSlotSites(ctx context.Context, slotId uint32) *[]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.totalSitesCount < uint32(c.opts.MinimumSitesCount) {
		// Minimum sites count condition is not satisfied - cluster is not large enough for expected load
		return &[]string{}
	}

	if slotId >= uint32(c.opts.TotalSlotsCount) {
		// Slot out of bounds
		return &[]string{}
	}

	matrix := c.getSitesSlotsMatrix()

	sites := (*matrix)[slotId]

	return sites
}

func (c *siteSlotsBalancer) GetTargetSlotsForSite(ctx context.Context, siteId string) *[]uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.totalSitesCount < uint32(c.opts.MinimumSitesCount) {
		// Minimum sites count condition is not satisfied - cluster is not large enough for expected load
		return &[]uint32{}
	}

	matrix := c.getSitesSlotsMatrix()

	slots := c.getSiteSlots(matrix, siteId)

	return slots
}

func (c *siteSlotsBalancer) getAllSlotsSitesMap(matrix *[]*[]string) *map[uint32]*[]string {
	result := make(map[uint32]*[]string)

	for slotId, sites := range *matrix {
		result[uint32(slotId)] = sites
	}

	return &result
}

func (c *siteSlotsBalancer) getSiteSlots(matrix *[]*[]string, siteId string) *[]uint32 {
	result := []uint32{}

	for slotId, sites := range *matrix {
		for index := 0; index < len(*sites) && index < c.opts.SlotReplicaCount; index++ {
			if (*sites)[index] == siteId {
				result = append(result, uint32(slotId))
			}
		}
	}

	return &result
}

type slotSiteHash struct {
	hash   uint32
	siteId string
}

func (c *siteSlotsBalancer) getSitesSlotsMatrix() *[]*[]string {
	if c.sitesSlotsMatrixCache != nil {
		return c.sitesSlotsMatrixCache
	}

	result := make([]*[]string, c.opts.TotalSlotsCount)

	for slotId := uint32(0); slotId < uint32(c.opts.TotalSlotsCount); slotId++ {
		var list []*slotSiteHash

		for siteId, _ := range c.sites {
			list = append(list, &slotSiteHash{
				hash:   calcSiteSlotHash(siteId, slotId, uint32(c.opts.TotalSlotsCount)),
				siteId: siteId,
			})
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].hash < list[j].hash
		})

		numSites := len(list)
		if numSites > c.opts.SlotReplicaCount {
			numSites = c.opts.SlotReplicaCount
		}
		sites := make([]string, numSites)

		for index, site := range list[:numSites] {
			sites[index] = site.siteId
		}

		result[slotId] = &sites
	}

	c.sitesSlotsMatrixCache = &result

	return &result
}

func calcSiteSlotHash(siteId string, slotId uint32, totalSlots uint32) uint32 {
	hasher := md5.New()
	hasher.Write([]byte(fmt.Sprintf("%v:%v:%v", siteId, slotId, totalSlots)))

	hex := hex.EncodeToString(hasher.Sum(nil))
	id, _ := strconv.ParseInt(hex[:4], 16, 0)

	return uint32(id + 1)
}
