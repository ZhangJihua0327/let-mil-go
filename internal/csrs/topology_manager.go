package csrs

import (
	"fmt"
	"sync"

	"github.com/let-mil-go/internal/hlc"
)

// RouterInfo holds metadata about a registered router.
type RouterInfo struct {
	RouterID string
	Address  string
}

// TopologyManager is the central component for managing cluster topology.
// It acts as the Config Server (source of truth) similar to MongoDB's CSRS.
type TopologyManager struct {
	mu              sync.RWMutex
	registry        *ShardRegistry
	hashRing        *HashRing
	topologyVersion uint64
	clock           *hlc.Clock
	routers         map[string]*RouterInfo // routerID -> RouterInfo
	nextRouterID    uint64
}

// NewTopologyManager creates a new TopologyManager instance.
func NewTopologyManager() *TopologyManager {
	return &TopologyManager{
		registry: NewShardRegistry(),
		hashRing: NewHashRing(),
		clock:    hlc.GetClock(),
		routers:  make(map[string]*RouterInfo),
	}
}

// NewTopologyManagerWithClock creates a new TopologyManager with a custom HLC clock (for testing).
func NewTopologyManagerWithClock(clock *hlc.Clock) *TopologyManager {
	return &TopologyManager{
		registry: NewShardRegistry(),
		hashRing: NewHashRing(),
		clock:    clock,
		routers:  make(map[string]*RouterInfo),
	}
}

// AddShard registers a new shard with the topology.
// Returns the new topology version if successful.
func (tm *TopologyManager) AddShard(info *ShardInfo) (uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if info.ShardID == "" {
		return tm.topologyVersion, fmt.Errorf("shard ID cannot be empty")
	}
	if info.Address == "" {
		return tm.topologyVersion, fmt.Errorf("shard address cannot be empty")
	}

	isNew := tm.registry.Register(info)
	if isNew {
		tm.hashRing.AddShard(info.ShardID)
		tm.incrementVersion()
	}

	return tm.topologyVersion, nil
}

// RemoveShard removes a shard from the topology.
// Returns the new topology version if successful.
func (tm *TopologyManager) RemoveShard(shardID string) (uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	info := tm.registry.Unregister(shardID)
	if info == nil {
		return tm.topologyVersion, fmt.Errorf("shard %s not found", shardID)
	}

	tm.hashRing.RemoveShard(shardID)
	tm.incrementVersion()

	return tm.topologyVersion, nil
}

// UpdateShardStatus updates the status of a shard.
// Returns the new topology version.
func (tm *TopologyManager) UpdateShardStatus(shardID string, status ShardStatus) (uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if err := tm.registry.UpdateStatus(shardID, status); err != nil {
		return tm.topologyVersion, err
	}

	tm.incrementVersion()
	return tm.topologyVersion, nil
}

// GetShardForKey returns the shard responsible for a given key.
func (tm *TopologyManager) GetShardForKey(key string) (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	shardID := tm.hashRing.GetShardForKey(key)
	if shardID == "" {
		return "", fmt.Errorf("no shards available")
	}
	return shardID, nil
}

// GetShardAddressForKey returns the network address of the shard responsible for a key.
func (tm *TopologyManager) GetShardAddressForKey(key string) (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	shardID := tm.hashRing.GetShardForKey(key)
	if shardID == "" {
		return "", fmt.Errorf("no shards available")
	}

	addr := tm.registry.GetAddress(shardID)
	if addr == "" {
		return "", fmt.Errorf("shard %s not found in registry", shardID)
	}

	return addr, nil
}

// GetNShardsForKey returns N shards for a key (useful for replication).
func (tm *TopologyManager) GetNShardsForKey(key string, n int) ([]string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	shards := tm.hashRing.GetNShards(key, n)
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}
	return shards, nil
}

// GetShardInfo returns the full info for a shard.
func (tm *TopologyManager) GetShardInfo(shardID string) *ShardInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.registry.Get(shardID)
}

// GetAllShards returns info for all registered shards.
func (tm *TopologyManager) GetAllShards() map[string]*ShardInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.registry.GetAll()
}

// GetActiveShards returns all shards with Active status.
func (tm *TopologyManager) GetActiveShards() []*ShardInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.registry.GetActive()
}

// GetTopologyVersion returns the current topology version (HLC timestamp).
// Routers can use this to detect if their cached view is stale.
func (tm *TopologyManager) GetTopologyVersion() uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.topologyVersion
}

// IsVersionStale checks if a given version is older than the current topology version.
func (tm *TopologyManager) IsVersionStale(version uint64) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return version < tm.topologyVersion
}

// GetTopologySnapshot returns a snapshot of the current topology state.
// Used by routers to sync their cached state.
type TopologySnapshot struct {
	Version uint64
	Shards  map[string]*ShardInfo
}

func (tm *TopologyManager) GetTopologySnapshot() *TopologySnapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return &TopologySnapshot{
		Version: tm.topologyVersion,
		Shards:  tm.registry.GetAll(),
	}
}

// ShardCount returns the number of registered shards.
func (tm *TopologyManager) ShardCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.registry.Count()
}

// GetShardAddressMap returns a map of shard-name to ip:port for all registered shards.
// This provides a global view of shard locations for routing and discovery.
func (tm *TopologyManager) GetShardAddressMap() map[string]string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	allShards := tm.registry.GetAll()
	result := make(map[string]string, len(allShards))
	for shardID, info := range allShards {
		result[shardID] = info.Address
	}
	return result
}

// BumpTopologyVersion manually increments the topology version.
func (tm *TopologyManager) BumpTopologyVersion() uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.incrementVersion()
	return tm.topologyVersion
}

// incrementVersion bumps the topology version using HLC.
// Must be called with tm.mu held.
func (tm *TopologyManager) incrementVersion() {
	tm.topologyVersion = tm.clock.Tick()
}

// RegisterRouter registers a new router and returns a unique router ID.
func (tm *TopologyManager) RegisterRouter(address string) (string, uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if address == "" {
		return "", tm.topologyVersion, fmt.Errorf("router address cannot be empty")
	}

	tm.nextRouterID++
	routerID := fmt.Sprintf("router_%d", tm.nextRouterID)

	tm.routers[routerID] = &RouterInfo{
		RouterID: routerID,
		Address:  address,
	}

	return routerID, tm.topologyVersion, nil
}

// UnregisterRouter removes a router from the registry.
func (tm *TopologyManager) UnregisterRouter(routerID string) (uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.routers[routerID]; !exists {
		return tm.topologyVersion, fmt.Errorf("router %s not found", routerID)
	}

	delete(tm.routers, routerID)
	return tm.topologyVersion, nil
}

// GetRouterInfo returns the info for a registered router.
func (tm *TopologyManager) GetRouterInfo(routerID string) *RouterInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.routers[routerID]
}

// GetAllRouters returns all registered routers.
func (tm *TopologyManager) GetAllRouters() map[string]*RouterInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make(map[string]*RouterInfo, len(tm.routers))
	for id, info := range tm.routers {
		result[id] = info
	}
	return result
}
