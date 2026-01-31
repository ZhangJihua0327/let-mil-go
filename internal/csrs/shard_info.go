package csrs

import (
	"fmt"
	"sync"
)

// ShardInfo represents the metadata of a single shard.
type ShardInfo struct {
	ShardID     string // Unique shard identifier (e.g., "shard0001")
	Address     string // Network address in format "host:port"
	ReplicaSet  string // Replica set name for high availability
	Priority    int    // Node priority within replica set
	Status      ShardStatus
}

// ShardStatus represents the operational status of a shard.
type ShardStatus int

const (
	ShardStatusUnknown ShardStatus = iota
	ShardStatusActive
	ShardStatusDraining
	ShardStatusOffline
)

func (s ShardStatus) String() string {
	switch s {
	case ShardStatusActive:
		return "ACTIVE"
	case ShardStatusDraining:
		return "DRAINING"
	case ShardStatusOffline:
		return "OFFLINE"
	default:
		return "UNKNOWN"
	}
}

// ShardRegistry maintains a thread-safe registry of active shards.
// Maps shardID -> ShardInfo for O(1) lookup.
type ShardRegistry struct {
	mu     sync.RWMutex
	shards map[string]*ShardInfo
}

// NewShardRegistry creates a new empty shard registry.
func NewShardRegistry() *ShardRegistry {
	return &ShardRegistry{
		shards: make(map[string]*ShardInfo),
	}
}

// Register adds or updates a shard in the registry.
// Returns true if this is a new shard, false if updating existing.
func (r *ShardRegistry) Register(info *ShardInfo) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.shards[info.ShardID]
	r.shards[info.ShardID] = info
	return !exists
}

// Unregister removes a shard from the registry.
// Returns the removed ShardInfo, or nil if not found.
func (r *ShardRegistry) Unregister(shardID string) *ShardInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.shards[shardID]
	if !exists {
		return nil
	}
	delete(r.shards, shardID)
	return info
}

// Get retrieves shard info by ID.
// Returns nil if shard not found.
func (r *ShardRegistry) Get(shardID string) *ShardInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.shards[shardID]
}

// GetAddress retrieves the network address for a shard.
// Returns empty string if shard not found.
func (r *ShardRegistry) GetAddress(shardID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if info, ok := r.shards[shardID]; ok {
		return info.Address
	}
	return ""
}

// GetActive returns all shards with Active status.
func (r *ShardRegistry) GetActive() []*ShardInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var active []*ShardInfo
	for _, info := range r.shards {
		if info.Status == ShardStatusActive {
			active = append(active, info)
		}
	}
	return active
}

// GetAll returns a copy of all registered shards.
func (r *ShardRegistry) GetAll() map[string]*ShardInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*ShardInfo, len(r.shards))
	for k, v := range r.shards {
		result[k] = v
	}
	return result
}

// Count returns the number of registered shards.
func (r *ShardRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.shards)
}

// UpdateStatus updates the status of a specific shard.
// Returns error if shard not found.
func (r *ShardRegistry) UpdateStatus(shardID string, status ShardStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.shards[shardID]
	if !exists {
		return fmt.Errorf("shard %s not found", shardID)
	}
	info.Status = status
	return nil
}
