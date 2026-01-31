package csrs

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the default number of virtual nodes per shard.
	// More virtual nodes provide better distribution but use more memory.
	DefaultVirtualNodes = 150
)

// HashRing implements consistent hashing for key-to-shard routing.
// Uses virtual nodes to ensure even distribution across shards.
type HashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         []uint32          // Sorted hash values
	nodeMap      map[uint32]string // hash -> shardID
	shardSet     map[string]bool   // Track which shards are in the ring
}

// NewHashRing creates a new hash ring with the default number of virtual nodes.
func NewHashRing() *HashRing {
	return NewHashRingWithVNodes(DefaultVirtualNodes)
}

// NewHashRingWithVNodes creates a new hash ring with custom virtual node count.
func NewHashRingWithVNodes(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &HashRing{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		shardSet:     make(map[string]bool),
	}
}

// AddShard adds a shard to the hash ring.
// Returns true if the shard was added, false if it already exists.
func (h *HashRing) AddShard(shardID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.shardSet[shardID] {
		return false
	}

	for i := 0; i < h.virtualNodes; i++ {
		hash := h.hashKey(virtualNodeKey(shardID, i))
		h.ring = append(h.ring, hash)
		h.nodeMap[hash] = shardID
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})

	h.shardSet[shardID] = true
	return true
}

// RemoveShard removes a shard from the hash ring.
// Returns true if the shard was removed, false if it didn't exist.
func (h *HashRing) RemoveShard(shardID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.shardSet[shardID] {
		return false
	}

	// Remove all virtual nodes for this shard
	newRing := make([]uint32, 0, len(h.ring)-h.virtualNodes)
	for _, hash := range h.ring {
		if h.nodeMap[hash] != shardID {
			newRing = append(newRing, hash)
		} else {
			delete(h.nodeMap, hash)
		}
	}
	h.ring = newRing
	delete(h.shardSet, shardID)
	return true
}

// GetShardForKey returns the shard ID responsible for the given key.
// Returns empty string if the ring is empty.
func (h *HashRing) GetShardForKey(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := h.hashKey(key)
	idx := h.search(hash)
	return h.nodeMap[h.ring[idx]]
}

// GetNShards returns N unique shards for the given key (for replication).
// Returns fewer shards if fewer are available.
func (h *HashRing) GetNShards(key string, n int) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	shardCount := len(h.shardSet)
	if shardCount == 0 {
		return nil
	}
	if n > shardCount {
		n = shardCount
	}

	hash := h.hashKey(key)
	idx := h.search(hash)

	result := make([]string, 0, n)
	seen := make(map[string]bool)

	for len(result) < n {
		shardID := h.nodeMap[h.ring[idx]]
		if !seen[shardID] {
			seen[shardID] = true
			result = append(result, shardID)
		}
		idx = (idx + 1) % len(h.ring)
	}

	return result
}

// Contains checks if a shard is in the ring.
func (h *HashRing) Contains(shardID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.shardSet[shardID]
}

// ShardCount returns the number of shards in the ring.
func (h *HashRing) ShardCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.shardSet)
}

// GetAllShards returns all shard IDs in the ring.
func (h *HashRing) GetAllShards() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	shards := make([]string, 0, len(h.shardSet))
	for shardID := range h.shardSet {
		shards = append(shards, shardID)
	}
	return shards
}

// search finds the first ring position >= hash using binary search.
// Returns 0 if hash > all ring positions (wrap around).
func (h *HashRing) search(hash uint32) int {
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})
	if idx >= len(h.ring) {
		return 0
	}
	return idx
}

// hashKey computes a 32-bit hash of the key using SHA-256.
func (h *HashRing) hashKey(key string) uint32 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(sum[:4])
}

// virtualNodeKey generates a unique key for a virtual node.
func virtualNodeKey(shardID string, index int) string {
	return shardID + "#" + string(rune('0'+index/100)) + string(rune('0'+(index%100)/10)) + string(rune('0'+index%10))
}
