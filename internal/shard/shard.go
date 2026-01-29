package shard

import (
	"github.com/let-mil-go/internal/common/hlc"
)

// Shard represents a single data shard in the distributed database.
type Shard struct {
	id    string
	store *MVCCStore
	clock *hlc.Clock
	now   uint64
}

// NewShard creates a new shard with the given ID.
func NewShard(id string) *Shard {
	return &Shard{
		id:    id,
		store: NewMVCCStore(),
		now:   uint64(0),
	}
}

// ID returns the shard identifier.
func (s *Shard) ID() string {
	return s.id
}

// Put stores a key-value pair with a specific version and transaction ID.
func (s *Shard) Put(key, value string, version uint64, txId string) error {
	return s.store.Put(key, value, version, txId)
}

// Get retrieves the value for a key at the specified version.
// Returns value, version, txId, and error.
func (s *Shard) Get(key string, version uint64) (string, uint64, string, error) {
	return s.store.Get(key, version)
}

// GetLatest retrieves the latest value for a key.
// Returns value, version, txId, and error.
func (s *Shard) GetLatest(key string) (string, uint64, string, error) {
	return s.store.GetLatest(key)
}

// Delete marks a key as deleted at a specific version with transaction ID.
func (s *Shard) Delete(key string, version uint64, txId string) error {
	return s.store.Delete(key, version, txId)
}

// GC performs garbage collection on versions older than minVersion.
func (s *Shard) GC(minVersion uint64) int {
	return s.store.GC(minVersion)
}
