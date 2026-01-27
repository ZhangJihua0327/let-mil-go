package shard

import (
	"sync/atomic"
)

// Shard represents a single data shard in the distributed database.
type Shard struct {
	id        string
	store     *MVCCStore
	versionTS uint64
}

// NewShard creates a new shard with the given ID.
func NewShard(id string) *Shard {
	return &Shard{
		id:        id,
		store:     NewMVCCStore(),
		versionTS: 0,
	}
}

// ID returns the shard identifier.
func (s *Shard) ID() string {
	return s.id
}

// NextVersion atomically increments and returns the next version timestamp.
func (s *Shard) NextVersion() uint64 {
	return atomic.AddUint64(&s.versionTS, 1)
}

// CurrentVersion returns the current version timestamp.
func (s *Shard) CurrentVersion() uint64 {
	return atomic.LoadUint64(&s.versionTS)
}

// SetVersion sets the version timestamp (used for synchronization).
func (s *Shard) SetVersion(v uint64) {
	atomic.StoreUint64(&s.versionTS, v)
}

// Put stores a key-value pair with automatic versioning.
func (s *Shard) Put(key, value string) (uint64, error) {
	version := s.NextVersion()
	err := s.store.Put(key, value, version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

// PutWithVersion stores a key-value pair with a specific version.
func (s *Shard) PutWithVersion(key, value string, version uint64) error {
	return s.store.Put(key, value, version)
}

// Get retrieves the value for a key at the specified version.
func (s *Shard) Get(key string, version uint64) (string, uint64, error) {
	return s.store.Get(key, version)
}

// GetLatest retrieves the latest value for a key.
func (s *Shard) GetLatest(key string) (string, uint64, error) {
	return s.store.GetLatest(key)
}

// Delete marks a key as deleted with automatic versioning.
func (s *Shard) Delete(key string) (uint64, error) {
	version := s.NextVersion()
	err := s.store.Delete(key, version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

// DeleteWithVersion marks a key as deleted at a specific version.
func (s *Shard) DeleteWithVersion(key string, version uint64) error {
	return s.store.Delete(key, version)
}

// GC performs garbage collection on versions older than minVersion.
func (s *Shard) GC(minVersion uint64) int {
	return s.store.GC(minVersion)
}
