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
		clock: hlc.New(),
		now:   uint64(0),
	}
}

// ID returns the shard identifier.
func (s *Shard) ID() string {
	return s.id
}

// Tick generates the next HLC timestamp.
func (s *Shard) Tick() uint64 {
	s.now = s.clock.Tick()
	return s.now
}

// CurrentTime returns the current HLC timestamp.
func (s *Shard) CurrentTime() uint64 {
	return s.now
}

// Update updates the clock with an incoming timestamp.
func (s *Shard) Update(incoming uint64) uint64 {
	s.now = s.clock.Update(incoming)
	return s.now
}

// Put stores a key-value pair with automatic HLC timestamping.
func (s *Shard) Put(key, value string) (uint64, error) {
	ts := s.Tick()
	err := s.store.Put(key, value, ts)
	if err != nil {
		return 0, err
	}
	return ts, nil
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

// Delete marks a key as deleted with automatic HLC timestamping.
func (s *Shard) Delete(key string) (uint64, error) {
	ts := s.clock.Tick()
	err := s.store.Delete(key, ts)
	if err != nil {
		return 0, err
	}
	return ts, nil
}

// DeleteWithVersion marks a key as deleted at a specific version.
func (s *Shard) DeleteWithVersion(key string, version uint64) error {
	return s.store.Delete(key, version)
}

// GC performs garbage collection on versions older than minVersion.
func (s *Shard) GC(minVersion uint64) int {
	return s.store.GC(minVersion)
}
