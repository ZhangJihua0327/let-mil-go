package component

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrVersionNotFound = errors.New("version not found")
	ErrVersionConflict = errors.New("version conflict")
)

// VersionedValue represents a value with its version metadata.
type VersionedValue struct {
	Value   string
	Version uint64
	Deleted bool
	TxId    string // Transaction ID that created this version
}

// MVCCStore is a multi-version concurrency control store.
// Each key maps to a list of versioned values, sorted by version (ascending).
type MVCCStore struct {
	mu   sync.RWMutex
	data map[string][]VersionedValue
}

// NewMVCCStore creates a new MVCC store.
func NewMVCCStore() *MVCCStore {
	return &MVCCStore{
		data: make(map[string][]VersionedValue),
	}
}

// Put inserts or updates a key-value pair at the specified version.
// If the version already exists, returns ErrVersionConflict.
func (s *MVCCStore) Put(key, value string, version uint64, txId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.data[key]
	idx := s.findVersionIndex(versions, version)

	if idx < len(versions) && versions[idx].Version == version {
		return ErrVersionConflict
	}

	vv := VersionedValue{
		Value:   value,
		Version: version,
		Deleted: false,
		TxId:    txId,
	}

	// Insert at correct position to maintain sorted order
	versions = append(versions, VersionedValue{})
	copy(versions[idx+1:], versions[idx:])
	versions[idx] = vv
	s.data[key] = versions

	return nil
}

// Get retrieves the value for a key at the specified version.
// Returns the latest version <= requested version, along with version and txId.
func (s *MVCCStore) Get(key string, version uint64) (string, uint64, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok || len(versions) == 0 {
		return "", 0, "", ErrKeyNotFound
	}

	// Find the latest version <= requested version
	idx := s.findVersionIndex(versions, version+1) - 1
	if idx < 0 {
		return "", 0, "", ErrVersionNotFound
	}

	vv := versions[idx]
	if vv.Deleted {
		return "", 0, "", ErrKeyNotFound
	}

	return vv.Value, vv.Version, vv.TxId, nil
}

// GetExact retrieves the value for a key at exactly the specified version.
func (s *MVCCStore) GetExact(key string, version uint64) (string, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok {
		return "", "", ErrKeyNotFound
	}

	idx := s.findVersionIndex(versions, version)
	if idx >= len(versions) || versions[idx].Version != version {
		return "", "", ErrVersionNotFound
	}

	if versions[idx].Deleted {
		return "", "", ErrKeyNotFound
	}

	return versions[idx].Value, versions[idx].TxId, nil
}

// Delete marks a key as deleted at the specified version.
func (s *MVCCStore) Delete(key string, version uint64, txId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.data[key]
	idx := s.findVersionIndex(versions, version)

	if idx < len(versions) && versions[idx].Version == version {
		return ErrVersionConflict
	}

	vv := VersionedValue{
		Value:   "",
		Version: version,
		Deleted: true,
		TxId:    txId,
	}

	versions = append(versions, VersionedValue{})
	copy(versions[idx+1:], versions[idx:])
	versions[idx] = vv
	s.data[key] = versions

	return nil
}

// GetLatest retrieves the latest value for a key.
// Returns value, version, txId, and error.
func (s *MVCCStore) GetLatest(key string) (string, uint64, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok || len(versions) == 0 {
		return "", 0, "", ErrKeyNotFound
	}

	latest := versions[len(versions)-1]
	if latest.Deleted {
		return "", 0, "", ErrKeyNotFound
	}

	return latest.Value, latest.Version, latest.TxId, nil
}

// GetAllVersions returns all versions for a key.
func (s *MVCCStore) GetAllVersions(key string) ([]VersionedValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	result := make([]VersionedValue, len(versions))
	copy(result, versions)
	return result, nil
}

// GC removes versions older than the specified version for garbage collection.
func (s *MVCCStore) GC(minVersion uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for key, versions := range s.data {
		idx := s.findVersionIndex(versions, minVersion)
		if idx > 1 {
			// Keep at least one version before minVersion for snapshot reads
			removed += idx - 1
			s.data[key] = versions[idx-1:]
		}
	}
	return removed
}

// findVersionIndex returns the index where version should be inserted.
// Uses binary search for efficiency.
func (s *MVCCStore) findVersionIndex(versions []VersionedValue, version uint64) int {
	left, right := 0, len(versions)
	for left < right {
		mid := (left + right) / 2
		if versions[mid].Version < version {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}
