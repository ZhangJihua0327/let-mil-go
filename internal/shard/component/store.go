package component

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrVersionNotFound = errors.New("version not found")
	ErrVersionConflict = errors.New("version conflict")
	ErrPendingRead     = errors.New("pending read blocked")
)

// VersionedValue represents a value with its version metadata.
type VersionedValue struct {
	Value       string
	Version     uint64
	Deleted     bool
	TxId        string // Transaction ID that created this version
	Pending     bool   // True if this version is pending (2PC not yet committed)
	PrepareTime uint64 // HLC timestamp when this version was prepared (0 if committed)
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
// If pending is true, the value is marked as pending (2PC prepare phase).
// If the version already exists, returns ErrVersionConflict.
func (s *MVCCStore) Put(key, value string, version uint64, txId string, pending bool, prepareTime uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.data[key]
	idx := s.findVersionIndex(versions, version)

	if idx < len(versions) && versions[idx].Version == version {
		return ErrVersionConflict
	}

	vv := VersionedValue{
		Value:       value,
		Version:     version,
		Deleted:     false,
		TxId:        txId,
		Pending:     pending,
		PrepareTime: prepareTime,
	}

	// Insert at correct position to maintain sorted order
	versions = append(versions, VersionedValue{})
	copy(versions[idx+1:], versions[idx:])
	versions[idx] = vv
	s.data[key] = versions

	return nil
}

// Get retrieves the value for a key at the specified snapshot time.
// This method handles pending versions:
//   - If snapshotTime < prepareTime of pending version: skip pending, read older version
//   - If snapshotTime >= prepareTime of pending version: return ErrPendingRead (caller should block/retry)
//
// Returns value, version, txId, and error.
func (s *MVCCStore) Get(key string, snapshotTime uint64) (string, uint64, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok || len(versions) == 0 {
		return "", 0, "", ErrKeyNotFound
	}

	// Find the latest version <= snapshotTime, skipping pending versions appropriately
	for i := len(versions) - 1; i >= 0; i-- {
		vv := versions[i]

		// Skip versions newer than snapshotTime
		if vv.Version > snapshotTime {
			continue
		}

		// Check if this is a pending version
		if vv.Pending {
			// If snapshotTime >= prepareTime, reader should be blocked waiting for commit/abort
			if snapshotTime >= vv.PrepareTime {
				return "", 0, "", ErrPendingRead
			}
			// If snapshotTime < prepareTime, skip this pending version and look for older ones
			continue
		}

		// Found a committed version <= snapshotTime
		if vv.Deleted {
			return "", 0, "", ErrKeyNotFound
		}
		return vv.Value, vv.Version, vv.TxId, nil
	}

	return "", 0, "", ErrVersionNotFound
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
// If pending is true, the deletion is marked as pending (2PC prepare phase).
func (s *MVCCStore) Delete(key string, version uint64, txId string, pending bool, prepareTime uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.data[key]
	idx := s.findVersionIndex(versions, version)

	if idx < len(versions) && versions[idx].Version == version {
		return ErrVersionConflict
	}

	vv := VersionedValue{
		Value:       "",
		Version:     version,
		Deleted:     true,
		TxId:        txId,
		Pending:     pending,
		PrepareTime: prepareTime,
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

// ConfirmPending confirms pending versions for a transaction, marking them as committed.
// Updates the version to the commit time.
func (s *MVCCStore) ConfirmPending(txId string, commitTime uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, versions := range s.data {
		modified := false
		for i := range versions {
			if versions[i].TxId == txId && versions[i].Pending {
				// Remove old pending entry and create new committed entry
				oldVersion := versions[i]
				// Remove this entry
				versions = append(versions[:i], versions[i+1:]...)
				modified = true

				// Insert at correct position with commitTime
				idx := s.findVersionIndex(versions, commitTime)
				vv := VersionedValue{
					Value:       oldVersion.Value,
					Version:     commitTime,
					Deleted:     oldVersion.Deleted,
					TxId:        txId,
					Pending:     false,
					PrepareTime: 0,
				}
				versions = append(versions, VersionedValue{})
				copy(versions[idx+1:], versions[idx:])
				versions[idx] = vv
				break // One pending entry per tx per key
			}
		}
		if modified {
			s.data[key] = versions
		}
	}
}

// RemovePending removes all pending versions for a transaction (used on abort).
func (s *MVCCStore) RemovePending(txId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, versions := range s.data {
		filtered := versions[:0]
		for _, v := range versions {
			if !(v.TxId == txId && v.Pending) {
				filtered = append(filtered, v)
			}
		}
		if len(filtered) != len(versions) {
			s.data[key] = filtered
		}
	}
}
