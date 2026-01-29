package shard

import (
	"errors"
	"sync"
)

var (
	ErrLockConflict  = errors.New("lock conflict")
	ErrLockNotHeld   = errors.New("lock not held by transaction")
	ErrDeadlock      = errors.New("deadlock detected")
)

// LockMode represents the type of lock.
type LockMode int

const (
	LockNone LockMode = iota
	LockRead          // Shared lock (S)
	LockWrite         // Exclusive lock (X)
)

func (m LockMode) String() string {
	switch m {
	case LockNone:
		return "NONE"
	case LockRead:
		return "READ"
	case LockWrite:
		return "WRITE"
	default:
		return "UNKNOWN"
	}
}

// lockEntry represents the lock state for a single key.
type lockEntry struct {
	mode    LockMode
	holders map[string]struct{} // txIds holding the lock
	waiters []string            // txIds waiting for the lock
}

// LockManager manages key-level locks for transactions.
type LockManager struct {
	mu    sync.Mutex
	locks map[string]*lockEntry // key -> lock entry
	txLocks map[string]map[string]LockMode // txId -> key -> mode
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks:   make(map[string]*lockEntry),
		txLocks: make(map[string]map[string]LockMode),
	}
}

// AcquireRead attempts to acquire a read lock on a key.
func (m *LockManager) AcquireRead(txId, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.getOrCreateEntry(key)

	// Check if already holding this lock
	if m.isHolding(txId, key, LockRead) || m.isHolding(txId, key, LockWrite) {
		return nil
	}

	// Read lock compatible with other read locks
	if entry.mode == LockNone || entry.mode == LockRead {
		entry.mode = LockRead
		entry.holders[txId] = struct{}{}
		m.recordTxLock(txId, key, LockRead)
		return nil
	}

	// Write lock held by another transaction
	if _, ok := entry.holders[txId]; !ok {
		return ErrLockConflict
	}

	return nil
}

// AcquireWrite attempts to acquire a write lock on a key.
func (m *LockManager) AcquireWrite(txId, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.getOrCreateEntry(key)

	// Check if already holding write lock
	if m.isHolding(txId, key, LockWrite) {
		return nil
	}

	// No lock held
	if entry.mode == LockNone {
		entry.mode = LockWrite
		entry.holders[txId] = struct{}{}
		m.recordTxLock(txId, key, LockWrite)
		return nil
	}

	// Upgrade from read to write (only if sole holder)
	if entry.mode == LockRead {
		if len(entry.holders) == 1 {
			if _, ok := entry.holders[txId]; ok {
				entry.mode = LockWrite
				m.recordTxLock(txId, key, LockWrite)
				return nil
			}
		}
		return ErrLockConflict
	}

	// Write lock already held
	if _, ok := entry.holders[txId]; ok {
		return nil // Already holding write lock
	}

	return ErrLockConflict
}

// Release releases all locks held by a transaction.
func (m *LockManager) Release(txId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	locks, ok := m.txLocks[txId]
	if !ok {
		return
	}

	for key := range locks {
		entry, ok := m.locks[key]
		if !ok {
			continue
		}

		delete(entry.holders, txId)
		if len(entry.holders) == 0 {
			entry.mode = LockNone
		}
	}

	delete(m.txLocks, txId)
}

// ReleaseKey releases a specific key lock held by a transaction.
func (m *LockManager) ReleaseKey(txId, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.locks[key]
	if !ok {
		return
	}

	delete(entry.holders, txId)
	if len(entry.holders) == 0 {
		entry.mode = LockNone
	}

	if locks, ok := m.txLocks[txId]; ok {
		delete(locks, key)
	}
}

// GetLockMode returns the current lock mode for a key.
func (m *LockManager) GetLockMode(key string) LockMode {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.locks[key]
	if !ok {
		return LockNone
	}
	return entry.mode
}

// GetTxLocks returns all locks held by a transaction.
func (m *LockManager) GetTxLocks(txId string) map[string]LockMode {
	m.mu.Lock()
	defer m.mu.Unlock()

	locks, ok := m.txLocks[txId]
	if !ok {
		return nil
	}

	result := make(map[string]LockMode, len(locks))
	for k, v := range locks {
		result[k] = v
	}
	return result
}

// IsLocked checks if a key is locked.
func (m *LockManager) IsLocked(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.locks[key]
	if !ok {
		return false
	}
	return entry.mode != LockNone
}

func (m *LockManager) getOrCreateEntry(key string) *lockEntry {
	entry, ok := m.locks[key]
	if !ok {
		entry = &lockEntry{
			mode:    LockNone,
			holders: make(map[string]struct{}),
			waiters: make([]string, 0),
		}
		m.locks[key] = entry
	}
	return entry
}

func (m *LockManager) isHolding(txId, key string, mode LockMode) bool {
	locks, ok := m.txLocks[txId]
	if !ok {
		return false
	}
	held, ok := locks[key]
	if !ok {
		return false
	}
	return held == mode || (mode == LockRead && held == LockWrite)
}

func (m *LockManager) recordTxLock(txId, key string, mode LockMode) {
	locks, ok := m.txLocks[txId]
	if !ok {
		locks = make(map[string]LockMode)
		m.txLocks[txId] = locks
	}
	locks[key] = mode
}
