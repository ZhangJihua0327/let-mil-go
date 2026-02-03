package component

import (
	"sync"
)

// WriteOp represents a pending write operation in the buffer.
type WriteOp struct {
	Key             string
	Value           string
	Deleted         bool
	OriginalVersion uint64 // Version of key before this write (0 if key not exists)
}

// ReadRecord represents a read operation with its version.
type ReadRecord struct {
	Key     string
	Version uint64 // Version read (0 if key not found)
	Found   bool
}

// TxBuffer stores pending writes and read records for a single transaction.
type TxBuffer struct {
	txId         string
	snapshotTime uint64                 // HLC timestamp for snapshot reads
	writes       map[string]*WriteOp    // key -> latest write op
	writeOrder   []string               // insertion order for deterministic apply
	reads        map[string]*ReadRecord // key -> read record (external reads only)
}

// NewTxBuffer creates a new transaction buffer with snapshot time.
func NewTxBuffer(txId string, snapshotTime uint64) *TxBuffer {
	return &TxBuffer{
		txId:         txId,
		snapshotTime: snapshotTime,
		writes:       make(map[string]*WriteOp),
		writeOrder:   make([]string, 0),
		reads:        make(map[string]*ReadRecord),
	}
}

// PutWrite adds a write operation to the buffer with original version.
func (b *TxBuffer) PutWrite(key, value string, originalVersion uint64) {
	if _, exists := b.writes[key]; !exists {
		b.writeOrder = append(b.writeOrder, key)
	}
	b.writes[key] = &WriteOp{
		Key:             key,
		Value:           value,
		Deleted:         false,
		OriginalVersion: originalVersion,
	}
}

// PutDelete adds a delete operation to the buffer with original version.
func (b *TxBuffer) PutDelete(key string, originalVersion uint64) {
	if _, exists := b.writes[key]; !exists {
		b.writeOrder = append(b.writeOrder, key)
	}
	b.writes[key] = &WriteOp{
		Key:             key,
		Value:           "",
		Deleted:         true,
		OriginalVersion: originalVersion,
	}
}

// GetWrite retrieves a pending write for a key (for read-your-writes).
func (b *TxBuffer) GetWrite(key string) (*WriteOp, bool) {
	op, ok := b.writes[key]
	return op, ok
}

// RecordRead records an external read operation.
func (b *TxBuffer) RecordRead(key string, version uint64, found bool) {
	// Only record if not already in write set (no need to track read-your-writes)
	if _, inWriteSet := b.writes[key]; !inWriteSet {
		b.reads[key] = &ReadRecord{
			Key:     key,
			Version: version,
			Found:   found,
		}
	}
}

// WriteKeys returns all keys in the write buffer.
func (b *TxBuffer) WriteKeys() []string {
	return b.writeOrder
}

// WriteOps returns all write operations in insertion order.
func (b *TxBuffer) WriteOps() []*WriteOp {
	result := make([]*WriteOp, 0, len(b.writeOrder))
	for _, key := range b.writeOrder {
		result = append(result, b.writes[key])
	}
	return result
}

// ReadRecords returns all read records.
func (b *TxBuffer) ReadRecords() []*ReadRecord {
	result := make([]*ReadRecord, 0, len(b.reads))
	for _, r := range b.reads {
		result = append(result, r)
	}
	return result
}

// TxId returns the transaction ID.
func (b *TxBuffer) TxId() string {
	return b.txId
}

// SnapshotTime returns the snapshot timestamp for this transaction.
func (b *TxBuffer) SnapshotTime() uint64 {
	return b.snapshotTime
}

// HasWrite checks if a key is in the write set.
func (b *TxBuffer) HasWrite(key string) bool {
	_, ok := b.writes[key]
	return ok
}

// TxBufferManager manages transaction buffers for all active transactions.
type TxBufferManager struct {
	mu      sync.RWMutex
	buffers map[string]*TxBuffer // txId -> buffer
}

// NewTxBufferManager creates a new transaction buffer manager.
func NewTxBufferManager() *TxBufferManager {
	return &TxBufferManager{
		buffers: make(map[string]*TxBuffer),
	}
}

// Start creates a new buffer for a transaction with snapshot time.
func (m *TxBufferManager) Start(txId string, snapshotTime uint64) *TxBuffer {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf := NewTxBuffer(txId, snapshotTime)
	m.buffers[txId] = buf
	return buf
}

// Get retrieves the buffer for a transaction.
func (m *TxBufferManager) Get(txId string) (*TxBuffer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	buf, ok := m.buffers[txId]
	return buf, ok
}

// Remove removes the buffer for a transaction.
func (m *TxBufferManager) Remove(txId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.buffers, txId)
}
