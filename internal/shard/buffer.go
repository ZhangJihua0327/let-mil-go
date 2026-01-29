package shard

import (
	"sync"
)

// WriteOp represents a pending write operation in the buffer.
type WriteOp struct {
	Key     string
	Value   string
	Deleted bool
}

// TxWriteBuffer stores pending writes for a single transaction.
type TxWriteBuffer struct {
	txId    string
	ops     map[string]*WriteOp // key -> latest op
	opOrder []string            // insertion order for deterministic apply
}

// NewTxWriteBuffer creates a new write buffer for a transaction.
func NewTxWriteBuffer(txId string) *TxWriteBuffer {
	return &TxWriteBuffer{
		txId:    txId,
		ops:     make(map[string]*WriteOp),
		opOrder: make([]string, 0),
	}
}

// Put adds a write operation to the buffer.
func (b *TxWriteBuffer) Put(key, value string) {
	if _, exists := b.ops[key]; !exists {
		b.opOrder = append(b.opOrder, key)
	}
	b.ops[key] = &WriteOp{Key: key, Value: value, Deleted: false}
}

// Delete adds a delete operation to the buffer.
func (b *TxWriteBuffer) Delete(key string) {
	if _, exists := b.ops[key]; !exists {
		b.opOrder = append(b.opOrder, key)
	}
	b.ops[key] = &WriteOp{Key: key, Value: "", Deleted: true}
}

// Get retrieves a pending write for a key (for read-your-writes).
func (b *TxWriteBuffer) Get(key string) (*WriteOp, bool) {
	op, ok := b.ops[key]
	return op, ok
}

// Keys returns all keys in the buffer.
func (b *TxWriteBuffer) Keys() []string {
	return b.opOrder
}

// Ops returns all operations in insertion order.
func (b *TxWriteBuffer) Ops() []*WriteOp {
	result := make([]*WriteOp, 0, len(b.opOrder))
	for _, key := range b.opOrder {
		result = append(result, b.ops[key])
	}
	return result
}

// TxId returns the transaction ID.
func (b *TxWriteBuffer) TxId() string {
	return b.txId
}

// WriteBufferManager manages write buffers for all active transactions.
type WriteBufferManager struct {
	mu      sync.RWMutex
	buffers map[string]*TxWriteBuffer // txId -> buffer
}

// NewWriteBufferManager creates a new write buffer manager.
func NewWriteBufferManager() *WriteBufferManager {
	return &WriteBufferManager{
		buffers: make(map[string]*TxWriteBuffer),
	}
}

// Begin creates a new write buffer for a transaction.
func (m *WriteBufferManager) Begin(txId string) *TxWriteBuffer {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf := NewTxWriteBuffer(txId)
	m.buffers[txId] = buf
	return buf
}

// Get retrieves the write buffer for a transaction.
func (m *WriteBufferManager) Get(txId string) (*TxWriteBuffer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	buf, ok := m.buffers[txId]
	return buf, ok
}

// Remove removes the write buffer for a transaction.
func (m *WriteBufferManager) Remove(txId string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.buffers, txId)
}

// GetOrCreate gets existing buffer or creates a new one.
func (m *WriteBufferManager) GetOrCreate(txId string) *TxWriteBuffer {
	m.mu.Lock()
	defer m.mu.Unlock()

	if buf, ok := m.buffers[txId]; ok {
		return buf
	}

	buf := NewTxWriteBuffer(txId)
	m.buffers[txId] = buf
	return buf
}
