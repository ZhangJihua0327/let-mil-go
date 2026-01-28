package model

import "sync"

// TxStatus represents the state of a transaction.
type TxStatus int

const (
	TxRunning TxStatus = iota
	TxPrepared
	TxCommitted
	TxAborted
)

func (s TxStatus) String() string {
	switch s {
	case TxRunning:
		return "RUNNING"
	case TxPrepared:
		return "PREPARED"
	case TxCommitted:
		return "COMMITTED"
	case TxAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// TxState holds the state of a single transaction.
type TxState struct {
	Status      TxStatus
	PrepareTime uint64 // HLC timestamp
}

// TxStatusTable tracks the state of transactions being processed by a shard.
type TxStatusTable struct {
	mu     sync.RWMutex
	states map[string]*TxState // TxId -> TxState
}

// NewTxStatusTable creates a new transaction status table.
func NewTxStatusTable() *TxStatusTable {
	return &TxStatusTable{
		states: make(map[string]*TxState),
	}
}

// Begin registers a new transaction as RUNNING.
func (t *TxStatusTable) Begin(txId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states[txId] = &TxState{Status: TxRunning}
}

// Prepare marks a transaction as PREPARED with the given HLC timestamp.
func (t *TxStatusTable) Prepare(txId string, prepareTime uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[txId]; ok {
		state.Status = TxPrepared
		state.PrepareTime = prepareTime
	}
}

// Commit marks a transaction as COMMITTED.
func (t *TxStatusTable) Commit(txId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[txId]; ok {
		state.Status = TxCommitted
	}
}

// Abort marks a transaction as ABORTED.
func (t *TxStatusTable) Abort(txId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.states[txId]; ok {
		state.Status = TxAborted
	}
}

// Get retrieves the state of a transaction.
func (t *TxStatusTable) Get(txId string) (*TxState, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state, ok := t.states[txId]
	if !ok {
		return nil, false
	}
	cp := *state
	return &cp, true
}

// Remove removes a transaction from the table.
func (t *TxStatusTable) Remove(txId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, txId)
}
