package component

// OpType represents the type of operation in the oplog.
type OpType int

const (
	OpTypeWrite OpType = iota
	OpTypeDelete
	OpTypeRead
	OpTypeCommit
	OpTypeAbort
	OpTypeRelease
)

// OpEntry represents a single operation log entry.
type OpEntry struct {
	Timestamp uint64 // HLC timestamp when the operation arrived at shard
	TxId      string // Transaction ID that performed this operation
	Key       string
	Value     string // Empty for delete operations
	OpType    OpType
}

// OpLog defines the interface for operation logging.
// Implementations should persist operations for replication and recovery.
type OpLog interface {
	// Append adds a new operation entry to the log.
	// Returns the assigned log sequence number (LSN).
	Append(entry *OpEntry) (uint64, error)

	// GetFrom retrieves all entries starting from the given timestamp.
	GetFrom(timestamp uint64) ([]*OpEntry, error)

	// GetRange retrieves entries within the timestamp range [start, end].
	GetRange(start, end uint64) ([]*OpEntry, error)

	// LastTimestamp returns the timestamp of the last entry.
	LastTimestamp() (uint64, error)
}

// NoOpLog is a no-operation implementation of OpLog.
// Used as placeholder until actual implementation is provided.
type NoOpLog struct{}

func NewNoOpLog() *NoOpLog {
	return &NoOpLog{}
}

func (l *NoOpLog) Append(entry *OpEntry) (uint64, error) {
	return 0, nil
}

func (l *NoOpLog) GetFrom(timestamp uint64) ([]*OpEntry, error) {
	return nil, nil
}

func (l *NoOpLog) GetRange(start, end uint64) ([]*OpEntry, error) {
	return nil, nil
}

func (l *NoOpLog) LastTimestamp() (uint64, error) {
	return 0, nil
}
