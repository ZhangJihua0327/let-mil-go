package component

import (
	"errors"

	"github.com/let-mil-go/internal/hlc"
	"github.com/let-mil-go/internal/model"
	pb "github.com/let-mil-go/proto/shardpb"
)

var (
	ErrTxAlreadyExists = errors.New("transaction already exists")
	ErrTxNotFound      = errors.New("transaction not found")
	ErrTxNoOps         = errors.New("transaction has no operations")
)

// Shard represents a single data shard in the distributed database.
// It acts as the main entry point for shard operations and manages transaction lifecycle.
type Shard struct {
	id          string
	store       *MVCCStore
	clock       *hlc.Clock
	bufferMgr   *TxBufferManager
	lockMgr     *LockManager
	txStatusTbl *model.TxStatusTable
	oplog       OpLog
}

// NewShard creates a new shard with the given ID.
func NewShard(id string) *Shard {
	return &Shard{
		id:          id,
		store:       NewMVCCStore(),
		clock:       hlc.GetClock(),
		bufferMgr:   NewTxBufferManager(),
		lockMgr:     NewLockManager(),
		txStatusTbl: model.NewTxStatusTable(),
		oplog:       NewNoOpLog(),
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

func (s *Shard) HlcTick() uint64 {
	return s.clock.Tick()
}

func (s *Shard) HlcUpdate(now uint64) uint64 {
	return s.clock.Update(now)
}

func (s *Shard) HlcNow() uint64 {
	return s.clock.Now()
}

// TxStart initializes a new transaction.
func (s *Shard) TxStart(txId string) error {
	// Check if transaction already exists
	if _, ok := s.bufferMgr.Get(txId); ok {
		return ErrTxAlreadyExists
	}

	// Create buffer for this transaction
	s.bufferMgr.Begin(txId)
	return nil
}

// TxRead reads a value within a transaction context.
func (s *Shard) TxRead(txId, key string, snapshotTime uint64) (string, uint64, bool, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return "", 0, false, ErrTxNotFound
	}

	// Check write buffer first (read-your-writes)
	if op, found := buf.GetWrite(key); found {
		if op.Deleted {
			return "", 0, false, nil
		}
		// Return original version for internal consistency check if needed
		return op.Value, op.OriginalVersion, true, nil
	}

	// Read from store at snapshot time (external read)
	value, version, _, err := s.store.Get(key, snapshotTime)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrVersionNotFound) {
			// Record external read even if not found (for SER validation)
			buf.RecordRead(key, 0, false)
			return "", 0, false, nil
		}
		return "", 0, false, err
	}

	// Record external read for SER isolation level validation
	buf.RecordRead(key, version, true)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       key,
		Value:     value,
		OpType:    OpTypeRead,
	})

	return value, version, true, nil
}

// TxWrite buffers a write operation.
func (s *Shard) TxWrite(txId, key, value string) (uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return 0, ErrTxNotFound
	}

	// Get original version of the key
	var originalVersion uint64 = 0
	_, version, _, err := s.store.GetLatest(key)
	if err == nil {
		originalVersion = version
	}

	buf.PutWrite(key, value, originalVersion)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       key,
		Value:     value,
		OpType:    OpTypeWrite,
	})

	return originalVersion, nil
}

// TxDelete buffers a delete operation.
func (s *Shard) TxDelete(txId, key string) (uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return 0, ErrTxNotFound
	}

	var originalVersion uint64 = 0
	_, version, _, err := s.store.GetLatest(key)
	if err == nil {
		originalVersion = version
	}

	buf.PutDelete(key, originalVersion)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       key,
		Value:     "",
		OpType:    OpTypeDelete,
	})
	return originalVersion, nil
}

// Prepare handles the 2PC prepare phase.
func (s *Shard) Prepare(txId string, isoLevel pb.IsolationLevel) (pb.Vote, uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return pb.Vote_VOTE_ABORT, 0, ErrTxNotFound
	}

	if len(buf.WriteOps()) == 0 && len(buf.ReadRecords()) == 0 {
		s.abortCleanup(txId)
		return pb.Vote_VOTE_ABORT, 0, ErrTxNoOps
	}

	s.txStatusTbl.Begin(txId)

	abortAndCleanup := func() (pb.Vote, uint64, error) {
		s.abortCleanup(txId)
		return pb.Vote_VOTE_ABORT, 0, nil
	}

	// Acquire write locks
	for _, key := range buf.WriteKeys() {
		if err := s.lockMgr.AcquireWrite(txId, key); err != nil {
			return abortAndCleanup()
		}
	}

	// CAS Validation
	if isoLevel == pb.IsolationLevel_ISOLATION_SI || isoLevel == pb.IsolationLevel_ISOLATION_SER {
		for _, op := range buf.WriteOps() {
			_, currentVersion, _, err := s.store.GetLatest(op.Key)
			if err != nil {
				if op.OriginalVersion != 0 {
					return abortAndCleanup()
				}
			} else {
				if currentVersion != op.OriginalVersion {
					return abortAndCleanup()
				}
			}
		}
	}

	if isoLevel == pb.IsolationLevel_ISOLATION_SER {
		for _, r := range buf.ReadRecords() {
			_, currentVersion, _, err := s.store.GetLatest(r.Key)
			if err != nil {
				if r.Found {
					return abortAndCleanup()
				}
			} else {
				if !r.Found {
					return abortAndCleanup()
				}
				if currentVersion != r.Version {
					return abortAndCleanup()
				}
			}
		}
	}

	prepareTime := s.HlcTick()
	s.txStatusTbl.Prepare(txId, prepareTime)

	return pb.Vote_VOTE_COMMIT, prepareTime, nil
}

// Commit handles the 2PC commit phase.
func (s *Shard) Commit(txId string, commitTime uint64) error {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return ErrTxNotFound
	}

	for _, op := range buf.WriteOps() {
		if op.Deleted {
			s.store.Delete(op.Key, commitTime, txId)
		} else {
			s.store.Put(op.Key, op.Value, commitTime, txId)
		}
	}

	s.clock.Update(commitTime)
	s.cleanup(txId)
	s.txStatusTbl.Commit(txId)
	s.oplog.Append(&OpEntry{
		Timestamp: commitTime,
		TxId:      txId,
		Key:       "",
		Value:     "",
		OpType:    OpTypeCommit,
	})

	return nil
}

// Abort handles the 2PC abort phase.
func (s *Shard) Abort(txId string) error {
	s.abortCleanup(txId)
	return nil
}

func (s *Shard) abortCleanup(txId string) {
	s.cleanup(txId)
	s.txStatusTbl.Abort(txId)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       "",
		Value:     "",
		OpType:    OpTypeAbort,
	})
}

func (s *Shard) cleanup(txId string) {
	s.lockMgr.Release(txId)
	s.bufferMgr.Remove(txId)
}
