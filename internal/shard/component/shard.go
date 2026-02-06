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
// For PC/SI/SER: uses Update(snapshotTime) to generate snapshot timestamp.
// For RA/CC/PSI: uses Tick() to generate snapshot timestamp.
func (s *Shard) TxStart(txId string, isoLevel pb.IsolationLevel, snapshotTime uint64) error {
	// Check if transaction already exists
	if _, ok := s.bufferMgr.Get(txId); ok {
		return ErrTxAlreadyExists
	}

	// Generate snapshot timestamp based on isolation level
	var ts uint64
	switch isoLevel {
	case pb.IsolationLevel_ISOLATION_PC, pb.IsolationLevel_ISOLATION_SI, pb.IsolationLevel_ISOLATION_SER:
		// Use external snapshot time for global consistency
		ts = s.clock.Update(snapshotTime)
	default:
		// RA/CC/PSI: use local tick
		ts = s.clock.Tick()
	}

	// Create buffer for this transaction with snapshot time and isolation level
	s.bufferMgr.Start(txId, ts, isoLevel, s.clock.Now())
	return nil
}

// TxRead reads a value within a transaction context.
// Uses the snapshot timestamp stored in the transaction buffer.
// If there's a pending write with prepareTime > snapshotTime, the read can skip
// the pending version and read older committed data (optimized for old snapshot reads).
// If snapshotTime >= prepareTime of a pending version, the read will be blocked (returns error).
// For SER isolation level, acquires read lock immediately to prevent conflicts.
func (s *Shard) TxRead(txId, key string) (string, uint64, bool, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return "", 0, false, ErrTxNotFound
	}

	// Check write buffer first (internal read)
	if op, found := buf.GetWrite(key); found {
		if op.Deleted {
			return "", 0, false, nil
		}
		// Return original version for internal consistency check if needed
		return op.Value, op.OriginalVersion, true, nil
	}

	// For SER isolation level, acquire read lock immediately (pessimistic locking)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
		if err := s.lockMgr.AcquireRead(txId, key); err != nil {
			return "", 0, false, err
		}
	}

	// Read from store at snapshot time
	// Handles pending versions:
	// - If snapshotTime < prepareTime: skip pending, read older version
	// - If snapshotTime >= prepareTime: return ErrPendingRead (caller should retry/wait)
	snapshotTime := buf.SnapshotTime()
	value, version, _, err := s.store.Get(key, snapshotTime)
	if err != nil {
		if err == ErrPendingRead {
			// The read is blocked by a pending write, caller should retry
			return "", 0, false, err
		}
		if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
			// Record external read even if not found (for tracking purposes)
			buf.RecordRead(key, 0, false)
		}
		return "", 0, false, err
	}

	// Record external read for SER isolation level (for tracking, not for validation)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
		buf.RecordRead(key, version, true)
	}
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
// For SER isolation level, acquires write lock immediately to prevent conflicts.
func (s *Shard) TxWrite(txId, key, value string) (uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return 0, ErrTxNotFound
	}

	// For SER isolation level, acquire write lock immediately (pessimistic locking)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
		if err := s.lockMgr.AcquireWrite(txId, key); err != nil {
			return 0, err
		}
	}

	var version uint64 = 0
	var err error = nil
	switch buf.IsoLevel() {
	case pb.IsolationLevel_ISOLATION_SI, pb.IsolationLevel_ISOLATION_SER:
		_, version, _, err = s.store.Get(key, buf.SnapshotTime())
	default:
		_, version, _, err = s.store.GetLatest(key)
	}
	if err != nil {
		version = 0
	}

	buf.PutWrite(key, value, version)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       key,
		Value:     value,
		OpType:    OpTypeWrite,
	})

	return version, nil
}

// TxDelete buffers a delete operation.
// For SER isolation level, acquires write lock immediately to prevent conflicts.
func (s *Shard) TxDelete(txId, key string) (uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return 0, ErrTxNotFound
	}

	// For SER isolation level, acquire write lock immediately (pessimistic locking)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
		if err := s.lockMgr.AcquireWrite(txId, key); err != nil {
			return 0, err
		}
	}

	var version uint64 = 0
	var err error = nil
	switch buf.IsoLevel() {
	case pb.IsolationLevel_ISOLATION_SI, pb.IsolationLevel_ISOLATION_SER:
		_, version, _, err = s.store.Get(key, buf.SnapshotTime())
	default:
		_, version, _, err = s.store.GetLatest(key)
	}
	if err != nil {
		version = 0
	}

	buf.PutDelete(key, version)
	s.oplog.Append(&OpEntry{
		Timestamp: s.HlcTick(),
		TxId:      txId,
		Key:       key,
		Value:     "",
		OpType:    OpTypeDelete,
	})
	return version, nil
}

// Prepare handles the 2PC prepare phase.
// After acquiring locks and validation, it pre-writes pending data to the MVCC store.
// The pending data will be confirmed on commit or removed on abort.
// For SER isolation level, locks are already acquired during read/write operations.
func (s *Shard) Prepare(txId string) (pb.Vote, uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return pb.Vote_VOTE_NOT_FOUND, 0, ErrTxNotFound
	}

	hasWrites := len(buf.WriteOps()) > 0
	hasReads := len(buf.ReadRecords()) > 0

	// No operations at all
	if !hasWrites && !hasReads {
		s.cleanup(txId)
		return pb.Vote_VOTE_NOOP, 0, nil
	}

	// No writes and reads don't need validation (not SER, or SER with locks already held)
	// Can return EMPTY and skip commit phase
	needsReadValidation := buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER
	if !hasWrites && !needsReadValidation {
		s.cleanup(txId)
		return pb.Vote_VOTE_NOOP, 0, nil
	}

	s.txStatusTbl.Start(txId)

	// Generate prepareTime first for use in locks and pending writes
	prepareTime := s.HlcTick()

	abortAndCleanup := func() (pb.Vote, uint64, error) {
		s.abortCleanup(txId)
		return pb.Vote_VOTE_ABORT, 0, nil
	}

	// Acquire write locks with prepareTime (for non-SER, or for SER keys not yet locked)
	// For SER, write locks are already acquired during TxWrite/TxDelete
	if buf.IsoLevel() != pb.IsolationLevel_ISOLATION_SER {
		for _, key := range buf.WriteKeys() {
			if err := s.lockMgr.AcquireWriteWithPrepareTime(txId, key, prepareTime); err != nil {
				return abortAndCleanup()
			}
		}
	}

	// CAS Validation for writes (SI and SER)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SI || buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
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

	// CAS Validation for reads is NOT needed for SER (locks already held)
	// This validation was only needed for optimistic locking approach

	// Pre-write pending data to MVCC store
	// Use prepareTime as the temporary version (will be updated to commitTime on commit)
	for _, op := range buf.WriteOps() {
		if op.Deleted {
			if err := s.store.Delete(op.Key, prepareTime, txId, true, prepareTime); err != nil {
				return abortAndCleanup()
			}
		} else {
			if err := s.store.Put(op.Key, op.Value, prepareTime, txId, true, prepareTime); err != nil {
				return abortAndCleanup()
			}
		}
	}

	s.txStatusTbl.Prepare(txId, prepareTime)

	return pb.Vote_VOTE_COMMIT, prepareTime, nil
}

// Commit handles the 2PC commit phase.
// It confirms pending data in the MVCC store with the commit time and releases locks.
func (s *Shard) Commit(txId string, commitTime uint64) error {

	_, ok := s.bufferMgr.Get(txId)
	if !ok {
		return ErrTxNotFound
	}
	if commitTime == 0 {
		commitTime = s.HlcTick()
	} else {
		commitTime = s.HlcUpdate(commitTime)
	}

	// Confirm pending writes with the commit time
	s.store.ConfirmPending(txId, commitTime)

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

// QuickCommit handles single-shard transactions without 2PC overhead.
// It performs validation, writes data directly to the MVCC store (not pending),
// and commits in a single atomic operation.
// Returns the commit time on success, or an error if validation fails.
// For SER isolation level, locks are already acquired during read/write operations.
func (s *Shard) QuickCommit(txId string, isoLevel pb.IsolationLevel) (uint64, error) {
	buf, ok := s.bufferMgr.Get(txId)
	if !ok {
		return 0, ErrTxNotFound
	}

	hasWrites := len(buf.WriteOps()) > 0
	hasReads := len(buf.ReadRecords()) > 0

	// No operations at all
	if !hasWrites && !hasReads {
		s.cleanup(txId)
		return 0, nil
	}

	// No writes and reads don't need validation (not SER, or SER with locks already held)
	needsReadValidation := isoLevel == pb.IsolationLevel_ISOLATION_SER
	if !hasWrites && !needsReadValidation {
		s.cleanup(txId)
		return 0, nil
	}

	s.txStatusTbl.Start(txId)

	// Generate commit time
	commitTime := s.HlcTick()

	abortAndCleanup := func() (uint64, error) {
		s.cleanup(txId)
		s.txStatusTbl.Abort(txId)
		return 0, ErrVersionConflict
	}

	// Acquire write locks (no need for prepareTime since we commit immediately)
	// For SER, write locks are already acquired during TxWrite/TxDelete
	if buf.IsoLevel() != pb.IsolationLevel_ISOLATION_SER {
		for _, key := range buf.WriteKeys() {
			if err := s.lockMgr.AcquireWrite(txId, key); err != nil {
				return abortAndCleanup()
			}
		}
	}

	// CAS Validation for writes (SI and SER)
	if buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SI || buf.IsoLevel() == pb.IsolationLevel_ISOLATION_SER {
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

	// CAS Validation for reads is NOT needed for SER (locks already held)
	// This validation was only needed for optimistic locking approach

	// Write data directly to MVCC store (not pending)
	for _, op := range buf.WriteOps() {
		if op.Deleted {
			if err := s.store.Delete(op.Key, commitTime, txId, false, 0); err != nil {
				return abortAndCleanup()
			}
		} else {
			if err := s.store.Put(op.Key, op.Value, commitTime, txId, false, 0); err != nil {
				return abortAndCleanup()
			}
		}
	}

	// Cleanup and commit
	s.cleanup(txId)
	s.txStatusTbl.Commit(txId)
	s.oplog.Append(&OpEntry{
		Timestamp: commitTime,
		TxId:      txId,
		Key:       "",
		Value:     "",
		OpType:    OpTypeCommit,
	})

	return commitTime, nil
}

// Abort handles the 2PC abort phase.
// This method can be called without prior Prepare - it will clean up any
// existing transaction state (buffer, locks) gracefully.
func (s *Shard) Abort(txId string) error {
	s.abortCleanup(txId)
	return nil
}

func (s *Shard) abortCleanup(txId string) {
	// Remove pending writes from MVCC store before releasing locks
	s.store.RemovePending(txId)
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
