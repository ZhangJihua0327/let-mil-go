package shard

import (
	"context"
	"errors"

	"github.com/let-mil-go/internal/shard/component"

	"github.com/let-mil-go/internal/common/hlc"
	"github.com/let-mil-go/internal/model"
	pb "github.com/let-mil-go/proto/shardpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the ShardService gRPC server.
type Server struct {
	pb.UnimplementedShardServiceServer
	shard       *component.Shard
	bufferMgr   *component.TxBufferManager
	lockMgr     *component.LockManager
	txStatusTbl *model.TxStatusTable
	hlc         *hlc.Clock
	oplog       component.OpLog
}

// NewServer creates a new gRPC server for the given shard.
func NewServer(shard *component.Shard) *Server {
	return &Server{
		shard:       shard,
		bufferMgr:   component.NewTxBufferManager(),
		lockMgr:     component.NewLockManager(),
		txStatusTbl: model.NewTxStatusTable(),
		hlc:         hlc.GetClock(),
		oplog:       component.NewNoOpLog(),
	}
}

func (s *Server) hlcTick() uint64 {
	return s.hlc.Tick()
}

func (s *Server) hlcUpdate(now uint64) uint64 {
	return s.hlc.Update(now)
}

func (s *Server) hlcNow() uint64 {
	return s.hlc.Now()
}

// TxStart initializes a new transaction on this shard.
// Creates the transaction buffer for subsequent operations.
func (s *Server) TxStart(ctx context.Context, req *pb.TxStartRequest) (*pb.TxStartResponse, error) {
	// Check if transaction already exists
	if _, ok := s.bufferMgr.Get(req.TxId); ok {
		return nil, status.Error(codes.AlreadyExists, "transaction already exists")
	}

	// Create buffer for this transaction
	s.bufferMgr.Begin(req.TxId)

	return &pb.TxStartResponse{}, nil
}

// TxRead reads a value within a transaction context.
// External reads are recorded in the buffer for SER isolation level validation.
func (s *Server) TxRead(ctx context.Context, req *pb.TxReadRequest) (*pb.TxReadResponse, error) {
	buf, ok := s.bufferMgr.Get(req.TxId)
	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found, call TxStart first")
	}

	// Check write buffer first (read-your-writes)
	// Internal reads don't need version tracking
	if op, found := buf.GetWrite(req.Key); found {
		if op.Deleted {
			return &pb.TxReadResponse{Found: false, Version: 0}, nil
		}
		return &pb.TxReadResponse{
			Value:   op.Value,
			Version: op.OriginalVersion, // Return original version for internal consistency
			Found:   true,
		}, nil
	}

	// Read from store at snapshot time (external read)
	value, version, _, err := s.shard.Get(req.Key, req.SnapshotTime)
	if err != nil {
		if errors.Is(err, component.ErrKeyNotFound) || errors.Is(err, component.ErrVersionNotFound) {
			// Record external read even if not found (for SER validation)
			buf.RecordRead(req.Key, 0, false)
			return &pb.TxReadResponse{Found: false, Version: 0}, nil
		}
		return nil, s.convertError(err)
	}

	// Record external read for SER isolation level validation
	buf.RecordRead(req.Key, version, true)
	s.oplog.Append(&component.OpEntry{
		Timestamp: s.hlcTick(),
		TxId:      req.TxId,
		Key:       req.Key,
		Value:     value,
		OpType:    component.OpTypeRead,
	})

	return &pb.TxReadResponse{
		Value:   value,
		Version: version,
		Found:   true,
	}, nil
}

// TxWrite buffers a write operation for a transaction.
// Returns the original version of the key before this write.
func (s *Server) TxWrite(ctx context.Context, req *pb.TxWriteRequest) (*pb.TxWriteResponse, error) {
	buf, ok := s.bufferMgr.Get(req.TxId)
	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found, call TxStart first")
	}

	// Get original version of the key (for CAS validation on commit)
	var originalVersion uint64 = 0
	_, version, _, err := s.shard.GetLatest(req.Key)
	if err == nil {
		originalVersion = version
	}
	// If key not found, originalVersion remains 0

	buf.PutWrite(req.Key, req.Value, originalVersion)
	s.oplog.Append(&component.OpEntry{
		Timestamp: s.hlcTick(),
		TxId:      req.TxId,
		Key:       req.Key,
		Value:     req.Value,
		OpType:    component.OpTypeWrite,
	})

	return &pb.TxWriteResponse{OriginalVersion: originalVersion}, nil
}

// TxDelete buffers a delete operation for a transaction.
// Returns the original version of the key before this delete.
func (s *Server) TxDelete(ctx context.Context, req *pb.TxDeleteRequest) (*pb.TxDeleteResponse, error) {
	buf, ok := s.bufferMgr.Get(req.TxId)
	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found, call TxStart first")
	}

	// Get original version of the key (for CAS validation on commit)
	var originalVersion uint64 = 0
	_, version, _, err := s.shard.GetLatest(req.Key)
	if err == nil {
		originalVersion = version
	}
	// If key not found, originalVersion remains 0

	buf.PutDelete(req.Key, originalVersion)
	s.oplog.Append(&component.OpEntry{
		Timestamp: s.hlcTick(),
		TxId:      req.TxId,
		Key:       req.Key,
		Value:     "",
		OpType:    component.OpTypeDelete,
	})
	return &pb.TxDeleteResponse{OriginalVersion: originalVersion}, nil
}

// Prepare handles the 2PC prepare phase.
// For SI/SER isolation levels, validates write set with CAS check.
// For SER isolation level, also validates read set with CAS check.
func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	// Get buffer - must exist from previous TxRead/TxWrite/TxDelete calls
	buf, ok := s.bufferMgr.Get(req.TxId)
	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}

	// Check if buffer has any operations
	if len(buf.WriteOps()) == 0 && len(buf.ReadRecords()) == 0 {
		s.lockMgr.Release(req.TxId)
		s.bufferMgr.Remove(req.TxId)
		s.txStatusTbl.Abort(req.TxId)
		s.oplog.Append(&component.OpEntry{
			Timestamp: s.hlcTick(),
			TxId:      req.TxId,
			OpType:    component.OpTypeRelease,
		})
		return nil, status.Error(codes.NotFound, "transaction has no operations")
	}

	// Register transaction
	s.txStatusTbl.Begin(req.TxId)

	// Helper to abort and cleanup
	abortAndCleanup := func() *pb.PrepareResponse {
		s.lockMgr.Release(req.TxId)
		s.bufferMgr.Remove(req.TxId)
		s.txStatusTbl.Abort(req.TxId)
		s.oplog.Append(&component.OpEntry{
			Timestamp: s.hlcTick(),
			TxId:      req.TxId,
			OpType:    component.OpTypeAbort,
		})
		return &pb.PrepareResponse{Vote: pb.Vote_VOTE_ABORT}
	}

	// Acquire write locks on all keys in write buffer
	for _, key := range buf.WriteKeys() {
		if err := s.lockMgr.AcquireWrite(req.TxId, key); err != nil {
			return abortAndCleanup(), nil
		}
	}

	// CAS validation based on isolation level
	isoLevel := req.IsolationLevel

	// For SI and SER: Check that write set keys haven't been modified
	if isoLevel == pb.IsolationLevel_ISOLATION_SI || isoLevel == pb.IsolationLevel_ISOLATION_SER {
		for _, op := range buf.WriteOps() {
			_, currentVersion, _, err := s.shard.GetLatest(op.Key)
			if err != nil {
				// Key not found - original version should be 0
				if op.OriginalVersion != 0 {
					return abortAndCleanup(), nil
				}
			} else {
				// Key exists - check version hasn't changed
				if currentVersion != op.OriginalVersion {
					return abortAndCleanup(), nil
				}
			}
		}
	}

	// For SER: Additionally check that read set keys haven't been modified
	if isoLevel == pb.IsolationLevel_ISOLATION_SER {
		for _, r := range buf.ReadRecords() {
			_, currentVersion, _, err := s.shard.GetLatest(r.Key)
			if err != nil {
				// Key not found now
				if r.Found {
					return abortAndCleanup(), nil
				}
			} else {
				// Key exists now
				if !r.Found {
					return abortAndCleanup(), nil
				}
				// Key was found during read - check version
				if currentVersion != r.Version {
					return abortAndCleanup(), nil
				}
			}
		}
	}

	// Generate prepare timestamp
	prepareTime := s.hlcTick()
	s.txStatusTbl.Prepare(req.TxId, prepareTime)

	return &pb.PrepareResponse{
		Vote:        pb.Vote_VOTE_COMMIT,
		PrepareTime: prepareTime,
	}, nil
}

// Commit handles the 2PC commit phase.
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	buf, ok := s.bufferMgr.Get(req.TxId)
	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}

	// Apply all buffered writes with commit timestamp and transaction ID
	for _, op := range buf.WriteOps() {
		if op.Deleted {
			s.shard.Delete(op.Key, req.CommitTime, req.TxId)
		} else {
			s.shard.Put(op.Key, op.Value, req.CommitTime, req.TxId)
		}
	}

	// Update HLC with commit time
	s.hlc.Update(req.CommitTime)

	// Cleanup
	s.lockMgr.Release(req.TxId)
	s.bufferMgr.Remove(req.TxId)
	s.txStatusTbl.Commit(req.TxId)
	s.oplog.Append(&component.OpEntry{
		Timestamp: req.CommitTime,
		TxId:      req.TxId,
		Key:       "",
		Value:     "",
		OpType:    component.OpTypeCommit,
	})

	return &pb.CommitResponse{}, nil
}

// Abort handles the 2PC abort phase.
func (s *Server) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	s.lockMgr.Release(req.TxId)
	s.bufferMgr.Remove(req.TxId)
	s.txStatusTbl.Abort(req.TxId)
	s.oplog.Append(&component.OpEntry{
		Timestamp: s.hlcTick(),
		TxId:      req.TxId,
		Key:       "",
		Value:     "",
		OpType:    component.OpTypeAbort,
	})
	return &pb.AbortResponse{}, nil
}

// GetCurrentTime returns the current HLC timestamp of the shard.
func (s *Server) GetCurrentTime(ctx context.Context, req *pb.GetCurrentTimeRequest) (*pb.GetCurrentTimeResponse, error) {
	return &pb.GetCurrentTimeResponse{Time: s.hlc.Now()}, nil
}

func (s *Server) convertError(err error) error {
	switch {
	case errors.Is(err, component.ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, component.ErrVersionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, component.ErrVersionConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, component.ErrLockConflict):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
