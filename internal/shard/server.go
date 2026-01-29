package shard

import (
	"context"
	"errors"

	"github.com/let-mil-go/internal/model"
	pb "github.com/let-mil-go/proto/shardpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the ShardService gRPC server.
type Server struct {
	pb.UnimplementedShardServiceServer
	shard       *Shard
	bufferMgr   *WriteBufferManager
	lockMgr     *LockManager
	txStatusTbl *model.TxStatusTable
}

// NewServer creates a new gRPC server for the given shard.
func NewServer(shard *Shard) *Server {
	return &Server{
		shard:       shard,
		bufferMgr:   NewWriteBufferManager(),
		lockMgr:     NewLockManager(),
		txStatusTbl: model.NewTxStatusTable(),
	}
}

// TxRead reads a value within a transaction context.
func (s *Server) TxRead(ctx context.Context, req *pb.TxReadRequest) (*pb.TxReadResponse, error) {
	// Check write buffer first (read-your-writes)
	if buf, ok := s.bufferMgr.Get(req.TxId); ok {
		if op, found := buf.Get(req.Key); found {
			if op.Deleted {
				return &pb.TxReadResponse{Found: false}, nil
			}
			return &pb.TxReadResponse{
				Value: op.Value,
				Found: true,
			}, nil
		}
	}

	// Read from store at snapshot time
	value, version, err := s.shard.Get(req.Key, req.SnapshotTime)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrVersionNotFound) {
			return &pb.TxReadResponse{Found: false}, nil
		}
		return nil, s.convertError(err)
	}

	return &pb.TxReadResponse{
		Value:   value,
		Version: version,
		Found:   true,
	}, nil
}

// TxWrite buffers a write operation for a transaction.
func (s *Server) TxWrite(ctx context.Context, req *pb.TxWriteRequest) (*pb.TxWriteResponse, error) {
	buf := s.bufferMgr.GetOrCreate(req.TxId)
	buf.Put(req.Key, req.Value)
	return &pb.TxWriteResponse{}, nil
}

// TxDelete buffers a delete operation for a transaction.
func (s *Server) TxDelete(ctx context.Context, req *pb.TxDeleteRequest) (*pb.TxDeleteResponse, error) {
	buf := s.bufferMgr.GetOrCreate(req.TxId)
	buf.Delete(req.Key)
	return &pb.TxDeleteResponse{}, nil
}

// Prepare handles the 2PC prepare phase.
func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	// Register transaction
	s.txStatusTbl.Begin(req.TxId)

	// Store write buffer from request
	buf := s.bufferMgr.GetOrCreate(req.TxId)
	for _, op := range req.WriteBuffer {
		if op.Deleted {
			buf.Delete(op.Key)
		} else {
			buf.Put(op.Key, op.Value)
		}
	}

	// Acquire write locks on all keys in write buffer
	for _, key := range buf.Keys() {
		if err := s.lockMgr.AcquireWrite(req.TxId, key); err != nil {
			// Lock conflict - abort
			s.lockMgr.Release(req.TxId)
			s.bufferMgr.Remove(req.TxId)
			s.txStatusTbl.Abort(req.TxId)
			return &pb.PrepareResponse{
				Vote: pb.Vote_VOTE_ABORT,
			}, nil
		}
	}

	// Validate check set (no writes since snapshot)
	for _, key := range req.CheckSet {
		_, version, err := s.shard.GetLatest(key)
		if err == nil && version > req.SnapshotTime {
			// Conflict detected
			s.lockMgr.Release(req.TxId)
			s.bufferMgr.Remove(req.TxId)
			s.txStatusTbl.Abort(req.TxId)
			return &pb.PrepareResponse{
				Vote: pb.Vote_VOTE_ABORT,
			}, nil
		}
	}

	// Generate prepare timestamp
	prepareTime := s.shard.Tick()
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

	// Apply all buffered writes with commit timestamp
	for _, op := range buf.Ops() {
		if op.Deleted {
			s.shard.DeleteWithVersion(op.Key, req.CommitTime)
		} else {
			s.shard.PutWithVersion(op.Key, op.Value, req.CommitTime)
		}
	}

	// Update HLC with commit time
	s.shard.Update(req.CommitTime)

	// Cleanup
	s.lockMgr.Release(req.TxId)
	s.bufferMgr.Remove(req.TxId)
	s.txStatusTbl.Commit(req.TxId)

	return &pb.CommitResponse{}, nil
}

// Abort handles the 2PC abort phase.
func (s *Server) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	s.lockMgr.Release(req.TxId)
	s.bufferMgr.Remove(req.TxId)
	s.txStatusTbl.Abort(req.TxId)
	return &pb.AbortResponse{}, nil
}

// GetCurrentTime returns the current HLC timestamp of the shard.
func (s *Server) GetCurrentTime(ctx context.Context, req *pb.GetCurrentTimeRequest) (*pb.GetCurrentTimeResponse, error) {
	return &pb.GetCurrentTimeResponse{Time: s.shard.CurrentTime()}, nil
}

func (s *Server) convertError(err error) error {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrVersionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrVersionConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrLockConflict):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
