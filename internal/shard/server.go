package shard

import (
	"context"
	"errors"

	"github.com/let-mil-go/internal/shard/component"

	pb "github.com/let-mil-go/proto/shardpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the ShardService gRPC server.
type Server struct {
	pb.UnimplementedShardServiceServer
	shard *component.Shard
}

// NewServer creates a new gRPC server for the given shard.
func NewServer(shard *component.Shard) *Server {
	return &Server{
		shard: shard,
	}
}

func (s *Server) hlcTick() uint64 {
	return s.shard.HlcTick()
}

func (s *Server) hlcUpdate(now uint64) uint64 {
	return s.shard.HlcUpdate(now)
}

func (s *Server) hlcNow() uint64 {
	return s.shard.HlcNow()
}

// TxStart initializes a new transaction on this shard.
// Returns the local start time for lazy buffer creation.
func (s *Server) TxStart(ctx context.Context, req *pb.TxStartRequest) (*pb.TxStartResponse, error) {
	startTime, err := s.shard.TxStart(req.TxId, req.IsolationLevel, req.SnapshotTime)
	if err != nil {
		if errors.Is(err, component.ErrUnsupportedIsolationLevel) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, s.convertError(err)
	}
	return &pb.TxStartResponse{StartTime: startTime}, nil
}

// TxRead reads a value within a transaction context.
// TxBuffer is lazily created if not exists.
func (s *Server) TxRead(ctx context.Context, req *pb.TxReadRequest) (*pb.TxReadResponse, error) {
	value, version, found, err := s.shard.TxRead(req.TxId, req.Key, req.SnapshotTime, req.IsolationLevel)
	if err != nil {
		return nil, s.convertError(err)
	}

	return &pb.TxReadResponse{
		Value:   value,
		Version: version,
		Found:   found,
	}, nil
}

// TxWrite buffers a write operation for a transaction.
// TxBuffer is lazily created if not exists.
// Returns the original version of the key before this write.
func (s *Server) TxWrite(ctx context.Context, req *pb.TxWriteRequest) (*pb.TxWriteResponse, error) {
	originalVersion, err := s.shard.TxWrite(req.TxId, req.Key, req.Value, req.SnapshotTime, req.IsolationLevel)
	if err != nil {
		return nil, s.convertError(err)
	}

	return &pb.TxWriteResponse{OriginalVersion: originalVersion}, nil
}

// TxDelete buffers a delete operation for a transaction.
// TxBuffer is lazily created if not exists.
// Returns the original version of the key before this delete.
func (s *Server) TxDelete(ctx context.Context, req *pb.TxDeleteRequest) (*pb.TxDeleteResponse, error) {
	originalVersion, err := s.shard.TxDelete(req.TxId, req.Key, req.SnapshotTime, req.IsolationLevel)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.TxDeleteResponse{OriginalVersion: originalVersion}, nil
}

// Prepare handles the 2PC prepare phase.
// For SI/SER isolation levels, validates write set with CAS check.
// For SER isolation level, also validates read set with CAS check.
func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	vote, prepareTime, err := s.shard.Prepare(req.TxId)
	if err != nil {
		if errors.Is(err, component.ErrTxNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		if errors.Is(err, component.ErrTxNoOps) {
			return nil, status.Error(codes.NotFound, "transaction has no operations")
		}
		return nil, s.convertError(err)
	}

	return &pb.PrepareResponse{
		Vote:        vote,
		PrepareTime: prepareTime,
	}, nil
}

// Commit handles the 2PC commit phase.
// Returns the local commit time on this shard.
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	commitTime, err := s.shard.Commit(req.TxId, req.CommitTime)
	if err != nil {
		if errors.Is(err, component.ErrTxNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		return nil, s.convertError(err)
	}

	return &pb.CommitResponse{CommitTime: commitTime}, nil
}

// Abort handles the 2PC abort phase.
func (s *Server) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	err := s.shard.Abort(req.TxId)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.AbortResponse{}, nil
}

// QuickCommit handles single-shard transactions without 2PC overhead.
// It performs validation and commits in a single atomic operation.
func (s *Server) QuickCommit(ctx context.Context, req *pb.QuickCommitRequest) (*pb.QuickCommitResponse, error) {
	commitTime, err := s.shard.QuickCommit(req.TxId, req.IsolationLevel)
	if err != nil {
		if errors.Is(err, component.ErrTxNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		if errors.Is(err, component.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "version conflict during validation")
		}
		return nil, s.convertError(err)
	}
	return &pb.QuickCommitResponse{CommitTime: commitTime}, nil
}

// QuickAbort handles abort for single-shard transactions without 2PC.
// This is a simplified version for the fast path.
func (s *Server) QuickAbort(ctx context.Context, req *pb.QuickAbortRequest) (*pb.QuickAbortResponse, error) {
	err := s.shard.QuickAbort(req.TxId)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.QuickAbortResponse{}, nil
}

// GetCurrentTime returns the current HLC timestamp of the shard.
func (s *Server) GetCurrentTime(ctx context.Context, req *pb.GetCurrentTimeRequest) (*pb.GetCurrentTimeResponse, error) {
	return &pb.GetCurrentTimeResponse{Time: s.shard.HlcNow()}, nil
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
