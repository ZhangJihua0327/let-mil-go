package shard

import (
	"context"
	"errors"

	pb "github.com/let-mil-go/proto/shardpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the ShardService gRPC server.
type Server struct {
	pb.UnimplementedShardServiceServer
	shard *Shard
}

// NewServer creates a new gRPC server for the given shard.
func NewServer(shard *Shard) *Server {
	return &Server{shard: shard}
}

// Put stores a key-value pair with automatic versioning.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	version, err := s.shard.Put(req.Key, req.Value)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.PutResponse{Version: version}, nil
}

// PutWithVersion stores a key-value pair with a specific version.
func (s *Server) PutWithVersion(ctx context.Context, req *pb.PutWithVersionRequest) (*pb.PutWithVersionResponse, error) {
	err := s.shard.PutWithVersion(req.Key, req.Value, req.Version)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.PutWithVersionResponse{}, nil
}

// Get retrieves a value at a specific version.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, version, err := s.shard.Get(req.Key, req.Version)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrVersionNotFound) {
			return &pb.GetResponse{Found: false}, nil
		}
		return nil, s.convertError(err)
	}
	return &pb.GetResponse{
		Value:   value,
		Version: version,
		Found:   true,
	}, nil
}

// GetLatest retrieves the latest value for a key.
func (s *Server) GetLatest(ctx context.Context, req *pb.GetLatestRequest) (*pb.GetLatestResponse, error) {
	value, version, err := s.shard.GetLatest(req.Key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return &pb.GetLatestResponse{Found: false}, nil
		}
		return nil, s.convertError(err)
	}
	return &pb.GetLatestResponse{
		Value:   value,
		Version: version,
		Found:   true,
	}, nil
}

// Delete marks a key as deleted with automatic versioning.
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	version, err := s.shard.Delete(req.Key)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.DeleteResponse{Version: version}, nil
}

// DeleteWithVersion marks a key as deleted at a specific version.
func (s *Server) DeleteWithVersion(ctx context.Context, req *pb.DeleteWithVersionRequest) (*pb.DeleteWithVersionResponse, error) {
	err := s.shard.DeleteWithVersion(req.Key, req.Version)
	if err != nil {
		return nil, s.convertError(err)
	}
	return &pb.DeleteWithVersionResponse{}, nil
}

// GetVersion returns the current version timestamp of the shard.
func (s *Server) GetVersion(ctx context.Context, req *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	return &pb.GetVersionResponse{Version: s.shard.CurrentVersion()}, nil
}

func (s *Server) convertError(err error) error {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrVersionNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrVersionConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
