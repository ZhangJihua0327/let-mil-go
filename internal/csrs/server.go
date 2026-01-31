package csrs

import (
	"context"

	pb "github.com/let-mil-go/proto/csrspb"
	"google.golang.org/grpc"
)

// Server implements the CSRSService gRPC server.
type Server struct {
	pb.UnimplementedCSRSServiceServer
	tm *TopologyManager
}

// NewServer creates a new gRPC server for the topology manager.
func NewServer(tm *TopologyManager) *Server {
	return &Server{tm: tm}
}

// Register registers the CSRS server with a gRPC server.
func (s *Server) Register(gs *grpc.Server) {
	pb.RegisterCSRSServiceServer(gs, s)
}

// GetShardAddressMap returns the global mapping of shard-name to ip:port.
func (s *Server) GetShardAddressMap(ctx context.Context, req *pb.GetShardAddressMapRequest) (*pb.GetShardAddressMapResponse, error) {
	addrMap := s.tm.GetShardAddressMap()
	version := s.tm.GetTopologyVersion()
	return &pb.GetShardAddressMapResponse{
		ShardAddresses:  addrMap,
		TopologyVersion: version,
	}, nil
}

// GetTopologyVersion returns the current topology version.
func (s *Server) GetTopologyVersion(ctx context.Context, req *pb.GetTopologyVersionRequest) (*pb.GetTopologyVersionResponse, error) {
	return &pb.GetTopologyVersionResponse{
		TopologyVersion: s.tm.GetTopologyVersion(),
	}, nil
}

// GetShardForKey returns the shard responsible for a given key.
func (s *Server) GetShardForKey(ctx context.Context, req *pb.GetShardForKeyRequest) (*pb.GetShardForKeyResponse, error) {
	shardID, err := s.tm.GetShardForKey(req.Key)
	if err != nil {
		return nil, err
	}
	addr, err := s.tm.GetShardAddressForKey(req.Key)
	if err != nil {
		return nil, err
	}
	return &pb.GetShardForKeyResponse{
		ShardId: shardID,
		Address: addr,
	}, nil
}

// RegisterShard registers a new shard with the topology.
func (s *Server) RegisterShard(ctx context.Context, req *pb.RegisterShardRequest) (*pb.RegisterShardResponse, error) {
	info := &ShardInfo{
		ShardID:    req.ShardId,
		Address:    req.Address,
		ReplicaSet: req.ReplicaSet,
		Priority:   int(req.Priority),
		Status:     ShardStatusActive,
	}
	version, err := s.tm.AddShard(info)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterShardResponse{TopologyVersion: version}, nil
}

// UnregisterShard removes a shard from the topology.
func (s *Server) UnregisterShard(ctx context.Context, req *pb.UnregisterShardRequest) (*pb.UnregisterShardResponse, error) {
	version, err := s.tm.RemoveShard(req.ShardId)
	if err != nil {
		return nil, err
	}
	return &pb.UnregisterShardResponse{TopologyVersion: version}, nil
}

// UpdateShardStatus updates the operational status of a shard.
func (s *Server) UpdateShardStatus(ctx context.Context, req *pb.UpdateShardStatusRequest) (*pb.UpdateShardStatusResponse, error) {
	version, err := s.tm.UpdateShardStatus(req.ShardId, ShardStatus(req.Status))
	if err != nil {
		return nil, err
	}
	return &pb.UpdateShardStatusResponse{TopologyVersion: version}, nil
}

// BumpTopologyVersion manually increments the topology version.
func (s *Server) BumpTopologyVersion(ctx context.Context, req *pb.BumpTopologyVersionRequest) (*pb.BumpTopologyVersionResponse, error) {
	version := s.tm.BumpTopologyVersion()
	return &pb.BumpTopologyVersionResponse{TopologyVersion: version}, nil
}
