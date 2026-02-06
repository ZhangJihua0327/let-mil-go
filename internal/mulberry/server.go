package mulberry

import (
	"context"

	"github.com/let-mil-go/proto/mulberrypb"
)

// Server implements mulberrypb.MulberryServiceServer
type Server struct {
	mulberrypb.UnimplementedMulberryServiceServer
	router *Router
}

// NewServer creates a new gRPC server for Mulberry
func NewServer(router *Router) *Server {
	return &Server{router: router}
}

func (s *Server) StartTransaction(ctx context.Context, req *mulberrypb.StartTransactionRequest) (*mulberrypb.StartTransactionResponse, error) {
	txID, err := s.router.StartTransaction(ctx, req.IsolationLevel)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.StartTransactionResponse{TxId: txID}, nil
}

func (s *Server) Read(ctx context.Context, req *mulberrypb.ReadRequest) (*mulberrypb.ReadResponse, error) {
	val, found, err := s.router.Read(ctx, req.TxId, req.Key)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.ReadResponse{Value: val, Found: found}, nil
}

func (s *Server) Write(ctx context.Context, req *mulberrypb.WriteRequest) (*mulberrypb.WriteResponse, error) {
	err := s.router.Write(ctx, req.TxId, req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.WriteResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *mulberrypb.DeleteRequest) (*mulberrypb.DeleteResponse, error) {
	err := s.router.Delete(ctx, req.TxId, req.Key)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.DeleteResponse{}, nil
}

func (s *Server) Commit(ctx context.Context, req *mulberrypb.CommitRequest) (*mulberrypb.CommitResponse, error) {
	err := s.router.Commit(ctx, req.TxId)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.CommitResponse{}, nil
}

func (s *Server) Abort(ctx context.Context, req *mulberrypb.AbortRequest) (*mulberrypb.AbortResponse, error) {
	err := s.router.Abort(ctx, req.TxId)
	if err != nil {
		return nil, err
	}
	return &mulberrypb.AbortResponse{}, nil
}
