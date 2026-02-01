package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/let-mil-go/internal/shard"
	"github.com/let-mil-go/internal/shard/component"
	pb "github.com/let-mil-go/proto/shardpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	csrspb "github.com/let-mil-go/proto/csrspb"
)

func main() {
	shardID := flag.String("id", "", "Shard ID (required)")
	port := flag.Int("port", 50051, "gRPC server port")
	csrsAddr := flag.String("csrs", "", "CSRS server address (optional, for auto-registration)")
	replicaSet := flag.String("replica-set", "", "Replica set name (optional)")
	priority := flag.Int("priority", 1, "Node priority in replica set")

	flag.Parse()

	if *shardID == "" {
		log.Fatal("shard ID is required, use -id flag")
	}

	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	shardComponent := component.NewShard(*shardID)
	server := shard.NewServer(shardComponent)

	grpcServer := grpc.NewServer()
	pb.RegisterShardServiceServer(grpcServer, server)

	log.Printf("Shard %s starting on %s", *shardID, addr)

	// Register with CSRS if address provided
	if *csrsAddr != "" {
		go func() {
			time.Sleep(2 * time.Second) // Wait for server to start
			if err := registerWithCSRS(*csrsAddr, *shardID, addr, *replicaSet, *priority); err != nil {
				log.Printf("Warning: failed to register with CSRS: %v", err)
			} else {
				log.Printf("Successfully registered shard %s with CSRS", *shardID)
			}
		}()
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Printf("Shutting down shard %s...", *shardID)
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func registerWithCSRS(csrsAddr, shardID, listenAddr, replicaSet string, priority int) error {
	conn, err := grpc.NewClient(csrsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to CSRS: %w", err)
	}
	defer conn.Close()

	client := csrspb.NewCSRSServiceClient(conn)

	// Get hostname for external address
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	// Use environment variable if set (for Docker)
	if envAddr := os.Getenv("SHARD_ADVERTISE_ADDR"); envAddr != "" {
		listenAddr = envAddr
	} else {
		// Extract port from listen address
		_, port, _ := net.SplitHostPort(listenAddr)
		listenAddr = fmt.Sprintf("%s:%s", hostname, port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.RegisterShard(ctx, &csrspb.RegisterShardRequest{
		ShardId:    shardID,
		Address:    listenAddr,
		ReplicaSet: replicaSet,
		Priority:   int32(priority),
	})
	if err != nil {
		return fmt.Errorf("RegisterShard failed: %w", err)
	}

	log.Printf("Registered with topology version: %d", resp.TopologyVersion)
	return nil
}
