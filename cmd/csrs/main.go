package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/let-mil-go/internal/csrs"

	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50050, "gRPC server port")
	shardAddrs := flag.String("shards", "", "Comma-separated list of initial shard addresses (shardID=addr,...)")

	flag.Parse()

	topology := csrs.NewTopologyManager()
	server := csrs.NewServer(topology)

	// Initialize with pre-configured shards
	if *shardAddrs != "" {
		for _, entry := range strings.Split(*shardAddrs, ",") {
			parts := strings.SplitN(entry, "=", 2)
			if len(parts) == 2 {
				shardID, addr := parts[0], parts[1]
				_, err := topology.AddShard(&csrs.ShardInfo{
					ShardID: shardID,
					Address: addr,
					Status:  csrs.ShardStatusActive,
				})
				if err != nil {
					log.Printf("Warning: failed to add shard %s: %v", shardID, err)
				} else {
					log.Printf("Pre-configured shard %s at %s", shardID, addr)
				}
			}
		}
	}

	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)

	log.Printf("CSRS (Config Server) starting on %s", addr)

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down CSRS...")
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
