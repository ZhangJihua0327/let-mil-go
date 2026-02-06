package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/let-mil-go/internal/csrs"
	"github.com/let-mil-go/internal/mulberry"
	"github.com/let-mil-go/proto/mulberrypb"

	csrspb "github.com/let-mil-go/proto/csrspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	port := flag.Int("port", 50052, "gRPC server port")
	csrsAddr := flag.String("csrs", "", "CSRS server address (for topology sync)")
	shardAddrs := flag.String("shards", "", "Comma-separated list of shard addresses (shardID=addr,...) for standalone mode")
	syncInterval := flag.Duration("sync-interval", 5*time.Second, "Topology sync interval")

	flag.Parse()

	topology := csrs.NewTopologyManager()

	// Initialize topology from command line (standalone mode)
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
					log.Printf("Added shard %s at %s", shardID, addr)
				}
			}
		}
	}

	// Start topology sync with CSRS if address provided
	var cancelSync context.CancelFunc
	var routerID string
	if *csrsAddr != "" {
		var ctx context.Context
		ctx, cancelSync = context.WithCancel(context.Background())

		// Register router with CSRS to get unique router ID
		var err error
		routerID, err = registerRouter(*csrsAddr, fmt.Sprintf(":%d", *port))
		if err != nil {
			log.Fatalf("failed to register router with CSRS: %v", err)
		}
		log.Printf("Registered with CSRS, router ID: %s", routerID)

		go syncTopology(ctx, *csrsAddr, topology, *syncInterval)
	} else {
		// Standalone mode: generate local router ID
		routerID = fmt.Sprintf("router_standalone_%d", time.Now().UnixNano())
		log.Printf("Running in standalone mode, router ID: %s", routerID)
	}

	router := mulberry.NewRouter(routerID, topology)
	server := mulberry.NewServer(router)

	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	mulberrypb.RegisterMulberryServiceServer(grpcServer, server)

	log.Printf("Mulberry router starting on %s", addr)

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down mulberry...")
		if cancelSync != nil {
			cancelSync()
		}
		router.Shutdown()
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func syncTopology(ctx context.Context, csrsAddr string, topology *csrs.TopologyManager, interval time.Duration) {
	log.Printf("Starting topology sync with CSRS at %s", csrsAddr)

	// Initial sync with retry
	for i := 0; i < 30; i++ {
		if err := doSync(csrsAddr, topology); err != nil {
			log.Printf("Topology sync attempt %d failed: %v", i+1, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		} else {
			log.Printf("Initial topology sync successful, %d shards", topology.ShardCount())
			break
		}
	}

	// Periodic sync
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := doSync(csrsAddr, topology); err != nil {
				log.Printf("Topology sync failed: %v", err)
			}
		}
	}
}

func doSync(csrsAddr string, topology *csrs.TopologyManager) error {
	conn, err := grpc.NewClient(csrsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to CSRS: %w", err)
	}
	defer conn.Close()

	client := csrspb.NewCSRSServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetShardAddressMap(ctx, &csrspb.GetShardAddressMapRequest{})
	if err != nil {
		return fmt.Errorf("GetShardAddressMap failed: %w", err)
	}

	// Update local topology
	currentShards := topology.GetAllShards()
	newShards := resp.ShardAddresses

	// Add/update shards
	for shardID, addr := range newShards {
		if existing, ok := currentShards[shardID]; !ok || existing.Address != addr {
			_, err := topology.AddShard(&csrs.ShardInfo{
				ShardID: shardID,
				Address: addr,
				Status:  csrs.ShardStatusActive,
			})
			if err != nil && existing == nil {
				log.Printf("Added shard %s at %s", shardID, addr)
			}
		}
	}

	// Remove stale shards
	for shardID := range currentShards {
		if _, ok := newShards[shardID]; !ok {
			topology.RemoveShard(shardID)
			log.Printf("Removed shard %s", shardID)
		}
	}

	return nil
}

func registerRouter(csrsAddr string, routerAddr string) (string, error) {
	conn, err := grpc.NewClient(csrsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to CSRS: %w", err)
	}
	defer conn.Close()

	client := csrspb.NewCSRSServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RegisterRouter(ctx, &csrspb.RegisterRouterRequest{
		Address: routerAddr,
	})
	if err != nil {
		return "", fmt.Errorf("RegisterRouter failed: %w", err)
	}

	return resp.RouterId, nil
}
