package mulberry

import (
	"sync"

	pb "github.com/let-mil-go/proto/shardpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ShardConnectionManager manages gRPC connections to shard servers.
// It maintains a pool of persistent connections, reusing them across requests.
type ShardConnectionManager struct {
	mu          sync.RWMutex
	connections map[string]*grpc.ClientConn
	stubs       map[string]pb.ShardServiceClient
}

// NewShardConnectionManager creates a new connection manager.
func NewShardConnectionManager() *ShardConnectionManager {
	return &ShardConnectionManager{
		connections: make(map[string]*grpc.ClientConn),
		stubs:       make(map[string]pb.ShardServiceClient),
	}
}

// GetStub returns a ShardServiceClient for the given address.
// Reuses existing connections if available, creates new ones if not.
func (m *ShardConnectionManager) GetStub(address string) (pb.ShardServiceClient, error) {
	m.mu.RLock()
	if stub, ok := m.stubs[address]; ok {
		m.mu.RUnlock()
		return stub, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if stub, ok := m.stubs[address]; ok {
		return stub, nil
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	stub := pb.NewShardServiceClient(conn)
	m.connections[address] = conn
	m.stubs[address] = stub

	return stub, nil
}

// GetConnection returns the raw gRPC connection for the given address.
func (m *ShardConnectionManager) GetConnection(address string) (*grpc.ClientConn, error) {
	m.mu.RLock()
	if conn, ok := m.connections[address]; ok {
		m.mu.RUnlock()
		return conn, nil
	}
	m.mu.RUnlock()

	// Ensure connection exists by calling GetStub
	_, err := m.GetStub(address)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connections[address], nil
}

// RemoveConnection closes and removes a specific connection.
func (m *ShardConnectionManager) RemoveConnection(address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, ok := m.connections[address]
	if !ok {
		return nil
	}

	err := conn.Close()
	delete(m.connections, address)
	delete(m.stubs, address)
	return err
}

// ConnectionCount returns the number of active connections.
func (m *ShardConnectionManager) ConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.connections)
}

// Shutdown closes all connections. Should be called when application stops.
func (m *ShardConnectionManager) Shutdown() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for addr, conn := range m.connections {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
		delete(m.connections, addr)
		delete(m.stubs, addr)
	}

	return lastErr
}
