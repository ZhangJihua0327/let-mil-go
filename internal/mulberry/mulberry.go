package mulberry

import (
	"context"
	"fmt"
	"github.com/let-mil-go/internal/hlc"
	"sync"

	"github.com/let-mil-go/internal/csrs"
	pb "github.com/let-mil-go/proto/shardpb"
)

// Router implements mongos-like functionality for the distributed database.
// It routes client requests to appropriate shards and coordinates transactions.
type Router struct {
	connMgr         *ShardConnectionManager
	topology        *csrs.TopologyManager
	clock           *hlc.Clock
	activeTxns      map[string]*TxContext
	mu              sync.RWMutex
	topologyVersion uint64
}

// TxContext holds the state of an active transaction.
type TxContext struct {
	TxID           string
	SnapshotTime   uint64
	IsolationLevel pb.IsolationLevel
	InvolvedShards map[string]bool // shardID -> participated
}

// NewRouter creates a new Router instance.
func NewRouter(topology *csrs.TopologyManager) *Router {
	return &Router{
		connMgr:    NewShardConnectionManager(),
		topology:   topology,
		clock:      hlc.GetClock(),
		activeTxns: make(map[string]*TxContext),
	}
}

// BeginTransaction starts a new distributed transaction.
// It generates a new TxID by calling the CSRS version increment.
func (r *Router) BeginTransaction(ctx context.Context, isolationLevel pb.IsolationLevel) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Generate TxID using CSRS version increment
	version := r.topology.BumpTopologyVersion()
	txID := fmt.Sprintf("tx_%d", version)

	if _, exists := r.activeTxns[txID]; exists {
		return "", fmt.Errorf("transaction %s already exists", txID)
	}

	snapshotTime := r.clock.Tick()

	r.activeTxns[txID] = &TxContext{
		TxID:           txID,
		SnapshotTime:   snapshotTime,
		IsolationLevel: isolationLevel,
		InvolvedShards: make(map[string]bool),
	}

	return txID, nil
}

// Read performs a transactional read operation.
func (r *Router) Read(ctx context.Context, txID string, key string) (string, bool, error) {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return "", false, err
	}

	shardID, addr, err := r.routeKey(key)
	if err != nil {
		return "", false, err
	}

	if err := r.ensureShardStarted(ctx, txCtx, shardID, addr); err != nil {
		return "", false, err
	}

	stub, err := r.connMgr.GetStub(addr)
	if err != nil {
		return "", false, fmt.Errorf("failed to get stub for %s: %w", addr, err)
	}

	resp, err := stub.TxRead(ctx, &pb.TxReadRequest{
		TxId:         txID,
		Key:          key,
		SnapshotTime: txCtx.SnapshotTime,
	})
	if err != nil {
		return "", false, fmt.Errorf("TxRead failed: %w", err)
	}

	return resp.Value, resp.Found, nil
}

// Write performs a transactional write operation.
func (r *Router) Write(ctx context.Context, txID string, key string, value string) error {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return err
	}

	shardID, addr, err := r.routeKey(key)
	if err != nil {
		return err
	}

	if err := r.ensureShardStarted(ctx, txCtx, shardID, addr); err != nil {
		return err
	}

	stub, err := r.connMgr.GetStub(addr)
	if err != nil {
		return fmt.Errorf("failed to get stub for %s: %w", addr, err)
	}

	_, err = stub.TxWrite(ctx, &pb.TxWriteRequest{
		TxId:  txID,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("TxWrite failed: %w", err)
	}

	return nil
}

// Delete performs a transactional delete operation.
func (r *Router) Delete(ctx context.Context, txID string, key string) error {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return err
	}

	shardID, addr, err := r.routeKey(key)
	if err != nil {
		return err
	}

	if err := r.ensureShardStarted(ctx, txCtx, shardID, addr); err != nil {
		return err
	}

	stub, err := r.connMgr.GetStub(addr)
	if err != nil {
		return fmt.Errorf("failed to get stub for %s: %w", addr, err)
	}

	_, err = stub.TxDelete(ctx, &pb.TxDeleteRequest{
		TxId: txID,
		Key:  key,
	})
	if err != nil {
		return fmt.Errorf("TxDelete failed: %w", err)
	}

	return nil
}

// Commit commits the transaction using 2PC protocol.
func (r *Router) Commit(ctx context.Context, txID string) error {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return err
	}

	// Phase 1: Prepare
	prepareResults := make(map[string]uint64)
	for shardID := range txCtx.InvolvedShards {
		info := r.topology.GetShardInfo(shardID)
		if info == nil {
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("shard %s not found during commit", shardID)
		}

		stub, err := r.connMgr.GetStub(info.Address)
		if err != nil {
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("failed to get stub for prepare: %w", err)
		}

		resp, err := stub.Prepare(ctx, &pb.PrepareRequest{
			TxId:           txID,
			SnapshotTime:   txCtx.SnapshotTime,
			IsolationLevel: txCtx.IsolationLevel,
		})
		if err != nil || resp.Vote != pb.Vote_VOTE_COMMIT {
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("prepare failed on shard %s", shardID)
		}

		prepareResults[shardID] = resp.PrepareTime
	}

	// Calculate commit time as max of all prepare times
	var commitTime uint64
	for _, pt := range prepareResults {
		if pt > commitTime {
			commitTime = pt
		}
	}
	commitTime = r.clock.Update(commitTime)

	// Phase 2: Commit
	for shardID := range txCtx.InvolvedShards {
		info := r.topology.GetShardInfo(shardID)
		stub, _ := r.connMgr.GetStub(info.Address)

		_, err := stub.Commit(ctx, &pb.CommitRequest{
			TxId:       txID,
			CommitTime: commitTime,
		})
		if err != nil {
			// Log error but continue - commit decision is final
			continue
		}
	}

	r.removeTxContext(txID)
	return nil
}

// Abort aborts the transaction.
func (r *Router) Abort(ctx context.Context, txID string) error {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return err
	}

	r.abortAll(ctx, txID, txCtx)
	return nil
}

// Shutdown closes all connections.
func (r *Router) Shutdown() error {
	return r.connMgr.Shutdown()
}

// routeKey returns the shard ID and address for the given key.
func (r *Router) routeKey(key string) (string, string, error) {
	shardID, err := r.topology.GetShardForKey(key)
	if err != nil {
		return "", "", err
	}

	info := r.topology.GetShardInfo(shardID)
	if info == nil {
		return "", "", fmt.Errorf("shard %s not found in topology", shardID)
	}

	return shardID, info.Address, nil
}

// ensureShardStarted ensures TxStart is called on the shard for this transaction.
func (r *Router) ensureShardStarted(ctx context.Context, txCtx *TxContext, shardID, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if txCtx.InvolvedShards[shardID] {
		return nil
	}

	stub, err := r.connMgr.GetStub(addr)
	if err != nil {
		return fmt.Errorf("failed to get stub: %w", err)
	}

	_, err = stub.TxStart(ctx, &pb.TxStartRequest{
		TxId:         txCtx.TxID,
		SnapshotTime: txCtx.SnapshotTime,
	})
	if err != nil {
		return fmt.Errorf("TxStart failed on shard %s: %w", shardID, err)
	}

	txCtx.InvolvedShards[shardID] = true
	return nil
}

// getTxContext retrieves the transaction context.
func (r *Router) getTxContext(txID string) (*TxContext, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	txCtx, exists := r.activeTxns[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txID)
	}
	return txCtx, nil
}

// removeTxContext removes the transaction context.
func (r *Router) removeTxContext(txID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.activeTxns, txID)
}

// abortAll sends abort to all involved shards.
func (r *Router) abortAll(ctx context.Context, txID string, txCtx *TxContext) {
	for shardID := range txCtx.InvolvedShards {
		info := r.topology.GetShardInfo(shardID)
		if info == nil {
			continue
		}

		stub, err := r.connMgr.GetStub(info.Address)
		if err != nil {
			continue
		}

		stub.Abort(ctx, &pb.AbortRequest{TxId: txID})
	}

	r.removeTxContext(txID)
}
