package mulberry

import (
	"context"
	"fmt"
	"sync"

	"github.com/let-mil-go/internal/hlc"

	"github.com/let-mil-go/internal/csrs"
	pb "github.com/let-mil-go/proto/shardpb"
)

// Router implements mongos-like functionality for the distributed database.
// It routes client requests to appropriate shards and coordinates transactions.
type Router struct {
	routerID            string
	connMgr             *ShardConnectionManager
	topology            *csrs.TopologyManager
	clock               *hlc.Clock
	activeTxns          map[string]*TxContext
	mu                  sync.RWMutex
	topologyVersion     uint64
	shardLastCommitTime sync.Map // shardID -> uint64 (last commit time)
}

// TxContext holds the state of an active transaction.
type TxContext struct {
	TxID           string
	SnapshotTime   uint64
	IsolationLevel pb.IsolationLevel
	InvolvedShards map[string]bool // shardID -> participated
}

// NewRouter creates a new Router instance with the given router ID.
func NewRouter(routerID string, topology *csrs.TopologyManager) *Router {
	return &Router{
		routerID:   routerID,
		connMgr:    NewShardConnectionManager(),
		topology:   topology,
		clock:      hlc.GetClock(),
		activeTxns: make(map[string]*TxContext),
	}
}

// GetRouterID returns the router's unique ID assigned by CSRS.
func (r *Router) GetRouterID() string {
	return r.routerID
}

// StartTransaction starts a new distributed transaction.
// It generates a new TxID using format Tx_{routerID}_{timestamp}.
func (r *Router) StartTransaction(ctx context.Context, isolationLevel pb.IsolationLevel) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Generate TxID using format Tx_{routerID}_{timestamp}
	timestamp := r.clock.Tick()
	txID := fmt.Sprintf("Tx_%s_%d", r.routerID, timestamp)

	if _, exists := r.activeTxns[txID]; exists {
		return "", fmt.Errorf("transaction %s already exists", txID)
	}

	snapshotTime := timestamp

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
		TxId: txID,
		Key:  key,
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

// Commit commits the transaction.
// For single-shard transactions, uses QuickCommit for better performance.
// For multi-shard transactions, uses 2PC protocol.
func (r *Router) Commit(ctx context.Context, txID string) error {
	txCtx, err := r.getTxContext(txID)
	if err != nil {
		return err
	}

	// Single-shard optimization: use QuickCommit
	if len(txCtx.InvolvedShards) == 1 {
		return r.quickCommit(ctx, txID, txCtx)
	}

	// Multi-shard: use 2PC
	return r.twoPhaseCommit(ctx, txID, txCtx)
}

// quickCommit handles single-shard transactions without 2PC overhead.
func (r *Router) quickCommit(ctx context.Context, txID string, txCtx *TxContext) error {
	// Get the only shard
	var shardID string
	for sid := range txCtx.InvolvedShards {
		shardID = sid
		break
	}

	info := r.topology.GetShardInfo(shardID)
	if info == nil {
		r.removeTxContext(txID)
		return fmt.Errorf("shard %s not found during commit", shardID)
	}

	stub, err := r.connMgr.GetStub(info.Address)
	if err != nil {
		r.removeTxContext(txID)
		return fmt.Errorf("failed to get stub for QuickCommit: %w", err)
	}

	resp, err := stub.QuickCommit(ctx, &pb.QuickCommitRequest{
		TxId:           txID,
		IsolationLevel: txCtx.IsolationLevel,
	})
	if err != nil {
		r.removeTxContext(txID)
		return fmt.Errorf("QuickCommit failed on shard %s: %w", shardID, err)
	}

	// Update shard's last commit time (thread-safe)
	if resp.CommitTime > 0 {
		r.updateShardLastCommitTime(shardID, resp.CommitTime)
	}

	r.removeTxContext(txID)
	return nil
}

// twoPhaseCommit handles multi-shard transactions using 2PC protocol.
// Phase 1: Prepare all shards. On first VOTE_ABORT, abort all and return immediately.
// Phase 2: Only commit shards that voted VOTE_COMMIT. Skip VOTE_EMPTY shards.
func (r *Router) twoPhaseCommit(ctx context.Context, txID string, txCtx *TxContext) error {
	// Phase 1: Prepare
	// Track shards that need commit (voted VOTE_COMMIT)
	commitShards := make(map[string]uint64) // shardID -> prepareTime

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
			IsolationLevel: txCtx.IsolationLevel,
		})
		if err != nil {
			// Connection error - abort all immediately
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("prepare failed on shard %s: %w", shardID, err)
		}

		switch resp.Vote {
		case pb.Vote_VOTE_COMMIT:
			// Record for phase 2
			commitShards[shardID] = resp.PrepareTime
		case pb.Vote_VOTE_NOOP, pb.Vote_VOTE_NOT_FOUND:
			// Shard already cleaned up, skip in phase 2
			continue
		case pb.Vote_VOTE_ABORT:
			// Abort all immediately without polling remaining shards
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("prepare aborted on shard %s", shardID)
		default:
			r.abortAll(ctx, txID, txCtx)
			return fmt.Errorf("unexpected vote %v on shard %s", resp.Vote, shardID)
		}
	}

	// If no shards need commit (all VOTE_EMPTY), just cleanup and return
	if len(commitShards) == 0 {
		r.removeTxContext(txID)
		return nil
	}

	// Calculate commit time as max of all prepare times
	var commitTime uint64
	for _, pt := range commitShards {
		if pt > commitTime {
			commitTime = pt
		}
	}
	commitTime = r.clock.Update(commitTime)

	// Phase 2: Commit only shards that voted VOTE_COMMIT
	for shardID := range commitShards {
		info := r.topology.GetShardInfo(shardID)
		stub, _ := r.connMgr.GetStub(info.Address)
		var commitErr error
		switch txCtx.IsolationLevel {
		case pb.IsolationLevel_ISOLATION_PC, pb.IsolationLevel_ISOLATION_SI, pb.IsolationLevel_ISOLATION_SER:
			_, commitErr = stub.Commit(ctx, &pb.CommitRequest{
				TxId:       txID,
				CommitTime: commitTime,
			})
		default:
			_, commitErr = stub.Commit(ctx, &pb.CommitRequest{
				TxId: txID,
			})
		}
		if commitErr == nil {
			// Update shard's last commit time on successful commit (thread-safe)
			r.updateShardLastCommitTime(shardID, commitTime)
		}
		// Log error but continue - commit decision is final
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
		TxId:           txCtx.TxID,
		IsolationLevel: txCtx.IsolationLevel,
		SnapshotTime:   txCtx.SnapshotTime,
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

// updateShardLastCommitTime updates the last commit time for a shard.
// Uses sync.Map for thread-safe updates without locking.
// Only updates if the new commitTime is greater than the existing one.
func (r *Router) updateShardLastCommitTime(shardID string, commitTime uint64) {
	for {
		oldVal, loaded := r.shardLastCommitTime.Load(shardID)
		if loaded {
			oldTime := oldVal.(uint64)
			if commitTime <= oldTime {
				// Current time is not newer, no update needed
				return
			}
		}
		// Try to store the new value
		if !loaded {
			// First time storing for this shard
			if r.shardLastCommitTime.CompareAndSwap(shardID, nil, commitTime) {
				return
			}
			// Someone else stored first, retry
			r.shardLastCommitTime.Store(shardID, commitTime)
			return
		}
		// Update existing value
		if r.shardLastCommitTime.CompareAndSwap(shardID, oldVal, commitTime) {
			return
		}
		// CAS failed, retry
	}
}

// GetShardLastCommitTime returns the last commit time for a shard.
// Returns 0 if the shard has no recorded commits.
func (r *Router) GetShardLastCommitTime(shardID string) uint64 {
	if val, ok := r.shardLastCommitTime.Load(shardID); ok {
		return val.(uint64)
	}
	return 0
}
