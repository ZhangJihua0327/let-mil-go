package shard

import (
	"context"
	"github.com/let-mil-go/internal/shard/component"
	pb "github.com/let-mil-go/proto/shardpb"
	"testing"
)

func TestSingleTransactionSuccess(t *testing.T) {
	// Initialize server
	s := NewServer(component.NewShard("shard-1"))
	ctx := context.Background()

	txId := "tx-1"
	key := "key-1"
	value := "value-1"

	// 0. Start Tx
	_, err := s.TxStart(ctx, &pb.TxStartRequest{TxId: txId})
	if err != nil {
		t.Fatalf("TxStart failed: %v", err)
	}

	// 1. Write
	_, err = s.TxWrite(ctx, &pb.TxWriteRequest{
		TxId:  txId,
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatalf("TxWrite failed: %v", err)
	}

	// 2. Prepare
	prepResp, err := s.Prepare(ctx, &pb.PrepareRequest{
		TxId:           txId,
		IsolationLevel: pb.IsolationLevel_ISOLATION_SI,
	})
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if prepResp.Vote != pb.Vote_VOTE_COMMIT {
		t.Fatalf("Prepare vote should be COMMIT, got %v", prepResp.Vote)
	}

	// 3. Commit
	commitTime := s.hlcTick()
	_, err = s.Commit(ctx, &pb.CommitRequest{
		TxId:       txId,
		CommitTime: commitTime,
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 4. Verify Data
	// Read directly from shard or via new transaction
	storedVal, _, _, err := s.shard.GetLatest(key)
	if err != nil {
		t.Fatalf("GetLatest failed: %v", err)
	}
	if storedVal != value {
		t.Errorf("Expected value %s, got %s", value, storedVal)
	}
}

func TestConcurrentTransactionsConflict(t *testing.T) {
	// Initialize server
	s := NewServer(component.NewShard("shard-1"))
	ctx := context.Background()

	key := "shared-key"

	// T1 starts first
	tx1 := "tx-1"
	_, err := s.TxStart(ctx, &pb.TxStartRequest{TxId: tx1})
	if err != nil {
		t.Fatalf("Tx1 Start failed: %v", err)
	}

	_, err = s.TxWrite(ctx, &pb.TxWriteRequest{
		TxId:  tx1,
		Key:   key,
		Value: "val-1",
	})
	if err != nil {
		t.Fatalf("Tx1 Write failed: %v", err)
	}

	// T2 starts later (after T1 writes)
	tx2 := "tx-2"
	_, err = s.TxStart(ctx, &pb.TxStartRequest{TxId: tx2})
	if err != nil {
		t.Fatalf("Tx2 Start failed: %v", err)
	}

	_, err = s.TxWrite(ctx, &pb.TxWriteRequest{
		TxId:  tx2,
		Key:   key, // Same key conflict
		Value: "val-2",
	})
	if err != nil {
		t.Fatalf("Tx2 Write failed: %v", err)
	}

	// T1 Commits (Prepare + Commit)
	// We ensure T1 Prepares first to acquire locks.

	// T1 Prepare (Acquires Lock)
	prepResp1, err := s.Prepare(ctx, &pb.PrepareRequest{
		TxId:           tx1,
		IsolationLevel: pb.IsolationLevel_ISOLATION_SI,
	})
	if err != nil {
		t.Fatalf("Tx1 Prepare failed: %v", err)
	}
	if prepResp1.Vote != pb.Vote_VOTE_COMMIT {
		t.Fatalf("Tx1 expected COMMIT vote")
	}

	// T2 tries to Prepare while T1 holds the lock.
	// This simulates the scenario where T2 is blocked (failed to acquire lock) and fails.

	prepResp2, err := s.Prepare(ctx, &pb.PrepareRequest{
		TxId:           tx2,
		IsolationLevel: pb.IsolationLevel_ISOLATION_SI,
	})

	// Expecting failure or Abort Vote due to Lock Conflict (Blocking simulated by immediate abort)
	if err != nil {
		// If it returns error, that's also a failure, but our server code returns VOTE_ABORT on lock conflict
		// So we strictly expect VOTE_ABORT and nil error
		t.Fatalf("Tx2 Prepare returned error instead of VOTE_ABORT: %v", err)
	}

	if prepResp2.Vote != pb.Vote_VOTE_ABORT {
		t.Fatalf("Tx2 expected ABORT vote due to lock conflict, got %v", prepResp2.Vote)
	}

	// Finish T1 to clean up
	_, err = s.Commit(ctx, &pb.CommitRequest{
		TxId:       tx1,
		CommitTime: s.hlcTick(),
	})
	if err != nil {
		t.Fatalf("Tx1 Commit failed: %v", err)
	}
}
