package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/let-mil-go/proto/mulberrypb"
	"github.com/let-mil-go/proto/shardpb"
)

// Client manages connections to multiple mulberry nodes with transaction affinity.
// For new transactions, it uses round-robin to select a mulberry.
// For ongoing transactions, all operations go to the same mulberry.
type Client struct {
	addresses []string
	conns     []*grpc.ClientConn
	clients   []mulberrypb.MulberryServiceClient

	// round-robin counter for new transactions
	counter uint32

	// current transaction state
	mu         sync.RWMutex
	currentTx  string
	currentIdx int // index of mulberry handling current transaction
}

// NewClient creates a new client connection to multiple mulberry services
func NewClient(addresses []string) (*Client, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	conns := make([]*grpc.ClientConn, len(addresses))
	clients := make([]mulberrypb.MulberryServiceClient, len(addresses))

	for i, addr := range addresses {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			// Close already created connections
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
		}
		conns[i] = conn
		clients[i] = mulberrypb.NewMulberryServiceClient(conn)
	}

	return &Client{
		addresses:  addresses,
		conns:      conns,
		clients:    clients,
		currentIdx: -1, // no active transaction
	}, nil
}

// Close closes all connections
func (c *Client) Close() error {
	for _, conn := range c.conns {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}

// getClient returns the appropriate mulberry client based on transaction state
// For new transactions: uses round-robin to select a mulberry
// For ongoing transactions: returns the same mulberry used for start
func (c *Client) getClient(txID string) (mulberrypb.MulberryServiceClient, int, error) {
	c.mu.RLock()
	// If there's an active transaction and txID matches, use the same mulberry
	if c.currentTx != "" && c.currentTx == txID && c.currentIdx >= 0 {
		idx := c.currentIdx
		c.mu.RUnlock()
		return c.clients[idx], idx, nil
	}
	c.mu.RUnlock()

	// For new transactions or unknown txID, use round-robin
	if txID == "" {
		// StartTransaction case - select new mulberry via round-robin
		idx := int(atomic.AddUint32(&c.counter, 1) % uint32(len(c.clients)))
		return c.clients[idx], idx, nil
	}

	// Transaction exists but not tracked locally - this shouldn't happen in normal flow
	return nil, -1, fmt.Errorf("transaction %s not found in local session", txID)
}

// StartTransaction starts a new transaction using round-robin to select mulberry
func (c *Client) StartTransaction(ctx context.Context, isolation shardpb.IsolationLevel) (string, error) {
	client, idx, err := c.getClient("")
	if err != nil {
		return "", err
	}

	resp, err := client.StartTransaction(ctx, &mulberrypb.StartTransactionRequest{
		IsolationLevel: isolation,
	})
	if err != nil {
		return "", err
	}

	// Record the transaction and its mulberry index
	c.mu.Lock()
	c.currentTx = resp.TxId
	c.currentIdx = idx
	c.mu.Unlock()

	return resp.TxId, nil
}

// Read reads a value for a key within a transaction (uses same mulberry as start)
func (c *Client) Read(ctx context.Context, txID, key string) (string, bool, error) {
	client, _, err := c.getClient(txID)
	if err != nil {
		return "", false, err
	}

	resp, err := client.Read(ctx, &mulberrypb.ReadRequest{
		TxId: txID,
		Key:  key,
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// Write writes a value for a key within a transaction (uses same mulberry as start)
func (c *Client) Write(ctx context.Context, txID, key, value string) error {
	client, _, err := c.getClient(txID)
	if err != nil {
		return err
	}

	_, err = client.Write(ctx, &mulberrypb.WriteRequest{
		TxId:  txID,
		Key:   key,
		Value: value,
	})
	return err
}

// Delete deletes a key within a transaction (uses same mulberry as start)
func (c *Client) Delete(ctx context.Context, txID, key string) error {
	client, _, err := c.getClient(txID)
	if err != nil {
		return err
	}

	_, err = client.Delete(ctx, &mulberrypb.DeleteRequest{
		TxId: txID,
		Key:  key,
	})
	return err
}

// Commit commits the transaction and clears local transaction state
func (c *Client) Commit(ctx context.Context, txID string) error {
	client, _, err := c.getClient(txID)
	if err != nil {
		return err
	}

	_, err = client.Commit(ctx, &mulberrypb.CommitRequest{
		TxId: txID,
	})

	// Clear transaction state regardless of commit success/failure
	c.mu.Lock()
	if c.currentTx == txID {
		c.currentTx = ""
		c.currentIdx = -1
	}
	c.mu.Unlock()

	return err
}

// Abort aborts the transaction and clears local transaction state
func (c *Client) Abort(ctx context.Context, txID string) error {
	client, _, err := c.getClient(txID)
	if err != nil {
		return err
	}

	_, err = client.Abort(ctx, &mulberrypb.AbortRequest{
		TxId: txID,
	})

	// Clear transaction state regardless of abort success/failure
	c.mu.Lock()
	if c.currentTx == txID {
		c.currentTx = ""
		c.currentIdx = -1
	}
	c.mu.Unlock()

	return err
}
