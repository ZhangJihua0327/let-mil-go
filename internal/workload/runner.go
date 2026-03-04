package workload

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/let-mil-go/internal/client"
)

// CmdResult holds the outcome of executing a single command.
type CmdResult struct {
	Cmd      Command
	TxID     string        // active transaction ID at execution time
	Output   string        // human-readable result
	Err      error         // nil on success
	Duration time.Duration // wall-clock time for this command
}

// WorkerResult holds the complete execution trace of one worker.
type WorkerResult struct {
	Name          string
	Results       []CmdResult
	TotalDuration time.Duration
}

// ClientPool holds one client per router address and assigns clients to
// transactions in round-robin order. A transaction is bound to its
// assigned client from start until commit/abort, guaranteeing that all
// RPCs within one transaction reach the same router.
type ClientPool struct {
	clients []*client.Client
	counter atomic.Uint64
}

// NewClientPool creates one client.Client per address.
func NewClientPool(addresses []string) (*ClientPool, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}
	pool := &ClientPool{}
	for _, addr := range addresses {
		c, err := client.NewClient([]string{addr})
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("connect to %s: %w", addr, err)
		}
		pool.clients = append(pool.clients, c)
	}
	return pool, nil
}

// Acquire picks the next client in round-robin order.
func (p *ClientPool) Acquire() *client.Client {
	idx := p.counter.Add(1) - 1
	return p.clients[idx%uint64(len(p.clients))]
}

// Close shuts down all clients in the pool.
func (p *ClientPool) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

// Run executes all workers concurrently. Each worker runs its commands
// sequentially. When a transaction starts, it acquires a client from the
// pool; all operations in that transaction use the same client.
// Results are returned in the same order as wl.Workers.
func Run(ctx context.Context, wl *Workload, pool *ClientPool) []*WorkerResult {
	results := make([]*WorkerResult, len(wl.Workers))
	var wg sync.WaitGroup

	for i, spec := range wl.Workers {
		wg.Add(1)
		go func(idx int, s WorkerSpec) {
			defer wg.Done()
			results[idx] = runWorker(ctx, s, pool)
		}(i, spec)
	}

	wg.Wait()
	return results
}

func runWorker(ctx context.Context, spec WorkerSpec, pool *ClientPool) *WorkerResult {
	wr := &WorkerResult{Name: spec.Name}
	workerStart := time.Now()
	var currentTxID string
	var txClient *client.Client // bound client for the active transaction

	for _, cmd := range spec.Commands {
		cmdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()

		var result CmdResult
		result.Cmd = cmd

		switch cmd.Type {
		case CmdStart:
			// Acquire a client from the pool for this transaction.
			txClient = pool.Acquire()
			txID, err := txClient.StartTransaction(cmdCtx, cmd.Isolation)
			if err != nil {
				result.Err = err
				result.Output = fmt.Sprintf("ERROR: %v", err)
				txClient = nil
			} else {
				currentTxID = txID
				result.Output = fmt.Sprintf("tx:%s", txID)
			}

		case CmdRead:
			if currentTxID == "" || txClient == nil {
				result.Err = fmt.Errorf("no active transaction")
				result.Output = "ERROR: no active transaction"
			} else {
				val, found, err := txClient.Read(cmdCtx, currentTxID, cmd.Key)
				if err != nil {
					result.Err = err
					result.Output = fmt.Sprintf("ERROR: %v", err)
				} else if !found {
					result.Output = "<not found>"
				} else {
					result.Output = fmt.Sprintf("%q", val)
				}
			}

		case CmdWrite:
			if currentTxID == "" || txClient == nil {
				result.Err = fmt.Errorf("no active transaction")
				result.Output = "ERROR: no active transaction"
			} else {
				err := txClient.Write(cmdCtx, currentTxID, cmd.Key, cmd.Value)
				if err != nil {
					result.Err = err
					result.Output = fmt.Sprintf("ERROR: %v", err)
				} else {
					result.Output = "OK"
				}
			}

		case CmdDelete:
			if currentTxID == "" || txClient == nil {
				result.Err = fmt.Errorf("no active transaction")
				result.Output = "ERROR: no active transaction"
			} else {
				err := txClient.Delete(cmdCtx, currentTxID, cmd.Key)
				if err != nil {
					result.Err = err
					result.Output = fmt.Sprintf("ERROR: %v", err)
				} else {
					result.Output = "OK"
				}
			}

		case CmdCommit:
			if currentTxID == "" || txClient == nil {
				result.Err = fmt.Errorf("no active transaction")
				result.Output = "ERROR: no active transaction"
			} else {
				err := txClient.Commit(cmdCtx, currentTxID)
				if err != nil {
					result.Err = err
					result.Output = fmt.Sprintf("ERROR: %v", err)
				} else {
					result.Output = "Committed"
				}
				currentTxID = ""
				txClient = nil
			}

		case CmdAbort:
			if currentTxID == "" || txClient == nil {
				result.Err = fmt.Errorf("no active transaction")
				result.Output = "ERROR: no active transaction"
			} else {
				err := txClient.Abort(cmdCtx, currentTxID)
				if err != nil {
					result.Err = err
					result.Output = fmt.Sprintf("ERROR: %v", err)
				} else {
					result.Output = "Aborted"
				}
				currentTxID = ""
				txClient = nil
			}
		}

		result.TxID = currentTxID
		result.Duration = time.Since(start)
		wr.Results = append(wr.Results, result)
		cancel()
	}

	wr.TotalDuration = time.Since(workerStart)
	return wr
}
