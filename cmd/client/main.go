package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/let-mil-go/internal/client"
	"github.com/let-mil-go/proto/shardpb"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "Mulberry server address (comma separated for multiple)")
	cmd := flag.String("cmd", "", "Command to execute (non-interactive): begin, read, write, delete, commit, abort")
	txIDFlag := flag.String("tx", "", "Transaction ID (required for non-interactive mode except begin)")
	keyFlag := flag.String("key", "", "Key for read/write/delete")
	valFlag := flag.String("val", "", "Value for write")

	flag.Parse()

	addrs := strings.Split(*addr, ",")
	c, err := client.NewClient(addrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	if *cmd != "" {
		runOneShot(c, *cmd, *txIDFlag, *keyFlag, *valFlag)
		return
	}

	runInteractive(c)
}

func runInteractive(c *client.Client) {
	reader := bufio.NewReader(os.Stdin)
	var currentTxID string
	fmt.Println("Mulberry Interactive Client")
	fmt.Println("Commands: start, read <key>, write <key> <val>, delete <key>, commit, abort, q")

	for {
		if currentTxID == "" {
			fmt.Print("(no-tx)> ")
		} else {
			fmt.Printf("(%s)> ", currentTxID)
		}

		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		parts := strings.Fields(line)

		if len(parts) == 0 {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		op := parts[0]

		switch op {
		case "q", "quit", "exit":
			cancel()
			return
		case "start", "begin":
			tid, err := c.BeginTransaction(ctx, shardpb.IsolationLevel_ISOLATION_SI)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				currentTxID = tid
				fmt.Printf("Transaction started: %s\n", tid)
			}
		case "read":
			if currentTxID == "" {
				fmt.Println("Error: No active transaction. Use 'start' first.")
				cancel()
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: read <key>")
				cancel()
				continue
			}
			val, found, err := c.Read(ctx, currentTxID, parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if !found {
				fmt.Println("<not found>")
			} else {
				fmt.Println(val)
			}
		case "write":
			if currentTxID == "" {
				fmt.Println("Error: No active transaction. Use 'start' first.")
				cancel()
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: write <key> <value>")
				cancel()
				continue
			}
			// Allow value to contain spaces
			key := parts[1]
			val := strings.Join(parts[2:], " ")
			err := c.Write(ctx, currentTxID, key, val)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "delete":
			if currentTxID == "" {
				fmt.Println("Error: No active transaction. Use 'start' first.")
				cancel()
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				cancel()
				continue
			}
			err := c.Delete(ctx, currentTxID, parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "commit":
			if currentTxID == "" {
				fmt.Println("Error: No active transaction.")
				cancel()
				continue
			}
			err := c.Commit(ctx, currentTxID)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Committed")
				currentTxID = ""
			}
		case "abort":
			if currentTxID == "" {
				fmt.Println("Error: No active transaction.")
				cancel()
				continue
			}
			err := c.Abort(ctx, currentTxID)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Aborted")
				currentTxID = ""
			}
		default:
			fmt.Println("Unknown command")
		}
		cancel()
	}
}

func runOneShot(c *client.Client, cmd, txID, key, val string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch cmd {
	case "begin":
		// Default to Snapshot Isolation for now
		tid, err := c.BeginTransaction(ctx, shardpb.IsolationLevel_ISOLATION_SI)
		if err != nil {
			die(err)
		}
		fmt.Println(tid)
	case "read":
		if txID == "" || key == "" {
			die(fmt.Errorf("tx and key required"))
		}
		v, found, err := c.Read(ctx, txID, key)
		if err != nil {
			die(err)
		}
		if !found {
			fmt.Println("<not found>")
		} else {
			fmt.Println(v)
		}
	case "write":
		if txID == "" || key == "" {
			die(fmt.Errorf("tx and key required"))
		}
		if err := c.Write(ctx, txID, key, val); err != nil {
			die(err)
		}
		fmt.Println("OK")
	case "delete":
		if txID == "" || key == "" {
			die(fmt.Errorf("tx and key required"))
		}
		if err := c.Delete(ctx, txID, key); err != nil {
			die(err)
		}
		fmt.Println("OK")
	case "commit":
		if txID == "" {
			die(fmt.Errorf("tx required"))
		}
		if err := c.Commit(ctx, txID); err != nil {
			die(err)
		}
		fmt.Println("Committed")
	case "abort":
		if txID == "" {
			die(fmt.Errorf("tx required"))
		}
		if err := c.Abort(ctx, txID); err != nil {
			die(err)
		}
		fmt.Println("Aborted")
	default:
		flag.Usage()
		os.Exit(1)
	}
}

func die(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
