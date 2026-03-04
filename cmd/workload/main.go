package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/let-mil-go/internal/workload"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "Mulberry server addresses (comma-separated)")
	file := flag.String("file", "", "Workload file path (required)")
	flag.Parse()

	if *file == "" {
		fmt.Fprintln(os.Stderr, "Error: -file is required")
		flag.Usage()
		os.Exit(1)
	}

	// Parse workload file.
	wl, err := workload.ParseFile(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing workload: %v\n", err)
		os.Exit(1)
	}

	if len(wl.Workers) != wl.WorkerCount {
		fmt.Fprintf(os.Stderr, "Warning: declared %d workers, but found %d\n", wl.WorkerCount, len(wl.Workers))
	}

	fmt.Printf("Workload: %d workers loaded\n", len(wl.Workers))

	// Create client pool: one client per router address.
	addrs := strings.Split(*addr, ",")
	pool, err := workload.NewClientPool(addrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating client pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	fmt.Printf("Connected to %d router(s): %s\n", len(addrs), *addr)

	// Run all workers.
	ctx := context.Background()
	results := workload.Run(ctx, wl, pool)

	// Print report.
	workload.PrintReport(results)
}
