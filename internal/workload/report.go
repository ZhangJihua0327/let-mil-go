package workload

import (
	"fmt"
	"io"
	"os"
	"time"
)

// PrintReport writes per-worker results and a global summary to stdout.
func PrintReport(results []*WorkerResult) {
	printReportTo(os.Stdout, results)
}

func printReportTo(w io.Writer, results []*WorkerResult) {
	for _, wr := range results {
		fmt.Fprintf(w, "\n=== Worker: %s ===\n", wr.Name)
		for _, cr := range wr.Results {
			label := cr.Cmd.Raw
			dur := formatDuration(cr.Duration)
			if cr.Err != nil {
				fmt.Fprintf(w, "  [%-20s] -> %-30s (%s)\n", label, cr.Output, dur)
			} else {
				fmt.Fprintf(w, "  [%-20s] -> %-30s (%s)\n", label, cr.Output, dur)
			}
		}
		fmt.Fprintf(w, "  Worker Total: %s\n", formatDuration(wr.TotalDuration))
	}

	// Global summary.
	totalCmds := 0
	succeeded := 0
	failed := 0
	var maxDur time.Duration
	var minDur time.Duration
	var sumDur time.Duration
	maxName := ""
	minName := ""

	for i, wr := range results {
		for _, cr := range wr.Results {
			totalCmds++
			if cr.Err != nil {
				failed++
			} else {
				succeeded++
			}
		}
		sumDur += wr.TotalDuration
		if i == 0 || wr.TotalDuration > maxDur {
			maxDur = wr.TotalDuration
			maxName = wr.Name
		}
		if i == 0 || wr.TotalDuration < minDur {
			minDur = wr.TotalDuration
			minName = wr.Name
		}
	}

	var avgDur time.Duration
	if len(results) > 0 {
		avgDur = sumDur / time.Duration(len(results))
	}

	fmt.Fprintf(w, "\n========== Summary ==========\n")
	fmt.Fprintf(w, "Total Workers:    %d\n", len(results))
	fmt.Fprintf(w, "Total Commands:   %d\n", totalCmds)
	fmt.Fprintf(w, "Succeeded:        %d\n", succeeded)
	fmt.Fprintf(w, "Failed:           %d\n", failed)
	fmt.Fprintf(w, "Wall Clock Time:  %s\n", formatDuration(maxDur))
	fmt.Fprintf(w, "Avg Worker Time:  %s\n", formatDuration(avgDur))
	fmt.Fprintf(w, "Max Worker Time:  %s  (%s)\n", formatDuration(maxDur), maxName)
	fmt.Fprintf(w, "Min Worker Time:  %s  (%s)\n", formatDuration(minDur), minName)
	fmt.Fprintf(w, "=============================\n")
}

func formatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.2fus", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}
