package workload

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/let-mil-go/proto/shardpb"
)

// CmdType enumerates the workload command types.
type CmdType int

const (
	CmdStart CmdType = iota
	CmdRead
	CmdWrite
	CmdDelete
	CmdCommit
	CmdAbort
)

// Command represents a single operation in a worker's command list.
type Command struct {
	Type      CmdType
	Key       string                  // used by read/write/delete
	Value     string                  // used by write
	Isolation shardpb.IsolationLevel  // used by start; default SI
	Raw       string                  // original line text for display
}

// WorkerSpec describes one worker's name and ordered command list.
type WorkerSpec struct {
	Name     string
	Commands []Command
}

// Workload is the parsed result of a workload file.
type Workload struct {
	WorkerCount int          // declared count on first line
	Workers     []WorkerSpec // actual parsed workers
}

var isolationMap = map[string]shardpb.IsolationLevel{
	"RA":  shardpb.IsolationLevel_ISOLATION_RA,
	"CC":  shardpb.IsolationLevel_ISOLATION_CC,
	"PC":  shardpb.IsolationLevel_ISOLATION_PC,
	"PSI": shardpb.IsolationLevel_ISOLATION_PSI,
	"SI":  shardpb.IsolationLevel_ISOLATION_SI,
	"SER": shardpb.IsolationLevel_ISOLATION_SER,
}

// ParseFile opens a workload file and parses it.
func ParseFile(path string) (*Workload, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open workload file: %w", err)
	}
	defer f.Close()
	return ParseReader(f)
}

// ParseReader parses a workload from an io.Reader.
func ParseReader(r io.Reader) (*Workload, error) {
	scanner := bufio.NewScanner(r)
	lineNum := 0

	// Skip blank lines to find worker count.
	var workerCount int
	foundCount := false
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: expected worker count (integer), got %q", lineNum, line)
		}
		workerCount = n
		foundCount = true
		break
	}
	if !foundCount {
		return nil, fmt.Errorf("empty workload file: no worker count found")
	}

	var workers []WorkerSpec
	var current *WorkerSpec

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Check for worker label (ends with ':').
		if strings.HasSuffix(line, ":") {
			if current != nil {
				workers = append(workers, *current)
			}
			current = &WorkerSpec{Name: strings.TrimSuffix(line, ":")}
			continue
		}

		if current == nil {
			return nil, fmt.Errorf("line %d: command %q found before any worker label", lineNum, line)
		}

		cmd, err := parseCommand(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		current.Commands = append(current.Commands, cmd)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading workload: %w", err)
	}
	if current != nil {
		workers = append(workers, *current)
	}

	return &Workload{
		WorkerCount: workerCount,
		Workers:     workers,
	}, nil
}

func parseCommand(line string) (Command, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return Command{}, fmt.Errorf("empty command")
	}

	op := strings.ToLower(parts[0])
	switch op {
	case "start":
		iso := shardpb.IsolationLevel_ISOLATION_SI // default
		if len(parts) >= 2 {
			mapped, ok := isolationMap[strings.ToUpper(parts[1])]
			if !ok {
				return Command{}, fmt.Errorf("unknown isolation level %q in: %s", parts[1], line)
			}
			iso = mapped
		}
		return Command{Type: CmdStart, Isolation: iso, Raw: line}, nil

	case "read":
		if len(parts) < 2 {
			return Command{}, fmt.Errorf("read requires a key: %s", line)
		}
		return Command{Type: CmdRead, Key: parts[1], Raw: line}, nil

	case "write":
		if len(parts) < 3 {
			return Command{}, fmt.Errorf("write requires key and value: %s", line)
		}
		val := strings.Join(parts[2:], " ")
		return Command{Type: CmdWrite, Key: parts[1], Value: val, Raw: line}, nil

	case "delete":
		if len(parts) < 2 {
			return Command{}, fmt.Errorf("delete requires a key: %s", line)
		}
		return Command{Type: CmdDelete, Key: parts[1], Raw: line}, nil

	case "commit":
		return Command{Type: CmdCommit, Raw: line}, nil

	case "abort":
		return Command{Type: CmdAbort, Raw: line}, nil

	default:
		return Command{}, fmt.Errorf("unknown command %q in: %s", op, line)
	}
}
