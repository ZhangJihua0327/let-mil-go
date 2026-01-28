package model

// Vote represents the vote in 2PC protocol.
type Vote int

const (
	VoteCommit Vote = iota
	VoteAbort
)

func (v Vote) String() string {
	switch v {
	case VoteCommit:
		return "COMMIT"
	case VoteAbort:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}

// WriteOp represents a single write operation in the write buffer.
type WriteOp struct {
	Key   string
	Value string
}

// PrepareMessage is sent from Router to Shard during 2PC prepare phase.
type PrepareMessage struct {
	TxId         string
	WriteBuffer  []WriteOp // Subset of writes for this shard
	CheckSet     []string  // Keys to check for conflicts
	SnapshotTime uint64    // HLC timestamp for snapshot read
}

// PrepareAck is sent from Shard to Coordinator after prepare phase.
type PrepareAck struct {
	TxId        string
	Vote        Vote
	PrepareTime uint64 // HLC timestamp when prepared
}

// CommitMessage is sent from Coordinator to Participants during commit phase.
type CommitMessage struct {
	TxId                 string
	FinalCommitTimestamp uint64 // HLC timestamp for final commit
}

// AbortMessage is sent from Coordinator to Participants to abort a transaction.
type AbortMessage struct {
	TxId string
}
