package model

// IsolationLevel defines the rules for visibility and conflicts.
type IsolationLevel int

const (
	RA  IsolationLevel = iota // Read Atomic
	CC                        // Causal Consistency
	PC                        // Prefix Consistency
	PSI                       // Parallel Snapshot Isolation
	SI                        // Snapshot Isolation
	SER                       // Serializable
)

func (l IsolationLevel) String() string {
	switch l {
	case RA:
		return "RA"
	case CC:
		return "CC"
	case PC:
		return "PC"
	case PSI:
		return "PSI"
	case SI:
		return "SI"
	case SER:
		return "SER"
	default:
		return "UNKNOWN"
	}
}
