package memo

// Policy selects which entry to evict when the cache is full.
type Policy int

const (
	// LRU evicts the least recently used entry.
	LRU Policy = iota
	// LFU evicts the least frequently used entry; ties go to the oldest access.
	LFU
	// FIFO evicts the oldest inserted entry, ignoring reads.
	FIFO
)

// String returns the policy name.
func (p Policy) String() string {
	switch p {
	case LRU:
		return "LRU"
	case LFU:
		return "LFU"
	case FIFO:
		return "FIFO"
	default:
		return "Policy(unknown)"
	}
}

func (p Policy) isValid() bool { return p >= LRU && p <= FIFO }
