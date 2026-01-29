package hlc

import (
	"sync"
	"time"
)

const (
	physicalBits = 48
	logicalBits  = 16
	logicalMask  = (1 << logicalBits) - 1
	maxLogical   = logicalMask
)

// Clock is a hybrid logical clock.
// High 48 bits: physical time in milliseconds
// Low 16 bits: logical counter
type Clock struct {
	mu    sync.Mutex
	ts    uint64
	nowFn func() uint64
}

var lock = &sync.Mutex{}
var singleton *Clock

func GetClock() *Clock {
	if singleton == nil {
		lock.Lock()
		defer lock.Unlock()
		if singleton == nil {
			singleton = NewWithNowFn(physicalNow)
		}
	}
	return singleton
}

// NewWithNowFn creates a new HLC clock with a custom time source (for testing).
func NewWithNowFn(nowFn func() uint64) *Clock {
	return &Clock{
		nowFn: nowFn,
	}
}

// Tick generates the next HLC timestamp.
// If physical time advances, resets logical counter.
// Otherwise, increments logical counter.
func (c *Clock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	physNow := c.nowFn()
	physCur := Physical(c.ts)

	if physNow > physCur {
		c.ts = Compose(physNow, 0)
	} else {
		logical := Logical(c.ts)
		if logical < maxLogical {
			c.ts = Compose(physCur, logical+1)
		} else {
			// Logical overflow: advance physical time by 1ms
			c.ts = Compose(physCur+1, 0)
		}
	}
	return c.ts
}

// Update updates the clock with an incoming timestamp.
// Returns the new local timestamp.
func (c *Clock) Update(incoming uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	physNow := c.nowFn()
	physLocal := Physical(c.ts)
	physIncoming := Physical(incoming)

	maxPhys := max3(physNow, physLocal, physIncoming)

	var logical uint16
	switch maxPhys {
	case physNow:
		if physNow == physLocal && physNow == physIncoming {
			logical = max(Logical(c.ts), Logical(incoming)) + 1
		} else if physNow == physLocal {
			logical = Logical(c.ts) + 1
		} else if physNow == physIncoming {
			logical = Logical(incoming) + 1
		} else {
			logical = 0
		}
	case physLocal:
		if physLocal == physIncoming {
			logical = max(Logical(c.ts), Logical(incoming)) + 1
		} else {
			logical = Logical(c.ts) + 1
		}
	default:
		logical = Logical(incoming) + 1
	}

	if logical > maxLogical {
		maxPhys++
		logical = 0
	}

	c.ts = Compose(maxPhys, logical)
	return c.ts
}

// Now returns the current HLC timestamp without advancing.
func (c *Clock) Now() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ts
}

// Compose creates an HLC timestamp from physical time and logical counter.
func Compose(physical uint64, logical uint16) uint64 {
	return (physical << logicalBits) | uint64(logical)
}

// Physical extracts the physical time (high 48 bits) from an HLC timestamp.
func Physical(ts uint64) uint64 {
	return ts >> logicalBits
}

// Logical extracts the logical counter (low 16 bits) from an HLC timestamp.
func Logical(ts uint64) uint16 {
	return uint16(ts & logicalMask)
}

func physicalNow() uint64 {
	return uint64(time.Now().UnixMilli())
}

func max3(a, b, c uint64) uint64 {
	if a >= b && a >= c {
		return a
	}
	if b >= c {
		return b
	}
	return c
}
