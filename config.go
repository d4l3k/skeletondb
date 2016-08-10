package skeleton

import (
	"errors"
	"time"
)

// DefaultConfig options.
var DefaultConfig = Config{
	MaxKeysPerNode: 100,
	MaxDeltaCount:  10,
	GCTime:         24 * time.Hour,
}

// Config holds configuration options for DB.
type Config struct {
	// MaxKeysPerNode controls how many keys can be in each node before the node
	// is split.
	MaxKeysPerNode int
	// MaxDeltaCount controls how many deltas can be in each node before
	// consolidation.
	MaxDeltaCount int
	// GCTime is the amount of time until data is garbage collected.
	GCTime time.Duration
}

// Verify returns an error if an invariant is violated.
func (c Config) Verify() error {
	if c.MaxKeysPerNode <= 0 {
		return errors.New("MaxKeysPerNode must be positive")
	}
	if c.MaxDeltaCount <= 0 {
		return errors.New("MaxDeltaCount must be positive")
	}
	if c.GCTime < 0 {
		return errors.New("GCTime must not be negative")
	}
	return nil
}
