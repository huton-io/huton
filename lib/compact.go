package huton

import (
	"fmt"
	"time"
)

const (
	defaultCompactionInterval = time.Hour
)

// Compactor is a function that performs cache compaction. Any errors encountered during compaction
//are pushed to the provided error channel. Make sure that something is consuming from this channel
//regularly, otherwise it may fill up, causing compaction to halt until more space is available.
// The last paramter is used to signal that the compactor should stop processing.
type Compactor func(*Cache, chan<- error, <-chan struct{})

// PeriodicCompactor is a Compactor that performs compaction on at least a scheduled interval.
// If the provided interval is less than the duration of the compaction process, drift will occur,
// and the next compaction task will not start until the previous one finishes. Finding the correct
// interval for your application will require some tuning. If the provided interval is less than or
// equal to zero, then a conservative default of 1 hour is used.
func PeriodicCompactor(interval time.Duration) Compactor {
	if interval <= 0 {
		interval = defaultCompactionInterval
	}
	return func(cache *Cache, errCh chan<- error, shutdownCh <-chan struct{}) {
		timer := time.NewTicker(interval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := cache.Compact(); err != nil {
					errCh <- fmt.Errorf("failed to perform compaction on cache %s: %s", cache.Name(), err)
				}
			case <-shutdownCh:
				return
			}
		}
	}
}
