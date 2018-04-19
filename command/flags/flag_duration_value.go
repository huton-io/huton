package flags

import (
	"time"
)

// DurationString is a flag.Value implementation that accepts a duration
// string and produces a time.Duration.
type DurationString time.Duration

// String returns the string representation of the DurationString.
func (s *DurationString) String() string {
	return time.Duration(*s).String()
}

// Set accepts a string value and parses it as a time.Duration to be used
// by a flag.FlagSet.
func (s *DurationString) Set(value string) error {
	d, err := time.ParseDuration(value)
	*s = DurationString(d)
	return err
}
