package command

import "strings"

// AppendSliceValue is a flag.Value implementation that accepts a comma delemited string and produces a string slice.
type AppendSliceValue []string

// String creates a string derived from s by joining each element in s with a ",".
func (s *AppendSliceValue) String() string {
	return strings.Join(*s, ",")
}

// Set accepts a string value and sets s to a string slice derived from value by splitting on ",".
func (s *AppendSliceValue) Set(value string) error {
	*s = strings.Split(value, ",")
	return nil
}
