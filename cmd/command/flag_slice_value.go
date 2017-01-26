package command

import "strings"

type AppendSliceValue []string

func (s *AppendSliceValue) String() string {
	return strings.Join(*s, ",")
}

func (s *AppendSliceValue) Set(value string) error {
	*s = strings.Split(value, ",")
	return nil
}
