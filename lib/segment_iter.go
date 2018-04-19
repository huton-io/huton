package huton

import (
	"io"
)

type segmentCursor struct {
	s     *segment
	start int
	end   int
	curr  int
}

func (c *segmentCursor) current() (op uint64, key, val []byte) {
	if c.curr >= c.start {
		op, key, val = c.s.getOpKeyVal(c.curr)
	}
	return op, key, val
}

func (c *segmentCursor) next() error {
	if c.curr >= c.end {
		return io.EOF
	}
	c.curr++
	return nil
}

func (s *segment) cursor() *segmentCursor {
	return &segmentCursor{
		s:   s,
		end: s.Len(),
	}
}
