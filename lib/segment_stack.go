package huton

import (
	"sync"
)

type segmentStack struct {
	segments []*segment
	mu       sync.Mutex
}

func (s *segmentStack) Get(key []byte) []byte {
	segStart := len(s.segments) - 1
	if segStart >= 0 {
		s.ensureSorted(0, segStart)
		for i := segStart; i >= 0; i-- {
			segment := s.segments[i]
			op, val := segment.get(key)
			if val != nil {
				if op == opDel {
					return nil
				}
				return val
			}
		}
	}
	return nil
}

func (s *segmentStack) ensureSorted(minSeg, maxSeg int) {
	sorted := true
	for seg := maxSeg; seg >= minSeg; seg-- {
		sorted = sorted && s.segments[seg].requestSort(false)
	}
	if !sorted {
		for seg := maxSeg; seg >= minSeg; seg-- {
			s.segments[seg].requestSort(true)
		}
	}
}
