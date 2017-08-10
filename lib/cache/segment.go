package cache

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

const (
	opSet = uint64(0x0100000000000000)
	opDel = uint64(0x0100000000000000)

	maxKeyLength = 1<<24 - 1
	maxValLength = 1<<28 - 1

	maskKeyLength = uint64(0x00FFFFFF00000000)
	maskValLength = uint64(0x000000000FFFFFFF)

	maskOperation = uint64(0x0F00000000000000)
)

var (
	ErrKeyTooLarge      = errors.New("key is too large")
	ErrValTooLarge      = errors.New("value is too large")
	ErrAlreadyCommitted = errors.New("Batch is already committed")
)

type segment struct {
	buf          []byte
	meta         []uint64
	sorterCh     chan bool
	commitMu     sync.Mutex
	commited     bool
	waitSortedCh chan struct{}
}

func (s *segment) Set(key, val []byte) error {
	return s.mutate(opSet, key, val)
}

func (s *segment) Del(key []byte) error {
	return s.mutate(opDel, key, nil)
}

func (s *segment) Len() int {
	return len(s.meta) / 2
}

func (s *segment) Swap(i, j int) {
	x := i * 2
	y := j * 2
	s.meta[x], s.meta[y] = s.meta[y], s.meta[x]
	x++
	y++
	s.meta[x], s.meta[y] = s.meta[y], s.meta[x]
}

func (s *segment) Less(i, j int) bool {
	x := i * 2
	y := j * 2
	kxLength := int((maskKeyLength & s.meta[x]) >> 32)
	kxStart := int(s.meta[x+1])
	kx := s.buf[kxStart : kxStart+kxLength]
	kyLength := int((maskKeyLength & s.meta[y]) >> 32)
	kyStart := int(s.meta[y+1])
	ky := s.buf[kyStart : kyStart+kyLength]
	return bytes.Compare(kx, ky) < 0
}

func (s *segment) get(key []byte) (uint64, []byte) {
	pos := s.findKeyPos(key)
	if pos >= 0 {
		op, _, val := s.getOpKeyVal(pos)
		return op, val
	}
	return 0, nil
}

func (s *segment) findKeyPos(key []byte) int {
	i, j := 0, s.Len()
	if i == j {
		return -1
	}
	// If key smaller than smallest key, return early.
	startKeyLen := int((maskKeyLength & s.meta[0]) >> 32)
	startKeyBeg := int(s.meta[1])
	startCmp := bytes.Compare(key, s.buf[startKeyBeg:startKeyBeg+startKeyLen])
	if startCmp < 0 {
		return -1
	}
	for i < j {
		h := i + (j-i)/2 // Keep i <= h < j.
		x := h * 2
		klen := int((maskKeyLength & s.meta[x]) >> 32)
		kbeg := int(s.meta[x+1])
		cmp := bytes.Compare(s.buf[kbeg:kbeg+klen], key)
		if cmp == 0 {
			return h
		} else if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return -1
}

func (s *segment) getOpKeyVal(pos int) (uint64, []byte, []byte) {
	x := pos * 2
	if x < len(s.meta) {
		opklvl := s.meta[x]
		kstart := int(s.meta[x+1])
		operation, keyLen, valLen := decode(opklvl)
		vstart := kstart + keyLen

		return operation, s.buf[kstart:vstart], s.buf[vstart : vstart+valLen]
	}

	return 0, nil, nil
}

func (s *segment) requestSort(synchronous bool) bool {
	if s.sorterCh == nil {
		return true
	}
	iAmSorter := <-s.sorterCh
	if iAmSorter {
		sort.Sort(s)
		close(s.waitSortedCh)
		return true
	}
	if synchronous {
		<-s.waitSortedCh
		return true
	}
	return false
}

func (s *segment) Sort() {
	sort.Sort(s)
}

func (s *segment) isCommitted() bool {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	return s.commited
}

func (s *segment) markCommitted() {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.commited = true
}

func (s *segment) mutate(op uint64, key, val []byte) error {
	if s.isCommitted() {
		return ErrAlreadyCommitted
	}
	keyStart := len(s.buf)
	s.buf = append(s.buf, key...)
	keyLength := len(s.buf) - keyStart
	valStart := len(s.buf)
	s.buf = append(s.buf, val...)
	valLength := len(s.buf) - valStart
	return s.mutateEx(op, keyStart, keyLength, valLength)
}

func (s *segment) mutateEx(op uint64, keyStart, keyLength, valLength int) error {
	if keyLength > maxKeyLength {
		return ErrKeyTooLarge
	}
	if valLength > maxValLength {
		return ErrValTooLarge
	}
	if keyStart <= 0 && valLength <= 0 {
		keyStart = 0
	}
	encoded := encode(op, keyLength, valLength)
	s.meta = append(s.meta, encoded, uint64(keyStart))
	return nil
}

func (s *segment) isEmpty() bool {
	return s.Len() <= 0
}

func encode(op uint64, keyLength, valLength int) uint64 {
	return (maskOperation & op) |
		(maskKeyLength & (uint64(keyLength) << 32)) |
		(maskValLength & (uint64(valLength)))
}

func decode(encoded uint64) (uint64, int, int) {
	op := maskOperation & encoded
	keyLength := int((maskKeyLength & encoded) >> 32)
	valLength := int(maskValLength & encoded)
	return op, keyLength, valLength
}

func newSegment(totalOps, totalBufSize int) *segment {
	return &segment{
		buf:  make([]byte, 0, totalBufSize),
		meta: make([]uint64, 0, totalOps),
	}
}
