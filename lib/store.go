package huton

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	//ErrNumBytesMismatch is an error returned when the number of bytes that were persisted or read did not match expectations.
	ErrNumBytesMismatch = errors.New("the number of bytes did not match the intended number")

	// Endianess is the endianess to use when serializing bytes.
	Endianess = binary.LittleEndian
)

func (s *segment) persist(w io.Writer) error {
	bufWidth := len(s.buf)
	var written int
	if err := binary.Write(w, Endianess, int64(bufWidth)); err != nil {
		return err
	}
	n, err := w.Write(s.buf)
	if err != nil {
		return err
	}
	written += n
	meta, err := Uint64SliceToByteSlice(s.meta)
	if err != nil {
		return err
	}
	metaWdith := len(meta)
	if err := binary.Write(w, Endianess, int64(metaWdith)); err != nil {
		return err
	}
	n, err = w.Write(meta)
	if err != nil {
		return err
	}
	written += n
	expected := bufWidth + metaWdith
	if written != expected {
		return ErrNumBytesMismatch
	}
	return nil
}

func (s *segmentStack) persist(w io.Writer) error {
	stackSize := len(s.segments)
	if err := binary.Write(w, Endianess, int64(stackSize)); err != nil {
		return err
	}
	for i := 0; i < stackSize; i++ {
		seg := s.segments[i]
		if err := seg.persist(w); err != nil {
			return err
		}
	}
	return nil
}

func (c *cache) persist(w io.Writer) error {
	name := []byte(c.name)
	if err := binary.Write(w, Endianess, int64(len(name))); err != nil {
		return err
	}
	if _, err := w.Write(name); err != nil {
		return err
	}
	return c.Snapshot().(*segmentStack).persist(w)
}

func loadSegment(r io.Reader) (*segment, error) {
	var width int64
	if err := binary.Read(r, Endianess, &width); err != nil {
		return nil, err
	}
	buf := make([]byte, width)
	n, err := r.Read(buf)
	if err != nil {
		return nil, err
	}
	if int64(n) != width {
		return nil, ErrNumBytesMismatch
	}
	if err := binary.Read(r, Endianess, &width); err != nil {
		return nil, err
	}
	metaBuf := make([]byte, width)
	n, err = r.Read(metaBuf)
	if err != nil {
		return nil, err
	}
	if int64(n) != width {
		return nil, ErrNumBytesMismatch
	}
	meta, err := ByteSliceToUint64Slice(metaBuf)
	if err != nil {
		return nil, err
	}
	return &segment{
		buf:  buf,
		meta: meta,
	}, nil
}

func loadSegmentStack(r io.Reader) (*segmentStack, error) {
	var numStacks int64
	if err := binary.Read(r, Endianess, &numStacks); err != nil {
		return nil, err
	}
	segments := make([]*segment, numStacks)
	var err error
	for i := int64(0); i < numStacks; i++ {
		segments[i], err = loadSegment(r)
		if err != nil {
			return nil, err
		}
	}
	return &segmentStack{
		segments: segments,
	}, nil
}

func loadCache(r io.Reader, ins *instance) (*cache, error) {
	var nameWidth int64
	if err := binary.Read(r, Endianess, &nameWidth); err != nil {
		return nil, err
	}
	nameBuf := make([]byte, nameWidth)
	n, err := r.Read(nameBuf)
	if err != nil {
		return nil, err
	}
	if int64(n) != nameWidth {
		return nil, ErrNumBytesMismatch
	}
	stack, err := loadSegmentStack(r)
	if err != nil {
		return nil, err
	}
	return &cache{
		name:     string(nameBuf),
		stack:    stack,
		instance: ins,
	}, nil
}
