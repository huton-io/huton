package huton

import (
	"reflect"
	"unsafe"
)

// Uint64SliceToByteSlice converts a []uint64 to a []byte.
func Uint64SliceToByteSlice(in []uint64) ([]byte, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []byte
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len * 8
	outHeader.Cap = inHeader.Cap * 8

	return out, nil
}

// ByteSliceToUint64Slice converts a []byte to a []uint64
func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []uint64
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 8
	outHeader.Cap = outHeader.Len

	return out, nil
}
