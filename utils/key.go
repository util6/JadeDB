package utils

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
	"unsafe"
)

// stringStruct 是一个用于保存字符串指针及其长度的结构体。
type stringStruct struct {
	str unsafe.Pointer
	len int
}

// memHash 是一个散列函数，使用 runtime.linkname 调用运行时函数。
// 它利用可用的硬件指令（如果存在 AES 指令则表现为 aeshash）。
// 注意：每个进程的散列种子都会变化，因此不能用作持久散列。
//
//go:noescape
//go:linkname memHash runtime.memHash
func memHash(p unsafe.Pointer, h, s uintptr) uintptr

// ParseKey 从键字节中解析实际的键。
// 如果键的长度小于 8 字节，则返回原始键。
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}

	return key[:len(key)-8]
}

// ParseTs 从键字节中解析时间戳。
// 如果键的长度小于等于 8 字节，则返回 0。
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// SameKey 检查两个键是否相等，忽略版本时间戳后缀。
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

// KeyWithTs 通过将时间戳附加到键末尾来生成新的键。
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// MemHash 是 Go 映射使用的散列函数，它利用可用的硬件指令（如果存在 AES 指令则表现为 aeshash）。
// 注意：每个进程的散列种子都会变化，因此不能用作持久散列。
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memHash(ss.str, 0, uintptr(ss.len)))
}

// MemHashString 是 Go 映射使用的散列函数，它利用可用的硬件指令（如果存在 AES 指令则表现为 aeshash）。
// 注意：每个进程的散列种子都会变化，因此不能用作持久散列。
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memHash(ss.str, 0, uintptr(ss.len)))
}

// SafeCopy 执行类似 append(a[:0], src...) 的操作。
func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}

// NewCurVersion 返回当前版本的时间戳（以秒为单位）。
func NewCurVersion() uint64 {
	return uint64(time.Now().UnixNano() / 1e9)
}
