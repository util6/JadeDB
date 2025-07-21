// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"
)

const (
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	ValueLogHeaderSize = 20
	vptrSize           = unsafe.Sizeof(ValuePtr{})
)

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

// Less 比较当前值指针与另一个值指针的大小。
// 这个方法首先比较 Fid，然后 Offset，最后 Len。
// 如果当前实例的值小于 o，则返回 true。
// 参数:
//
//	o (*ValuePtr): 用于比较的值指针。
//
// 返回值:
//
//	bool: 如果当前实例的值小于 o，则返回 true；否则返回 false。
func (p ValuePtr) Less(o *ValuePtr) bool {
	// 检查 o 是否为 nil，如果是，则返回 false。
	if o == nil {
		return false
	}

	// 比较 Fid，如果不同，则返回较小的 Fid。
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}

	// Fid 相同，比较 Offset，如果不同，则返回较小的 Offset。
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}

	// Offset 相同，比较 Len，返回较小的 Len。
	return p.Len < o.Len
}

func (p ValuePtr) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

// Encode encodes Pointer into byte buffer.
func (p ValuePtr) Encode() []byte {
	b := make([]byte, vptrSize)
	// Copy over the content from p to b.
	*(*ValuePtr)(unsafe.Pointer(&b[0])) = p
	return b
}

// Decode decodes the valueIndex pointer into the provided byte buffer.
func (p *ValuePtr) Decode(b []byte) {
	// Copy over data from b into p. Using *p=unsafe.pointer(...) leads to
	copy(((*[vptrSize]byte)(unsafe.Pointer(p))[:]), b[:vptrSize])
}
func IsValuePtr(e *Entry) bool {
	return e.Meta&BitValuePointer > 0
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// BytesToU64 _
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U32SliceToBytes 将给定的uint32切片转换为字节切片
// 此函数主要用于将uint32类型的切片数据无损地转换为字节切片形式，以便进行网络传输或存储
// 参数u32s: 输入的uint32类型切片，表示需要转换的数据
// 返回值: 返回转换后的字节切片如果输入切片为空，则返回nil
func U32SliceToBytes(u32s []uint32) []byte {
	// 检查输入切片是否为空，如果为空则直接返回nil
	if len(u32s) == 0 {
		return nil
	}
	// 初始化一个空的字节切片，用于存储转换后的数据
	var b []byte
	// 通过反射获取字节切片的指针，以便进行底层数据操作
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	// 设置字节切片的长度为uint32切片长度乘以4（因为uint32为4字节）
	hdr.Len = len(u32s) * 4
	// 设置字节切片的容量为与其长度相同，表示连续内存区域的大小
	hdr.Cap = hdr.Len
	// 将字节切片的数据指针指向uint32切片的起始地址，实现数据共享
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	// 返回转换后的字节切片
	return b
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

// ValuePtrCodec _
func ValuePtrCodec(vp *ValuePtr) []byte {
	return []byte{}
}

// RunCallback _
func RunCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

// DiscardEntry 判断是否丢弃给定的条目。根据版本、元数据和过期状态来决定。
// 参数:
//   e: 要评估的条目。
//   vs: 用于比较的标准条目。
// 返回值:
//   如果应丢弃该条目则返回 true；否则返回 false。

func DiscardEntry(e, vs *Entry) bool {
	// TODO 版本这个信息应该被弱化掉 在后面上 MVCC 或者多版本查询的时候再考虑
	// if vs.Version != ParseTs(e.Key) {
	// 	// Version not found. Discard.
	// 	return true
	// }
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	if (vs.Meta & BitValuePointer) == 0 {
		// Key 也存储了 LSM 中的 valueIndex。丢弃。
		return true
	}
	return false
}
