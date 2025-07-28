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

/*
JadeDB 值处理模块

本模块负责处理数据库中的值相关操作，包括：
- 值指针（ValuePtr）的管理和编解码
- 大值的分离存储策略
- 字节序列化和反序列化工具
- 值的生命周期管理

核心概念：
1. 值指针：指向值日志中实际数据的引用
2. 值分离：大值存储在值日志中，LSM 树只存储指针
3. 编解码：高效的二进制序列化格式
4. 生命周期：值的创建、访问、过期和清理

设计优势：
- 减少 LSM 树的写放大问题
- 提高大值存储和访问的效率
- 支持高效的垃圾回收机制
- 优化内存使用和磁盘空间

适用场景：
- 存储大型对象（图片、文档等）
- 减少索引结构的大小
- 优化压缩和合并性能
- 实现高效的数据清理
*/

package utils

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"
)

const (
	// ValueLogHeaderSize 定义值日志文件头的字节大小。
	// 值日志文件头包含以下信息：
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	// - keyID: 8字节的密钥标识符，用于加密和验证
	// - baseIV: 12字节的初始化向量，用于加密算法
	// 总共 20 字节的头部信息，用于文件完整性和安全性验证。
	ValueLogHeaderSize = 20

	// vptrSize 是 ValuePtr 结构体的字节大小。
	// 通过 unsafe.Sizeof 在编译时计算得出，确保跨平台一致性。
	// 用于内存分配和序列化操作的大小计算。
	vptrSize = unsafe.Sizeof(ValuePtr{})
)

// ValuePtr 表示指向值日志中数据的指针。
// 这是 JadeDB 实现值分离存储的核心数据结构。
//
// 设计原理：
// 当值的大小超过配置的阈值时，实际的值数据存储在值日志文件中，
// 而 LSM 树中只存储这个轻量级的指针结构。
//
// 优势：
// 1. 减少 LSM 树的大小，提高索引性能
// 2. 减少写放大，因为大值不需要在压缩时重复写入
// 3. 支持高效的垃圾回收，可以批量清理无效值
// 4. 优化内存使用，避免在内存中缓存大值
//
// 使用场景：
// - 存储大于阈值的值（通常是几KB到几MB）
// - 需要高效垃圾回收的场景
// - 写入密集型应用，减少写放大
type ValuePtr struct {
	// Len 指定值的字节长度。
	// 用于从值日志中读取正确数量的数据。
	// 最大支持 4GB 的单个值（uint32 的最大值）。
	Len uint32

	// Offset 指定值在文件中的字节偏移量。
	// 从文件开头计算的绝对位置。
	// 用于快速定位和读取值数据。
	Offset uint32

	// Fid 是值日志文件的标识符。
	// 每个值日志文件都有唯一的 ID。
	// 用于确定值存储在哪个文件中。
	Fid uint32
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
	// 版本信息处理策略：
	// 当前实现中版本信息被弱化处理，不进行严格的版本比较
	// 这是为了简化当前的实现，在后续引入MVCC或多版本查询时再考虑严格的版本控制
	//
	// 被注释的版本检查逻辑：
	// if vs.Version != ParseTs(e.Key) {
	// 	// Version not found. Discard.
	// 	return true
	// }

	// 检查条目是否已删除或过期
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}

	// 检查是否为值指针类型
	if (vs.Meta & BitValuePointer) == 0 {
		// Key 也存储了 LSM 中的 valueIndex。丢弃。
		return true
	}

	return false
}
