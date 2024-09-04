/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// offsetSize 定义了用于偏移量的大小，使用 unsafe 包来获取系统级的大小信息。
	// 这里使用 uint32 类型的 0 值的大小来初始化。值为4
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// nodeAlign 用于确保节点在 64 位边界上对齐，即使在 32 位架构上也是如此。
	// 这是因为 node.getValueOffset 使用了原子操作 atomic.LoadUint64，该操作要求其输入指针必须是 64 位对齐的。
	// 这里通过减去 1 来确保得到的值是 64 位的对齐偏移量。
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	// MaxNodeSize 定义了节点的最大大小，通过使用 unsafe 包来获取 node 结构体的系统级大小。
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// Arena 是一个无锁的内存管理结构，用于高效地管理和分配内存。
// 它通过避免使用锁机制来提高并发性能。
type Arena struct {
	// n 用于记录当前Arena中已分配的字节数。
	n uint32

	// shouldGrow 标志指示是否应该在分配新对象时增加缓冲区大小。
	shouldGrow bool

	// buf 是一个字节数组，用于存储分配的内存对象。
	buf []byte
}

// newArena 创建一个新的 Arena 实例，用于管理缓冲区。
// 参数 n 指定 Arena 缓冲区的初始大小。
// 不在位置 0 存储数据，以便将偏移量 offset=0 用作一种空指针。
func newArena(n int64) *Arena {
	// 初始化 Arena 结构体，n 设置为 1，表示至少有一个元素。
	// buf 是一个字节数组，用于存储 Arena 中的数据。
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	// 返回初始化后的 Arena 指针。
	return out
}

// allocate 从 Arena 的缓冲区中分配指定大小的内存空间。
// 它会更新已分配的内存偏移量，并在必要时扩容缓冲区。
// 参数 sz: 要分配的内存大小（以字节为单位）。
// 返回值: 分配的内存空间的起始偏移量。
func (s *Arena) allocate(sz uint32) uint32 {
	// 相当于线程安全的：offset = s.n+sz

	offset := atomic.AddUint32(&s.n, sz)

	// 如果不应扩容，那么检查偏移量是否超出当前缓冲区的范围。
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	if int(offset) > len(s.buf)-MaxNodeSize {

		//初始化扩容大小 growBy 为当前缓冲区的长度。
		growBy := uint32(len(s.buf))

		//检查扩容大小是否超过了 1GB (1<<30 字节)。
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		//即 sz<=growBy<=1GB

		// 扩容缓冲区,+int(growBy)是为了避免 checkptr 错误，我们需要在缓冲区末尾保留额外的字节。
		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// memoryAligned计算并返回对齐后的内存偏移量。
// 这个方法考虑到Arena中由于高度不足而无法使用的部分，
// 并确保返回的指针是按照nodeAlign对齐的。
// 参数height是当前Arena的高度，用于计算无法使用的部分大小。
// 返回值是经过对齐处理后的内存偏移量。
func (s *Arena) memoryAligned(height int) uint32 {
	// 计算由于高度小于maxHeight而永远无法使用的塔的部分大小。
	unusedSize := (maxHeight - height) * offsetSize

	// 为分配加上足够的字节，以确保指针对齐。
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	// 返回对齐后的偏移量。
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// Put 会将 val 复制到 arena 中。为了更好地利用此功能，请重用输入 val 缓冲区。
// 返回值是 val 在 buf 中的偏移量。用户负责记住 val 的大小。
// 我们也可以在 arena 内部存储这个大小，但是这样做会导致编码和解码时产生额外开销。
//
// 参数:
//
//	v: 要存储到 arena 中的 ValueStruct 实例。
//
// 返回:
//
//	uint32: 编码后的值在 buf 中的起始偏移量。
func (s *Arena) putVal(v ValueStruct) uint32 {
	// 计算 v 编码后的大小
	l := uint32(v.EncodedSize())

	// 在 arena 中分配空间
	offset := s.allocate(l)

	// 将 v 编码并存入已分配的空间
	v.EncodeValue(s.buf[offset:])

	// 返回存储位置的偏移量
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode 根据给定的偏移量返回指向节点的指针。如果偏移量为零，则返回nil指针。
// 该方法用于在Arena的内部缓冲区中定位特定的节点。
// 参数:
//
//	offset - 节点在缓冲区中的偏移量，如果偏移量为0，则假定该节点不存在。
//
// 返回值:
//
//	如果偏移量非零，则返回该偏移量处的节点指针；否则返回nil。
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		// 当偏移量为0时，表示没有有效的节点，返回nil指针。
		return nil
	}
	// 通过偏移量计算节点在缓冲区中的位置，并返回该位置的节点指针。
	// 这里使用了不安全的操作，将缓冲区中的数据视为node结构体。
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

// getKey returns byte slice at offset.
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// getVal returns byte slice at offset. The given size should be just the valueIndex
// size and should NOT include the meta bytes.
func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
// Arena.getNodeOffset 获取节点在Arena中的偏移量
// 参数:
//
//	nd: 指向node的指针
//
// 返回值:
//
//	uint32: 节点在Arena中的偏移量，如果输入为nil，则返回0
func (s *Arena) getNodeOffset(nd *node) uint32 {
	// 当输入节点为nil时，直接返回0
	if nd == nil {
		return 0
	}
	// TODO: 实现获取节点偏移量的逻辑
	// 计算并返回节点在Arena中的偏移量
	// 使用unsafe包来计算指针之间的偏移量
	// 这里通过计算节点指针nd与Arena缓冲区起始指针的差值，得到节点在Arena中的偏移量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
