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

/*
Arena 内存管理模块

Arena 是一个高性能的内存分配器，专门为 JadeDB 的跳表数据结构设计。
它提供了无锁的内存分配机制，避免了频繁的内存分配和释放开销。

主要特性：
- 无锁设计：使用原子操作实现并发安全的内存分配
- 连续内存：所有分配的内存都在一个连续的缓冲区中
- 自动扩容：当内存不足时自动扩展缓冲区大小
- 内存对齐：确保分配的内存满足特定的对齐要求
- 高效访问：通过偏移量而非指针访问内存，减少指针开销

设计原理：
Arena 将所有数据存储在一个大的字节数组中，通过偏移量来定位数据。
这种设计避免了传统内存分配器的碎片化问题，提高了内存利用率。
同时，使用偏移量而非指针可以减少内存占用，特别是在 64 位系统上。

适用场景：
- 跳表节点的内存分配
- 键值对数据的存储
- 需要高性能内存分配的场景
- 内存使用模式相对固定的场景
*/

package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// offsetSize 定义了偏移量字段的字节大小。
	// 使用 uint32 类型来存储偏移量，在大多数情况下足够使用，
	// 同时比 uint64 节省一半的内存空间。
	// 在 32 位和 64 位系统上，uint32 的大小都是 4 字节。
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// nodeAlign 定义了节点内存对齐的要求。
	// 设置为 7（8-1），确保节点在 8 字节边界上对齐。
	// 这是因为跳表节点中使用了 atomic.LoadUint64 等原子操作，
	// 这些操作在某些架构上要求 64 位对齐才能正确工作。
	// 通过减去 1，我们得到了用于位运算的对齐掩码。
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	// MaxNodeSize 定义了单个跳表节点的最大字节大小。
	// 这个值通过 unsafe.Sizeof 在编译时计算得出，
	// 确保为节点分配足够的内存空间。
	// 实际的节点大小可能因高度不同而变化，但不会超过这个最大值。
	MaxNodeSize = int(unsafe.Sizeof(node{}))

	// maxCapacity 定义了 Arena 单次扩容的最大字节数。
	// 设置为 1GB（1 << 30），防止单次分配过大的内存块，
	// 避免内存不足或系统不稳定的问题。
	// 这个限制确保了内存使用的可控性和系统的稳定性。
	maxCapacity = 1 << 30
)

// Arena 是一个高性能的无锁内存分配器。
// 它专门为跳表等数据结构设计，提供连续的内存分配和高效的访问机制。
//
// 核心设计思想：
// 1. 所有数据存储在一个连续的字节数组中
// 2. 使用偏移量而非指针来引用数据，节省内存
// 3. 通过原子操作实现无锁的并发安全
// 4. 支持自动扩容，适应动态的内存需求
//
// 内存布局：
// - 偏移量 0 保留作为 NULL 指针的表示
// - 偏移量 1 开始存储实际数据
// - 所有分配都是连续的，没有内存碎片
//
// 并发安全：
// Arena 使用原子操作来管理内存分配，支持多个 goroutine 并发访问。
// 不需要额外的锁机制，提供了优秀的并发性能。
type Arena struct {
	// n 记录当前已分配的字节数（偏移量）。
	// 使用原子操作来确保并发安全，支持多线程同时分配内存。
	// 初始值为 1，因为偏移量 0 被保留用作 NULL 指针。
	n uint32

	// shouldGrow 控制是否允许自动扩容。
	// 当设置为 true 时，Arena 会在内存不足时自动扩展缓冲区。
	// 当设置为 false 时，内存分配会严格限制在当前缓冲区大小内。
	// 这个标志允许在不同场景下灵活控制内存使用策略。
	shouldGrow bool

	// buf 是存储所有数据的连续字节数组。
	// 这是 Arena 的核心存储空间，所有分配的数据都存储在这里。
	// 通过偏移量来访问其中的数据，避免了指针的开销。
	// 在需要时可以动态扩展这个数组的大小。
	buf []byte
}

// newArena 创建并初始化一个新的 Arena 实例。
// 这是 Arena 的构造函数，设置初始的内存池和相关参数。
//
// 参数说明：
// n: Arena 缓冲区的初始大小（字节）
//
// 返回值：
// 初始化完成的 Arena 实例指针
//
// 设计考虑：
// 1. 偏移量从 1 开始，保留 0 作为 NULL 指针
// 2. 初始缓冲区大小应该根据预期使用量合理设置
// 3. 过小的初始大小会导致频繁扩容，影响性能
// 4. 过大的初始大小会浪费内存资源
//
// 使用建议：
// - 对于跳表，建议初始大小为预期数据量的 1.5-2 倍
// - 考虑到扩容的开销，适当预留一些空间
// - 在内存受限的环境中，可以设置较小的初始值
func newArena(n int64) *Arena {
	// 创建 Arena 实例并初始化字段
	out := &Arena{
		n:          1,               // 从偏移量 1 开始，保留 0 作为 NULL
		shouldGrow: true,            // 默认允许自动扩容
		buf:        make([]byte, n), // 分配初始缓冲区
	}

	return out
}

// allocate 从 Arena 中分配指定大小的连续内存空间。
// 这是 Arena 的核心方法，使用原子操作实现无锁的内存分配。
//
// 参数说明：
// sz: 要分配的内存大小（字节）
//
// 返回值：
// 分配的内存空间的起始偏移量
//
// 工作原理：
// 1. 使用原子操作更新分配指针，确保并发安全
// 2. 检查是否需要扩容缓冲区
// 3. 如果需要扩容，计算合适的扩容大小并执行扩容
// 4. 返回分配的内存起始偏移量
//
// 扩容策略：
// - 默认扩容大小为当前缓冲区大小（翻倍策略）
// - 单次扩容不超过 1GB，避免内存压力
// - 确保扩容后有足够空间容纳当前分配请求
//
// 并发安全：
// 使用 atomic.AddUint32 原子操作更新分配指针，
// 多个 goroutine 可以安全地并发调用此方法。
func (s *Arena) allocate(sz uint32) uint32 {
	// 使用原子操作更新已分配字节数，相当于线程安全的：
	// oldOffset := s.n
	// s.n += sz
	// return oldOffset
	offset := atomic.AddUint32(&s.n, sz)

	// 如果禁用自动扩容，严格检查内存边界
	if !s.shouldGrow {
		// 确保分配的内存不超出缓冲区范围
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	// 检查是否需要扩容
	// 预留 MaxNodeSize 的空间，确保后续的节点分配不会越界
	if int(offset) > len(s.buf)-MaxNodeSize {
		// 计算扩容大小，默认为当前缓冲区大小（翻倍策略）
		growBy := uint32(len(s.buf))

		// 限制单次扩容的最大大小，避免内存压力
		if growBy > maxCapacity {
			growBy = maxCapacity
		}

		// 确保扩容大小至少能容纳当前分配请求
		if growBy < sz {
			growBy = sz
		}

		// 执行扩容：创建新的更大的缓冲区
		newBuf := make([]byte, len(s.buf)+int(growBy))

		// 将原有数据复制到新缓冲区
		// 使用 AssertTrue 确保复制操作完全成功
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))

		// 更新缓冲区引用
		s.buf = newBuf
	}

	// 返回分配的内存起始偏移量
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
	// 实现获取节点偏移量的逻辑
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
