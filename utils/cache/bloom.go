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
JadeDB 布隆过滤器实现

布隆过滤器是一种空间效率极高的概率型数据结构，用于快速判断元素是否存在于集合中。
在 JadeDB 的缓存系统中，布隆过滤器用于准入控制，防止缓存污染。

核心特性：
1. 空间高效：使用位数组存储，内存占用极小
2. 查询快速：O(k) 时间复杂度，k 为哈希函数数量
3. 无假阴性：如果元素存在，一定返回 true
4. 有假阳性：如果元素不存在，可能返回 true

工作原理：
- 插入：使用 k 个哈希函数计算位置，设置对应位为 1
- 查询：使用相同的 k 个哈希函数检查对应位
- 如果任何一位为 0，元素一定不存在
- 如果所有位都为 1，元素可能存在

在缓存系统中的应用：
- 准入控制：只有在布隆过滤器中存在的数据才能进入长期缓存
- 污染防护：防止一次性访问的数据污染缓存
- 访问历史：记录数据是否曾经被访问过
- 性能优化：快速过滤，减少不必要的缓存操作

参数调优：
- 位数组大小：影响假阳性率和内存使用
- 哈希函数数量：影响计算开销和假阳性率
- 最优公式：k = (m/n) * ln(2)，m为位数，n为元素数
- 假阳性率：p = (1 - e^(-kn/m))^k

性能优化：
- 高效哈希：使用快速的哈希函数
- 位操作：直接操作位数组，避免额外开销
- 缓存友好：紧凑的内存布局
- 并发安全：支持并发读取操作
*/

package cache

import "math"

const (
	// BloomFilterMaxHashes 定义布隆过滤器中允许的最大哈希函数数量。
	// 限制哈希函数数量可以：
	// 1. 控制计算开销，避免过多的哈希计算
	// 2. 防止配置错误，确保合理的参数范围
	// 3. 保持性能稳定，避免极端情况下的性能下降
	// 4. 简化实现，减少边界条件的处理
	BloomFilterMaxHashes = 30
)

// Filter 表示编码后的布隆过滤器位数组。
// 它是一个字节数组，紧凑地存储了过滤器的所有位信息。
//
// 存储格式：
// - 每个字节包含 8 个位
// - 位的索引从 0 开始计算
// - 支持任意大小的位数组
//
// 设计优势：
// - 内存紧凑：每个位只占用 1 bit 空间
// - 访问高效：通过位运算快速访问和设置
// - 可序列化：可以直接写入磁盘或网络传输
type Filter []byte

// BloomFilter 实现了高性能的布隆过滤器。
// 它使用多个哈希函数和位数组来实现概率型的集合成员测试。
//
// 核心组件：
// - 位数组：存储过滤器的状态信息
// - 哈希函数数量：控制假阳性率和性能
//
// 算法特点：
// - 插入操作：设置多个位为 1
// - 查询操作：检查多个位的状态
// - 删除操作：不支持（布隆过滤器的固有限制）
//
// 性能特征：
// - 时间复杂度：O(k)，k 为哈希函数数量
// - 空间复杂度：O(m)，m 为位数组大小
// - 假阳性率：可配置，通常在 1%-10% 之间
type BloomFilter struct {
	// bitmap 是布隆过滤器的位数组。
	// 存储过滤器的所有状态信息。
	// 每个位表示一个哈希位置的状态。
	bitmap Filter

	// k 是哈希函数的数量。
	// 影响假阳性率和计算开销。
	// 通常根据预期元素数量和位数组大小计算得出。
	k uint8
}

// MayContainKey 检查给定的键是否可能存在于过滤器中。
// 这是面向用户的主要接口，内部会计算键的哈希值。
//
// 参数说明：
// k: 要检查的键（字节数组）
//
// 返回值：
// true: 键可能存在（可能是假阳性）
// false: 键一定不存在
//
// 使用场景：
// - 缓存准入控制：检查数据是否曾经被访问
// - 快速过滤：避免不必要的缓存操作
// - 访问历史：判断数据的访问历史
//
// 注意事项：
// - 返回 true 不保证键真实存在
// - 返回 false 保证键一定不存在
// - 假阳性率取决于过滤器的配置参数
func (f *BloomFilter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain 检查给定的哈希值是否可能存在于过滤器中。
// 这是布隆过滤器的核心查询方法，实现了概率型的成员测试。
//
// 参数说明：
// h: 要检查的哈希值
//
// 返回值：
// true: 哈希值可能存在（可能是假阳性）
// false: 哈希值一定不存在
//
// 算法实现：
// 1. 检查过滤器的有效性
// 2. 使用多个哈希函数计算位置
// 3. 检查所有对应位是否都为 1
// 4. 任何一位为 0 则返回 false
//
// 性能优化：
// - 早期退出：任何一位为 0 立即返回 false
// - 高效哈希：使用位运算生成多个哈希值
// - 缓存友好：顺序访问位数组
//
// 边界处理：
// - 空过滤器：长度小于 2 时返回 false
// - 参数异常：哈希函数数量过大时返回 true
func (f *BloomFilter) MayContain(h uint32) bool {
	// 检查过滤器是否有效
	if f.Len() < 2 {
		return false
	}

	k := f.k
	// 处理异常情况：哈希函数数量过大
	if k > 30 {
		// 这是为短布隆过滤器的新编码格式保留的
		// 保守地认为是匹配
		return true
	}

	// 计算有效位数（排除最后一个字节用于存储 k 值）
	nBits := uint32(8 * (f.Len() - 1))

	// 生成哈希增量，用于产生多个不同的哈希值
	// 使用位运算实现高效的哈希值变换
	delta := h>>17 | h<<15

	// 检查所有哈希函数对应的位
	for j := uint8(0); j < k; j++ {
		// 计算当前哈希值对应的位位置
		bitPos := h % nBits

		// 检查对应的位是否为 1
		// bitPos/8 确定字节位置，bitPos%8 确定位位置
		if f.bitmap[bitPos/8]&(1<<(bitPos%8)) == 0 {
			// 任何一位为 0，元素一定不存在
			return false
		}

		// 更新哈希值，生成下一个哈希函数的值
		h += delta
	}

	// 所有位都为 1，元素可能存在
	return true
}

func (f *BloomFilter) Len() int32 {
	return int32(len(f.bitmap))
}

func (f *BloomFilter) InsertKey(k []byte) bool {
	return f.Insert(Hash(k))
}

// Insert 将一个元素插入到布隆过滤器中。
// 该方法返回一个布尔值，表示插入是否成功。
// 参数:
//
//	h - 要插入元素的哈希值。
//
// 返回值:
//
//	插入操作的结果，目前总是返回true。
func (f *BloomFilter) Insert(h uint32) bool {
	k := f.k
	// 当k大于30时，认为这是一个为短布隆过滤器可能的新编码保留的情况。
	// 直接认为匹配成功。
	if k > 30 {
		return true
	}
	// 计算布隆过滤器比特数组的大小（比特数）。
	nBits := uint32(8 * (f.Len() - 1))
	// 计算一个用于后续哈希值变化的delta值。
	delta := h>>17 | h<<15
	// 对于每个哈希函数，计算比特位置并设置相应的比特为1。
	for j := uint8(0); j < k; j++ {
		// 计算当前哈希函数对应的比特位置。
		bitPos := h % uint32(nBits)
		// 设置比特位置为1。
		f.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		// 更新哈希值以使用下一个哈希函数。
		h += delta
	}
	// 插入操作成功。
	return true
}

func (f *BloomFilter) AllowKey(k []byte) bool {
	if f == nil {
		return true
	}
	already := f.MayContainKey(k)
	if !already {
		f.InsertKey(k)
	}
	return already
}

// Allow 方法用于判断给定的元素是否已经被布隆过滤器接受。
// 如果元素已经被接受，则返回 true；否则，将其插入过滤器并返回 false。
// 这个方法首先检查自身是否为空，如果为空，则直接返回 true， 表示任何元素都可以通过空的布隆过滤器。
// 如果过滤器不为空且元素尚未被包含，则将其插入过滤器中。
// 如果元素已经被过滤器包含，则返回 true；否则返回 false。
func (f *BloomFilter) Allow(h uint32) bool {
	// 检查布隆过滤器是否为空，如果为空，则任何元素都可以通过
	if f == nil {
		return true
	}
	// 检查元素是否已经存在于布隆过滤器中
	already := f.MayContain(h)
	// 如果元素不存在，则将其插入过滤器
	if !already {
		f.Insert(h)
	}
	// 返回元素是否已经存在的结果
	return already
}

func (f *BloomFilter) reset() {
	if f == nil {
		return
	}
	for i := range f.bitmap {
		f.bitmap[i] = 0
	}
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func newFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := bloomBitsPerKey(numEntries, falsePositive)
	return initFilter(numEntries, bitsPerKey)
}

// BloomBitsPerKey 计算布隆过滤器每个关键字所需的位数。
// 该函数基于给定的关键字数量和期望的误报率来计算。
// 参数:
//
//	numEntries - 布隆过滤器中预计要插入的关键字数量。
//	fp - 期望的误报率，表示为浮点数（例如，0.01表示1%的误报率）。
// 返回值:
//	返回每个关键字推荐的位数，以整数形式表示。

func bloomBitsPerKey(numEntries int, fp float64) int {
	// 计算Bloom过滤器大小（位数）所需的哈希函数数量。
	// 使用自然对数来估算误报率的公式。
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	// 计算每个键所需的位数，向上取整以确保有足够的位数。
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// initFilter 初始化一个BloomFilter实例。
// 参数 numEntries 表示预计添加到过滤器的元素数量。
// 参数 bitsPerKey 用于估算过滤器中每个元素占用的位数。
// 返回值为一个指向初始化后的BloomFilter的指针。
func initFilter(numEntries int, bitsPerKey int) *BloomFilter {
	// 创建一个BloomFilter实例
	bf := &BloomFilter{}

	// 确保bitsPerKey不为负值，负值在此上下文中无意义
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	// 计算哈希函数的数量，0.69是大约ln(2)
	k := uint32(float64(bitsPerKey) * 0.69)
	// 确保哈希函数数量至少为1
	if k < 1 {
		k = 1
	}
	// 将哈希函数数量限制在30以内，以避免过多哈希函数造成的效率降低
	if k > BloomFilterMaxHashes {
		k = BloomFilterMaxHashes
	}
	// 设置BloomFilter的哈希函数数量
	bf.k = uint8(k)

	// 计算BloomFilter应包含的总位数
	nBits := numEntries * int(bitsPerKey)
	// 对于较小的元素数量，可能会看到很高的假阳性率。通过强制实行最小BloomFilter长度来修正
	if nBits < 64 {
		nBits = 64
	}
	// 计算位数对应的字节数
	nBytes := (nBits + 7) / 8
	// 重新调整位数到正好填充字节数
	nBits = nBytes * 8
	// 初始化过滤器字节数组
	filter := make([]byte, nBytes+1)

	// 在字节数组的末尾记录BloomFilter的哈希函数数量
	filter[nBytes] = uint8(k)

	// 设置BloomFilter的位图
	bf.bitmap = filter
	// 返回初始化后的BloomFilter实例
	return bf
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
