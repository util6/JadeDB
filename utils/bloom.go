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
JadeDB 布隆过滤器模块

布隆过滤器是一种空间效率极高的概率型数据结构，用于快速判断元素是否存在于集合中。
在 JadeDB 中，布隆过滤器主要用于 SSTable 的查询优化。

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

在 LSM 树中的应用：
- 每个 SSTable 都有对应的布隆过滤器
- 查询时先检查布隆过滤器，避免不必要的磁盘访问
- 显著减少读放大，提高查询性能

参数调优：
- bitsPerKey：每个键分配的位数，影响假阳性率
- 推荐值：10 位/键，假阳性率约 1%
- 哈希函数数量：根据 bitsPerKey 自动计算最优值

性能优化：
- 使用高效的哈希函数
- 紧凑的位数组存储格式
- 针对缓存友好的访问模式
*/

package utils

import "math"

// Filter 表示一个编码的布隆过滤器。
// 它是一个字节数组，紧凑地存储了位向量和元数据。
//
// 存储格式：
// [位向量数据...][哈希函数数量]
// - 位向量：存储实际的过滤器位
// - 最后一个字节：存储使用的哈希函数数量
//
// 设计考虑：
// - 紧凑存储：最小化内存占用
// - 自包含：包含重建过滤器所需的所有信息
// - 可序列化：可以直接写入磁盘或网络传输
type Filter []byte

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
// - SSTable 查询前的快速过滤
// - 减少不必要的磁盘 I/O
// - 提高查询性能
//
// 注意事项：
// - 返回 true 不保证键真实存在
// - 返回 false 保证键一定不存在
// - 假阳性率取决于过滤器的配置参数
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain 方法用于检查给定的哈希值h是否可能在布隆过滤器中。
// 它通过检查过滤器中的特定位来判断，这些位是根据哈希值和过滤器的大小计算得出的。
// 参数:
//
//	h: 一个uint32类型的哈希值，用于检查过滤器。
//
// 返回值:
//
//	如果给定的哈希值可能在过滤器中返回true，否则返回false。
func (f Filter) MayContain(h uint32) bool {
	// 如果过滤器太短（少于2个元素），直接返回false。
	// 这是因为有效的布隆过滤器至少需要两个元素来存储位信息。
	if len(f) < 2 {
		return false
	}
	// k表示过滤器应用的哈希函数数量，这里取自过滤器的最后一个元素。
	k := f[len(f)-1]
	// 如果k大于30，这可能表示是为短布隆过滤器预留的新编码。
	// 在这种情况下，认为查找匹配成功。
	if k > 30 {

		return true
	}
	// nBits计算过滤器总的位数，不包括最后一个用于存储k的字节。
	nBits := uint32(8 * (len(f) - 1))
	// delta用于后续迭代中改变哈希值，以产生不同的位位置。
	delta := h>>17 | h<<15
	// 遍历k（哈希函数的数量），检查每个函数对应的位是否被设置。
	for j := uint8(0); j < k; j++ {
		// bitPos计算当前哈希函数指定的位位置。
		bitPos := h % nBits
		// 如果该位未被设置，则说明h可能不在过滤器中，返回false。
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		// 使用delta更新h，准备下一轮检查。
		h += delta
	}
	// 所有位检查都通过，认为h可能在过滤器中。
	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	// 0.69 is approximately ln(2).
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	nBits := len(keys) * int(bitsPerKey)
	// For small len(keys), we can see a very high false positive rate. Fix it
	// by enforcing a minimum bloom filter length.
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)

	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	//record the K value of this Bloom Filter
	filter[nBytes] = uint8(k)

	return filter
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
