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

package cache

import "math"

const (
	//布隆过滤器中允许的最大哈希函数数量
	BloomFilterMaxHashes = 30
)

// Filter is an encoded set of []byte keys.
type Filter []byte

type BloomFilter struct {
	bitmap Filter
	k      uint8
}

// MayContainKey _
func (f *BloomFilter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f *BloomFilter) MayContain(h uint32) bool {
	if f.Len() < 2 {
		return false
	}
	k := f.k
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (f.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f.bitmap[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
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
