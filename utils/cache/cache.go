package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

/*
Cache 结构体代表一个缓存系统，该系统采用多种缓存策略和技术来优化存储和检索效率。
它结合了限长缓存（window LRU），分段 LRU（segmented LRU），布隆过滤器（Bloom Filter）
以及计数迷你缩略图（count-min sketch）等多种方法来管理缓存数据。

	缓存策略：

如果需要淘汰的时候，需要判断是否出现过，bloom,出现过才能往slu放，只访问一次的也就是说，如果一个数据来了一次，在win被淘汰，如果slru满了，
如果w满了，slu也满了，但是这个数据没出现过，会导致w中的直接淘汰了，但是此时bloom会有计数。如果下一次又进入Iru,又被淘汰，就会进入probation
综上
如果只出现一次，会在window-lru被淘汰。
如果突发性的频繁访问，可能会保留在window-lru,或者进入slru的`probation`区，并经过保活机制。
如果热点数据，肯定会进入slru的protected区。
*/
type Cache struct {
	// m 用于同步访问缓存数据，确保并发安全。
	m sync.RWMutex
	// lru 作为短时高频数据的缓存策略，windowLRU 是一种基于滑动窗口的 LRU 缓存策略。
	lru *windowLRU
	// slru 用于存储较长时间未被访问的数据，segmentedLRU 是一种分段的 LRU 缓存策略，
	// 它可以更有效地管理大量缓存数据。
	slru *segmentedLRU
	// door 用作快速判断某数据是否存在，使用布隆过滤器可以快速判断某数据是否存在缓存中，
	// 但可能会有误判。
	door *BloomFilter
	// c 用于估计大量数据的频次，count-min sketch 是一种概率数据结构，用于估计数据流中
	// 各项的频次。
	c *cmSketch
	// t 用于调整缓存策略的灵敏度，是一个动态调整的阈值。
	t int32
	// threshold 用于区分数据是否应该从 lru 晋级到 slru，确保高频数据能够被快速访问。
	threshold int32
	// data 用于存储具体的缓存数据，键是数据的唯一标识，值是双向链表的元素，
	// 这样可以在 O(1) 的时间复杂度内访问和移除数据。
	data map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

// NewCache size 指的是要缓存的数据个数
// NewCache 创建并返回一个新的缓存实例。

func NewCache(size int) *Cache {
	// 定义 window 部分缓存所占百分比，这里定义为1%
	const lruPct = 10
	// 计算出来 widow 部分的容量
	lruSz := (lruPct * size) / 100

	// 确保 lruSz 至少为1
	if lruSz < 1 {
		lruSz = 1
	}

	// 计算 LFU 部分的缓存容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))

	// 确保 slruSz 至少为1
	if slruSz < 1 {
		slruSz = 1
	}

	// LFU 分为两部分，stageOne 部分占比20%
	slruO := int(0.2 * float64(slruSz))

	// 确保 stageOne 部分至少为1
	if slruO < 1 {
		slruO = 1
	}

	// 初始化缓存数据结构
	data := make(map[uint64]*list.Element, size)

	// 创建并返回缓存实例
	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01), // 布隆过滤器设置误差率为0.01
		c:    newCmSketch(int64(size)),
		data: data, // 共用同一个 map 存储数据
	}
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	// keyHash 用来快速定位，conflict 用来判断冲突
	keyHash, conflictHash := c.keyToHash(key)

	// 刚放进去的缓存都先放到 window lru 中，所以 stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 如果 window 已满，要返回被淘汰的数据
	eItem, evicted := c.lru.add(i)

	if !evicted {
		return true
	}

	// 如果 window 中有被淘汰的数据，会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者
	// 二者进行 PK
	victim := c.slru.victim()

	// 走到这里是因为 LFU 未满，那么 window lru 的淘汰数据，可以进入 stageOne
	if victim == nil {
		c.slru.add(eItem)
		return true
	}

	// 这里进行 PK，必须在 bloomfilter 中出现过一次，才允许 PK
	// 在 bf 中出现，说明访问频率 >= 2
	if !c.door.Allow(uint32(eItem.key)) {
		return true
	}

	// 估算 windowlru 和 LFU 中淘汰数据，历史访问频次
	// 访问频率高的，被认为更有资格留下来
	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(eItem.key)

	if ocount < vcount {
		return true
	}

	// 留下来的人进入 stageOne
	c.slru.add(eItem)
	return true
}

// Get 从缓存中获取与指定键关联的值。
// 该方法使用读锁来确保在读取缓存时不会被写操作阻塞，
// 同时也保证了并发访问时的数据安全。
// 参数:
//
//	key - 用于检索值的键。类型为interface{}，使得可以支持多种类型的键。
//
// 返回值:
//   - 接口类型的值，对应于提供的键；如果键不存在，则返回nil。
//   - bool 类型的值，表示是否成功找到了与键关联的值。
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	// 加读锁
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

// get 从缓存中获取与指定键关联的值。
// 它首先检查键是否存在，如果不存在，则返回nil和false。
// 如果键存在，它会检查是否存在冲突，并相应地更新缓存状态。
//
//	如果成功检索到值，则返回true；如果键不存在或有冲突，则返回false。
func (c *Cache) get(key interface{}) (interface{}, bool) {
	// 增加操作计数器
	c.t++
	// 检查是否达到阈值，达到则重置缓存状态
	if c.t == c.threshold {
		c.c.Reset()
		c.door.reset()
		c.t = 0
	}

	// 将键转换为哈希值，用于缓存查找
	keyHash, conflictHash := c.keyToHash(key)

	// 尝试从缓存中获取键对应的值
	val, ok := c.data[keyHash]
	if !ok {
		// 如果键不存在，允许对该哈希进行访问，并增加计数
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	// 获取存储项的详细信息
	item := val.Value.(*storeItem)

	// 检查是否存在冲突，如果存在，则不允许访问
	if item.conflict != conflictHash {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	// 允许对该哈希进行访问，并增加计数
	c.door.Allow(uint32(keyHash))
	c.c.Increment(item.key)

	// 获取与键关联的值
	v := item.value

	// 根据项的状态，更新LRU或SLRU缓存状态
	if item.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}

	// 返回与键关联的值和成功标志
	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

// del 从缓存中删除指定键的条目。
// 它接受一个键作为输入参数，并返回被删除项的冲突哈希值（如果有的话）以及一个布尔值，表示删除是否成功。
func (c *Cache) del(key interface{}) (interface{}, bool) {
	// 将键转换为哈希值，同时检查是否存在哈希冲突。
	keyHash, conflictHash := c.keyToHash(key)

	// 尝试从缓存数据中获取与键关联的值。
	val, ok := c.data[keyHash]
	if !ok {
		// 如果键不存在于缓存中，则返回失败。
		return 0, false
	}

	// 获取存储项的具体值。
	item := val.Value.(*storeItem)

	// 检查冲突哈希值是否匹配，如果不匹配，则返回失败。
	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	// 从缓存中删除键。
	delete(c.data, keyHash)
	// 返回被删除项的冲突哈希值（如果有的话）以及表示删除成功的布尔值。
	return item.conflict, true
}

// keyToHash 根据给定的键生成一个哈希值对。
// 键可以是多种类型，包括nil、uint64、string、[]byte、byte、int、int32、uint32、int64。
// 如果键的类型不受支持，将触发panic。
func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	// 如果键是nil，返回0, 0。
	if key == nil {
		return 0, 0
	}
	// 根据键的类型，选择合适的哈希方法。
	switch k := key.(type) {
	case uint64:
		// 如果键是uint64类型，直接返回键值和0。
		return k, 0
	case string:
		// 如果键是string类型，使用两种不同的哈希方法计算哈希值。
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		// 如果键是[]byte类型，使用两种不同的哈希方法计算哈希值。
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		// 如果键是byte类型，转换为uint64并返回。
		return uint64(k), 0
	case int:
		// 如果键是int类型，转换为uint64并返回。
		return uint64(k), 0
	case int32:
		// 如果键是int32类型，转换为uint64并返回。
		return uint64(k), 0
	case uint32:
		// 如果键是uint32类型，转换为uint64并返回。
		return uint64(k), 0
	case int64:
		// 如果键是int64类型，转换为uint64并返回。
		return uint64(k), 0
	default:
		// 如果键的类型不受支持，触发panic。
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
