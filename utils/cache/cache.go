/*
JadeDB 高性能缓存系统

本模块实现了一个高性能的多层级缓存系统，结合了多种先进的缓存算法和数据结构。
该缓存系统专门为 JadeDB 的读取密集型工作负载优化，提供卓越的命中率和性能。

核心设计理念：
1. 多层级架构：Window LRU + Segmented LRU 的两级缓存结构
2. 智能准入控制：使用布隆过滤器和频率估计进行准入决策
3. 自适应调整：动态调整缓存策略以适应不同的访问模式
4. 高并发支持：读写锁保护，支持高并发访问

缓存层级结构：
┌─────────────┐    ┌──────────────────────────────┐
│ Window LRU  │───▶│     Segmented LRU            │
│ (热点缓存)   │    │ ┌─────────────┬─────────────┐ │
│             │    │ │ Probation   │ Protected   │ │
│             │    │ │ (试用区)     │ (保护区)     │ │
└─────────────┘    │ └─────────────┴─────────────┘ │
                   └──────────────────────────────┘

算法特性：
- Window LRU：快速响应突发访问，捕获短期热点
- Segmented LRU：长期热点数据保护，防止缓存污染
- 布隆过滤器：快速过滤一次性访问，减少缓存污染
- Count-Min Sketch：高效的频率估计，支持准入决策

性能优势：
- 高命中率：多层级设计适应不同访问模式
- 低延迟：O(1) 访问时间，高效的数据结构
- 内存效率：紧凑的数据布局，最小化内存开销
- 并发友好：读写锁设计，支持高并发访问

适用场景：
- 数据库缓存系统
- 高频读取的键值存储
- 需要高命中率的缓存场景
- 访问模式复杂的应用系统
*/

package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

// Cache 实现了一个高性能的多层级缓存系统。
// 它结合了 Window LRU、Segmented LRU、布隆过滤器和 Count-Min Sketch 等多种技术。
//
// 缓存策略详解：
//
// 1. 数据首次访问：
//   - 进入 Window LRU（热点捕获区）
//   - 布隆过滤器记录访问历史
//   - Count-Min Sketch 记录访问频率
//
// 2. 数据再次访问：
//   - 如果在 Window LRU 中：提升优先级
//   - 如果被淘汰但有访问历史：进入 Segmented LRU 的 Probation 区
//
// 3. 高频数据晋升：
//   - 从 Probation 区晋升到 Protected 区
//   - 长期保护热点数据不被淘汰
//
// 4. 缓存污染防护：
//   - 一次性访问的数据只在 Window LRU 中短暂停留
//   - 布隆过滤器防止无效数据进入长期缓存
//
// 并发安全：
// 使用读写锁保护所有操作，支持多读者单写者模式。
type Cache struct {
	// 并发控制

	// m 保护缓存的所有数据结构。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作（插入、删除、更新）需要独占访问。
	m sync.RWMutex

	// 第一级缓存：Window LRU

	// lru 是窗口 LRU 缓存，用于捕获短期热点数据。
	// 特点：
	// - 容量较小（通常占总容量的 10%）
	// - 快速响应突发访问模式
	// - 作为进入长期缓存的门槛
	lru *windowLRU

	// 第二级缓存：Segmented LRU

	// slru 是分段 LRU 缓存，用于长期存储热点数据。
	// 分为两个区域：
	// - Probation：试用区，新晋升的数据
	// - Protected：保护区，经过验证的热点数据
	slru *segmentedLRU

	// 准入控制组件

	// door 是布隆过滤器，用于快速判断数据是否曾经被访问。
	// 功能：
	// - 防止一次性访问数据污染长期缓存
	// - 快速过滤，减少不必要的缓存操作
	// - 可能有假阳性，但无假阴性
	door *BloomFilter

	// 频率估计组件

	// c 是 Count-Min Sketch，用于估计数据的访问频率。
	// 功能：
	// - 高效的频率统计，空间复杂度低
	// - 支持大规模数据的频率估计
	// - 用于缓存晋升和淘汰决策
	c *cmSketch

	// 动态调整参数

	// t 是动态调整的阈值，用于控制缓存策略的敏感度。
	// 根据访问模式自动调整，优化缓存性能。
	t int32

	// threshold 是频率阈值，用于判断数据是否应该从 Window LRU 晋升到 Segmented LRU。
	// 基于 Count-Min Sketch 的频率估计进行判断。
	threshold int32

	// 数据存储

	// data 是主要的数据存储结构。
	// 键：数据的哈希值（uint64）
	// 值：双向链表中的元素指针
	// 设计优势：
	// - O(1) 的查找时间复杂度
	// - O(1) 的插入和删除时间复杂度
	// - 内存效率高，减少指针开销
	data map[uint64]*list.Element
}

// Options 定义缓存的配置选项。
// 当前主要用于控制各个缓存层级的容量分配比例。
type Options struct {
	// lruPct 指定 Window LRU 占总容量的百分比。
	// 影响短期热点数据的捕获能力。
	lruPct uint8
}

// NewCache 创建一个新的多层级缓存实例。
// 这是缓存系统的主要构造函数，负责初始化所有组件和设置容量分配。
//
// 参数说明：
// size: 缓存的总容量（条目数量）
//
// 返回值：
// 完全初始化的缓存实例
//
// 容量分配策略：
// 1. Window LRU：10% 的总容量，用于捕获短期热点
// 2. Segmented LRU：90% 的总容量，分为两个阶段：
//   - Probation（试用区）：20% 的 SLRU 容量
//   - Protected（保护区）：80% 的 SLRU 容量
//
// 设计考虑：
// - 10% 的 Window LRU 足以捕获大部分突发访问
// - 90% 的 Segmented LRU 提供长期的热点保护
// - 2:8 的试用区/保护区比例平衡了过滤效果和保护能力
//
// 组件初始化：
// - 布隆过滤器：1% 的假阳性率，平衡准确性和性能
// - Count-Min Sketch：基于总容量设置，提供频率估计
// - 共享哈希表：所有层级共享，提高查找效率
func NewCache(size int) *Cache {
	// 容量分配常量
	const lruPct = 10 // Window LRU 占总容量的 10%

	// 计算 Window LRU 的容量
	lruSz := (lruPct * size) / 100
	if lruSz < 1 {
		lruSz = 1 // 确保至少有 1 个条目
	}

	// 计算 Segmented LRU 的总容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))
	if slruSz < 1 {
		slruSz = 1 // 确保至少有 1 个条目
	}

	// 计算 Segmented LRU 中试用区的容量（占 SLRU 的 20%）
	slruO := int(0.2 * float64(slruSz))
	if slruO < 1 {
		slruO = 1 // 确保至少有 1 个条目
	}

	// 初始化共享的数据存储结构
	// 预分配容量以减少哈希表的重新分配
	data := make(map[uint64]*list.Element, size)

	// 创建并返回完整的缓存实例
	return &Cache{
		// Window LRU：第一级缓存，捕获短期热点
		lru: newWindowLRU(lruSz, data),

		// Segmented LRU：第二级缓存，保护长期热点
		// slruO: 试用区容量，slruSz-slruO: 保护区容量
		slru: newSLRU(data, slruO, slruSz-slruO),

		// 布隆过滤器：准入控制，1% 假阳性率
		door: newFilter(size, 0.01),

		// Count-Min Sketch：频率估计器
		c: newCmSketch(int64(size)),

		// 共享数据存储：所有层级共享同一个哈希表
		data: data,
	}
}

// Set 向缓存中插入一个键值对。
// 这是缓存的主要写入接口，支持并发安全的数据插入。
//
// 参数说明：
// key: 要插入的键，支持任意类型
// value: 要插入的值，支持任意类型
//
// 返回值：
// true: 插入成功
// false: 插入失败（通常由于准入控制拒绝）
//
// 并发安全：
// 使用写锁保护整个插入过程，确保数据一致性。
//
// 插入策略：
// 1. 所有新数据首先进入 Window LRU
// 2. 通过准入控制决定是否接受数据
// 3. 可能触发缓存层级间的数据迁移
func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

// set 是内部的插入实现，假设调用者已经持有锁。
// 实现了完整的多层级缓存插入逻辑。
//
// 参数说明：
// key: 要插入的键
// value: 要插入的值
//
// 返回值：
// true: 插入成功，false: 插入失败
//
// 插入流程：
// 1. 计算键的哈希值和冲突检测哈希
// 2. 创建存储项，初始阶段为 Window LRU
// 3. 执行准入控制和层级管理
// 4. 更新频率统计和布隆过滤器
func (c *Cache) set(key, value interface{}) bool {
	// 计算键的哈希值
	// keyHash: 主哈希值，用于快速定位
	// conflictHash: 冲突检测哈希，用于处理哈希冲突
	keyHash, conflictHash := c.keyToHash(key)

	// 创建存储项，所有新数据都从 Window LRU 开始
	i := storeItem{
		stage:    0,            // 阶段 0 表示 Window LRU
		key:      keyHash,      // 主键哈希值
		conflict: conflictHash, // 冲突检测哈希值
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
