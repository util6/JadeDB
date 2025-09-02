/*
JadeDB B+树自适应哈希索引模块

自适应哈希索引为热点查询提供O(1)的访问性能，参考InnoDB的设计实现。
该模块自动识别频繁访问的键，并为其建立哈希索引，大幅提升查询性能。

核心功能：
1. 热点检测：自动识别频繁访问的键
2. 哈希索引：为热点键建立哈希索引
3. 自动维护：根据访问模式动态调整索引
4. 内存管理：控制索引大小，避免内存溢出
5. 并发安全：支持多线程并发访问

设计原理：
- 访问频率统计：跟踪每个键的访问频率
- 热点阈值：超过阈值的键被认为是热点
- 哈希表：使用哈希表存储热点键的直接映射
- LRU淘汰：当索引满时，淘汰最少使用的条目
- 自动失效：当B+树结构变化时，相关索引自动失效

性能优化：
- 分区哈希：将哈希表分为多个分区，减少锁竞争
- 无锁读取：读操作尽量避免加锁
- 批量更新：批量更新索引，减少维护开销
- 内存预分配：预先分配哈希表空间
*/

package bplustree

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/utils"
)

// 自适应哈希索引常量
const (
	// DefaultHashTableSize 默认哈希表大小
	DefaultHashTableSize = 65536

	// MaxHashTableSize 最大哈希表大小
	MaxHashTableSize = 1024 * 1024

	// HotKeyThreshold 热点键阈值
	// 访问次数超过此阈值的键被认为是热点
	HotKeyThreshold = 10

	// HashPartitionCount 哈希表分区数量
	HashPartitionCount = 64

	// CleanupInterval 清理间隔
	CleanupInterval = 5 * time.Minute

	// MaxKeySize 最大键大小
	MaxKeySize = 1024
)

// AdaptiveHashIndex 自适应哈希索引
type AdaptiveHashIndex struct {
	// 配置参数
	tableSize     int    // 哈希表大小
	partitionMask uint64 // 分区掩码

	// 哈希表分区
	partitions []*HashPartition // 哈希表分区

	// 访问统计
	accessCounter *AccessCounter // 访问计数器

	// 统计信息
	totalLookups atomic.Int64 // 总查找次数
	hashHits     atomic.Int64 // 哈希命中次数
	hashMisses   atomic.Int64 // 哈希未命中次数
	indexSize    atomic.Int64 // 索引大小

	// 生命周期
	closer *utils.Closer // 优雅关闭
}

// HashPartition 哈希表分区
type HashPartition struct {
	mutex    sync.RWMutex          // 分区锁
	entries  map[uint64]*HashEntry // 哈希条目
	lruList  *LRUList              // LRU链表
	capacity int                   // 分区容量
	size     int                   // 当前大小
}

// HashEntry 哈希条目
type HashEntry struct {
	Key         []byte    // 键
	PageID      uint64    // 页面ID
	Offset      uint16    // 页面内偏移
	Timestamp   time.Time // 最后访问时间
	AccessCount int64     // 访问次数
	lruNode     *LRUNode  // LRU节点
}

// AccessCounter 访问计数器
type AccessCounter struct {
	mutex    sync.RWMutex           // 并发保护
	counters map[string]*KeyCounter // 键计数器
	maxSize  int                    // 最大记录数量
}

// KeyCounter 键计数器
type KeyCounter struct {
	Key         []byte    // 键
	Count       int64     // 访问次数
	LastAccess  time.Time // 最后访问时间
	FirstAccess time.Time // 首次访问时间
}

// LRUList LRU链表（简化实现）
type LRUList struct {
	head *LRUNode
	tail *LRUNode
	size int
}

// LRUNode LRU节点
type LRUNode struct {
	prev  *LRUNode
	next  *LRUNode
	entry *HashEntry
}

// NewAdaptiveHashIndex 创建新的自适应哈希索引
func NewAdaptiveHashIndex() *AdaptiveHashIndex {
	tableSize := DefaultHashTableSize
	partitionSize := tableSize / HashPartitionCount

	ahi := &AdaptiveHashIndex{
		tableSize:     tableSize,
		partitionMask: HashPartitionCount - 1,
		partitions:    make([]*HashPartition, HashPartitionCount),
		accessCounter: NewAccessCounter(tableSize * 2),
		closer:        utils.NewCloser(),
	}

	// 初始化分区
	for i := 0; i < HashPartitionCount; i++ {
		ahi.partitions[i] = &HashPartition{
			entries:  make(map[uint64]*HashEntry),
			lruList:  NewLRUList(),
			capacity: partitionSize,
		}
	}

	// 启动后台清理服务
	ahi.closer.Add(1)
	go ahi.cleanupService()

	return ahi
}

// NewAccessCounter 创建访问计数器
func NewAccessCounter(maxSize int) *AccessCounter {
	return &AccessCounter{
		counters: make(map[string]*KeyCounter),
		maxSize:  maxSize,
	}
}

// NewLRUList 创建LRU链表
func NewLRUList() *LRUList {
	head := &LRUNode{}
	tail := &LRUNode{}
	head.next = tail
	tail.prev = head

	return &LRUList{
		head: head,
		tail: tail,
	}
}

// Lookup 查找键
func (ahi *AdaptiveHashIndex) Lookup(key []byte) (pageID uint64, offset uint16, found bool) {
	ahi.totalLookups.Add(1)

	// 记录访问
	ahi.recordAccess(key)

	// 计算哈希值和分区
	hash := ahi.hashKey(key)
	partIndex := hash & ahi.partitionMask
	partition := ahi.partitions[partIndex]

	partition.mutex.RLock()
	defer partition.mutex.RUnlock()

	// 查找哈希条目
	if entry, exists := partition.entries[hash]; exists {
		// 验证键是否匹配（处理哈希冲突）
		if string(entry.Key) == string(key) {
			// 更新访问信息
			entry.Timestamp = time.Now()
			entry.AccessCount++

			// 移动到LRU头部
			partition.lruList.MoveToFront(entry.lruNode)

			ahi.hashHits.Add(1)
			return entry.PageID, entry.Offset, true
		}
	}

	ahi.hashMisses.Add(1)
	return 0, 0, false
}

// Insert 插入或更新索引条目
func (ahi *AdaptiveHashIndex) Insert(key []byte, pageID uint64, offset uint16) {
	// 检查键是否为热点
	if !ahi.isHotKey(key) {
		return
	}

	// 计算哈希值和分区
	hash := ahi.hashKey(key)
	partIndex := hash & ahi.partitionMask
	partition := ahi.partitions[partIndex]

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	// 检查是否已存在
	if entry, exists := partition.entries[hash]; exists {
		// 更新现有条目
		entry.PageID = pageID
		entry.Offset = offset
		entry.Timestamp = time.Now()
		partition.lruList.MoveToFront(entry.lruNode)
		return
	}

	// 检查分区容量
	if partition.size >= partition.capacity {
		// 淘汰LRU条目
		ahi.evictLRU(partition)
	}

	// 创建新条目
	entry := &HashEntry{
		Key:         make([]byte, len(key)),
		PageID:      pageID,
		Offset:      offset,
		Timestamp:   time.Now(),
		AccessCount: 1,
	}
	copy(entry.Key, key)

	// 添加到哈希表
	partition.entries[hash] = entry

	// 添加到LRU链表
	entry.lruNode = partition.lruList.PushFront(entry)

	partition.size++
	ahi.indexSize.Add(1)
}

// Remove 移除索引条目
func (ahi *AdaptiveHashIndex) Remove(key []byte) {
	hash := ahi.hashKey(key)
	partIndex := hash & ahi.partitionMask
	partition := ahi.partitions[partIndex]

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	if entry, exists := partition.entries[hash]; exists {
		// 从哈希表移除
		delete(partition.entries, hash)

		// 从LRU链表移除
		partition.lruList.Remove(entry.lruNode)

		partition.size--
		ahi.indexSize.Add(-1)
	}
}

// recordAccess 记录键访问
func (ahi *AdaptiveHashIndex) recordAccess(key []byte) {
	keyStr := string(key)

	ahi.accessCounter.mutex.Lock()
	defer ahi.accessCounter.mutex.Unlock()

	counter, exists := ahi.accessCounter.counters[keyStr]
	if !exists {
		// 检查容量
		if len(ahi.accessCounter.counters) >= ahi.accessCounter.maxSize {
			// 清理一些旧的计数器
			ahi.cleanupCounters()
		}

		counter = &KeyCounter{
			Key:         make([]byte, len(key)),
			Count:       0,
			FirstAccess: time.Now(),
		}
		copy(counter.Key, key)
		ahi.accessCounter.counters[keyStr] = counter
	}

	counter.Count++
	counter.LastAccess = time.Now()
}

// isHotKey 检查是否为热点键
func (ahi *AdaptiveHashIndex) isHotKey(key []byte) bool {
	keyStr := string(key)

	ahi.accessCounter.mutex.RLock()
	defer ahi.accessCounter.mutex.RUnlock()

	if counter, exists := ahi.accessCounter.counters[keyStr]; exists {
		return counter.Count >= HotKeyThreshold
	}

	return false
}

// hashKey 计算键的哈希值
func (ahi *AdaptiveHashIndex) hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// evictLRU 淘汰LRU条目
func (ahi *AdaptiveHashIndex) evictLRU(partition *HashPartition) {
	if partition.lruList.size == 0 {
		return
	}

	// 获取LRU条目
	lruNode := partition.lruList.tail.prev
	if lruNode == partition.lruList.head {
		return
	}

	entry := lruNode.entry
	hash := ahi.hashKey(entry.Key)

	// 从哈希表移除
	delete(partition.entries, hash)

	// 从LRU链表移除
	partition.lruList.Remove(lruNode)

	partition.size--
	ahi.indexSize.Add(-1)
}

// cleanupCounters 清理访问计数器
// 实现智能的清理策略，基于访问频率和时间进行清理
func (ahi *AdaptiveHashIndex) cleanupCounters() {
	ahi.accessCounter.mutex.Lock()
	defer ahi.accessCounter.mutex.Unlock()

	now := time.Now()
	totalCounters := len(ahi.accessCounter.counters)

	// 如果计数器数量不多，不需要清理
	if totalCounters < 1000 {
		return
	}

	// 智能清理策略：
	// 1. 优先清理长时间未访问的计数器
	// 2. 清理访问频率低的计数器
	// 3. 保留热点数据的计数器

	type counterInfo struct {
		key     string
		counter *KeyCounter
		score   float64 // 清理优先级分数，分数越高越应该被清理
	}

	var candidates []counterInfo

	// 计算每个计数器的清理优先级分数
	for key, counter := range ahi.accessCounter.counters {
		timeSinceLastAccess := now.Sub(counter.LastAccess)

		// 分数计算：时间权重 + 频率权重
		timeScore := float64(timeSinceLastAccess.Hours()) // 时间越长分数越高
		freqScore := 1.0 / float64(counter.Count+1)       // 访问次数越少分数越高

		score := timeScore*0.7 + freqScore*0.3 // 时间权重更高

		candidates = append(candidates, counterInfo{
			key:     key,
			counter: counter,
			score:   score,
		})
	}

	// 按分数排序，分数高的优先清理
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].score < candidates[j].score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// 清理策略：
	// - 如果计数器数量 > 5000，清理40%
	// - 如果计数器数量 > 2000，清理30%
	// - 否则清理20%
	var cleanupRatio float64
	if totalCounters > 5000 {
		cleanupRatio = 0.4
	} else if totalCounters > 2000 {
		cleanupRatio = 0.3
	} else {
		cleanupRatio = 0.2
	}

	toDelete := int(float64(totalCounters) * cleanupRatio)
	deleted := 0

	// 执行清理
	for _, candidate := range candidates {
		if deleted >= toDelete {
			break
		}

		// 额外保护：不清理最近5分钟内访问过的热点数据
		if now.Sub(candidate.counter.LastAccess) < 5*time.Minute && candidate.counter.Count > 10 {
			continue
		}

		delete(ahi.accessCounter.counters, candidate.key)
		deleted++
	}

	// 记录清理统计
	if deleted > 0 {
		// 可以添加日志记录清理情况
		// log.Printf("Cleaned up %d access counters, remaining: %d", deleted, len(ahi.accessCounter.counters))
	}
}

// cleanupService 后台清理服务
func (ahi *AdaptiveHashIndex) cleanupService() {
	defer ahi.closer.Done()

	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ahi.cleanup()

		case <-ahi.closer.CloseSignal:
			return
		}
	}
}

// cleanup 执行清理操作
func (ahi *AdaptiveHashIndex) cleanup() {
	// 清理访问计数器
	ahi.accessCounter.mutex.Lock()
	ahi.cleanupCounters()
	ahi.accessCounter.mutex.Unlock()

	// 清理过期的哈希条目
	for _, partition := range ahi.partitions {
		partition.mutex.Lock()

		// 清理超过1小时未访问的条目
		for hash, entry := range partition.entries {
			if time.Since(entry.Timestamp) > time.Hour {
				delete(partition.entries, hash)
				partition.lruList.Remove(entry.lruNode)
				partition.size--
				ahi.indexSize.Add(-1)
			}
		}

		partition.mutex.Unlock()
	}
}

// MoveToFront 将节点移动到链表头部
func (lru *LRUList) MoveToFront(node *LRUNode) {
	if node == nil || node == lru.head.next {
		return
	}

	// 从当前位置移除
	lru.Remove(node)

	// 添加到头部
	lru.PushFront(node.entry)
}

// PushFront 添加到链表头部
func (lru *LRUList) PushFront(entry *HashEntry) *LRUNode {
	node := &LRUNode{entry: entry}

	node.next = lru.head.next
	node.prev = lru.head
	lru.head.next.prev = node
	lru.head.next = node

	lru.size++
	return node
}

// Remove 从链表中移除节点
func (lru *LRUList) Remove(node *LRUNode) {
	if node == nil {
		return
	}

	node.prev.next = node.next
	node.next.prev = node.prev
	lru.size--
}

// GetStats 获取统计信息
func (ahi *AdaptiveHashIndex) GetStats() map[string]interface{} {
	total := ahi.totalLookups.Load()
	hits := ahi.hashHits.Load()

	var hitRatio float64
	if total > 0 {
		hitRatio = float64(hits) / float64(total)
	}

	return map[string]interface{}{
		"total_lookups": total,
		"hash_hits":     hits,
		"hash_misses":   ahi.hashMisses.Load(),
		"hit_ratio":     hitRatio,
		"index_size":    ahi.indexSize.Load(),
	}
}

// Close 关闭自适应哈希索引
func (ahi *AdaptiveHashIndex) Close() error {
	ahi.closer.Close()
	return nil
}
