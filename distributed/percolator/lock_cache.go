package percolator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/storage"
)

// LockCacheEntry 锁缓存条目
type LockCacheEntry struct {
	Key        string                // 缓存键
	LockRecord *PercolatorLockRecord // 锁记录
	AccessTime time.Time             // 最后访问时间
	Prev       *LockCacheEntry       // 前一个节点
	Next       *LockCacheEntry       // 后一个节点
}

// LockCache Lock列族LRU缓存
type LockCache struct {
	mu sync.RWMutex

	// 缓存配置
	capacity    int           // 缓存容量
	ttl         time.Duration // 缓存TTL
	enableStats bool          // 是否启用统计

	// 缓存数据结构
	cache map[string]*LockCacheEntry // 哈希表
	head  *LockCacheEntry            // 链表头（最新）
	tail  *LockCacheEntry            // 链表尾（最旧）
	size  int                        // 当前大小

	// 存储引擎
	storage storage.Engine

	// 性能统计
	stats *LockCacheStats
}

// LockCacheStats 锁缓存统计信息
type LockCacheStats struct {
	// 基本统计
	Hits      uint64 // 缓存命中次数
	Misses    uint64 // 缓存未命中次数
	Evictions uint64 // 淘汰次数
	Inserts   uint64 // 插入次数
	Updates   uint64 // 更新次数
	Deletes   uint64 // 删除次数

	// 性能指标
	AvgGetTime    time.Duration // 平均获取时间
	AvgPutTime    time.Duration // 平均写入时间
	AvgDeleteTime time.Duration // 平均删除时间

	// 容量统计
	CurrentSize int       // 当前大小
	MaxSize     int       // 最大容量
	HitRate     float64   // 命中率
	LastUpdated time.Time // 最后更新时间
}

// LockCacheConfig 锁缓存配置
type LockCacheConfig struct {
	Capacity        int           // 缓存容量
	TTL             time.Duration // 缓存TTL
	EnableStats     bool          // 是否启用统计
	CleanupInterval time.Duration // 清理间隔
	PreloadKeys     []string      // 预加载的键
}

// NewLockCache 创建新的锁缓存
func NewLockCache(storage storage.Engine, config *LockCacheConfig) *LockCache {
	if config == nil {
		config = DefaultLockCacheConfig()
	}

	cache := &LockCache{
		capacity:    config.Capacity,
		ttl:         config.TTL,
		enableStats: config.EnableStats,
		cache:       make(map[string]*LockCacheEntry),
		storage:     storage,
		stats: &LockCacheStats{
			MaxSize:     config.Capacity,
			LastUpdated: time.Now(),
		},
	}

	// 初始化双向链表
	cache.head = &LockCacheEntry{}
	cache.tail = &LockCacheEntry{}
	cache.head.Next = cache.tail
	cache.tail.Prev = cache.head

	return cache
}

// DefaultLockCacheConfig 默认锁缓存配置
func DefaultLockCacheConfig() *LockCacheConfig {
	return &LockCacheConfig{
		Capacity:        10000,
		TTL:             30 * time.Second,
		EnableStats:     true,
		CleanupInterval: 5 * time.Second,
		PreloadKeys:     nil,
	}
}

// Get 从缓存获取锁记录
func (lc *LockCache) Get(ctx context.Context, key []byte) (*PercolatorLockRecord, error) {
	start := time.Now()
	defer func() {
		if lc.enableStats {
			lc.updateGetStats(time.Since(start))
		}
	}()

	keyStr := string(key)

	lc.mu.Lock()
	defer lc.mu.Unlock()

	// 1. 检查缓存
	if entry, exists := lc.cache[keyStr]; exists {
		// 检查是否过期
		if lc.isExpired(entry) {
			lc.removeEntry(entry)
			lc.stats.Misses++
		} else {
			// 移动到链表头部
			lc.moveToHead(entry)
			lc.stats.Hits++
			return entry.LockRecord, nil
		}
	}

	// 2. 缓存未命中，从存储引擎读取
	lc.stats.Misses++

	// 释放锁进行存储操作
	lc.mu.Unlock()
	lockRecord, err := lc.loadFromStorage(ctx, key)
	lc.mu.Lock()

	if err != nil {
		return nil, err
	}

	if lockRecord == nil {
		return nil, nil // 锁不存在
	}

	// 3. 将记录添加到缓存
	lc.putInternal(keyStr, lockRecord)

	return lockRecord, nil
}

// Put 向缓存写入锁记录
func (lc *LockCache) Put(ctx context.Context, key []byte, lockRecord *PercolatorLockRecord) error {
	start := time.Now()
	defer func() {
		if lc.enableStats {
			lc.updatePutStats(time.Since(start))
		}
	}()

	keyStr := string(key)

	// 1. 写入存储引擎
	if err := lc.saveToStorage(ctx, key, lockRecord); err != nil {
		return err
	}

	// 2. 更新缓存
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.putInternal(keyStr, lockRecord)
	return nil
}

// Delete 从缓存删除锁记录
func (lc *LockCache) Delete(ctx context.Context, key []byte) error {
	start := time.Now()
	defer func() {
		if lc.enableStats {
			lc.updateDeleteStats(time.Since(start))
		}
	}()

	keyStr := string(key)

	// 1. 从存储引擎删除
	lockKey := lc.makeLockKey(key)
	if err := lc.storage.Delete(lockKey); err != nil {
		return err
	}

	// 2. 从缓存删除
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if entry, exists := lc.cache[keyStr]; exists {
		lc.removeEntry(entry)
		lc.stats.Deletes++
	}

	return nil
}

// putInternal 内部写入方法（需要持有锁）
func (lc *LockCache) putInternal(key string, lockRecord *PercolatorLockRecord) {
	if entry, exists := lc.cache[key]; exists {
		// 更新现有条目
		entry.LockRecord = lockRecord
		entry.AccessTime = time.Now()
		lc.moveToHead(entry)
		lc.stats.Updates++
	} else {
		// 创建新条目
		entry := &LockCacheEntry{
			Key:        key,
			LockRecord: lockRecord,
			AccessTime: time.Now(),
		}

		// 检查容量
		if lc.size >= lc.capacity {
			lc.evictLRU()
		}

		// 添加到缓存
		lc.cache[key] = entry
		lc.addToHead(entry)
		lc.size++
		lc.stats.Inserts++
	}

	lc.updateCacheStats()
}

// 双向链表操作方法
func (lc *LockCache) addToHead(entry *LockCacheEntry) {
	entry.Prev = lc.head
	entry.Next = lc.head.Next
	lc.head.Next.Prev = entry
	lc.head.Next = entry
}

func (lc *LockCache) removeEntry(entry *LockCacheEntry) {
	entry.Prev.Next = entry.Next
	entry.Next.Prev = entry.Prev
	delete(lc.cache, entry.Key)
	lc.size--
}

func (lc *LockCache) moveToHead(entry *LockCacheEntry) {
	lc.removeFromList(entry)
	lc.addToHead(entry)
}

func (lc *LockCache) removeFromList(entry *LockCacheEntry) {
	entry.Prev.Next = entry.Next
	entry.Next.Prev = entry.Prev
}

func (lc *LockCache) evictLRU() {
	tail := lc.tail.Prev
	if tail != lc.head {
		lc.removeEntry(tail)
		lc.stats.Evictions++
	}
}

// 存储操作方法
func (lc *LockCache) loadFromStorage(ctx context.Context, key []byte) (*PercolatorLockRecord, error) {
	lockKey := lc.makeLockKey(key)
	data, err := lc.storage.Get(lockKey)
	if err != nil || data == nil {
		return nil, err
	}

	var lockRecord PercolatorLockRecord
	if err := json.Unmarshal(data, &lockRecord); err != nil {
		return nil, err
	}

	// 检查锁是否过期
	if time.Since(lockRecord.CreatedAt) > time.Duration(lockRecord.TTL)*time.Second {
		// 锁已过期，删除它
		lc.storage.Delete(lockKey)
		return nil, nil
	}

	return &lockRecord, nil
}

func (lc *LockCache) saveToStorage(ctx context.Context, key []byte, lockRecord *PercolatorLockRecord) error {
	data, err := json.Marshal(lockRecord)
	if err != nil {
		return err
	}

	lockKey := lc.makeLockKey(key)
	return lc.storage.Put(lockKey, data)
}

func (lc *LockCache) makeLockKey(key []byte) []byte {
	return []byte(fmt.Sprintf("lock_%s", string(key)))
}

// 辅助方法
func (lc *LockCache) isExpired(entry *LockCacheEntry) bool {
	if lc.ttl <= 0 {
		return false
	}
	return time.Since(entry.AccessTime) > lc.ttl
}

// 统计更新方法
func (lc *LockCache) updateGetStats(duration time.Duration) {
	if lc.stats.AvgGetTime == 0 {
		lc.stats.AvgGetTime = duration
	} else {
		lc.stats.AvgGetTime = (lc.stats.AvgGetTime + duration) / 2
	}
}

func (lc *LockCache) updatePutStats(duration time.Duration) {
	if lc.stats.AvgPutTime == 0 {
		lc.stats.AvgPutTime = duration
	} else {
		lc.stats.AvgPutTime = (lc.stats.AvgPutTime + duration) / 2
	}
}

func (lc *LockCache) updateDeleteStats(duration time.Duration) {
	if lc.stats.AvgDeleteTime == 0 {
		lc.stats.AvgDeleteTime = duration
	} else {
		lc.stats.AvgDeleteTime = (lc.stats.AvgDeleteTime + duration) / 2
	}
}

func (lc *LockCache) updateCacheStats() {
	lc.stats.CurrentSize = lc.size
	total := lc.stats.Hits + lc.stats.Misses
	if total > 0 {
		lc.stats.HitRate = float64(lc.stats.Hits) / float64(total)
	}
	lc.stats.LastUpdated = time.Now()
}

// GetStats 获取缓存统计信息
func (lc *LockCache) GetStats() *LockCacheStats {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	// 返回统计信息的副本
	stats := *lc.stats
	return &stats
}

// Clear 清空缓存
func (lc *LockCache) Clear() {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.cache = make(map[string]*LockCacheEntry)
	lc.head.Next = lc.tail
	lc.tail.Prev = lc.head
	lc.size = 0

	// 重置统计信息
	lc.stats = &LockCacheStats{
		MaxSize:     lc.capacity,
		LastUpdated: time.Now(),
	}
}

// Size 获取缓存大小
func (lc *LockCache) Size() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.size
}

// Capacity 获取缓存容量
func (lc *LockCache) Capacity() int {
	return lc.capacity
}

// Contains 检查缓存是否包含指定键
func (lc *LockCache) Contains(key []byte) bool {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	keyStr := string(key)
	entry, exists := lc.cache[keyStr]
	if !exists {
		return false
	}

	return !lc.isExpired(entry)
}

// Warmup 预热缓存
func (lc *LockCache) Warmup(ctx context.Context, keys [][]byte) error {
	for _, key := range keys {
		if _, err := lc.Get(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
