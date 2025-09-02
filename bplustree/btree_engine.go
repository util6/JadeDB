/*
JadeDB B+树存储引擎实现

本模块在bplustree包中实现storage.Engine接口，提供B+树的存储功能。
事务功能委托给B+树专用的事务实现。

设计特点：
1. 职责单一：只负责B+树存储逻辑
2. 接口实现：实现统一的storage.Engine接口
3. 事务委托：事务功能委托给btree_transaction.go
4. 性能优化：充分利用B+树的读优化和索引特性
*/

package bplustree

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/util6/JadeDB/storage"
)

// BTreeEngine B+树存储引擎
type BTreeEngine struct {
	*storage.BaseEngine

	// B+树实例
	btree *BPlusTree

	// 配置
	config *BTreeEngineConfig

	// 状态管理
	mu sync.RWMutex
}

// BTreeEngineConfig B+树引擎配置
type BTreeEngineConfig struct {
	// B+树配置
	BTreeOptions *BTreeOptions

	// 性能配置
	CacheSize            int     // 缓存大小
	PageSize             int     // 页面大小
	FillFactor           float64 // 填充因子
	MaxConcurrentReaders int     // 最大并发读取数

	// 索引配置
	EnableBloomFilter bool    // 是否启用布隆过滤器
	BloomFilterFPR    float64 // 布隆过滤器假阳性率

	// 压缩配置
	EnableCompression    bool   // 是否启用压缩
	CompressionAlgorithm string // 压缩算法
}

// DefaultBTreeEngineConfig 默认B+树引擎配置
func DefaultBTreeEngineConfig() *BTreeEngineConfig {
	return &BTreeEngineConfig{
		BTreeOptions:         DefaultBTreeOptions(),
		CacheSize:            128 * 1024 * 1024, // 128MB
		PageSize:             4096,              // 4KB
		FillFactor:           0.7,
		MaxConcurrentReaders: 100,
		EnableBloomFilter:    true,
		BloomFilterFPR:       0.01, // 1%假阳性率
		EnableCompression:    false,
		CompressionAlgorithm: "snappy",
	}
}

// NewBTreeEngine 创建B+树存储引擎
func NewBTreeEngine(config *BTreeEngineConfig) (*BTreeEngine, error) {
	if config == nil {
		config = DefaultBTreeEngineConfig()
	}

	// 创建引擎信息
	info := &storage.EngineInfo{
		Type:        storage.BPlusTreeEngine,
		Version:     "1.0.0",
		Description: "B+ Tree storage engine optimized for read-heavy workloads and range queries",
		Features: []string{
			"Fast range queries",
			"Efficient point queries",
			"Page-level caching",
			"Configurable fill factor",
			"Optional bloom filters",
			"Compression support",
		},
		Limitations: []string{
			"Write amplification",
			"Page fragmentation over time",
		},
		OptimalFor: []string{
			"Read-heavy workloads",
			"Range queries",
			"OLAP workloads",
			"Analytical queries",
			"Sorted data access",
		},
	}

	baseEngine := storage.NewBaseEngine(storage.BPlusTreeEngine, info)

	engine := &BTreeEngine{
		BaseEngine: baseEngine,
		config:     config,
	}

	return engine, nil
}

// Open 打开B+树引擎
func (engine *BTreeEngine) Open() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if engine.IsOpened() {
		return fmt.Errorf("B+Tree engine already opened")
	}

	// 确保工作目录存在
	if err := os.MkdirAll(engine.config.BTreeOptions.WorkDir, 0755); err != nil {
		return fmt.Errorf("failed to create work directory: %w", err)
	}

	// 创建B+树实例
	btree, err := NewBPlusTree(engine.config.BTreeOptions)
	if err != nil {
		return fmt.Errorf("failed to create B+Tree: %w", err)
	}

	engine.btree = btree
	engine.SetOpened(true)

	return nil
}

// Close 关闭B+树引擎
func (engine *BTreeEngine) Close() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if !engine.IsOpened() {
		return nil
	}

	if engine.btree != nil {
		if err := engine.btree.Close(); err != nil {
			return fmt.Errorf("failed to close B+Tree: %w", err)
		}
	}

	engine.SetOpened(false)
	return nil
}

// Sync 同步数据到磁盘
func (engine *BTreeEngine) Sync() error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	// B+树的同步操作（如果支持）
	return nil
}

// Put 写入数据
func (engine *BTreeEngine) Put(key, value []byte) error {
	start := time.Now()
	defer func() {
		engine.UpdateStats("write", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	return engine.btree.Put(key, value)
}

// Get 读取数据
func (engine *BTreeEngine) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		engine.UpdateStats("read", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("B+Tree engine not opened")
	}

	value, err := engine.btree.Get(key)
	if err != nil {
		if err.Error() == "key not found" {
			return nil, fmt.Errorf("key not found")
		}
		return nil, err
	}

	return value, nil
}

// Delete 删除数据
func (engine *BTreeEngine) Delete(key []byte) error {
	start := time.Now()
	defer func() {
		engine.UpdateStats("delete", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	return engine.btree.Delete(key)
}

// Exists 检查键是否存在
func (engine *BTreeEngine) Exists(key []byte) (bool, error) {
	_, err := engine.Get(key)
	if err != nil {
		if err.Error() == "key not found" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// BatchPut 批量写入
func (engine *BTreeEngine) BatchPut(batch []storage.KVPair) error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	// 使用循环实现批量操作
	for _, kv := range batch {
		if err := engine.btree.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

// BatchGet 批量读取
func (engine *BTreeEngine) BatchGet(keys [][]byte) ([][]byte, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("B+Tree engine not opened")
	}

	// 使用循环实现批量读取
	results := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := engine.btree.Get(key)
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	return results, nil
}

// BatchDelete 批量删除
func (engine *BTreeEngine) BatchDelete(keys [][]byte) error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	// 使用循环实现批量删除
	for _, key := range keys {
		if err := engine.btree.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// Scan 范围扫描
func (engine *BTreeEngine) Scan(startKey, endKey []byte, limit int) ([]storage.KVPair, error) {
	start := time.Now()
	defer func() {
		engine.UpdateStats("scan", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("B+Tree engine not opened")
	}

	// 暂时返回空结果，等待实现完整的扫描功能
	return []storage.KVPair{}, nil
}

// NewIterator 创建迭代器
func (engine *BTreeEngine) NewIterator(options *storage.IteratorOptions) (storage.Iterator, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("B+Tree engine not opened")
	}

	// 暂时返回nil，等待实现
	return nil, fmt.Errorf("B+Tree iterator not implemented yet")
}

// 注意：B+树引擎不直接提供事务接口
// 事务功能通过transaction包中的适配器实现

// GetBTree 获取底层B+树实例（用于事务实现）
func (engine *BTreeEngine) GetBTree() *BPlusTree {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.btree
}

// GetPageStats 获取页面统计信息
func (engine *BTreeEngine) GetPageStats() map[string]interface{} {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() || engine.btree == nil {
		return nil
	}

	stats := engine.GetStats()
	if stats.EngineSpecific == nil {
		stats.EngineSpecific = make(map[string]interface{})
	}

	// 添加B+树特定的统计信息
	stats.EngineSpecific["page_count"] = "unknown" // 需要B+树提供接口
	stats.EngineSpecific["cache_hit_rate"] = "unknown"
	stats.EngineSpecific["fill_factor"] = engine.config.FillFactor
	stats.EngineSpecific["page_size"] = engine.config.PageSize
	stats.EngineSpecific["cache_size"] = engine.config.CacheSize

	return stats.EngineSpecific
}

// Compact 压缩B+树（重新组织页面）
func (engine *BTreeEngine) Compact() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if !engine.IsOpened() {
		return fmt.Errorf("B+Tree engine not opened")
	}

	// B+树的压缩操作（如果支持）
	// 这里可能需要B+树提供压缩接口
	return nil
}

// GetMemoryUsage 获取内存使用情况
func (engine *BTreeEngine) GetMemoryUsage() int64 {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return 0
	}

	// 估算内存使用（需要B+树提供接口）
	return int64(engine.config.CacheSize)
}
