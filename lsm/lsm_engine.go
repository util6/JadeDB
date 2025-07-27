/*
JadeDB LSM存储引擎实现

本模块在LSM包中实现storage.Engine接口，提供LSM树的存储功能。
事务功能委托给LSM专用的事务实现。

设计特点：
1. 职责单一：只负责LSM存储逻辑
2. 接口实现：实现统一的storage.Engine接口
3. 事务委托：事务功能委托给lsm_transaction.go
4. 性能优化：充分利用LSM树的写优化特性
*/

package lsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/utils"
)

// LSMEngine LSM存储引擎
type LSMEngine struct {
	*storage.BaseEngine

	// LSM树实例
	lsmTree *LSM

	// 配置
	config *LSMEngineConfig

	// 状态管理
	mu sync.RWMutex
}

// LSMEngineConfig LSM引擎配置
type LSMEngineConfig struct {
	// LSM配置
	LSMOptions *Options

	// 性能配置
	WriteBufferSize int    // 写缓冲区大小
	BloomFilterBits int    // 布隆过滤器位数
	CompressionType string // 压缩类型
	MaxLevels       int    // 最大层数

	// 压缩配置
	CompactionStrategy string // 压缩策略
	CompactionThreads  int    // 压缩线程数
}

// DefaultLSMEngineConfig 默认LSM引擎配置
func DefaultLSMEngineConfig() *LSMEngineConfig {
	return &LSMEngineConfig{
		LSMOptions:         NewDefaultOptions(),
		WriteBufferSize:    64 * 1024 * 1024, // 64MB
		BloomFilterBits:    10,
		CompressionType:    "snappy",
		MaxLevels:          7,
		CompactionStrategy: "leveled",
		CompactionThreads:  2,
	}
}

// NewLSMEngine 创建LSM存储引擎
func NewLSMEngine(config *LSMEngineConfig) (*LSMEngine, error) {
	if config == nil {
		config = DefaultLSMEngineConfig()
	}

	// 创建引擎信息
	info := &storage.EngineInfo{
		Type:        storage.LSMTreeEngine,
		Version:     "1.0.0",
		Description: "LSM Tree storage engine optimized for write-heavy workloads",
		Features: []string{
			"High write throughput",
			"Efficient compaction",
			"Bloom filter optimization",
			"Configurable compression",
			"Multi-level storage",
		},
		Limitations: []string{
			"Read amplification",
			"Space amplification during compaction",
		},
		OptimalFor: []string{
			"Write-heavy workloads",
			"Time-series data",
			"Log data",
			"High-throughput OLTP",
		},
	}

	baseEngine := storage.NewBaseEngine(storage.LSMTreeEngine, info)

	engine := &LSMEngine{
		BaseEngine: baseEngine,
		config:     config,
	}

	return engine, nil
}

// Open 打开LSM引擎
func (engine *LSMEngine) Open() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if engine.IsOpened() {
		return fmt.Errorf("LSM engine already opened")
	}

	// 创建LSM树实例
	lsmTree := NewLSM(engine.config.LSMOptions)

	engine.lsmTree = lsmTree
	engine.SetOpened(true)

	return nil
}

// Close 关闭LSM引擎
func (engine *LSMEngine) Close() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if !engine.IsOpened() {
		return nil
	}

	if engine.lsmTree != nil {
		if err := engine.lsmTree.Close(); err != nil {
			return fmt.Errorf("failed to close LSM tree: %w", err)
		}
	}

	engine.SetOpened(false)
	return nil
}

// Sync 同步数据到磁盘
func (engine *LSMEngine) Sync() error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("LSM engine not opened")
	}

	// LSM树的同步操作（如果支持）
	return nil
}

// Put 写入数据
func (engine *LSMEngine) Put(key, value []byte) error {
	start := time.Now()
	defer func() {
		engine.UpdateStats("write", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("LSM engine not opened")
	}

	// 使用LSM的Set方法
	entry := &utils.Entry{
		Key:   key,
		Value: value,
	}

	return engine.lsmTree.Set(entry)
}

// Get 读取数据
func (engine *LSMEngine) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		engine.UpdateStats("read", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("LSM engine not opened")
	}

	entry, err := engine.lsmTree.Get(key)
	if err != nil {
		return nil, err
	}

	if entry == nil || entry.Value == nil {
		return nil, fmt.Errorf("key not found")
	}

	return entry.Value, nil
}

// Delete 删除数据
func (engine *LSMEngine) Delete(key []byte) error {
	start := time.Now()
	defer func() {
		engine.UpdateStats("delete", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("LSM engine not opened")
	}

	// LSM树中删除通过设置墓碑标记实现
	entry := &utils.Entry{
		Key:   key,
		Value: nil, // nil表示删除
	}

	return engine.lsmTree.Set(entry)
}

// Exists 检查键是否存在
func (engine *LSMEngine) Exists(key []byte) (bool, error) {
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
func (engine *LSMEngine) BatchPut(batch []storage.KVPair) error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("LSM engine not opened")
	}

	entries := make([]*utils.Entry, len(batch))
	for i, kv := range batch {
		entries[i] = &utils.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}

	return engine.lsmTree.BatchSet(entries)
}

// BatchGet 批量读取
func (engine *LSMEngine) BatchGet(keys [][]byte) ([][]byte, error) {
	results := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := engine.Get(key)
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	return results, nil
}

// BatchDelete 批量删除
func (engine *LSMEngine) BatchDelete(keys [][]byte) error {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return fmt.Errorf("LSM engine not opened")
	}

	entries := make([]*utils.Entry, len(keys))
	for i, key := range keys {
		entries[i] = &utils.Entry{
			Key:   key,
			Value: nil, // nil表示删除
		}
	}

	return engine.lsmTree.BatchSet(entries)
}

// Scan 范围扫描
func (engine *LSMEngine) Scan(startKey, endKey []byte, limit int) ([]storage.KVPair, error) {
	start := time.Now()
	defer func() {
		engine.UpdateStats("scan", time.Since(start))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("LSM engine not opened")
	}

	// 暂时返回空结果，等待实现完整的扫描功能
	return []storage.KVPair{}, nil
}

// NewIterator 创建迭代器
func (engine *LSMEngine) NewIterator(options *storage.IteratorOptions) (storage.Iterator, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	if !engine.IsOpened() {
		return nil, fmt.Errorf("LSM engine not opened")
	}

	// 暂时返回nil，等待实现
	return nil, fmt.Errorf("LSM iterator not implemented yet")
}

// 注意：LSM引擎不直接提供事务接口
// 事务功能通过transaction包中的适配器实现

// GetLSMTree 获取底层LSM树实例（用于事务实现）
func (engine *LSMEngine) GetLSMTree() *LSM {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.lsmTree
}
