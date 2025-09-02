/*
JadeDB 引擎适配器

本模块实现了存储引擎的适配器，提供了统一的API接口，
支持在LSM树和B+树引擎之间进行无感切换。

主要功能：
1. 统一接口：提供与原有DB接口兼容的API
2. 引擎切换：支持运行时动态切换存储引擎
3. 数据迁移：支持引擎切换时的数据迁移
4. 性能监控：提供引擎性能统计和监控
*/

package JadeDB

import (
	"fmt"
	"github.com/util6/JadeDB/utils"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/storage"
)

// EngineAdapter 引擎适配器
type EngineAdapter struct {
	mu sync.RWMutex

	// 当前活跃引擎
	currentEngine storage.Engine
	currentType   EngineType

	// 引擎工厂
	factory *EngineFactory

	// 配置
	config *EngineAdapterConfig

	// 状态管理
	isClosed int32

	// 性能统计
	stats *AdapterStats
}

// EngineAdapterConfig 适配器配置
type EngineAdapterConfig struct {
	// 默认引擎类型
	DefaultEngineType EngineType

	// 引擎配置映射
	EngineConfigs map[EngineType]interface{}

	// 切换配置
	EnableHotSwitch     bool          // 是否启用热切换
	SwitchTimeout       time.Duration // 切换超时时间
	EnableDataMigration bool          // 是否启用数据迁移

	// 性能配置
	EnableStats     bool          // 是否启用统计
	StatsInterval   time.Duration // 统计间隔
	EnableProfiling bool          // 是否启用性能分析
}

// AdapterStats 适配器统计信息
type AdapterStats struct {
	// 引擎切换统计
	SwitchCount    int64
	LastSwitchTime time.Time
	SwitchErrors   int64

	// 操作统计
	TotalOperations int64
	SuccessfulOps   int64
	FailedOps       int64

	// 性能指标
	AvgLatency time.Duration
	MaxLatency time.Duration
	MinLatency time.Duration

	// 引擎特定统计
	EngineStats map[EngineType]*storage.EngineStats
}

// DefaultEngineAdapterConfig 默认适配器配置
func DefaultEngineAdapterConfig() *EngineAdapterConfig {
	return &EngineAdapterConfig{
		DefaultEngineType:   LSMEngine,
		EngineConfigs:       make(map[EngineType]interface{}),
		EnableHotSwitch:     true,
		SwitchTimeout:       30 * time.Second,
		EnableDataMigration: true,
		EnableStats:         true,
		StatsInterval:       time.Minute,
		EnableProfiling:     false,
	}
}

// NewEngineAdapter 创建引擎适配器
func NewEngineAdapter(config *EngineAdapterConfig) (*EngineAdapter, error) {
	if config == nil {
		config = DefaultEngineAdapterConfig()
	}

	adapter := &EngineAdapter{
		factory: GetGlobalEngineFactory(),
		config:  config,
		stats: &AdapterStats{
			EngineStats: make(map[EngineType]*storage.EngineStats),
		},
	}

	// 初始化默认引擎
	if err := adapter.initializeEngine(config.DefaultEngineType); err != nil {
		return nil, fmt.Errorf("failed to initialize default engine: %w", err)
	}

	// 启动统计收集
	if config.EnableStats {
		go adapter.collectStats()
	}

	return adapter, nil
}

// initializeEngine 初始化引擎
func (adapter *EngineAdapter) initializeEngine(engineType EngineType) error {
	// 获取引擎配置
	engineConfig := adapter.config.EngineConfigs[engineType]

	// 创建引擎
	engine, err := adapter.factory.CreateEngine(engineType, engineConfig)
	if err != nil {
		return fmt.Errorf("failed to create engine %v: %w", engineType, err)
	}

	// 打开引擎
	if err := engine.Open(); err != nil {
		return fmt.Errorf("failed to open engine %v: %w", engineType, err)
	}

	adapter.currentEngine = engine
	adapter.currentType = engineType

	return nil
}

// SwitchEngine 切换存储引擎
func (adapter *EngineAdapter) SwitchEngine(targetType EngineType) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	if atomic.LoadInt32(&adapter.isClosed) != 0 {
		return fmt.Errorf("adapter is closed")
	}

	if adapter.currentType == targetType {
		return nil // 已经是目标引擎
	}

	start := time.Now()
	defer func() {
		atomic.AddInt64(&adapter.stats.SwitchCount, 1)
		adapter.stats.LastSwitchTime = time.Now()
	}()

	// 创建新引擎
	newEngineConfig := adapter.config.EngineConfigs[targetType]
	newEngine, err := adapter.factory.CreateEngine(targetType, newEngineConfig)
	if err != nil {
		atomic.AddInt64(&adapter.stats.SwitchErrors, 1)
		return fmt.Errorf("failed to create new engine: %w", err)
	}

	// 打开新引擎
	if err := newEngine.Open(); err != nil {
		atomic.AddInt64(&adapter.stats.SwitchErrors, 1)
		return fmt.Errorf("failed to open new engine: %w", err)
	}

	// 数据迁移（如果启用）
	if adapter.config.EnableDataMigration {
		if err := adapter.migrateData(adapter.currentEngine, newEngine); err != nil {
			newEngine.Close()
			atomic.AddInt64(&adapter.stats.SwitchErrors, 1)
			return fmt.Errorf("failed to migrate data: %w", err)
		}
	}

	// 关闭旧引擎
	oldEngine := adapter.currentEngine
	oldType := adapter.currentType

	// 切换到新引擎
	adapter.currentEngine = newEngine
	adapter.currentType = targetType

	// 异步关闭旧引擎
	go func() {
		if err := oldEngine.Close(); err != nil {
			// 记录错误但不影响切换
			fmt.Printf("Warning: failed to close old engine %v: %v\n", oldType, err)
		}
	}()

	fmt.Printf("Successfully switched from %v to %v in %v\n",
		oldType, targetType, time.Since(start))

	return nil
}

// migrateData 数据迁移
func (adapter *EngineAdapter) migrateData(source, target storage.Engine) error {
	// 创建迭代器遍历源引擎的所有数据
	iter, err := source.NewIterator(&storage.IteratorOptions{})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	// 批量迁移数据
	batch := make([]storage.KVPair, 0, 1000)
	batchSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10MB

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		kv := storage.KVPair{
			Key:   iter.Key(),
			Value: iter.Value(),
		}

		batch = append(batch, kv)
		batchSize += len(kv.Key) + len(kv.Value)

		// 当批次达到大小限制时，执行批量写入
		if len(batch) >= 1000 || batchSize >= maxBatchSize {
			if err := target.BatchPut(batch); err != nil {
				return fmt.Errorf("failed to batch put during migration: %w", err)
			}

			batch = batch[:0]
			batchSize = 0
		}
	}

	// 写入剩余数据
	if len(batch) > 0 {
		if err := target.BatchPut(batch); err != nil {
			return fmt.Errorf("failed to batch put remaining data: %w", err)
		}
	}

	return nil
}

// Set 存储键值对（兼容原有接口）
func (adapter *EngineAdapter) Set(data *utils.Entry) error {
	start := time.Now()
	defer adapter.updateLatencyStats(time.Since(start))

	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if atomic.LoadInt32(&adapter.isClosed) != 0 {
		return fmt.Errorf("adapter is closed")
	}

	atomic.AddInt64(&adapter.stats.TotalOperations, 1)

	// 为键添加时间戳，确保与LSM引擎兼容
	// 使用 MaxUint32 作为时间戳，表示这是最新版本
	keyWithTs := utils.KeyWithTs(data.Key, math.MaxUint32)

	err := adapter.currentEngine.Put(keyWithTs, data.Value)
	if err != nil {
		atomic.AddInt64(&adapter.stats.FailedOps, 1)
		return err
	}

	atomic.AddInt64(&adapter.stats.SuccessfulOps, 1)
	return nil
}

// Get 获取值（兼容原有接口）
func (adapter *EngineAdapter) Get(key []byte) (*utils.Entry, error) {
	start := time.Now()
	defer adapter.updateLatencyStats(time.Since(start))

	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if atomic.LoadInt32(&adapter.isClosed) != 0 {
		return nil, fmt.Errorf("adapter is closed")
	}

	atomic.AddInt64(&adapter.stats.TotalOperations, 1)

	// 为键添加时间戳进行查找
	// 使用 MaxUint32 表示查找最新版本
	keyWithTs := utils.KeyWithTs(key, math.MaxUint32)

	value, err := adapter.currentEngine.Get(keyWithTs)
	if err != nil {
		atomic.AddInt64(&adapter.stats.FailedOps, 1)
		return nil, err
	}

	atomic.AddInt64(&adapter.stats.SuccessfulOps, 1)

	return &utils.Entry{
		Key:   key, // 返回原始键，不包含时间戳
		Value: value,
	}, nil
}

// Del 删除键（兼容原有接口）
func (adapter *EngineAdapter) Del(key []byte) error {
	start := time.Now()
	defer adapter.updateLatencyStats(time.Since(start))

	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if atomic.LoadInt32(&adapter.isClosed) != 0 {
		return fmt.Errorf("adapter is closed")
	}

	atomic.AddInt64(&adapter.stats.TotalOperations, 1)

	// 为键添加时间戳进行删除
	// 使用 MaxUint32 表示删除最新版本
	keyWithTs := utils.KeyWithTs(key, math.MaxUint32)

	err := adapter.currentEngine.Delete(keyWithTs)
	if err != nil {
		atomic.AddInt64(&adapter.stats.FailedOps, 1)
		return err
	}

	atomic.AddInt64(&adapter.stats.SuccessfulOps, 1)
	return nil
}

// NewIterator 创建迭代器（兼容原有接口）
func (adapter *EngineAdapter) NewIterator(opt *utils.Options) utils.Iterator {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if atomic.LoadInt32(&adapter.isClosed) != 0 {
		return nil
	}

	// 转换选项格式
	storageOpt := &storage.IteratorOptions{}
	if opt != nil {
		storageOpt.Prefix = opt.Prefix
		// 注意：utils.Options可能没有Reverse字段，需要检查
		// storageOpt.Reverse = opt.Reverse
	}

	iter, err := adapter.currentEngine.NewIterator(storageOpt)
	if err != nil {
		return nil
	}

	// 包装为utils.Iterator接口
	return &iteratorWrapper{iter: iter}
}

// Info 获取统计信息（兼容原有接口）
func (adapter *EngineAdapter) Info() *Stats {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	// 获取当前引擎统计
	_ = adapter.currentEngine.GetStats()

	// 转换为原有格式
	return &Stats{
		// 这里需要根据原有Stats结构进行转换
		// 暂时返回空结构
	}
}

// Close 关闭适配器
func (adapter *EngineAdapter) Close() error {
	if !atomic.CompareAndSwapInt32(&adapter.isClosed, 0, 1) {
		return nil // 已经关闭
	}

	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	if adapter.currentEngine != nil {
		return adapter.currentEngine.Close()
	}

	return nil
}

// GetCurrentEngineType 获取当前引擎类型
func (adapter *EngineAdapter) GetCurrentEngineType() EngineType {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.currentType
}

// GetCurrentEngine 获取当前引擎
func (adapter *EngineAdapter) GetCurrentEngine() storage.Engine {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.currentEngine
}

// GetAdapterStats 获取适配器统计信息
func (adapter *EngineAdapter) GetAdapterStats() *AdapterStats {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	// 更新引擎统计
	if adapter.currentEngine != nil {
		adapter.stats.EngineStats[adapter.currentType] = adapter.currentEngine.GetStats()
	}

	return adapter.stats
}

// updateLatencyStats 更新延迟统计
func (adapter *EngineAdapter) updateLatencyStats(latency time.Duration) {
	if latency > adapter.stats.MaxLatency {
		adapter.stats.MaxLatency = latency
	}
	if adapter.stats.MinLatency == 0 || latency < adapter.stats.MinLatency {
		adapter.stats.MinLatency = latency
	}
	adapter.stats.AvgLatency = (adapter.stats.AvgLatency + latency) / 2
}

// collectStats 收集统计信息
func (adapter *EngineAdapter) collectStats() {
	ticker := time.NewTicker(adapter.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			adapter.mu.RLock()
			if adapter.currentEngine != nil {
				adapter.stats.EngineStats[adapter.currentType] = adapter.currentEngine.GetStats()
			}
			adapter.mu.RUnlock()
		}
	}
}

// iteratorWrapper 迭代器包装器
type iteratorWrapper struct {
	iter storage.Iterator
}

func (w *iteratorWrapper) Valid() bool {
	return w.iter.Valid()
}

func (w *iteratorWrapper) Next() {
	w.iter.Next()
}

func (w *iteratorWrapper) Prev() {
	w.iter.Prev()
}

func (w *iteratorWrapper) Seek(key []byte) {
	w.iter.Seek(key)
}

func (w *iteratorWrapper) SeekToFirst() {
	w.iter.SeekToFirst()
}

func (w *iteratorWrapper) SeekToLast() {
	w.iter.SeekToLast()
}

func (w *iteratorWrapper) Key() []byte {
	return w.iter.Key()
}

func (w *iteratorWrapper) Value() []byte {
	return w.iter.Value()
}

func (w *iteratorWrapper) Close() error {
	return w.iter.Close()
}

func (w *iteratorWrapper) Item() utils.Item {
	// 创建一个简单的Item实现
	return &simpleItem{
		key:   w.iter.Key(),
		value: w.iter.Value(),
	}
}

func (w *iteratorWrapper) Rewind() {
	w.iter.SeekToFirst()
}

// simpleItem 简单的Item实现
type simpleItem struct {
	key   []byte
	value []byte
}

func (item *simpleItem) Entry() *utils.Entry {
	return &utils.Entry{
		Key:   item.key,
		Value: item.value,
	}
}
