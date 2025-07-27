/*
JadeDB 统一存储引擎接口

本模块定义了所有存储引擎都必须实现的统一接口。
各个引擎包（lsm、bplustree）在自己的包中实现这些接口。

设计原则：
1. 接口分离：存储操作接口与事务接口分离
2. 包职责清晰：每个包负责自己的事务实现
3. 统一抽象：提供统一的存储引擎抽象
4. 扩展性：易于添加新的存储引擎类型
*/

package storage

import (
	"time"
)

// 存储引擎不直接处理事务，而是通过TransactionAdapter进行集成
// 这样可以避免循环依赖，保持架构清晰

// Engine 统一存储引擎接口
// 所有存储引擎（LSM、B+树等）都必须实现这个接口
type Engine interface {
	// 基本存储操作
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Exists(key []byte) (bool, error)

	// 批量操作
	BatchPut(batch []KVPair) error
	BatchGet(keys [][]byte) ([][]byte, error)
	BatchDelete(keys [][]byte) error

	// 范围操作
	Scan(startKey, endKey []byte, limit int) ([]KVPair, error)
	NewIterator(options *IteratorOptions) (Iterator, error)

	// 生命周期管理
	Open() error
	Close() error
	Sync() error

	// 元数据
	GetEngineType() EngineType
	GetEngineInfo() *EngineInfo
	GetStats() *EngineStats
}

// EngineType 存储引擎类型
type EngineType int

const (
	LSMTreeEngine EngineType = iota
	BPlusTreeEngine
	HashTableEngine
	ColumnStoreEngine
	DistributedEngineType
)

func (t EngineType) String() string {
	switch t {
	case LSMTreeEngine:
		return "LSMTree"
	case BPlusTreeEngine:
		return "BPlusTree"
	case HashTableEngine:
		return "HashTable"
	case ColumnStoreEngine:
		return "ColumnStore"
	case DistributedEngineType:
		return "Distributed"
	default:
		return "Unknown"
	}
}

// EngineInfo 存储引擎信息
type EngineInfo struct {
	Type        EngineType
	Version     string
	Description string
	Features    []string
	Limitations []string
	OptimalFor  []string
}

// EngineStats 存储引擎统计信息
type EngineStats struct {
	// 基本统计
	KeyCount  int64
	DataSize  int64
	IndexSize int64

	// 操作统计
	ReadCount   int64
	WriteCount  int64
	DeleteCount int64
	ScanCount   int64

	// 性能指标
	AvgReadLatency  time.Duration
	AvgWriteLatency time.Duration
	AvgScanLatency  time.Duration

	// 引擎特定统计
	EngineSpecific map[string]interface{}
}

// KVPair 键值对
type KVPair struct {
	Key   []byte
	Value []byte
}

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	StartKey []byte
	EndKey   []byte
	Prefix   []byte
	Reverse  bool
	KeyOnly  bool
}

// Iterator 迭代器接口
type Iterator interface {
	Valid() bool
	Next()
	Prev()
	Seek(key []byte)
	SeekToFirst()
	SeekToLast()
	Key() []byte
	Value() []byte
	Close() error
}

// EngineFactory 存储引擎工厂接口
type EngineFactory interface {
	CreateEngine(engineType EngineType, config interface{}) (Engine, error)
	GetSupportedTypes() []EngineType
	GetDefaultConfig(engineType EngineType) interface{}
	ValidateConfig(engineType EngineType, config interface{}) error
}

// BaseEngine 基础存储引擎实现
// 提供通用的统计和状态管理功能
type BaseEngine struct {
	engineType EngineType
	info       *EngineInfo
	stats      *EngineStats
	opened     bool
}

// NewBaseEngine 创建基础存储引擎
func NewBaseEngine(engineType EngineType, info *EngineInfo) *BaseEngine {
	return &BaseEngine{
		engineType: engineType,
		info:       info,
		stats: &EngineStats{
			EngineSpecific: make(map[string]interface{}),
		},
		opened: false,
	}
}

// GetEngineType 获取引擎类型
func (base *BaseEngine) GetEngineType() EngineType {
	return base.engineType
}

// GetEngineInfo 获取引擎信息
func (base *BaseEngine) GetEngineInfo() *EngineInfo {
	return base.info
}

// GetStats 获取统计信息
func (base *BaseEngine) GetStats() *EngineStats {
	return base.stats
}

// IsOpened 检查是否已打开
func (base *BaseEngine) IsOpened() bool {
	return base.opened
}

// SetOpened 设置打开状态
func (base *BaseEngine) SetOpened(opened bool) {
	base.opened = opened
}

// UpdateStats 更新统计信息
func (base *BaseEngine) UpdateStats(operation string, latency time.Duration) {
	switch operation {
	case "read":
		base.stats.ReadCount++
		base.stats.AvgReadLatency = (base.stats.AvgReadLatency + latency) / 2
	case "write":
		base.stats.WriteCount++
		base.stats.AvgWriteLatency = (base.stats.AvgWriteLatency + latency) / 2
	case "delete":
		base.stats.DeleteCount++
	case "scan":
		base.stats.ScanCount++
		base.stats.AvgScanLatency = (base.stats.AvgScanLatency + latency) / 2
	}
}
