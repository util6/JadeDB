/*
JadeDB 存储引擎状态机适配器

本模块实现了Raft状态机与存储引擎的深度集成，将分布式共识层与存储层连接起来。
通过这个适配器，Raft算法可以直接操作LSM树或B+树存储引擎，实现分布式数据库功能。

核心功能：
1. 状态机集成：将Raft状态机操作直接应用到存储引擎
2. 操作日志：记录和重放存储引擎操作
3. 快照管理：基于存储引擎的高效快照机制
4. 事务支持：与分布式事务系统集成
5. 故障恢复：基于存储引擎的故障恢复机制

设计特点：
- 引擎无关：支持LSM树和B+树等多种存储引擎
- 高性能：最小化序列化开销，优化批量操作
- 事务一致性：与Percolator事务模型集成
- 可扩展：支持自定义操作类型和处理器
*/

package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/storage"
)

// StorageEngineStateMachine 存储引擎状态机适配器
type StorageEngineStateMachine struct {
	mu sync.RWMutex

	// 存储引擎
	engine storage.Engine

	// Raft节点引用
	raftNode *RaftNode

	// 操作日志管理
	operationLog *OperationLog

	// 快照管理器
	snapshotManager *SnapshotManager

	// 配置
	config *StorageStateMachineConfig

	// 监控指标
	metrics *StorageStateMachineMetrics

	// 日志记录器
	logger *log.Logger

	// 状态
	lastAppliedIndex uint64
	lastAppliedTerm  uint64
	isRunning        bool
}

// StorageStateMachineConfig 存储状态机配置
type StorageStateMachineConfig struct {
	// 快照配置
	SnapshotThreshold uint64        // 快照触发阈值
	SnapshotInterval  time.Duration // 快照间隔
	MaxSnapshotSize   int64         // 最大快照大小
	CompressSnapshot  bool          // 是否压缩快照

	// 操作日志配置
	MaxOperationLogSize   int64         // 最大操作日志大小
	OperationLogRetention time.Duration // 操作日志保留时间

	// 性能配置
	BatchSize        int           // 批量操作大小
	FlushInterval    time.Duration // 刷新间隔
	EnableAsyncApply bool          // 启用异步应用
}

// StorageStateMachineMetrics 存储状态机监控指标
type StorageStateMachineMetrics struct {
	// 操作统计
	AppliedOperations uint64
	FailedOperations  uint64
	SnapshotCount     uint64
	RestoreCount      uint64

	// 性能指标
	AvgApplyLatency    time.Duration
	AvgSnapshotLatency time.Duration
	AvgRestoreLatency  time.Duration

	// 存储统计
	TotalKeys        uint64
	TotalDataSize    uint64
	LastSnapshotSize uint64
	LastSnapshotTime time.Time

	// 错误统计
	SerializationErrors uint64
	StorageErrors       uint64
	ConsistencyErrors   uint64
}

// StorageOperation 存储操作定义
type StorageOperation struct {
	Type      string                 `json:"type"` // 操作类型：PUT, DELETE, BATCH_PUT, BATCH_DELETE
	Key       []byte                 `json:"key,omitempty"`
	Value     []byte                 `json:"value,omitempty"`
	Keys      [][]byte               `json:"keys,omitempty"`
	Values    [][]byte               `json:"values,omitempty"`
	KVPairs   []storage.KVPair       `json:"kv_pairs,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp uint64                 `json:"timestamp"`
}

// OperationLog 操作日志管理器
type OperationLog struct {
	mu         sync.RWMutex
	operations []StorageOperation
	maxSize    int64
	retention  time.Duration
}

// SnapshotManager 快照管理器
type SnapshotManager struct {
	mu                sync.RWMutex
	engine            storage.Engine
	config            *StorageStateMachineConfig
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	lastSnapshotTime  time.Time
}

// NewStorageEngineStateMachine 创建存储引擎状态机
func NewStorageEngineStateMachine(engine storage.Engine, config *StorageStateMachineConfig) *StorageEngineStateMachine {
	if config == nil {
		config = DefaultStorageStateMachineConfig()
	}

	sm := &StorageEngineStateMachine{
		engine:          engine,
		config:          config,
		operationLog:    NewOperationLog(config.MaxOperationLogSize, config.OperationLogRetention),
		snapshotManager: NewSnapshotManager(engine, config),
		metrics:         &StorageStateMachineMetrics{},
		logger:          log.New(log.Writer(), "[StorageStateMachine] ", log.LstdFlags),
		isRunning:       false,
	}

	return sm
}

// DefaultStorageStateMachineConfig 默认配置
func DefaultStorageStateMachineConfig() *StorageStateMachineConfig {
	return &StorageStateMachineConfig{
		SnapshotThreshold:     10000,
		SnapshotInterval:      5 * time.Minute,
		MaxSnapshotSize:       100 * 1024 * 1024, // 100MB
		CompressSnapshot:      true,
		MaxOperationLogSize:   10 * 1024 * 1024, // 10MB
		OperationLogRetention: 24 * time.Hour,
		BatchSize:             100,
		FlushInterval:         1 * time.Second,
		EnableAsyncApply:      true,
	}
}

// Apply 应用日志条目到存储引擎
func (sm *StorageEngineStateMachine) Apply(entry LogEntry) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		sm.metrics.AvgApplyLatency = (sm.metrics.AvgApplyLatency + latency) / 2
	}()

	// 解析操作
	var operation StorageOperation
	if err := json.Unmarshal(entry.Data, &operation); err != nil {
		sm.metrics.SerializationErrors++
		return &StorageOperationResult{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal operation: %v", err),
		}
	}

	// 应用操作到存储引擎
	if err := sm.applyOperation(&operation); err != nil {
		sm.metrics.FailedOperations++
		sm.metrics.StorageErrors++
		return &StorageOperationResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	// 更新状态
	sm.lastAppliedIndex = entry.Index
	sm.lastAppliedTerm = entry.Term
	sm.metrics.AppliedOperations++

	// 记录操作日志
	sm.operationLog.AddOperation(operation)

	// 检查是否需要创建快照
	if sm.shouldCreateSnapshot() {
		go sm.createSnapshotAsync()
	}

	sm.logger.Printf("Applied operation %s at index %d, term %d",
		operation.Type, entry.Index, entry.Term)

	return &StorageOperationResult{
		Success: true,
		Index:   entry.Index,
		Term:    entry.Term,
	}
}

// applyOperation 应用具体操作到存储引擎
func (sm *StorageEngineStateMachine) applyOperation(op *StorageOperation) error {
	switch op.Type {
	case "PUT":
		return sm.engine.Put(op.Key, op.Value)

	case "DELETE":
		return sm.engine.Delete(op.Key)

	case "BATCH_PUT":
		if len(op.KVPairs) > 0 {
			return sm.engine.BatchPut(op.KVPairs)
		}
		// 兼容旧格式
		if len(op.Keys) == len(op.Values) {
			pairs := make([]storage.KVPair, len(op.Keys))
			for i := range op.Keys {
				pairs[i] = storage.KVPair{Key: op.Keys[i], Value: op.Values[i]}
			}
			return sm.engine.BatchPut(pairs)
		}
		return fmt.Errorf("invalid batch put operation: keys and values length mismatch")

	case "BATCH_DELETE":
		if len(op.Keys) > 0 {
			return sm.engine.BatchDelete(op.Keys)
		}
		return fmt.Errorf("invalid batch delete operation: no keys provided")

	case "SYNC":
		return sm.engine.Sync()

	default:
		return fmt.Errorf("unknown operation type: %s", op.Type)
	}
}

// Snapshot 创建状态机快照
func (sm *StorageEngineStateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		sm.metrics.AvgSnapshotLatency = (sm.metrics.AvgSnapshotLatency + latency) / 2
		sm.metrics.SnapshotCount++
		sm.metrics.LastSnapshotTime = time.Now()
	}()

	return sm.snapshotManager.CreateSnapshot(sm.lastAppliedIndex, sm.lastAppliedTerm)
}

// Restore 从快照恢复状态机
func (sm *StorageEngineStateMachine) Restore(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		sm.metrics.AvgRestoreLatency = (sm.metrics.AvgRestoreLatency + latency) / 2
		sm.metrics.RestoreCount++
	}()

	index, term, err := sm.snapshotManager.RestoreSnapshot(data)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	sm.lastAppliedIndex = index
	sm.lastAppliedTerm = term

	sm.logger.Printf("Restored from snapshot: index=%d, term=%d", index, term)
	return nil
}

// shouldCreateSnapshot 检查是否应该创建快照
func (sm *StorageEngineStateMachine) shouldCreateSnapshot() bool {
	// 基于应用的操作数量
	if sm.metrics.AppliedOperations%sm.config.SnapshotThreshold == 0 {
		return true
	}

	// 基于时间间隔
	if time.Since(sm.metrics.LastSnapshotTime) > sm.config.SnapshotInterval {
		return true
	}

	return false
}

// createSnapshotAsync 异步创建快照
func (sm *StorageEngineStateMachine) createSnapshotAsync() {
	if sm.raftNode != nil {
		if err := sm.raftNode.CreateSnapshot(); err != nil {
			sm.logger.Printf("Failed to create snapshot: %v", err)
		}
	}
}

// GetMetrics 获取监控指标
func (sm *StorageEngineStateMachine) GetMetrics() *StorageStateMachineMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 更新存储引擎统计
	if stats := sm.engine.GetStats(); stats != nil {
		sm.metrics.TotalKeys = uint64(stats.KeyCount)
		sm.metrics.TotalDataSize = uint64(stats.DataSize)
	}

	return sm.metrics
}

// Close 关闭状态机
func (sm *StorageEngineStateMachine) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.isRunning = false
	sm.logger.Printf("Storage engine state machine closed")
	return nil
}

// NewOperationLog 创建操作日志管理器
func NewOperationLog(maxSize int64, retention time.Duration) *OperationLog {
	return &OperationLog{
		operations: make([]StorageOperation, 0),
		maxSize:    maxSize,
		retention:  retention,
	}
}

// AddOperation 添加操作到日志
func (ol *OperationLog) AddOperation(op StorageOperation) {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	ol.operations = append(ol.operations, op)

	// 检查是否需要清理旧操作
	ol.cleanup()
}

// cleanup 清理过期操作
func (ol *OperationLog) cleanup() {
	now := time.Now()
	cutoff := now.Add(-ol.retention)

	// 找到第一个未过期的操作
	var newStart int
	for i, op := range ol.operations {
		if time.Unix(0, int64(op.Timestamp)).After(cutoff) {
			newStart = i
			break
		}
	}

	// 移除过期操作
	if newStart > 0 {
		ol.operations = ol.operations[newStart:]
	}

	// 检查大小限制
	if int64(len(ol.operations)) > ol.maxSize {
		excess := int64(len(ol.operations)) - ol.maxSize
		ol.operations = ol.operations[excess:]
	}
}

// GetOperations 获取操作历史
func (ol *OperationLog) GetOperations(since time.Time) []StorageOperation {
	ol.mu.RLock()
	defer ol.mu.RUnlock()

	var result []StorageOperation
	sinceNano := since.UnixNano()

	for _, op := range ol.operations {
		if int64(op.Timestamp) >= sinceNano {
			result = append(result, op)
		}
	}

	return result
}

// NewSnapshotManager 创建快照管理器
func NewSnapshotManager(engine storage.Engine, config *StorageStateMachineConfig) *SnapshotManager {
	return &SnapshotManager{
		engine: engine,
		config: config,
	}
}

// CreateSnapshot 创建快照
func (sm *SnapshotManager) CreateSnapshot(index, term uint64) ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	start := time.Now()

	// 创建快照数据结构
	snapshot := &StorageSnapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Timestamp:         uint64(start.UnixNano()),
		EngineType:        sm.engine.GetEngineType(),
		Data:              make(map[string][]byte),
	}

	// 遍历存储引擎获取所有数据
	iter, err := sm.engine.NewIterator(&storage.IteratorOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		value := make([]byte, len(iter.Value()))
		copy(key, iter.Key())
		copy(value, iter.Value())
		snapshot.Data[string(key)] = value
	}

	// 序列化快照
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// 更新快照状态
	sm.lastSnapshotIndex = index
	sm.lastSnapshotTerm = term
	sm.lastSnapshotTime = start

	return data, nil
}

// RestoreSnapshot 恢复快照
func (sm *SnapshotManager) RestoreSnapshot(data []byte) (uint64, uint64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 反序列化快照
	var snapshot StorageSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return 0, 0, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// 验证引擎类型兼容性
	if snapshot.EngineType != sm.engine.GetEngineType() {
		return 0, 0, fmt.Errorf("engine type mismatch: snapshot=%s, current=%s",
			snapshot.EngineType, sm.engine.GetEngineType())
	}

	// 批量写入数据
	var pairs []storage.KVPair
	for key, value := range snapshot.Data {
		pairs = append(pairs, storage.KVPair{
			Key:   []byte(key),
			Value: value,
		})
	}

	if len(pairs) > 0 {
		if err := sm.engine.BatchPut(pairs); err != nil {
			return 0, 0, fmt.Errorf("failed to restore data: %w", err)
		}
	}

	// 同步到磁盘
	if err := sm.engine.Sync(); err != nil {
		return 0, 0, fmt.Errorf("failed to sync after restore: %w", err)
	}

	return snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, nil
}

// StorageSnapshot 存储快照数据结构
type StorageSnapshot struct {
	LastIncludedIndex uint64                 `json:"last_included_index"`
	LastIncludedTerm  uint64                 `json:"last_included_term"`
	Timestamp         uint64                 `json:"timestamp"`
	EngineType        storage.EngineType     `json:"engine_type"`
	Data              map[string][]byte      `json:"data"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// StorageOperationResult 存储操作结果
type StorageOperationResult struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Index   uint64      `json:"index,omitempty"`
	Term    uint64      `json:"term,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}
