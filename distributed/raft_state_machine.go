/*
JadeDB Raft状态机实现

本模块实现了Raft状态机，负责应用已提交的日志条目到实际的业务状态。
状态机是Raft算法的核心组件，确保所有节点以相同的顺序应用相同的操作，
从而保证分布式系统的一致性。

核心功能：
1. 日志应用：将已提交的日志条目应用到状态机
2. 快照管理：创建和恢复状态机快照，压缩日志
3. 状态查询：提供状态机当前状态的查询接口
4. 事务支持：支持事务性的状态变更操作
5. 持久化：状态机状态的持久化存储

设计特点：
- 确定性：相同的输入产生相同的输出
- 幂等性：重复应用相同的操作不会改变结果
- 原子性：每个操作要么完全成功要么完全失败
- 一致性：保证状态机在所有节点上的一致性
*/

package distributed

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"
)

// KVStateMachine 键值存储状态机
type KVStateMachine struct {
	mu       sync.RWMutex
	data     map[string][]byte      // 键值数据
	metadata map[string]interface{} // 元数据

	// 快照相关
	lastAppliedIndex uint64
	lastAppliedTerm  uint64

	// 配置
	config *StateMachineConfig

	// 监控
	metrics *StateMachineMetrics
	logger  *log.Logger
}

// StateMachineConfig 状态机配置
type StateMachineConfig struct {
	SnapshotThreshold   int           // 快照阈值
	SnapshotInterval    time.Duration // 快照间隔
	EnableCompression   bool          // 是否启用压缩
	MaxSnapshotSize     int64         // 最大快照大小
	RetainSnapshotCount int           // 保留快照数量
}

// DefaultStateMachineConfig 默认状态机配置
func DefaultStateMachineConfig() *StateMachineConfig {
	return &StateMachineConfig{
		SnapshotThreshold:   1000,
		SnapshotInterval:    time.Hour,
		EnableCompression:   true,
		MaxSnapshotSize:     100 * 1024 * 1024, // 100MB
		RetainSnapshotCount: 3,
	}
}

// StateMachineMetrics 状态机监控指标
type StateMachineMetrics struct {
	AppliedEntries   int64         // 已应用条目数
	SnapshotCount    int64         // 快照数量
	LastSnapshotTime time.Time     // 最后快照时间
	DataSize         int64         // 数据大小
	ApplyLatency     time.Duration // 应用延迟
}

// NewKVStateMachine 创建键值状态机
func NewKVStateMachine(config *StateMachineConfig) *KVStateMachine {
	if config == nil {
		config = DefaultStateMachineConfig()
	}

	return &KVStateMachine{
		data:     make(map[string][]byte),
		metadata: make(map[string]interface{}),
		config:   config,
		metrics:  &StateMachineMetrics{},
		logger:   log.New(log.Writer(), "[STATE-MACHINE] ", log.LstdFlags),
	}
}

// Apply 应用日志条目到状态机
func (sm *KVStateMachine) Apply(entry LogEntry) interface{} {
	start := time.Now()
	defer func() {
		sm.metrics.ApplyLatency = time.Since(start)
		sm.metrics.AppliedEntries++
	}()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 更新应用索引
	sm.lastAppliedIndex = entry.Index
	sm.lastAppliedTerm = entry.Term

	// 解析操作
	op, err := sm.parseOperation(entry.Data)
	if err != nil {
		sm.logger.Printf("Failed to parse operation: %v", err)
		return &OperationResult{
			Success: false,
			Error:   err.Error(),
		}
	}

	// 执行操作
	result := sm.executeOperation(op)

	sm.logger.Printf("Applied operation: %s, index: %d, term: %d",
		op.Type, entry.Index, entry.Term)

	return result
}

// Snapshot 创建状态机快照
func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snapshot := &StateMachineSnapshot{
		Data:             make(map[string][]byte),
		Metadata:         make(map[string]interface{}),
		LastAppliedIndex: sm.lastAppliedIndex,
		LastAppliedTerm:  sm.lastAppliedTerm,
		Timestamp:        time.Now(),
	}

	// 复制数据
	for k, v := range sm.data {
		snapshot.Data[k] = make([]byte, len(v))
		copy(snapshot.Data[k], v)
	}

	// 复制元数据
	for k, v := range sm.metadata {
		snapshot.Metadata[k] = v
	}

	// 序列化快照
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(snapshot); err != nil {
		return nil, fmt.Errorf("failed to encode snapshot: %w", err)
	}

	sm.metrics.SnapshotCount++
	sm.metrics.LastSnapshotTime = time.Now()

	sm.logger.Printf("Created snapshot: index=%d, term=%d, size=%d",
		snapshot.LastAppliedIndex, snapshot.LastAppliedTerm, buf.Len())

	return buf.Bytes(), nil
}

// Restore 从快照恢复状态机
func (sm *KVStateMachine) Restore(snapshotData []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 反序列化快照
	var snapshot StateMachineSnapshot
	buf := bytes.NewBuffer(snapshotData)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// 恢复数据
	sm.data = make(map[string][]byte)
	for k, v := range snapshot.Data {
		sm.data[k] = make([]byte, len(v))
		copy(sm.data[k], v)
	}

	// 恢复元数据
	sm.metadata = make(map[string]interface{})
	for k, v := range snapshot.Metadata {
		sm.metadata[k] = v
	}

	// 恢复状态
	sm.lastAppliedIndex = snapshot.LastAppliedIndex
	sm.lastAppliedTerm = snapshot.LastAppliedTerm

	sm.logger.Printf("Restored from snapshot: index=%d, term=%d, keys=%d",
		snapshot.LastAppliedIndex, snapshot.LastAppliedTerm, len(sm.data))

	return nil
}

// Get 获取键值
func (sm *KVStateMachine) Get(key string) ([]byte, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	value, exists := sm.data[key]
	if !exists {
		return nil, false
	}

	// 返回副本
	result := make([]byte, len(value))
	copy(result, value)
	return result, true
}

// GetAll 获取所有键值对
func (sm *KVStateMachine) GetAll() map[string][]byte {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string][]byte)
	for k, v := range sm.data {
		result[k] = make([]byte, len(v))
		copy(result[k], v)
	}

	return result
}

// GetMetrics 获取监控指标
func (sm *KVStateMachine) GetMetrics() *StateMachineMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 计算数据大小
	dataSize := int64(0)
	for k, v := range sm.data {
		dataSize += int64(len(k) + len(v))
	}
	sm.metrics.DataSize = dataSize

	return sm.metrics
}

// GetLastApplied 获取最后应用的索引和任期
func (sm *KVStateMachine) GetLastApplied() (uint64, uint64) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.lastAppliedIndex, sm.lastAppliedTerm
}

// StateMachineSnapshot 状态机快照
type StateMachineSnapshot struct {
	Data             map[string][]byte      // 键值数据
	Metadata         map[string]interface{} // 元数据
	LastAppliedIndex uint64                 // 最后应用索引
	LastAppliedTerm  uint64                 // 最后应用任期
	Timestamp        time.Time              // 快照时间戳
}

// Operation 状态机操作
type Operation struct {
	Type  string      // 操作类型：PUT, DELETE, BATCH
	Key   string      // 键
	Value []byte      // 值
	Batch []BatchItem // 批量操作
}

// BatchItem 批量操作项
type BatchItem struct {
	Type  string // PUT, DELETE
	Key   string
	Value []byte
}

// OperationResult 操作结果
type OperationResult struct {
	Success bool        // 是否成功
	Value   []byte      // 返回值（用于GET操作）
	Error   string      // 错误信息
	Data    interface{} // 其他数据
}

// parseOperation 解析操作
func (sm *KVStateMachine) parseOperation(data []byte) (*Operation, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty operation data")
	}

	var op Operation
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&op); err != nil {
		return nil, fmt.Errorf("failed to decode operation: %w", err)
	}

	return &op, nil
}

// executeOperation 执行操作
func (sm *KVStateMachine) executeOperation(op *Operation) *OperationResult {
	switch op.Type {
	case "PUT":
		return sm.executePut(op.Key, op.Value)
	case "DELETE":
		return sm.executeDelete(op.Key)
	case "BATCH":
		return sm.executeBatch(op.Batch)
	default:
		return &OperationResult{
			Success: false,
			Error:   fmt.Sprintf("unknown operation type: %s", op.Type),
		}
	}
}

// executePut 执行PUT操作
func (sm *KVStateMachine) executePut(key string, value []byte) *OperationResult {
	// 复制值
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	sm.data[key] = valueCopy

	return &OperationResult{
		Success: true,
	}
}

// executeDelete 执行DELETE操作
func (sm *KVStateMachine) executeDelete(key string) *OperationResult {
	_, existed := sm.data[key]
	delete(sm.data, key)

	return &OperationResult{
		Success: true,
		Data:    existed,
	}
}

// executeBatch 执行批量操作
func (sm *KVStateMachine) executeBatch(batch []BatchItem) *OperationResult {
	results := make([]bool, len(batch))

	for i, item := range batch {
		switch item.Type {
		case "PUT":
			valueCopy := make([]byte, len(item.Value))
			copy(valueCopy, item.Value)
			sm.data[item.Key] = valueCopy
			results[i] = true
		case "DELETE":
			delete(sm.data, item.Key)
			results[i] = true
		default:
			results[i] = false
		}
	}

	return &OperationResult{
		Success: true,
		Data:    results,
	}
}

// EncodeOperation 编码操作
func EncodeOperation(op *Operation) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(op); err != nil {
		return nil, fmt.Errorf("failed to encode operation: %w", err)
	}
	return buf.Bytes(), nil
}

// CreatePutOperation 创建PUT操作
func CreatePutOperation(key string, value []byte) ([]byte, error) {
	op := &Operation{
		Type:  "PUT",
		Key:   key,
		Value: value,
	}
	return EncodeOperation(op)
}

// CreateDeleteOperation 创建DELETE操作
func CreateDeleteOperation(key string) ([]byte, error) {
	op := &Operation{
		Type: "DELETE",
		Key:  key,
	}
	return EncodeOperation(op)
}

// CreateBatchOperation 创建批量操作
func CreateBatchOperation(batch []BatchItem) ([]byte, error) {
	op := &Operation{
		Type:  "BATCH",
		Batch: batch,
	}
	return EncodeOperation(op)
}
