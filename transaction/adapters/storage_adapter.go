/*
存储引擎事务适配器

本模块实现了存储引擎与事务系统的集成适配器。
通过适配器模式，将不同的存储引擎（LSM、B+树等）集成到统一的事务框架中。

设计原则：
1. 分离关注点：存储引擎专注存储，事务系统专注事务
2. 适配器模式：通过适配器实现两者的集成
3. 避免循环依赖：单向依赖关系
4. 统一接口：为上层提供一致的事务API

架构层次：
- SQL层 -> Transaction接口
- Transaction层 -> StorageAdapter -> Storage引擎
- Storage引擎 -> 底层存储（LSM/B+树）
*/

package adapters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/txnwal"
)

// IteratorAdapter 迭代器适配器，将 storage.Iterator 适配为 common.Iterator
type IteratorAdapter struct {
	iter storage.Iterator
}

func (ia *IteratorAdapter) Next() bool {
	ia.iter.Next()
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) Prev() bool {
	ia.iter.Prev()
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) Valid() bool {
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) Key() []byte {
	return ia.iter.Key()
}

func (ia *IteratorAdapter) Value() []byte {
	return ia.iter.Value()
}

func (ia *IteratorAdapter) Seek(key []byte) bool {
	ia.iter.Seek(key)
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) SeekToFirst() bool {
	ia.iter.SeekToFirst()
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) SeekToLast() bool {
	ia.iter.SeekToLast()
	return ia.iter.Valid()
}

func (ia *IteratorAdapter) Close() error {
	return ia.iter.Close()
}

// StorageTransactionAdapter 存储引擎事务适配器
// 将存储引擎包装成支持事务的接口
type StorageTransactionAdapter struct {
	// 基本属性
	txnID    string                    // 事务唯一标识符
	startTs  uint64                    // 事务开始时间戳
	commitTs uint64                    // 事务提交时间戳
	engine   storage.Engine            // 底层存储引擎
	txnMgr   common.TransactionManager // 事务管理器

	// 事务WAL
	txnWAL txnwal.TxnWALManager // 事务WAL管理器

	// 事务配置
	isolation common.IsolationLevel // 隔离级别
	readOnly  bool                  // 是否只读事务
	timeout   time.Duration         // 事务超时时间

	// 事务状态
	state common.TransactionState // 事务状态
	mu    sync.RWMutex            // 保护事务状态的锁

	// 数据缓冲区（用于事务隔离）
	writeBuffer map[string][]byte // 写缓冲区：key -> value
	deleteSet   map[string]bool   // 删除集合：记录被删除的key
	readSet     map[string]uint64 // 读集合：key -> 读取时的版本号

	// 统计信息
	readCount  int64 // 读操作计数
	writeCount int64 // 写操作计数

	// 生命周期管理
	startTime time.Time // 事务开始时间
	closed    bool      // 是否已关闭
}

// NewStorageTransactionAdapter 创建存储引擎事务适配器
func NewStorageTransactionAdapter(
	txnID string,
	engine storage.Engine,
	txnMgr common.TransactionManager,
	txnWAL txnwal.TxnWALManager,
	options *common.TransactionOptions,
) (*StorageTransactionAdapter, error) {

	if engine == nil {
		return nil, fmt.Errorf("storage engine cannot be nil")
	}

	if txnMgr == nil {
		return nil, fmt.Errorf("transaction manager cannot be nil")
	}

	if txnWAL == nil {
		return nil, fmt.Errorf("transaction WAL cannot be nil")
	}

	adapter := &StorageTransactionAdapter{
		txnID:       txnID,
		startTs:     uint64(time.Now().UnixNano()),
		engine:      engine,
		txnMgr:      txnMgr,
		txnWAL:      txnWAL,
		isolation:   common.ReadCommitted, // 默认隔离级别
		readOnly:    false,
		timeout:     30 * time.Second, // 默认30秒超时
		state:       common.TxnActive,
		writeBuffer: make(map[string][]byte),
		deleteSet:   make(map[string]bool),
		readSet:     make(map[string]uint64),
		startTime:   time.Now(),
	}

	// 应用选项配置
	if options != nil {
		adapter.isolation = options.IsolationLevel
		adapter.timeout = options.Timeout
		adapter.readOnly = options.ReadOnly
	}

	// 记录事务开始的WAL日志
	if err := adapter.writeTransactionWAL(txnwal.LogTransactionBegin); err != nil {
		return nil, fmt.Errorf("failed to write transaction begin WAL: %w", err)
	}

	return adapter, nil
}

// GetID 获取事务ID
func (adapter *StorageTransactionAdapter) GetID() string {
	return adapter.txnID
}

// GetStartTimestamp 获取开始时间戳
func (adapter *StorageTransactionAdapter) GetStartTimestamp() uint64 {
	return adapter.startTs
}

// GetCommitTimestamp 获取提交时间戳
func (adapter *StorageTransactionAdapter) GetCommitTimestamp() uint64 {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.commitTs
}

// GetState 获取事务状态
func (adapter *StorageTransactionAdapter) GetState() common.TransactionState {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.state
}

// GetIsolationLevel 获取隔离级别
func (adapter *StorageTransactionAdapter) GetIsolationLevel() common.IsolationLevel {
	return adapter.isolation
}

// IsReadOnly 检查是否为只读事务
func (adapter *StorageTransactionAdapter) IsReadOnly() bool {
	return adapter.readOnly
}

// IsDistributed 检查是否为分布式事务
func (adapter *StorageTransactionAdapter) IsDistributed() bool {
	return false // 存储适配器目前不支持分布式
}

// Put 写入键值对
func (adapter *StorageTransactionAdapter) Put(key, value []byte) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	// 检查事务状态
	if adapter.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	if adapter.readOnly {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	// 检查超时
	if time.Since(adapter.startTime) > adapter.timeout {
		adapter.state = common.TxnAborted
		return fmt.Errorf("transaction timeout")
	}

	keyStr := string(key)

	// 先记录WAL日志
	if err := adapter.writeWALRecord(txnwal.LogDataInsert, key, value); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// 根据隔离级别决定处理方式
	switch adapter.isolation {
	case common.ReadUncommitted, common.ReadCommitted:
		// 直接写入存储引擎
		if err := adapter.engine.Put(key, value); err != nil {
			return err
		}
	case common.RepeatableRead, common.Serializable:
		// 写入缓冲区，提交时批量写入
		adapter.writeBuffer[keyStr] = value
		// 从删除集合中移除（如果存在）
		delete(adapter.deleteSet, keyStr)
	}

	adapter.writeCount++
	return nil
}

// Get 读取键值
func (adapter *StorageTransactionAdapter) Get(key []byte) ([]byte, error) {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	// 检查事务状态
	if adapter.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 根据隔离级别决定读取策略
	switch adapter.isolation {
	case common.RepeatableRead, common.Serializable:
		// 1. 先检查删除集合
		if adapter.deleteSet[keyStr] {
			return nil, fmt.Errorf("key deleted in transaction")
		}

		// 2. 检查写缓冲区
		if value, exists := adapter.writeBuffer[keyStr]; exists {
			adapter.readCount++
			return value, nil
		}

		// 3. 记录读取版本
		adapter.readSet[keyStr] = adapter.startTs
	}

	// 从存储引擎读取
	value, err := adapter.engine.Get(key)
	if err != nil {
		return nil, err
	}

	adapter.readCount++
	return value, nil
}

// Delete 删除键
func (adapter *StorageTransactionAdapter) Delete(key []byte) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	// 检查事务状态
	if adapter.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	if adapter.readOnly {
		return fmt.Errorf("cannot delete in read-only transaction")
	}

	keyStr := string(key)

	// 先记录WAL日志
	if err := adapter.writeWALRecord(txnwal.LogDataDelete, key, nil); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// 根据隔离级别决定处理方式
	switch adapter.isolation {
	case common.ReadUncommitted, common.ReadCommitted:
		// 直接从存储引擎删除
		if err := adapter.engine.Delete(key); err != nil {
			return err
		}
	case common.RepeatableRead, common.Serializable:
		// 添加到删除集合
		adapter.deleteSet[keyStr] = true
		// 从写缓冲区移除（如果存在）
		delete(adapter.writeBuffer, keyStr)
	}

	adapter.writeCount++
	return nil
}

// Exists 检查键是否存在
func (adapter *StorageTransactionAdapter) Exists(key []byte) (bool, error) {
	_, err := adapter.Get(key)
	if err != nil {
		if err.Error() == "key deleted in transaction" || err.Error() == "key not found" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// BatchPut 批量写入
func (adapter *StorageTransactionAdapter) BatchPut(batch []common.KVPair) error {
	for _, kv := range batch {
		if err := adapter.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

// BatchGet 批量读取
func (adapter *StorageTransactionAdapter) BatchGet(keys [][]byte) ([][]byte, error) {
	results := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := adapter.Get(key)
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	return results, nil
}

// BatchDelete 批量删除
func (adapter *StorageTransactionAdapter) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		if err := adapter.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// Commit 提交事务
func (adapter *StorageTransactionAdapter) Commit() error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	// 检查事务状态
	if adapter.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	// 设置提交时间戳
	adapter.commitTs = uint64(time.Now().UnixNano())

	// 对于需要缓冲的隔离级别，执行批量提交
	switch adapter.isolation {
	case common.RepeatableRead, common.Serializable:
		// 执行写操作
		if err := adapter.applyWrites(); err != nil {
			adapter.state = common.TxnAborted
			return fmt.Errorf("failed to apply writes: %w", err)
		}

		// 执行删除操作
		if err := adapter.applyDeletes(); err != nil {
			adapter.state = common.TxnAborted
			return fmt.Errorf("failed to apply deletes: %w", err)
		}
	}

	// 记录事务提交的WAL日志
	if err := adapter.writeTransactionWAL(txnwal.LogTransactionCommit); err != nil {
		adapter.state = common.TxnAborted
		return fmt.Errorf("failed to write transaction commit WAL: %w", err)
	}

	// 强制刷新WAL确保持久性
	ctx := context.Background()
	if err := adapter.txnWAL.Flush(ctx); err != nil {
		adapter.state = common.TxnAborted
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	adapter.state = common.TxnCommitted
	return nil
}

// Rollback 回滚事务
func (adapter *StorageTransactionAdapter) Rollback() error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	// 记录事务回滚的WAL日志
	if err := adapter.writeTransactionWAL(txnwal.LogTransactionRollback); err != nil {
		// 回滚失败也要继续清理，但记录错误
		fmt.Printf("Warning: failed to write transaction rollback WAL: %v\n", err)
	}

	// 清空缓冲区
	adapter.writeBuffer = make(map[string][]byte)
	adapter.deleteSet = make(map[string]bool)
	adapter.readSet = make(map[string]uint64)

	adapter.state = common.TxnAborted
	return nil
}

// Close 关闭事务
func (adapter *StorageTransactionAdapter) Close() error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	if adapter.closed {
		return nil
	}

	// 如果事务还在活跃状态，自动回滚
	if adapter.state == common.TxnActive {
		adapter.Rollback()
	}

	adapter.closed = true
	return nil
}

// applyWrites 应用写操作到存储引擎
func (adapter *StorageTransactionAdapter) applyWrites() error {
	batch := make([]storage.KVPair, 0, len(adapter.writeBuffer))
	for keyStr, value := range adapter.writeBuffer {
		batch = append(batch, storage.KVPair{
			Key:   []byte(keyStr),
			Value: value,
		})
	}

	if len(batch) > 0 {
		return adapter.engine.BatchPut(batch)
	}
	return nil
}

// applyDeletes 应用删除操作到存储引擎
func (adapter *StorageTransactionAdapter) applyDeletes() error {
	keys := make([][]byte, 0, len(adapter.deleteSet))
	for keyStr := range adapter.deleteSet {
		keys = append(keys, []byte(keyStr))
	}

	if len(keys) > 0 {
		return adapter.engine.BatchDelete(keys)
	}
	return nil
}

// GetStatistics 获取事务统计信息
func (adapter *StorageTransactionAdapter) GetStatistics() map[string]interface{} {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	return map[string]interface{}{
		"txn_id":            adapter.txnID,
		"start_ts":          adapter.startTs,
		"commit_ts":         adapter.commitTs,
		"state":             adapter.state.String(),
		"isolation":         adapter.isolation.String(),
		"read_only":         adapter.readOnly,
		"read_count":        adapter.readCount,
		"write_count":       adapter.writeCount,
		"duration":          time.Since(adapter.startTime).String(),
		"write_buffer_size": len(adapter.writeBuffer),
		"delete_set_size":   len(adapter.deleteSet),
		"read_set_size":     len(adapter.readSet),
		"engine_type":       fmt.Sprintf("%T", adapter.engine),
	}
}

// 实现Transaction接口的其他方法

// GetTxnID 获取事务ID（兼容Transaction接口）
func (adapter *StorageTransactionAdapter) GetTxnID() string {
	return adapter.GetID()
}

// GetStartTime 获取开始时间
func (adapter *StorageTransactionAdapter) GetStartTime() time.Time {
	return adapter.startTime
}

// Scan 范围扫描
func (adapter *StorageTransactionAdapter) Scan(startKey, endKey []byte, limit int) ([]common.KVPair, error) {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if adapter.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// 委托给存储引擎
	kvPairs, err := adapter.engine.Scan(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}

	// 转换类型
	result := make([]common.KVPair, len(kvPairs))
	for i, kv := range kvPairs {
		result[i] = common.KVPair{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}

	return result, nil
}

// NewIterator 创建迭代器
func (adapter *StorageTransactionAdapter) NewIterator(options *common.IteratorOptions) (common.Iterator, error) {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()

	if adapter.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// 转换迭代器选项类型
	storageOptions := &storage.IteratorOptions{
		StartKey: options.StartKey,
		EndKey:   options.EndKey,
		Reverse:  options.Reverse,
		KeyOnly:  options.KeyOnly,
	}

	// 委托给存储引擎
	storageIter, err := adapter.engine.NewIterator(storageOptions)
	if err != nil {
		return nil, err
	}

	// 包装为 common.Iterator
	return &IteratorAdapter{iter: storageIter}, nil
}

// AcquireLock 获取锁（委托给事务管理器）
func (adapter *StorageTransactionAdapter) AcquireLock(key []byte, lockType common.LockType) error {
	// 这里应该通过事务管理器的接口方法来获取锁
	// 由于 TransactionManager 是接口，我们暂时返回 nil
	// 在实际实现中，应该有专门的锁管理方法
	return nil
}

// ReleaseLock 释放锁（委托给事务管理器）
func (adapter *StorageTransactionAdapter) ReleaseLock(key []byte) error {
	// 这里应该通过事务管理器的接口方法来释放锁
	// 由于 TransactionManager 是接口，我们暂时返回 nil
	// 在实际实现中，应该有专门的锁管理方法
	return nil
}

// CreateSavepoint 创建保存点（暂不实现）
func (adapter *StorageTransactionAdapter) CreateSavepoint(name string) error {
	return fmt.Errorf("savepoints not supported in storage adapter")
}

// RollbackToSavepoint 回滚到保存点（暂不实现）
func (adapter *StorageTransactionAdapter) RollbackToSavepoint(name string) error {
	return fmt.Errorf("savepoints not supported in storage adapter")
}

// ReleaseSavepoint 释放保存点（暂不实现）
func (adapter *StorageTransactionAdapter) ReleaseSavepoint(name string) error {
	return fmt.Errorf("savepoints not supported in storage adapter")
}

// writeWALRecord 写入WAL日志记录
func (adapter *StorageTransactionAdapter) writeWALRecord(recordType txnwal.TxnLogRecordType, key, value []byte) error {
	// 构造WAL记录
	record := &txnwal.TxnLogRecord{
		Type:  recordType,
		TxnID: adapter.txnID,
		Data:  adapter.encodeKVData(key, value),
	}

	// 写入WAL
	ctx := context.Background()
	_, err := adapter.txnWAL.WriteRecord(ctx, record)
	return err
}

// encodeKVData 编码键值数据
func (adapter *StorageTransactionAdapter) encodeKVData(key, value []byte) []byte {
	// 简单的编码格式：[keyLen(4字节)][key][valueLen(4字节)][value]
	keyLen := len(key)
	valueLen := len(value)
	data := make([]byte, 4+keyLen+4+valueLen)

	// 编码key长度和key
	data[0] = byte(keyLen >> 24)
	data[1] = byte(keyLen >> 16)
	data[2] = byte(keyLen >> 8)
	data[3] = byte(keyLen)
	copy(data[4:], key)

	// 编码value长度和value
	offset := 4 + keyLen
	data[offset] = byte(valueLen >> 24)
	data[offset+1] = byte(valueLen >> 16)
	data[offset+2] = byte(valueLen >> 8)
	data[offset+3] = byte(valueLen)
	copy(data[offset+4:], value)

	return data
}

// writeTransactionWAL 写入事务相关的WAL记录
func (adapter *StorageTransactionAdapter) writeTransactionWAL(recordType txnwal.TxnLogRecordType) error {
	if adapter.txnWAL == nil {
		return fmt.Errorf("txnWAL is nil")
	}

	// 构造WAL记录
	record := &txnwal.TxnLogRecord{
		Type:  recordType,
		TxnID: adapter.txnID,
		Data:  []byte{}, // 事务记录不需要额外数据
	}

	// 写入WAL
	ctx := context.Background()
	_, err := adapter.txnWAL.WriteRecord(ctx, record)
	return err
}
