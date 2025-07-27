/*
JadeDB 本地事务实现

本模块实现了单机环境下的事务，基于MVCC和锁管理提供完整的ACID保证。
支持多种隔离级别，为SQL层提供标准的事务接口。

核心功能：
1. ACID保证：原子性、一致性、隔离性、持久性
2. 多隔离级别：支持读未提交到串行化的各种隔离级别
3. MVCC支持：基于时间戳的多版本并发控制
4. 锁管理：细粒度锁控制，支持锁升级和死锁检测
5. 保存点：支持嵌套事务和部分回滚

设计特点：
- 高性能：基于MVCC减少锁竞争
- 灵活性：支持多种隔离级别和事务模式
- 可扩展：为分布式事务提供基础接口
- SQL就绪：完整支持SQL事务语义
*/

package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LocalTransaction 本地事务实现
type LocalTransaction struct {
	// 基本信息
	txnID     string
	startTime time.Time
	startTs   uint64
	commitTs  uint64

	// 配置
	isolation IsolationLevel
	timeout   time.Duration
	readOnly  bool

	// 状态
	mu    sync.RWMutex
	state TransactionState

	// 管理器引用
	manager     *TransactionManager
	mvccManager *MVCCManager
	lockManager *LockManager

	// 读写集合
	readSet   map[string]uint64 // key -> read timestamp
	writeSet  map[string][]byte // key -> value
	deleteSet map[string]bool   // key -> deleted

	// 保存点
	savepoints map[string]*Savepoint

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc

	// 监控
	startTimeNano int64
}

// Savepoint 保存点
type Savepoint struct {
	Name      string
	ReadSet   map[string]uint64
	WriteSet  map[string][]byte
	DeleteSet map[string]bool
	CreatedAt time.Time
}

// NewLocalTransaction 创建本地事务
func NewLocalTransaction(txnID string, startTs uint64, options *TransactionOptions, manager *TransactionManager) (Transaction, error) {
	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)

	txn := &LocalTransaction{
		txnID:         txnID,
		startTime:     time.Now(),
		startTs:       startTs,
		isolation:     options.Isolation,
		timeout:       options.Timeout,
		readOnly:      options.ReadOnly,
		state:         TxnActive,
		manager:       manager,
		mvccManager:   manager.mvccManager,
		lockManager:   manager.lockManager,
		readSet:       make(map[string]uint64),
		writeSet:      make(map[string][]byte),
		deleteSet:     make(map[string]bool),
		savepoints:    make(map[string]*Savepoint),
		ctx:           ctx,
		cancel:        cancel,
		startTimeNano: time.Now().UnixNano(),
	}

	// 在MVCC管理器中注册事务
	manager.mvccManager.RegisterTransaction(txnID, startTs)

	return txn, nil
}

// Put 写入数据
func (txn *LocalTransaction) Put(key, value []byte) error {
	if txn.readOnly {
		return fmt.Errorf("cannot write in read-only transaction")
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 根据隔离级别决定是否需要加锁
	if txn.needsLocking() {
		if err := txn.lockManager.AcquireLock(txn.txnID, key, ExclusiveLock); err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
	}

	// 添加到写集合
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	txn.writeSet[keyStr] = valueCopy

	// 从删除集合中移除（如果存在）
	delete(txn.deleteSet, keyStr)

	return nil
}

// Get 读取数据
func (txn *LocalTransaction) Get(key []byte) ([]byte, error) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if txn.state != TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 首先检查本事务的写集合
	if value, exists := txn.writeSet[keyStr]; exists {
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}

	// 检查本事务的删除集合
	if txn.deleteSet[keyStr] {
		return nil, fmt.Errorf("key deleted in current transaction")
	}

	// 根据隔离级别决定是否需要加锁
	if txn.needsReadLocking() {
		if err := txn.lockManager.AcquireLock(txn.txnID, key, SharedLock); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
	}

	// 从MVCC管理器读取
	readTs := txn.getReadTimestamp()
	value, err := txn.mvccManager.Get(key, txn.txnID, readTs)
	if err != nil {
		return nil, err
	}

	// 添加到读集合
	txn.readSet[keyStr] = readTs

	return value, nil
}

// Delete 删除数据
func (txn *LocalTransaction) Delete(key []byte) error {
	if txn.readOnly {
		return fmt.Errorf("cannot delete in read-only transaction")
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 根据隔离级别决定是否需要加锁
	if txn.needsLocking() {
		if err := txn.lockManager.AcquireLock(txn.txnID, key, ExclusiveLock); err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
	}

	// 添加到删除集合
	txn.deleteSet[keyStr] = true

	// 从写集合中移除（如果存在）
	delete(txn.writeSet, keyStr)

	return nil
}

// Exists 检查键是否存在
func (txn *LocalTransaction) Exists(key []byte) (bool, error) {
	_, err := txn.Get(key)
	if err != nil {
		if err.Error() == "key not found" || err.Error() == "key deleted" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// BatchPut 批量写入
func (txn *LocalTransaction) BatchPut(batch []KVPair) error {
	for _, kv := range batch {
		if err := txn.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

// BatchGet 批量读取
func (txn *LocalTransaction) BatchGet(keys [][]byte) ([][]byte, error) {
	results := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := txn.Get(key)
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	return results, nil
}

// BatchDelete 批量删除
func (txn *LocalTransaction) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// Scan 范围扫描
func (txn *LocalTransaction) Scan(startKey, endKey []byte, limit int) ([]KVPair, error) {
	// TODO: 实现范围扫描
	return nil, fmt.Errorf("scan not implemented")
}

// NewIterator 创建迭代器
func (txn *LocalTransaction) NewIterator(options *IteratorOptions) (Iterator, error) {
	// TODO: 实现迭代器
	return nil, fmt.Errorf("iterator not implemented")
}

// Commit 提交事务
func (txn *LocalTransaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	// 更新状态
	txn.state = TxnCommitting

	// 获取提交时间戳
	commitTs, err := txn.manager.timestampOracle.GetTimestamp()
	if err != nil {
		txn.state = TxnAborted
		return fmt.Errorf("failed to get commit timestamp: %w", err)
	}
	txn.commitTs = commitTs

	// 验证读集合（可重复读和串行化隔离级别）
	if txn.isolation >= RepeatableRead {
		if err := txn.validateReadSet(); err != nil {
			txn.state = TxnAborted
			return fmt.Errorf("read validation failed: %w", err)
		}
	}

	// 应用写操作到MVCC管理器
	for keyStr, value := range txn.writeSet {
		key := []byte(keyStr)
		if err := txn.mvccManager.Put(key, value, txn.txnID, commitTs); err != nil {
			txn.state = TxnAborted
			return fmt.Errorf("failed to apply write: %w", err)
		}
	}

	// 应用删除操作到MVCC管理器
	for keyStr := range txn.deleteSet {
		key := []byte(keyStr)
		if err := txn.mvccManager.Delete(key, txn.txnID, commitTs); err != nil {
			txn.state = TxnAborted
			return fmt.Errorf("failed to apply delete: %w", err)
		}
	}

	// 在MVCC管理器中提交事务
	txn.mvccManager.CommitTransaction(txn.txnID, commitTs)

	// 释放所有锁
	txn.lockManager.ReleaseAllLocks(txn.txnID)

	// 更新状态
	txn.state = TxnCommitted

	// 从事务管理器中注销
	txn.manager.unregisterTransaction(txn.txnID)

	// 更新指标
	txn.manager.metrics.CommittedTransactions.Add(1)

	return nil
}

// Rollback 回滚事务
func (txn *LocalTransaction) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state == TxnCommitted {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	if txn.state == TxnAborted {
		return nil // 已经回滚
	}

	// 更新状态
	txn.state = TxnAborting

	// 在MVCC管理器中中止事务
	txn.mvccManager.AbortTransaction(txn.txnID)

	// 释放所有锁
	txn.lockManager.ReleaseAllLocks(txn.txnID)

	// 清理本地状态
	txn.readSet = make(map[string]uint64)
	txn.writeSet = make(map[string][]byte)
	txn.deleteSet = make(map[string]bool)
	txn.savepoints = make(map[string]*Savepoint)

	// 更新状态
	txn.state = TxnAborted

	// 从事务管理器中注销
	txn.manager.unregisterTransaction(txn.txnID)

	// 更新指标
	txn.manager.metrics.AbortedTransactions.Add(1)

	return nil
}

// 获取事务信息的方法
func (txn *LocalTransaction) GetTxnID() string {
	return txn.txnID
}

func (txn *LocalTransaction) GetStartTime() time.Time {
	return txn.startTime
}

func (txn *LocalTransaction) GetIsolationLevel() IsolationLevel {
	return txn.isolation
}

func (txn *LocalTransaction) IsReadOnly() bool {
	return txn.readOnly
}

func (txn *LocalTransaction) IsDistributed() bool {
	return false
}

// 锁操作（为SQL层预留）
func (txn *LocalTransaction) AcquireLock(key []byte, lockType LockType) error {
	return txn.lockManager.AcquireLock(txn.txnID, key, lockType)
}

func (txn *LocalTransaction) ReleaseLock(key []byte) error {
	return txn.lockManager.ReleaseLock(txn.txnID, key)
}

// 保存点操作（为SQL层预留）
func (txn *LocalTransaction) CreateSavepoint(name string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	// 创建保存点
	savepoint := &Savepoint{
		Name:      name,
		ReadSet:   make(map[string]uint64),
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		CreatedAt: time.Now(),
	}

	// 复制当前状态
	for k, v := range txn.readSet {
		savepoint.ReadSet[k] = v
	}
	for k, v := range txn.writeSet {
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		savepoint.WriteSet[k] = valueCopy
	}
	for k, v := range txn.deleteSet {
		savepoint.DeleteSet[k] = v
	}

	txn.savepoints[name] = savepoint
	return nil
}

func (txn *LocalTransaction) RollbackToSavepoint(name string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	savepoint, exists := txn.savepoints[name]
	if !exists {
		return fmt.Errorf("savepoint %s not found", name)
	}

	// 恢复到保存点状态
	txn.readSet = make(map[string]uint64)
	txn.writeSet = make(map[string][]byte)
	txn.deleteSet = make(map[string]bool)

	for k, v := range savepoint.ReadSet {
		txn.readSet[k] = v
	}
	for k, v := range savepoint.WriteSet {
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		txn.writeSet[k] = valueCopy
	}
	for k, v := range savepoint.DeleteSet {
		txn.deleteSet[k] = v
	}

	return nil
}

func (txn *LocalTransaction) ReleaseSavepoint(name string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	delete(txn.savepoints, name)
	return nil
}

// Close 关闭事务
func (txn *LocalTransaction) Close() error {
	if txn.state == TxnActive {
		return txn.Rollback()
	}

	if txn.cancel != nil {
		txn.cancel()
	}

	return nil
}

// 辅助方法
func (txn *LocalTransaction) needsLocking() bool {
	// 串行化隔离级别总是需要锁
	return txn.isolation == Serializable
}

func (txn *LocalTransaction) needsReadLocking() bool {
	// 串行化隔离级别需要读锁
	return txn.isolation == Serializable
}

func (txn *LocalTransaction) getReadTimestamp() uint64 {
	switch txn.isolation {
	case ReadUncommitted:
		// 读未提交：使用最新时间戳
		ts, _ := txn.manager.timestampOracle.GetTimestamp()
		return ts
	case ReadCommitted:
		// 读已提交：每次读取使用新的时间戳
		ts, _ := txn.manager.timestampOracle.GetTimestamp()
		return ts
	case RepeatableRead, SnapshotIsolation:
		// 可重复读/快照隔离：使用事务开始时间戳
		return txn.startTs
	case Serializable:
		// 串行化：使用事务开始时间戳
		return txn.startTs
	default:
		return txn.startTs
	}
}

func (txn *LocalTransaction) validateReadSet() error {
	// 验证读集合中的数据是否仍然有效
	for keyStr, readTs := range txn.readSet {
		key := []byte(keyStr)

		// 检查是否有更新的版本
		currentValue, err := txn.mvccManager.Get(key, txn.txnID, readTs)
		if err != nil {
			// 如果读取失败，可能是数据被删除或修改
			return fmt.Errorf("read validation failed for key %s: %w", keyStr, err)
		}

		// 重新读取最新版本进行比较
		latestTs, _ := txn.manager.timestampOracle.GetTimestamp()
		latestValue, err := txn.mvccManager.Get(key, txn.txnID, latestTs)
		if err != nil {
			// 最新版本读取失败，可能是数据被删除
			return fmt.Errorf("read validation failed for key %s: data may have been deleted", keyStr)
		}

		// 比较值是否相同
		if string(currentValue) != string(latestValue) {
			return fmt.Errorf("read validation failed for key %s: data has been modified", keyStr)
		}
	}

	return nil
}
