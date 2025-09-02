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

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
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
	state common.TransactionState

	// 管理器引用
	manager     *TransactionManager
	mvccManager common.MVCCManager
	lockManager common.LockManager

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

// TransactionIterator 事务迭代器
// 支持MVCC的事务级别迭代器，能够正确处理事务的读写集合
type TransactionIterator struct {
	// 事务引用
	txn *LocalTransaction

	// 迭代器配置
	options *IteratorOptions
	readTs  uint64

	// 当前状态
	data      []KVPair // 预加载的数据
	index     int      // 当前索引
	currentKV KVPair   // 当前键值对

	// 状态标志
	valid  bool // 迭代器是否有效
	closed bool // 迭代器是否已关闭
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
		state:         common.TxnActive,
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

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 根据隔离级别决定是否需要加锁
	if txn.needsLocking() {
		if err := txn.lockManager.AcquireLock(txn.txnID, key, common.ExclusiveLock, txn.timeout); err != nil {
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

	if txn.state != common.TxnActive {
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
		if err := txn.lockManager.AcquireLock(txn.txnID, key, common.SharedLock, txn.timeout); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
	}

	// 从MVCC管理器读取
	readTs := txn.getReadTimestamp()
	value, err := txn.mvccManager.GetVersion(key, readTs)
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

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// 根据隔离级别决定是否需要加锁
	if txn.needsLocking() {
		if err := txn.lockManager.AcquireLock(txn.txnID, key, common.ExclusiveLock, txn.timeout); err != nil {
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
// 实现事务级别的范围扫描，支持MVCC和隔离级别
func (txn *LocalTransaction) Scan(startKey, endKey []byte, limit int) ([]KVPair, error) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if txn.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// 参数验证
	if limit <= 0 {
		limit = 1000 // 默认限制，避免返回过多数据
	}

	// 如果startKey > endKey，返回空结果
	if startKey != nil && endKey != nil && string(startKey) > string(endKey) {
		return []KVPair{}, nil
	}

	// 获取读时间戳
	readTs := txn.getReadTimestamp()

	// 收集所有符合条件的键值对
	var results []KVPair

	// 首先从本事务的写集合中收集数据
	for keyStr, value := range txn.writeSet {
		key := []byte(keyStr)

		// 检查键是否在范围内
		if txn.isKeyInRange(key, startKey, endKey) {
			// 检查是否被本事务删除
			if !txn.deleteSet[keyStr] {
				results = append(results, KVPair{
					Key:   key,
					Value: value,
				})
			}
		}
	}

	// 然后从MVCC管理器中扫描数据
	mvccResults, err := txn.scanFromMVCC(startKey, endKey, readTs, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to scan from MVCC: %w", err)
	}

	// 合并结果，本事务的写集合优先
	resultMap := make(map[string]KVPair)

	// 先添加本事务的写集合
	for _, kv := range results {
		resultMap[string(kv.Key)] = kv
	}

	// 再添加MVCC的结果，但不覆盖本事务的写集合
	for _, kv := range mvccResults {
		keyStr := string(kv.Key)
		if _, exists := resultMap[keyStr]; !exists && !txn.deleteSet[keyStr] {
			resultMap[keyStr] = kv
		}
	}

	// 转换为切片并排序
	finalResults := make([]KVPair, 0, len(resultMap))
	for _, kv := range resultMap {
		finalResults = append(finalResults, kv)
	}

	// 按键排序
	txn.sortKVPairs(finalResults)

	// 应用限制
	if len(finalResults) > limit {
		finalResults = finalResults[:limit]
	}

	// 添加到读集合
	for _, kv := range finalResults {
		txn.readSet[string(kv.Key)] = readTs
	}

	return finalResults, nil
}

// NewIterator 创建迭代器
// 创建支持MVCC的事务迭代器
func (txn *LocalTransaction) NewIterator(options *IteratorOptions) (Iterator, error) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if txn.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	if options == nil {
		options = &IteratorOptions{}
	}

	// 获取读时间戳
	readTs := txn.getReadTimestamp()

	// 创建事务迭代器
	iter := &TransactionIterator{
		txn:       txn,
		options:   options,
		readTs:    readTs,
		valid:     false,
		closed:    false,
		currentKV: KVPair{},
	}

	// 初始化迭代器数据
	if err := iter.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize iterator: %w", err)
	}

	return iter, nil
}

// Commit 提交事务
func (txn *LocalTransaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active")
	}

	// 更新状态
	txn.state = common.TxnCommitting

	// 获取提交时间戳
	commitTs, err := txn.manager.timestampOracle.GetTimestamp()
	if err != nil {
		txn.state = common.TxnAborted
		return fmt.Errorf("failed to get commit timestamp: %w", err)
	}
	txn.commitTs = commitTs

	// 验证读集合（可重复读和串行化隔离级别）
	if txn.isolation >= RepeatableRead {
		if err := txn.validateReadSet(); err != nil {
			txn.state = common.TxnAborted
			return fmt.Errorf("read validation failed: %w", err)
		}
	}

	// 应用写操作到MVCC管理器
	for keyStr, value := range txn.writeSet {
		key := []byte(keyStr)
		if err := txn.mvccManager.PutVersion(key, value, commitTs); err != nil {
			txn.state = common.TxnAborted
			return fmt.Errorf("failed to apply write: %w", err)
		}
	}

	// 应用删除操作到MVCC管理器
	for keyStr := range txn.deleteSet {
		key := []byte(keyStr)
		if err := txn.mvccManager.DeleteVersion(key, commitTs); err != nil {
			txn.state = common.TxnAborted
			return fmt.Errorf("failed to apply delete: %w", err)
		}
	}

	// 在MVCC管理器中注销事务
	txn.mvccManager.UnregisterTransaction(txn.txnID)

	// 释放所有锁
	txn.lockManager.ReleaseAllLocks(txn.txnID)

	// 更新状态
	txn.state = common.TxnCommitted

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

	if txn.state == common.TxnCommitted {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	if txn.state == common.TxnAborted {
		return nil // 已经回滚
	}

	// 更新状态
	txn.state = common.TxnAborting

	// 在MVCC管理器中注销事务
	txn.mvccManager.UnregisterTransaction(txn.txnID)

	// 释放所有锁
	txn.lockManager.ReleaseAllLocks(txn.txnID)

	// 清理本地状态
	txn.readSet = make(map[string]uint64)
	txn.writeSet = make(map[string][]byte)
	txn.deleteSet = make(map[string]bool)
	txn.savepoints = make(map[string]*Savepoint)

	// 更新状态
	txn.state = common.TxnAborted

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
func (txn *LocalTransaction) AcquireLock(key []byte, lockType common.LockType) error {
	return txn.lockManager.AcquireLock(txn.txnID, key, lockType, txn.timeout)
}

func (txn *LocalTransaction) ReleaseLock(key []byte) error {
	return txn.lockManager.ReleaseLock(txn.txnID, key)
}

// 保存点操作（为SQL层预留）
func (txn *LocalTransaction) CreateSavepoint(name string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
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

	if txn.state != common.TxnActive {
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
	if txn.state == common.TxnActive {
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
		currentValue, err := txn.mvccManager.GetVersion(key, readTs)
		if err != nil {
			// 如果读取失败，可能是数据被删除或修改
			return fmt.Errorf("read validation failed for key %s: %w", keyStr, err)
		}

		// 重新读取最新版本进行比较
		latestTs, _ := txn.manager.timestampOracle.GetTimestamp()
		latestValue, err := txn.mvccManager.GetVersion(key, latestTs)
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

// isKeyInRange 检查键是否在指定范围内
func (txn *LocalTransaction) isKeyInRange(key, startKey, endKey []byte) bool {
	// 如果没有指定范围，则所有键都在范围内
	if startKey == nil && endKey == nil {
		return true
	}

	// 检查起始键
	if startKey != nil && string(key) < string(startKey) {
		return false
	}

	// 检查结束键
	if endKey != nil && string(key) > string(endKey) {
		return false
	}

	return true
}

// scanFromMVCC 从MVCC管理器中扫描数据
func (txn *LocalTransaction) scanFromMVCC(startKey, endKey []byte, readTs uint64, limit int) ([]KVPair, error) {
	// 由于MVCC管理器目前没有直接的Scan方法，我们需要通过其他方式实现
	// 这里先返回空结果，等待MVCC管理器实现Scan方法
	// TODO: 等待MVCC管理器实现Scan方法后，调用manager.Scan(startKey, endKey, txn.txnID, readTs, limit)
	return []KVPair{}, nil
}

// sortKVPairs 对键值对切片按键排序
func (txn *LocalTransaction) sortKVPairs(kvs []KVPair) {
	// 使用简单的冒泡排序，对于小数据集足够了
	n := len(kvs)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if string(kvs[j].Key) > string(kvs[j+1].Key) {
				kvs[j], kvs[j+1] = kvs[j+1], kvs[j]
			}
		}
	}
}

// TransactionIterator 方法实现

// initialize 初始化迭代器数据
func (iter *TransactionIterator) initialize() error {
	// 使用Scan方法获取所有数据
	data, err := iter.txn.Scan(iter.options.StartKey, iter.options.EndKey, 0) // 0表示无限制
	if err != nil {
		return err
	}

	// 根据选项过滤数据
	if iter.options.Prefix != nil {
		filtered := make([]KVPair, 0)
		for _, kv := range data {
			if iter.hasPrefix(kv.Key, iter.options.Prefix) {
				filtered = append(filtered, kv)
			}
		}
		data = filtered
	}

	// 如果是反向迭代，反转数据
	if iter.options.Reverse {
		iter.reverseKVPairs(data)
	}

	iter.data = data
	iter.index = -1 // 初始位置在第一个元素之前
	iter.valid = false

	// 移动到第一个有效位置
	iter.Next()

	return nil
}

// Valid 检查迭代器是否有效
func (iter *TransactionIterator) Valid() bool {
	return iter.valid && !iter.closed
}

// Next 移动到下一个元素
func (iter *TransactionIterator) Next() {
	if iter.closed {
		iter.valid = false
		return
	}

	iter.index++
	if iter.index >= len(iter.data) {
		iter.valid = false
		return
	}

	iter.currentKV = iter.data[iter.index]
	iter.valid = true
}

// Prev 移动到上一个元素
func (iter *TransactionIterator) Prev() {
	if iter.closed {
		iter.valid = false
		return
	}

	iter.index--
	if iter.index < 0 {
		iter.valid = false
		return
	}

	iter.currentKV = iter.data[iter.index]
	iter.valid = true
}

// Seek 定位到指定键
func (iter *TransactionIterator) Seek(key []byte) {
	if iter.closed {
		iter.valid = false
		return
	}

	keyStr := string(key)

	// 在数据中查找第一个大于等于key的位置
	for i, kv := range iter.data {
		if string(kv.Key) >= keyStr {
			iter.index = i
			iter.currentKV = kv
			iter.valid = true
			return
		}
	}

	// 没找到，设置为无效
	iter.valid = false
}

// SeekToFirst 定位到第一个元素
func (iter *TransactionIterator) SeekToFirst() {
	if iter.closed || len(iter.data) == 0 {
		iter.valid = false
		return
	}

	iter.index = 0
	iter.currentKV = iter.data[0]
	iter.valid = true
}

// SeekToLast 定位到最后一个元素
func (iter *TransactionIterator) SeekToLast() {
	if iter.closed || len(iter.data) == 0 {
		iter.valid = false
		return
	}

	iter.index = len(iter.data) - 1
	iter.currentKV = iter.data[iter.index]
	iter.valid = true
}

// Key 获取当前键
func (iter *TransactionIterator) Key() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.currentKV.Key
}

// Value 获取当前值
func (iter *TransactionIterator) Value() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.currentKV.Value
}

// Close 关闭迭代器
func (iter *TransactionIterator) Close() error {
	iter.closed = true
	iter.valid = false
	iter.data = nil
	return nil
}

// hasPrefix 检查键是否有指定前缀
func (iter *TransactionIterator) hasPrefix(key, prefix []byte) bool {
	if len(prefix) > len(key) {
		return false
	}

	for i, b := range prefix {
		if key[i] != b {
			return false
		}
	}

	return true
}

// reverseKVPairs 反转键值对切片
func (iter *TransactionIterator) reverseKVPairs(kvs []KVPair) {
	n := len(kvs)
	for i := 0; i < n/2; i++ {
		kvs[i], kvs[n-1-i] = kvs[n-1-i], kvs[i]
	}
}
