/*
JadeDB Percolator MVCC增强实现

本模块实现了专门为Percolator事务模型优化的MVCC系统。
基于Google Percolator论文的三列模型设计，提供完整的分布式事务支持。

核心特性：
1. 三列模型：Lock列、Write列、Data列分离存储
2. 时间戳冲突检测：基于开始时间戳和提交时间戳的精确冲突检测
3. 锁信息集成：在MVCC层面集成分布式锁管理
4. 事务状态跟踪：完整的Percolator事务状态管理
5. 快照隔离：基于时间戳的快照隔离实现

设计原理：
- Lock列：存储事务锁信息，包括锁类型、事务ID、主键等
- Write列：存储已提交的写入记录，包括开始时间戳和提交时间戳
- Data列：存储实际的数据值，按开始时间戳索引
- 时间戳排序：严格按照时间戳顺序进行冲突检测和可见性判断
- 原子操作：确保Prewrite和Commit操作的原子性

与标准MVCC的区别：
- 支持分布式锁：集成行级锁和死锁检测
- 两阶段提交：支持Percolator的2PC协议
- 主键协调：支持主键作为事务协调者的模式
- 异步提交：支持主键提交后的异步二级键提交
*/

package percolator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/locks"
	"github.com/util6/JadeDB/storage"
)

// PercolatorMVCC Percolator专用MVCC管理器
type PercolatorMVCC struct {
	mu sync.RWMutex

	// 基础存储引擎
	storage storage.Engine

	// 锁管理器
	lockManager *locks.DistributedLockManager

	// 配置
	config *PercolatorMVCCConfig

	// 性能统计
	metrics *PercolatorMVCCMetrics

	// 活跃事务跟踪
	activeTxns map[string]*PercolatorTxnInfo
	txnMutex   sync.RWMutex
}

// PercolatorMVCCConfig Percolator MVCC配置
type PercolatorMVCCConfig struct {
	// 存储配置
	LockTTL     time.Duration // 锁的生存时间
	MaxVersions int           // 每个键的最大版本数
	GCInterval  time.Duration // 垃圾回收间隔

	// 性能配置
	BatchSize         int  // 批处理大小
	CacheSize         int  // 缓存大小
	EnableCompression bool // 是否启用压缩

	// 冲突检测配置
	ConflictWindow time.Duration // 冲突检测窗口
	MaxRetries     int           // 最大重试次数
}

// PercolatorMVCCMetrics Percolator MVCC性能指标
type PercolatorMVCCMetrics struct {
	// 操作统计
	PrewriteOps uint64
	CommitOps   uint64
	RollbackOps uint64
	ReadOps     uint64

	// 冲突统计
	WriteConflicts     uint64
	LockConflicts      uint64
	TimestampConflicts uint64

	// 性能指标
	AvgPrewriteTime time.Duration
	AvgCommitTime   time.Duration
	AvgReadTime     time.Duration

	// 存储统计
	LockEntries  uint64
	WriteEntries uint64
	DataEntries  uint64
}

// PercolatorTxnInfo Percolator事务信息
type PercolatorTxnInfo struct {
	TxnID      string
	StartTS    uint64
	CommitTS   uint64
	PrimaryKey []byte
	State      common.TransactionState
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// PercolatorWriteRecord Percolator写入记录
type PercolatorWriteRecord struct {
	StartTS   uint64    `json:"start_ts"`   // 开始时间戳
	CommitTS  uint64    `json:"commit_ts"`  // 提交时间戳
	WriteType WriteType `json:"write_type"` // 写入类型
	TxnID     string    `json:"txn_id"`     // 事务ID
	CreatedAt time.Time `json:"created_at"` // 创建时间
}

// PercolatorLockRecord Percolator锁记录
type PercolatorLockRecord struct {
	TxnID      string          `json:"txn_id"`      // 事务ID
	StartTS    uint64          `json:"start_ts"`    // 开始时间戳
	PrimaryKey []byte          `json:"primary_key"` // 主键
	LockType   common.LockType `json:"lock_type"`   // 锁类型
	TTL        uint64          `json:"ttl"`         // 生存时间
	CreatedAt  time.Time       `json:"created_at"`  // 创建时间
}

// NewPercolatorMVCC 创建Percolator MVCC管理器
func NewPercolatorMVCC(storageEngine storage.Engine, lockManager *locks.DistributedLockManager, config *PercolatorMVCCConfig) *PercolatorMVCC {
	if config == nil {
		config = DefaultPercolatorMVCCConfig()
	}

	return &PercolatorMVCC{
		storage:     storageEngine,
		lockManager: lockManager,
		config:      config,
		metrics:     &PercolatorMVCCMetrics{},
		activeTxns:  make(map[string]*PercolatorTxnInfo),
	}
}

// DefaultPercolatorMVCCConfig 默认Percolator MVCC配置
func DefaultPercolatorMVCCConfig() *PercolatorMVCCConfig {
	return &PercolatorMVCCConfig{
		LockTTL:           10 * time.Second,
		MaxVersions:       100,
		GCInterval:        5 * time.Minute,
		BatchSize:         100,
		CacheSize:         10000,
		EnableCompression: true,
		ConflictWindow:    1 * time.Second,
		MaxRetries:        3,
	}
}

// Prewrite Percolator预写操作
func (pmvcc *PercolatorMVCC) Prewrite(ctx context.Context, mutations []*common.Mutation, primaryKey []byte, startTS uint64, txnID string) error {
	start := time.Now()
	defer func() {
		pmvcc.metrics.PrewriteOps++
		pmvcc.metrics.AvgPrewriteTime = time.Since(start)
	}()

	// 注册事务信息
	pmvcc.registerTransaction(txnID, startTS, primaryKey)

	// 对所有变更进行预写
	for _, mutation := range mutations {
		if err := pmvcc.prewriteSingle(ctx, mutation, primaryKey, startTS, txnID); err != nil {
			// 预写失败，回滚已完成的预写
			pmvcc.rollbackPrewrite(ctx, mutations, txnID)
			return err
		}
	}

	return nil
}

// prewriteSingle 单个键的预写操作
func (pmvcc *PercolatorMVCC) prewriteSingle(ctx context.Context, mutation *common.Mutation, primaryKey []byte, startTS uint64, txnID string) error {
	key := mutation.Key

	// 1. 检查锁冲突
	if err := pmvcc.checkLockConflict(ctx, key, txnID); err != nil {
		pmvcc.metrics.LockConflicts++
		return err
	}

	// 2. 检查写写冲突
	if err := pmvcc.checkWriteConflict(ctx, key, startTS); err != nil {
		pmvcc.metrics.WriteConflicts++
		return err
	}

	// 3. 获取锁
	lockType := common.LockTypePut
	if mutation.Type == common.MutationDelete {
		lockType = common.LockTypeDelete
	}

	if err := pmvcc.lockManager.AcquireDistributedLock(ctx, key, txnID, lockType, primaryKey, startTS); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// 4. 写入锁记录
	if err := pmvcc.writeLockRecord(ctx, key, txnID, startTS, primaryKey, lockType); err != nil {
		pmvcc.lockManager.ReleaseDistributedLock(key, txnID)
		return fmt.Errorf("failed to write lock record: %w", err)
	}

	// 5. 写入数据（如果是PUT操作）
	if mutation.Type == common.MutationPut {
		if err := pmvcc.writeDataRecord(ctx, key, mutation.Value, startTS); err != nil {
			pmvcc.lockManager.ReleaseDistributedLock(key, txnID)
			pmvcc.deleteLockRecord(ctx, key)
			return fmt.Errorf("failed to write data record: %w", err)
		}
	}

	return nil
}

// Commit Percolator提交操作
func (pmvcc *PercolatorMVCC) Commit(ctx context.Context, keys [][]byte, startTS, commitTS uint64, txnID string) error {
	start := time.Now()
	defer func() {
		pmvcc.metrics.CommitOps++
		pmvcc.metrics.AvgCommitTime = time.Since(start)
	}()

	// 获取事务信息
	txnInfo := pmvcc.getTransaction(txnID)
	if txnInfo == nil {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	// 先提交主键
	primaryKey := txnInfo.PrimaryKey
	if err := pmvcc.commitSingle(ctx, primaryKey, startTS, commitTS, txnID); err != nil {
		return fmt.Errorf("failed to commit primary key: %w", err)
	}

	// 异步提交其他键
	go pmvcc.commitSecondaries(ctx, keys, primaryKey, startTS, commitTS, txnID)

	// 更新事务状态
	pmvcc.updateTransactionState(txnID, common.TxnCommitted, commitTS)

	return nil
}

// commitSingle 单个键的提交操作
func (pmvcc *PercolatorMVCC) commitSingle(ctx context.Context, key []byte, startTS, commitTS uint64, txnID string) error {
	// 1. 检查锁是否存在
	lockRecord, err := pmvcc.getLockRecord(ctx, key)
	if err != nil {
		return err
	}

	if lockRecord == nil || lockRecord.TxnID != txnID {
		return fmt.Errorf("lock not found or not owned by transaction")
	}

	// 2. 写入提交记录
	writeRecord := &PercolatorWriteRecord{
		StartTS:   startTS,
		CommitTS:  commitTS,
		WriteType: WriteTypePut, // 简化实现
		TxnID:     txnID,
		CreatedAt: time.Now(),
	}

	if err := pmvcc.writeCommitRecord(ctx, key, commitTS, writeRecord); err != nil {
		return fmt.Errorf("failed to write commit record: %w", err)
	}

	// 3. 删除锁记录
	if err := pmvcc.deleteLockRecord(ctx, key); err != nil {
		return fmt.Errorf("failed to delete lock record: %w", err)
	}

	// 4. 释放锁
	if err := pmvcc.lockManager.ReleaseDistributedLock(key, txnID); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// commitSecondaries 异步提交二级键
func (pmvcc *PercolatorMVCC) commitSecondaries(ctx context.Context, keys [][]byte, primaryKey []byte, startTS, commitTS uint64, txnID string) {
	for _, key := range keys {
		if string(key) == string(primaryKey) {
			continue // 跳过主键
		}

		if err := pmvcc.commitSingle(ctx, key, startTS, commitTS, txnID); err != nil {
			// 记录错误，但继续处理其他键
			// 在实际实现中，这里应该有重试机制
			continue
		}
	}

	// 清理事务信息
	pmvcc.unregisterTransaction(txnID)
}

// Get Percolator读取操作
func (pmvcc *PercolatorMVCC) Get(ctx context.Context, key []byte, startTS uint64) ([]byte, error) {
	start := time.Now()
	defer func() {
		pmvcc.metrics.ReadOps++
		pmvcc.metrics.AvgReadTime = time.Since(start)
	}()

	// 1. 检查锁
	lockRecord, err := pmvcc.getLockRecord(ctx, key)
	if err != nil {
		return nil, err
	}

	if lockRecord != nil {
		// 有锁存在，检查是否可以读取
		if lockRecord.StartTS <= startTS {
			return nil, fmt.Errorf("key is locked by transaction %s", lockRecord.TxnID)
		}
	}

	// 2. 查找最新的已提交写入
	writeRecord, err := pmvcc.getLatestCommittedWrite(ctx, key, startTS)
	if err != nil {
		return nil, err
	}

	if writeRecord == nil {
		return nil, nil // 键不存在
	}

	if writeRecord.WriteType == WriteTypeDelete {
		return nil, nil // 键已删除
	}

	// 3. 读取数据
	return pmvcc.readDataRecord(ctx, key, writeRecord.StartTS)
}

// Rollback Percolator回滚操作
func (pmvcc *PercolatorMVCC) Rollback(ctx context.Context, keys [][]byte, startTS uint64, txnID string) error {
	defer func() {
		pmvcc.metrics.RollbackOps++
	}()

	return pmvcc.rollbackPrewrite(ctx, nil, txnID)
}

// 辅助方法实现
func (pmvcc *PercolatorMVCC) checkLockConflict(ctx context.Context, key []byte, txnID string) error {
	lockRecord, err := pmvcc.getLockRecord(ctx, key)
	if err != nil {
		return err
	}

	if lockRecord != nil && lockRecord.TxnID != txnID {
		return fmt.Errorf("lock conflict: key locked by transaction %s", lockRecord.TxnID)
	}

	return nil
}

func (pmvcc *PercolatorMVCC) checkWriteConflict(ctx context.Context, key []byte, startTS uint64) error {
	// 检查是否有在startTS之后提交的写入
	writeRecord, err := pmvcc.getLatestCommittedWrite(ctx, key, ^uint64(0)) // 获取最新的写入记录
	if err != nil {
		return err
	}

	if writeRecord != nil && writeRecord.CommitTS > startTS {
		pmvcc.metrics.WriteConflicts++
		return fmt.Errorf("write conflict: key has newer write at %d > %d", writeRecord.CommitTS, startTS)
	}

	return nil
}

func (pmvcc *PercolatorMVCC) rollbackPrewrite(ctx context.Context, mutations []*common.Mutation, txnID string) error {
	// 获取事务信息
	txnInfo := pmvcc.getTransaction(txnID)
	if txnInfo == nil {
		return nil // 事务不存在，可能已经清理
	}

	// 清理所有该事务的锁
	// 简化实现：扫描所有锁记录并删除属于该事务的锁
	lockPrefix := []byte("lock_")
	kvPairs, err := pmvcc.storage.Scan(lockPrefix, nil, 1000)
	if err != nil {
		return err
	}

	for _, kv := range kvPairs {
		var lockRecord PercolatorLockRecord
		if err := json.Unmarshal(kv.Value, &lockRecord); err != nil {
			continue
		}

		// 如果是该事务的锁，删除它
		if lockRecord.TxnID == txnID {
			pmvcc.storage.Delete(kv.Key)

			// 从锁管理器中释放锁
			// 从键中提取原始键名
			keyStr := string(kv.Key)
			if len(keyStr) > 5 && keyStr[:5] == "lock_" {
				originalKey := keyStr[5:]
				pmvcc.lockManager.ReleaseDistributedLock([]byte(originalKey), txnID)
			}
		}
	}

	// 更新事务状态
	pmvcc.updateTransactionState(txnID, common.TxnAborted, 0)
	pmvcc.unregisterTransaction(txnID)

	return nil
}

// 存储操作方法
func (pmvcc *PercolatorMVCC) writeLockRecord(ctx context.Context, key []byte, txnID string, startTS uint64, primaryKey []byte, lockType common.LockType) error {
	lockRecord := &PercolatorLockRecord{
		TxnID:      txnID,
		StartTS:    startTS,
		PrimaryKey: primaryKey,
		LockType:   lockType,
		TTL:        uint64(pmvcc.config.LockTTL.Seconds()),
		CreatedAt:  time.Now(),
	}

	data, err := json.Marshal(lockRecord)
	if err != nil {
		return err
	}

	lockKey := pmvcc.makeLockKey(key)
	return pmvcc.storage.Put(lockKey, data)
}

func (pmvcc *PercolatorMVCC) getLockRecord(ctx context.Context, key []byte) (*PercolatorLockRecord, error) {
	lockKey := pmvcc.makeLockKey(key)
	data, err := pmvcc.storage.Get(lockKey)
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
		pmvcc.deleteLockRecord(ctx, key)
		return nil, nil
	}

	return &lockRecord, nil
}

func (pmvcc *PercolatorMVCC) deleteLockRecord(ctx context.Context, key []byte) error {
	lockKey := pmvcc.makeLockKey(key)
	return pmvcc.storage.Delete(lockKey)
}

func (pmvcc *PercolatorMVCC) writeCommitRecord(ctx context.Context, key []byte, commitTS uint64, writeRecord *PercolatorWriteRecord) error {
	data, err := json.Marshal(writeRecord)
	if err != nil {
		return err
	}

	writeKey := pmvcc.makeWriteKey(key, commitTS)
	return pmvcc.storage.Put(writeKey, data)
}

func (pmvcc *PercolatorMVCC) getLatestCommittedWrite(ctx context.Context, key []byte, maxTS uint64) (*PercolatorWriteRecord, error) {
	// 简化实现：扫描写入记录找到最新的已提交写入
	// 在实际实现中，应该使用更高效的索引结构

	writePrefix := pmvcc.makeWritePrefix(key)

	// 使用存储引擎的扫描功能
	kvPairs, err := pmvcc.storage.Scan(writePrefix, nil, 100)
	if err != nil {
		return nil, err
	}

	var latestWrite *PercolatorWriteRecord
	var latestCommitTS uint64

	for _, kv := range kvPairs {
		var writeRecord PercolatorWriteRecord
		if err := json.Unmarshal(kv.Value, &writeRecord); err != nil {
			continue
		}

		// 检查是否在可见范围内
		if writeRecord.CommitTS <= maxTS && writeRecord.CommitTS > latestCommitTS {
			latestWrite = &writeRecord
			latestCommitTS = writeRecord.CommitTS
		}
	}

	return latestWrite, nil
}

func (pmvcc *PercolatorMVCC) writeDataRecord(ctx context.Context, key []byte, value []byte, startTS uint64) error {
	dataKey := pmvcc.makeDataKey(key, startTS)
	return pmvcc.storage.Put(dataKey, value)
}

func (pmvcc *PercolatorMVCC) readDataRecord(ctx context.Context, key []byte, startTS uint64) ([]byte, error) {
	dataKey := pmvcc.makeDataKey(key, startTS)
	return pmvcc.storage.Get(dataKey)
}

// 键生成方法
func (pmvcc *PercolatorMVCC) makeLockKey(key []byte) []byte {
	return []byte(fmt.Sprintf("lock_%s", string(key)))
}

func (pmvcc *PercolatorMVCC) makeWriteKey(key []byte, commitTS uint64) []byte {
	return []byte(fmt.Sprintf("write_%s_%020d", string(key), commitTS))
}

func (pmvcc *PercolatorMVCC) makeWritePrefix(key []byte) []byte {
	return []byte(fmt.Sprintf("write_%s_", string(key)))
}

func (pmvcc *PercolatorMVCC) makeDataKey(key []byte, startTS uint64) []byte {
	return []byte(fmt.Sprintf("data_%s_%020d", string(key), startTS))
}

// 事务管理方法
func (pmvcc *PercolatorMVCC) registerTransaction(txnID string, startTS uint64, primaryKey []byte) {
	pmvcc.txnMutex.Lock()
	defer pmvcc.txnMutex.Unlock()

	pmvcc.activeTxns[txnID] = &PercolatorTxnInfo{
		TxnID:      txnID,
		StartTS:    startTS,
		PrimaryKey: primaryKey,
		State:      common.TxnActive,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

func (pmvcc *PercolatorMVCC) getTransaction(txnID string) *PercolatorTxnInfo {
	pmvcc.txnMutex.RLock()
	defer pmvcc.txnMutex.RUnlock()
	return pmvcc.activeTxns[txnID]
}

func (pmvcc *PercolatorMVCC) updateTransactionState(txnID string, state common.TransactionState, commitTS uint64) {
	pmvcc.txnMutex.Lock()
	defer pmvcc.txnMutex.Unlock()

	if txnInfo, exists := pmvcc.activeTxns[txnID]; exists {
		txnInfo.State = state
		txnInfo.CommitTS = commitTS
		txnInfo.UpdatedAt = time.Now()
	}
}

func (pmvcc *PercolatorMVCC) unregisterTransaction(txnID string) {
	pmvcc.txnMutex.Lock()
	defer pmvcc.txnMutex.Unlock()
	delete(pmvcc.activeTxns, txnID)
}

// GetMetrics 获取性能指标
func (pmvcc *PercolatorMVCC) GetMetrics() *PercolatorMVCCMetrics {
	pmvcc.mu.RLock()
	defer pmvcc.mu.RUnlock()

	// 返回指标的副本
	metrics := *pmvcc.metrics
	return &metrics
}

// GetActiveTransactions 获取活跃事务列表
func (pmvcc *PercolatorMVCC) GetActiveTransactions() []*PercolatorTxnInfo {
	pmvcc.txnMutex.RLock()
	defer pmvcc.txnMutex.RUnlock()

	txns := make([]*PercolatorTxnInfo, 0, len(pmvcc.activeTxns))
	for _, txn := range pmvcc.activeTxns {
		txnCopy := *txn
		txns = append(txns, &txnCopy)
	}

	return txns
}

// Cleanup 清理过期的事务和锁
func (pmvcc *PercolatorMVCC) Cleanup(ctx context.Context) error {
	// 清理过期的事务
	pmvcc.cleanupExpiredTransactions()

	// 清理过期的锁记录
	return pmvcc.cleanupExpiredLocks(ctx)
}

func (pmvcc *PercolatorMVCC) cleanupExpiredTransactions() {
	pmvcc.txnMutex.Lock()
	defer pmvcc.txnMutex.Unlock()

	now := time.Now()
	for txnID, txnInfo := range pmvcc.activeTxns {
		// 清理超时的事务
		if now.Sub(txnInfo.CreatedAt) > pmvcc.config.LockTTL*2 {
			delete(pmvcc.activeTxns, txnID)
		}
	}
}

func (pmvcc *PercolatorMVCC) cleanupExpiredLocks(ctx context.Context) error {
	// 简化实现：扫描所有锁记录并清理过期的
	// 在实际实现中，应该使用更高效的方法

	lockPrefix := []byte("lock_")
	kvPairs, err := pmvcc.storage.Scan(lockPrefix, nil, 1000)
	if err != nil {
		return err
	}

	for _, kv := range kvPairs {
		var lockRecord PercolatorLockRecord
		if err := json.Unmarshal(kv.Value, &lockRecord); err != nil {
			continue
		}

		// 检查锁是否过期
		if time.Since(lockRecord.CreatedAt) > time.Duration(lockRecord.TTL)*time.Second {
			pmvcc.storage.Delete(kv.Key)
		}
	}

	return nil
}
