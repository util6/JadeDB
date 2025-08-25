/*
JadeDB Percolator 分布式事务模型

本模块实现了Google Percolator分布式事务模型，这是TiDB的核心事务机制。
Percolator基于两阶段提交协议，提供跨分片的ACID事务保证。

核心特性：
1. 两阶段提交：Prewrite + Commit两阶段确保事务原子性
2. 乐观并发控制：基于时间戳的冲突检测
3. 分布式锁：行级锁定，支持死锁检测
4. 故障恢复：事务状态持久化，支持崩溃恢复
5. 高性能：批量操作，异步提交优化

设计原理：
- Primary Key：每个事务选择一个主键作为事务协调者
- Lock Column：存储锁信息和事务状态
- Write Column：存储已提交的写入记录
- Data Column：存储实际数据
- 时间戳排序：使用TSO保证全局事务顺序
*/

package percolator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/coordination"
	"github.com/util6/JadeDB/distributed/locks"
	"github.com/util6/JadeDB/storage"
)

// PercolatorTransaction Percolator事务接口
type PercolatorTransaction interface {
	// Get 读取数据
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Put 写入数据
	Put(ctx context.Context, key, value []byte) error

	// Delete 删除数据
	Delete(ctx context.Context, key []byte) error

	// Commit 提交事务
	Commit(ctx context.Context) error

	// Rollback 回滚事务
	Rollback(ctx context.Context) error

	// GetStartTS 获取事务开始时间戳
	GetStartTS() uint64

	// GetCommitTS 获取事务提交时间戳
	GetCommitTS() uint64

	// IsReadOnly 检查是否为只读事务
	IsReadOnly() bool
}

// PercolatorEngine Percolator事务引擎
type PercolatorEngine struct {
	mu sync.RWMutex

	// 基础组件
	tso         coordination.TimestampOracle  // 时间戳服务器
	storage     storage.Engine                // 底层存储引擎
	lockManager *locks.DistributedLockManager // 锁管理器
	mvcc        *PercolatorMVCC               // Percolator MVCC管理器
	logger      *log.Logger

	// 配置参数
	config *PercolatorConfig

	// 事务管理
	activeTxns map[uint64]*PercolatorTxn // 活跃事务

	// 性能统计
	metrics *PercolatorMetrics
}

// PercolatorTxn Percolator事务实现
type PercolatorTxn struct {
	mu sync.RWMutex

	// 事务标识
	startTS  uint64 // 开始时间戳
	commitTS uint64 // 提交时间戳
	txnID    string // 事务ID

	// 事务状态
	state      common.TransactionState // 事务状态
	primaryKey []byte                  // 主键
	isReadOnly bool                    // 是否只读

	// 操作记录
	mutations map[string]*common.Mutation // 变更操作
	reads     map[string]uint64           // 读取记录

	// 引擎引用
	engine *PercolatorEngine

	// 性能统计
	startTime    time.Time
	prewriteTime time.Time
	commitTime   time.Time
}

// 注意：直接使用 common.TransactionState，避免混乱的常量别名
// Percolator 事务状态映射：
// - common.TxnActive: 活跃状态
// - common.TxnPrepared: 预写完成状态
// - common.TxnCommitted: 已提交状态
// - common.TxnAborted: 已回滚状态

// 注意：Mutation和MutationType已移至common包，避免重复定义

// LockInfo 锁信息
type LockInfo struct {
	TxnID      string          `json:"txn_id"`      // 事务ID
	StartTS    uint64          `json:"start_ts"`    // 开始时间戳
	Key        []byte          `json:"key"`         // 锁定的键
	PrimaryKey []byte          `json:"primary_key"` // 主键
	LockType   common.LockType `json:"lock_type"`   // 锁类型
	TTL        uint64          `json:"ttl"`         // 生存时间
	CreatedAt  time.Time       `json:"created_at"`  // 创建时间
}

// 注意：LockType已移至common包，避免重复定义

// WriteInfo 写入信息
type WriteInfo struct {
	StartTS   uint64    `json:"start_ts"`   // 开始时间戳
	CommitTS  uint64    `json:"commit_ts"`  // 提交时间戳
	WriteType WriteType `json:"write_type"` // 写入类型
}

// WriteType 写入类型
type WriteType int

const (
	WriteTypePut      WriteType = iota // 写入
	WriteTypeDelete                    // 删除
	WriteTypeRollback                  // 回滚
)

// PercolatorConfig Percolator配置
type PercolatorConfig struct {
	// 事务配置
	MaxTxnDuration  time.Duration // 最大事务持续时间
	LockTTL         time.Duration // 锁的生存时间
	CleanupInterval time.Duration // 清理间隔

	// 性能配置
	BatchSize  int           // 批处理大小
	MaxRetries int           // 最大重试次数
	RetryDelay time.Duration // 重试延迟

	// 并发配置
	MaxConcurrentTxns int // 最大并发事务数
	WorkerPoolSize    int // 工作池大小
}

// PercolatorMetrics Percolator性能指标
type PercolatorMetrics struct {
	// 事务统计
	TotalTxns      uint64
	CommittedTxns  uint64
	RolledBackTxns uint64
	ActiveTxns     uint64

	// 性能指标
	AvgTxnDuration  time.Duration
	AvgPrewriteTime time.Duration
	AvgCommitTime   time.Duration

	// 冲突统计
	WriteConflicts   uint64
	LockConflicts    uint64
	DeadlockDetected uint64

	// 错误统计
	PrewriteErrors uint64
	CommitErrors   uint64
	CleanupErrors  uint64
}

// NewPercolatorEngine 创建Percolator事务引擎
func NewPercolatorEngine(tso coordination.TimestampOracle, storageEngine storage.Engine, config *PercolatorConfig) *PercolatorEngine {
	if config == nil {
		config = DefaultPercolatorConfig()
	}

	lockManager := locks.NewDistributedLockManager("percolator", storageEngine, nil)
	mvccConfig := DefaultPercolatorMVCCConfig()
	mvcc := NewPercolatorMVCC(storageEngine, lockManager, mvccConfig)

	engine := &PercolatorEngine{
		tso:         tso,
		storage:     storageEngine,
		lockManager: lockManager,
		mvcc:        mvcc,
		logger:      log.New(log.Writer(), "[PERCOLATOR] ", log.LstdFlags),
		config:      config,
		activeTxns:  make(map[uint64]*PercolatorTxn),
		metrics:     &PercolatorMetrics{},
	}

	// 启动后台清理任务
	go engine.runCleanupLoop()

	return engine
}

// DefaultPercolatorConfig 默认Percolator配置
func DefaultPercolatorConfig() *PercolatorConfig {
	return &PercolatorConfig{
		MaxTxnDuration:    30 * time.Second,
		LockTTL:           10 * time.Second,
		CleanupInterval:   5 * time.Second,
		BatchSize:         100,
		MaxRetries:        3,
		RetryDelay:        100 * time.Millisecond,
		MaxConcurrentTxns: 1000,
		WorkerPoolSize:    10,
	}
}

// BeginTransaction 开始新事务
func (pe *PercolatorEngine) BeginTransaction(ctx context.Context) (PercolatorTransaction, error) {
	// 获取开始时间戳
	startTS, err := pe.tso.GetTimestamp(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get start timestamp: %w", err)
	}

	// 创建事务
	txn := &PercolatorTxn{
		startTS:    startTS,
		txnID:      fmt.Sprintf("txn_%d_%d", startTS, time.Now().UnixNano()),
		state:      common.TxnActive,
		mutations:  make(map[string]*common.Mutation),
		reads:      make(map[string]uint64),
		engine:     pe,
		startTime:  time.Now(),
		isReadOnly: true,
	}

	// 注册活跃事务
	pe.mu.Lock()
	pe.activeTxns[startTS] = txn
	pe.metrics.ActiveTxns++
	pe.metrics.TotalTxns++
	pe.mu.Unlock()

	pe.logger.Printf("Started transaction: %s, startTS: %d", txn.txnID, startTS)
	return txn, nil
}

// Get 读取数据
func (txn *PercolatorTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
		return nil, fmt.Errorf("transaction is not active: %s", txn.state)
	}

	keyStr := string(key)

	// 检查本地写入缓存
	if mutation, exists := txn.mutations[keyStr]; exists {
		if mutation.Type == common.MutationPut {
			return mutation.Value, nil
		} else {
			return nil, nil // 已删除
		}
	}

	// 从存储引擎读取
	value, err := txn.readFromStorage(ctx, key)
	if err != nil {
		return nil, err
	}

	// 记录读取操作
	txn.reads[keyStr] = txn.startTS

	return value, nil
}

// Put 写入数据
func (txn *PercolatorTxn) Put(ctx context.Context, key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active: %s", txn.state)
	}

	keyStr := string(key)

	// 添加到变更列表
	txn.mutations[keyStr] = &common.Mutation{
		Type:  common.MutationPut,
		Key:   key,
		Value: value,
	}

	// 设置主键（第一个写入的键）
	if txn.primaryKey == nil {
		txn.primaryKey = key
	}

	txn.isReadOnly = false
	return nil
}

// Delete 删除数据
func (txn *PercolatorTxn) Delete(ctx context.Context, key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active: %s", txn.state)
	}

	keyStr := string(key)

	// 添加到变更列表
	txn.mutations[keyStr] = &common.Mutation{
		Type:  common.MutationDelete,
		Key:   key,
		Value: nil,
	}

	// 设置主键（第一个写入的键）
	if txn.primaryKey == nil {
		txn.primaryKey = key
	}

	txn.isReadOnly = false
	return nil
}

// GetStartTS 获取事务开始时间戳
func (txn *PercolatorTxn) GetStartTS() uint64 {
	return txn.startTS
}

// GetCommitTS 获取事务提交时间戳
func (txn *PercolatorTxn) GetCommitTS() uint64 {
	return txn.commitTS
}

// IsReadOnly 检查是否为只读事务
func (txn *PercolatorTxn) IsReadOnly() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.isReadOnly
}

// Commit 提交事务（两阶段提交）
func (txn *PercolatorTxn) Commit(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != common.TxnActive {
		return fmt.Errorf("transaction is not active: %s", txn.state)
	}

	// 只读事务直接提交
	if txn.isReadOnly {
		txn.state = common.TxnCommitted
		txn.cleanup()
		return nil
	}

	// 阶段1：Prewrite
	if err := txn.prewrite(ctx); err != nil {
		txn.engine.metrics.PrewriteErrors++
		return fmt.Errorf("prewrite failed: %w", err)
	}

	txn.state = common.TxnPrepared
	txn.prewriteTime = time.Now()

	// 获取提交时间戳
	commitTS, err := txn.engine.tso.GetTimestamp(ctx)
	if err != nil {
		// Prewrite成功但获取提交时间戳失败，需要回滚
		txn.rollbackPrewrite(ctx)
		return fmt.Errorf("failed to get commit timestamp: %w", err)
	}

	txn.commitTS = commitTS

	// 阶段2：Commit
	if err := txn.commit(ctx); err != nil {
		txn.engine.metrics.CommitErrors++
		return fmt.Errorf("commit failed: %w", err)
	}

	txn.state = common.TxnCommitted
	txn.commitTime = time.Now()
	txn.cleanup()

	// 更新性能指标
	txn.engine.updateMetrics(txn)

	txn.engine.logger.Printf("Committed transaction: %s, startTS: %d, commitTS: %d",
		txn.txnID, txn.startTS, txn.commitTS)
	return nil
}

// Rollback 回滚事务
func (txn *PercolatorTxn) Rollback(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state == common.TxnCommitted {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	if txn.state == common.TxnAborted {
		return nil // 已经回滚
	}

	// 如果已经预写，需要清理锁
	if txn.state == common.TxnPrepared {
		txn.rollbackPrewrite(ctx)
	}

	txn.state = common.TxnAborted
	txn.cleanup()

	txn.engine.metrics.RolledBackTxns++
	txn.engine.logger.Printf("Rolled back transaction: %s, startTS: %d", txn.txnID, txn.startTS)
	return nil
}

// prewrite 预写阶段
func (txn *PercolatorTxn) prewrite(ctx context.Context) error {
	// 准备变更列表
	mutations := make([]*common.Mutation, 0, len(txn.mutations))
	for _, mutation := range txn.mutations {
		mutations = append(mutations, mutation)
	}

	// 使用增强的MVCC进行预写
	return txn.engine.mvcc.Prewrite(ctx, mutations, txn.primaryKey, txn.startTS, txn.txnID)
}

// commit 提交阶段
func (txn *PercolatorTxn) commit(ctx context.Context) error {
	// 准备键列表
	keys := make([][]byte, 0, len(txn.mutations))
	for _, mutation := range txn.mutations {
		keys = append(keys, mutation.Key)
	}

	// 使用增强的MVCC进行提交
	return txn.engine.mvcc.Commit(ctx, keys, txn.startTS, txn.commitTS, txn.txnID)
}

// checkConflicts 方法已经被PercolatorMVCC替代，不再需要

// readFromStorage 从存储引擎读取数据
func (txn *PercolatorTxn) readFromStorage(ctx context.Context, key []byte) ([]byte, error) {
	// 使用增强的MVCC进行读取
	return txn.engine.mvcc.Get(ctx, key, txn.startTS)
}

// 辅助方法实现
func (txn *PercolatorTxn) cleanup() {
	// 从活跃事务列表中移除
	txn.engine.mu.Lock()
	delete(txn.engine.activeTxns, txn.startTS)
	txn.engine.metrics.ActiveTxns--
	txn.engine.mu.Unlock()
}

func (txn *PercolatorTxn) rollbackPrewrite(ctx context.Context) {
	// 准备键列表
	keys := make([][]byte, 0, len(txn.mutations))
	for _, mutation := range txn.mutations {
		keys = append(keys, mutation.Key)
	}

	// 使用增强的MVCC进行回滚
	txn.engine.mvcc.Rollback(ctx, keys, txn.startTS, txn.txnID)
}

// 这些方法已经被PercolatorMVCC替代，不再需要

func (txn *PercolatorTxn) makeDataKey(key []byte, ts uint64) []byte {
	return []byte(fmt.Sprintf("data_%s_%d", string(key), ts))
}

func (txn *PercolatorTxn) makeWriteKey(key []byte, ts uint64) []byte {
	return []byte(fmt.Sprintf("write_%s_%d", string(key), ts))
}

func (txn *PercolatorTxn) makeWritePrefix(key []byte) []byte {
	return []byte(fmt.Sprintf("write_%s_", string(key)))
}

func (txn *PercolatorTxn) makeLockKey(key []byte) []byte {
	return []byte(fmt.Sprintf("lock_%s", string(key)))
}

func (pe *PercolatorEngine) updateMetrics(txn *PercolatorTxn) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	pe.metrics.CommittedTxns++

	duration := txn.commitTime.Sub(txn.startTime)
	if pe.metrics.AvgTxnDuration == 0 {
		pe.metrics.AvgTxnDuration = duration
	} else {
		pe.metrics.AvgTxnDuration = (pe.metrics.AvgTxnDuration + duration) / 2
	}

	if !txn.prewriteTime.IsZero() {
		prewriteDuration := txn.prewriteTime.Sub(txn.startTime)
		if pe.metrics.AvgPrewriteTime == 0 {
			pe.metrics.AvgPrewriteTime = prewriteDuration
		} else {
			pe.metrics.AvgPrewriteTime = (pe.metrics.AvgPrewriteTime + prewriteDuration) / 2
		}
	}

	if !txn.commitTime.IsZero() && !txn.prewriteTime.IsZero() {
		commitDuration := txn.commitTime.Sub(txn.prewriteTime)
		if pe.metrics.AvgCommitTime == 0 {
			pe.metrics.AvgCommitTime = commitDuration
		} else {
			pe.metrics.AvgCommitTime = (pe.metrics.AvgCommitTime + commitDuration) / 2
		}
	}
}

func (pe *PercolatorEngine) runCleanupLoop() {
	ticker := time.NewTicker(pe.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pe.cleanupExpiredTransactions()
		}
	}
}

func (pe *PercolatorEngine) cleanupExpiredTransactions() {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	now := time.Now()
	for startTS, txn := range pe.activeTxns {
		if now.Sub(txn.startTime) > pe.config.MaxTxnDuration {
			// 强制回滚超时事务
			go func(t *PercolatorTxn) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				t.Rollback(ctx)
			}(txn)

			delete(pe.activeTxns, startTS)
			pe.metrics.ActiveTxns--
		}
	}
}
