/*
JadeDB 统一事务管理系统

本模块实现了JadeDB的统一事务架构，支持从单机到分布式的平滑扩展。
采用分层设计，为SQL层提供完整的ACID事务保证。

架构层次：
1. 事务接口层：统一的事务API，支持单机和分布式
2. 事务协调层：负责事务的生命周期管理和协调
3. 并发控制层：MVCC、锁管理、冲突检测
4. 存储引擎层：与底层存储引擎的适配

核心特性：
- 统一接口：单机和分布式使用相同的事务API
- ACID保证：完整的事务ACID特性支持
- MVCC支持：多版本并发控制，快照隔离
- 分布式事务：2PC协议，跨节点事务一致性
- 可扩展性：支持从单机平滑扩展到分布式
- SQL就绪：为SQL层预留完整的事务接口

设计目标：
- 替代现有的txn.go，提供更全面的事务支持
- 为分布式SQL系统提供坚实的事务基础
- 支持复杂的SQL事务语义（隔离级别、锁等待等）
*/

package core

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/transaction/locks"
	"github.com/util6/JadeDB/transaction/mvcc"
	"github.com/util6/JadeDB/txnwal"
)

// 前向声明，避免循环依赖
type Engine interface {
	BeginTransaction(txnManager *TransactionManager, options *TransactionOptions) (Transaction, error)
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Exists(key []byte) (bool, error)
	BatchPut(batch []KVPair) error
	BatchGet(keys [][]byte) ([][]byte, error)
	BatchDelete(keys [][]byte) error
	Scan(startKey, endKey []byte, limit int) ([]KVPair, error)
	Open() error
	Close() error
	Sync() error
}

// KVPair 键值对
type KVPair struct {
	Key   []byte
	Value []byte
}

// TransactionManager 统一事务管理器
// 这是整个事务系统的核心，负责事务的创建、协调和管理
type TransactionManager struct {
	// 基本配置
	config *TransactionConfig

	// 时间戳管理
	timestampOracle *TimestampOracle

	// 并发控制
	lockManager common.LockManager
	mvccManager common.MVCCManager

	// 分布式支持
	isDistributed bool

	// Percolator支持
	isPercolatorEnabled bool
	percolatorEngine    interface{} // 避免循环依赖，使用interface{}

	// 默认存储引擎
	defaultEngine storage.Engine

	// 事务WAL管理器
	txnWAL txnwal.TxnWALManager

	// 事务跟踪
	mu         sync.RWMutex
	activeTxns map[string]*Transaction
	nextTxnID  atomic.Int64

	// 性能监控
	metrics *TransactionMetrics

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TransactionConfig 事务配置
type TransactionConfig struct {
	// 基本配置
	MaxConcurrentTxns int            // 最大并发事务数
	DefaultTimeout    time.Duration  // 默认事务超时
	DefaultIsolation  IsolationLevel // 默认隔离级别

	// MVCC配置
	EnableMVCC  bool          // 是否启用MVCC
	MaxVersions int           // 最大版本数
	GCInterval  time.Duration // 垃圾回收间隔

	// 锁配置
	EnableDeadlockDetect  bool          // 是否启用死锁检测
	LockTimeout           time.Duration // 锁超时时间
	DeadlockCheckInterval time.Duration // 死锁检测间隔

	// 分布式配置
	EnableDistributed bool          // 是否启用分布式事务
	PrepareTimeout    time.Duration // 准备阶段超时
	CommitTimeout     time.Duration // 提交阶段超时
	MaxRetries        int           // 最大重试次数

	// Percolator配置
	EnablePercolator  bool          // 是否启用Percolator模式
	PercolatorLockTTL time.Duration // Percolator锁的生存时间
}

// DefaultTransactionConfig 默认事务配置
func DefaultTransactionConfig() *TransactionConfig {
	return &TransactionConfig{
		MaxConcurrentTxns:     1000,
		DefaultTimeout:        30 * time.Second,
		DefaultIsolation:      ReadCommitted,
		EnableMVCC:            true,
		MaxVersions:           100,
		GCInterval:            time.Minute,
		EnableDeadlockDetect:  true,
		LockTimeout:           10 * time.Second,
		DeadlockCheckInterval: time.Second,
		EnableDistributed:     false,
		PrepareTimeout:        10 * time.Second,
		CommitTimeout:         10 * time.Second,
		MaxRetries:            3,
		EnablePercolator:      false,            // 默认不启用Percolator模式
		PercolatorLockTTL:     10 * time.Second, // Percolator锁默认TTL
	}
}

// IsolationLevel 事务隔离级别
type IsolationLevel int

const (
	ReadUncommitted   IsolationLevel = iota // 读未提交
	ReadCommitted                           // 读已提交
	RepeatableRead                          // 可重复读
	Serializable                            // 串行化
	SnapshotIsolation                       // 快照隔离
)

func (level IsolationLevel) String() string {
	switch level {
	case ReadUncommitted:
		return "READ_UNCOMMITTED"
	case ReadCommitted:
		return "READ_COMMITTED"
	case RepeatableRead:
		return "REPEATABLE_READ"
	case Serializable:
		return "SERIALIZABLE"
	case SnapshotIsolation:
		return "SNAPSHOT_ISOLATION"
	default:
		return "UNKNOWN"
	}
}

// Transaction 统一事务接口
// 这个接口同时支持单机和分布式事务，为SQL层提供统一的事务API
type Transaction interface {
	// 基本操作
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

	// 事务控制
	Commit() error
	Rollback() error

	// 事务状态
	GetTxnID() string
	GetStartTime() time.Time
	GetIsolationLevel() IsolationLevel
	IsReadOnly() bool
	IsDistributed() bool

	// 锁操作（为SQL层预留）
	AcquireLock(key []byte, lockType common.LockType) error
	ReleaseLock(key []byte) error

	// 保存点（为SQL层预留）
	CreateSavepoint(name string) error
	RollbackToSavepoint(name string) error
	ReleaseSavepoint(name string) error

	// 生命周期
	Close() error
}

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	Prefix   []byte
	StartKey []byte
	EndKey   []byte
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

// LockType 锁类型
type LockType int

const (
	SharedLock             LockType = iota // 共享锁（读锁）
	ExclusiveLock                          // 排他锁（写锁）
	IntentionSharedLock                    // 意向共享锁
	IntentionExclusiveLock                 // 意向排他锁
)

// 注意：TransactionState 已移至 common 包，避免重复定义
// 使用 common.TransactionState 替代本地定义

// TransactionMetrics 事务监控指标
type TransactionMetrics struct {
	// 事务统计
	TotalTransactions     atomic.Int64 // 总事务数
	ActiveTransactions    atomic.Int64 // 活跃事务数
	CommittedTransactions atomic.Int64 // 已提交事务数
	AbortedTransactions   atomic.Int64 // 已中止事务数

	// 性能指标
	AvgTransactionTime atomic.Int64 // 平均事务时间（纳秒）
	AvgCommitTime      atomic.Int64 // 平均提交时间（纳秒）

	// 并发控制
	LockWaits      atomic.Int64 // 锁等待次数
	Deadlocks      atomic.Int64 // 死锁次数
	ConflictAborts atomic.Int64 // 冲突中止次数

	// 分布式事务
	DistributedTxns atomic.Int64 // 分布式事务数
	PrepareFailures atomic.Int64 // 准备阶段失败数
	CommitFailures  atomic.Int64 // 提交阶段失败数
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(config *TransactionConfig) (*TransactionManager, error) {
	if config == nil {
		config = DefaultTransactionConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	tm := &TransactionManager{
		config:        config,
		activeTxns:    make(map[string]*Transaction),
		metrics:       &TransactionMetrics{},
		ctx:           ctx,
		cancel:        cancel,
		isDistributed: config.EnableDistributed,
	}

	// 初始化时间戳管理器
	var err error
	timestampConfig := &TimestampConfig{
		StartTimestamp:    1000,
		BatchSize:         100, // 默认批量大小
		EnableDistributed: config.EnableDistributed,
		SyncInterval:      time.Second,
	}
	tm.timestampOracle, err = NewTimestampOracle(timestampConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create timestamp oracle: %w", err)
	}

	// 转换配置为 common.TransactionConfig
	commonConfig := &common.TransactionConfig{
		MaxVersions:       config.MaxVersions,
		GCInterval:        config.GCInterval,
		MaxTxnDuration:    config.DefaultTimeout,
		LockTTL:           config.LockTimeout,
		DeadlockDetection: config.EnableDeadlockDetect,
		BatchSize:         1000, // 默认值
		MaxConcurrentTxns: config.MaxConcurrentTxns,
		EnablePercolator:  config.EnablePercolator, // 使用配置中的Percolator设置
		CleanupInterval:   time.Minute,             // 默认值
	}

	// 初始化锁管理器
	lockMgr, err := locks.NewLockManager(commonConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create lock manager: %w", err)
	}
	tm.lockManager = lockMgr

	// 初始化MVCC管理器
	mvccMgr, err := mvcc.NewMVCCManager(commonConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create MVCC manager: %w", err)
	}
	tm.mvccManager = mvccMgr

	// 初始化事务WAL管理器
	walOptions := txnwal.DefaultTxnWALOptions()
	walOptions.Directory = "./txnwal_" + fmt.Sprintf("%d", time.Now().UnixNano())
	tm.txnWAL, err = txnwal.NewFileTxnWALManager(walOptions)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create transaction WAL: %w", err)
	}

	// 如果启用分布式，设置标志
	if config.EnableDistributed {
		tm.isDistributed = true
	}

	// 如果启用Percolator，设置标志并配置MVCC
	if config.EnablePercolator {
		tm.isPercolatorEnabled = true
		// 启用MVCC管理器的Percolator模式
		if mvccMgr, ok := tm.mvccManager.(*mvcc.MVCCManager); ok {
			mvccMgr.EnablePercolatorMode()
		}
	}

	// 启动后台服务
	tm.startBackgroundServices()

	return tm, nil
}

// BeginTransaction 开始事务
func (tm *TransactionManager) BeginTransaction(options *TransactionOptions) (Transaction, error) {
	// 检查并发限制
	if tm.getActiveTxnCount() >= int64(tm.config.MaxConcurrentTxns) {
		return nil, fmt.Errorf("too many concurrent transactions")
	}

	// 生成事务ID
	txnID := tm.generateTxnID()

	// 获取时间戳
	startTs, err := tm.timestampOracle.GetTimestamp()
	if err != nil {
		return nil, fmt.Errorf("failed to get timestamp: %w", err)
	}

	// 创建事务选项
	if options == nil {
		options = &TransactionOptions{
			Isolation: tm.config.DefaultIsolation,
			Timeout:   tm.config.DefaultTimeout,
			ReadOnly:  false,
		}
	}

	// 创建事务实例
	var txn Transaction
	if tm.isDistributed {
		txn, err = tm.createDistributedTransaction(txnID, startTs, options)
	} else {
		txn, err = tm.createLocalTransaction(txnID, startTs, options)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// 注册事务
	tm.registerTransaction(txnID, &txn)

	// 更新指标
	tm.metrics.TotalTransactions.Add(1)
	tm.metrics.ActiveTransactions.Add(1)

	return txn, nil
}

// TransactionOptions 事务选项
type TransactionOptions struct {
	Isolation IsolationLevel // 隔离级别
	Timeout   time.Duration  // 超时时间
	ReadOnly  bool           // 是否只读
}

// 辅助方法
func (tm *TransactionManager) getActiveTxnCount() int64 {
	return tm.metrics.ActiveTransactions.Load()
}

func (tm *TransactionManager) generateTxnID() string {
	return fmt.Sprintf("txn_%d_%d", time.Now().UnixNano(), tm.metrics.TotalTransactions.Load())
}

func (tm *TransactionManager) registerTransaction(txnID string, txn *Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.activeTxns[txnID] = txn
}

func (tm *TransactionManager) unregisterTransaction(txnID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.activeTxns, txnID)
	tm.metrics.ActiveTransactions.Add(-1)
}

func (tm *TransactionManager) createLocalTransaction(txnID string, startTs uint64, options *TransactionOptions) (Transaction, error) {
	// 暂时只使用本地事务，存储适配器需要接口匹配修复
	// TODO: 修复接口匹配问题后启用存储适配器
	return NewLocalTransaction(txnID, startTs, options, tm)
}

func (tm *TransactionManager) createDistributedTransaction(txnID string, startTs uint64, options *TransactionOptions) (Transaction, error) {
	// 暂时返回本地事务，分布式事务将在storage层实现
	return tm.createLocalTransaction(txnID, startTs, options)
}

// SetDefaultEngine 设置默认存储引擎
func (tm *TransactionManager) SetDefaultEngine(engine storage.Engine) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.defaultEngine = engine
}

// GetDefaultEngine 获取默认存储引擎
func (tm *TransactionManager) GetDefaultEngine() storage.Engine {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.defaultEngine
}

// BeginTransactionWithEngine 使用指定引擎开始事务
func (tm *TransactionManager) BeginTransactionWithEngine(engine storage.Engine, options *TransactionOptions) (Transaction, error) {
	if options == nil {
		options = &TransactionOptions{
			Isolation: tm.config.DefaultIsolation,
			Timeout:   tm.config.DefaultTimeout,
			ReadOnly:  false,
		}
	}

	// 生成事务ID
	txnID := fmt.Sprintf("txn_%d_%d", time.Now().UnixNano(), tm.nextTxnID.Add(1))

	// 暂时使用本地事务，存储适配器需要接口匹配修复
	// TODO: 修复接口匹配问题后启用存储适配器
	startTs, _ := tm.timestampOracle.GetTimestamp()
	return NewLocalTransaction(txnID, startTs, options, tm)
}

// GetMVCCManager 获取MVCC管理器
func (tm *TransactionManager) GetMVCCManager() common.MVCCManager {
	return tm.mvccManager
}

// GetLockManager 获取锁管理器
func (tm *TransactionManager) GetLockManager() common.LockManager {
	return tm.lockManager
}

// GetWALStatistics 获取WAL统计信息
func (tm *TransactionManager) GetWALStatistics() interface{} {
	if tm.txnWAL != nil {
		return tm.txnWAL.GetStatistics()
	}
	return nil
}

// GetWALLatestLSN 获取WAL最新LSN
func (tm *TransactionManager) GetWALLatestLSN() uint64 {
	if tm.txnWAL != nil {
		return uint64(tm.txnWAL.GetLatestLSN())
	}
	return 0
}

// ReadWALRecord 读取WAL记录
func (tm *TransactionManager) ReadWALRecord(ctx context.Context, lsn uint64) (interface{}, error) {
	if tm.txnWAL != nil {
		return tm.txnWAL.ReadRecord(ctx, txnwal.TxnLSN(lsn))
	}
	return nil, fmt.Errorf("WAL not initialized")
}

// ReadWALRange 读取WAL范围记录
func (tm *TransactionManager) ReadWALRange(ctx context.Context, startLSN, endLSN uint64) ([]interface{}, error) {
	if tm.txnWAL != nil {
		records, err := tm.txnWAL.ReadRange(ctx, txnwal.TxnLSN(startLSN), txnwal.TxnLSN(endLSN))
		if err != nil {
			return nil, err
		}
		result := make([]interface{}, len(records))
		for i, record := range records {
			result[i] = record
		}
		return result, nil
	}
	return nil, fmt.Errorf("WAL not initialized")
}

// ReadWALFrom 从指定LSN开始读取WAL
func (tm *TransactionManager) ReadWALFrom(ctx context.Context, startLSN uint64) (interface{}, error) {
	if tm.txnWAL != nil {
		return tm.txnWAL.ReadFrom(ctx, txnwal.TxnLSN(startLSN))
	}
	return nil, fmt.Errorf("WAL not initialized")
}

// GetWALManager 获取WAL管理器（用于测试）
func (tm *TransactionManager) GetWALManager() txnwal.TxnWALManager {
	return tm.txnWAL
}

// GetTimestampOracle 获取时间戳管理器
func (tm *TransactionManager) GetTimestampOracle() *TimestampOracle {
	return tm.timestampOracle
}

func (tm *TransactionManager) startBackgroundServices() {
	// 启动垃圾回收服务
	tm.wg.Add(1)
	go tm.gcService()

	// 启动死锁检测服务
	if tm.config.EnableDeadlockDetect {
		tm.wg.Add(1)
		go tm.deadlockDetectionService()
	}

	// 启动指标收集服务
	tm.wg.Add(1)
	go tm.metricsService()
}

// gcService 垃圾回收服务
func (tm *TransactionManager) gcService() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.config.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			return
		case <-ticker.C:
			tm.mvccManager.GC(uint64(time.Now().Add(-time.Hour).UnixNano()))
		}
	}
}

// deadlockDetectionService 死锁检测服务
func (tm *TransactionManager) deadlockDetectionService() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.config.DeadlockCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			return
		case <-ticker.C:
			deadlockedTxns, err := tm.lockManager.DetectDeadlock()
			if err == nil && len(deadlockedTxns) > 0 {
				// 简化处理：记录死锁事务，实际处理留给具体的事务实现
				log.Printf("Detected deadlocked transactions: %v", deadlockedTxns)
			}
		}
	}
}

// metricsService 指标收集服务
func (tm *TransactionManager) metricsService() {
	defer tm.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			return
		case <-ticker.C:
			tm.updateMetrics()
		}
	}
}

func (tm *TransactionManager) resolveDeadlock(deadlock *common.Deadlock) {
	// 简单策略：中止最年轻的事务
	// TODO: 实现更智能的死锁解决策略
	tm.metrics.Deadlocks.Add(1)
}

func (tm *TransactionManager) updateMetrics() {
	// 更新各种性能指标
	// TODO: 实现详细的指标收集逻辑
}

// Close 关闭事务管理器
func (tm *TransactionManager) Close() error {
	tm.cancel()
	tm.wg.Wait()

	// 中止所有活跃事务
	tm.mu.Lock()
	for _, txn := range tm.activeTxns {
		(*txn).Rollback()
	}
	tm.mu.Unlock()

	// 关闭各个组件
	if lockMgr, ok := tm.lockManager.(*locks.LockManager); ok {
		lockMgr.Close()
	}
	if mvccMgr, ok := tm.mvccManager.(*mvcc.MVCCManager); ok {
		mvccMgr.Close()
	}
	tm.timestampOracle.Close()

	return nil
}

// GetMetrics 获取事务指标
func (tm *TransactionManager) GetMetrics() *TransactionMetrics {
	return tm.metrics
}

// === Percolator模式支持方法 ===

// EnablePercolatorMode 启用Percolator模式
// 实现传统MVCC和Percolator MVCC的切换机制
func (tm *TransactionManager) EnablePercolatorMode() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.isPercolatorEnabled {
		return nil // 已经启用
	}

	// 检查是否有活跃事务
	if len(tm.activeTxns) > 0 {
		return fmt.Errorf("cannot enable Percolator mode while transactions are active")
	}

	// 启用MVCC管理器的Percolator模式
	if mvccMgr, ok := tm.mvccManager.(*mvcc.MVCCManager); ok {
		mvccMgr.EnablePercolatorMode()
	}

	tm.isPercolatorEnabled = true
	tm.config.EnablePercolator = true

	log.Printf("Percolator mode enabled")
	return nil
}

// DisablePercolatorMode 禁用Percolator模式
func (tm *TransactionManager) DisablePercolatorMode() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.isPercolatorEnabled {
		return nil // 已经禁用
	}

	// 检查是否有活跃事务
	if len(tm.activeTxns) > 0 {
		return fmt.Errorf("cannot disable Percolator mode while transactions are active")
	}

	// 禁用MVCC管理器的Percolator模式
	if mvccMgr, ok := tm.mvccManager.(*mvcc.MVCCManager); ok {
		mvccMgr.DisablePercolatorMode()
	}

	tm.isPercolatorEnabled = false
	tm.config.EnablePercolator = false

	log.Printf("Percolator mode disabled")
	return nil
}

// IsPercolatorModeEnabled 检查是否启用了Percolator模式
func (tm *TransactionManager) IsPercolatorModeEnabled() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.isPercolatorEnabled
}

// SetPercolatorEngine 设置Percolator引擎（用于分布式场景）
func (tm *TransactionManager) SetPercolatorEngine(engine interface{}) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.percolatorEngine = engine
}

// GetPercolatorEngine 获取Percolator引擎
func (tm *TransactionManager) GetPercolatorEngine() interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.percolatorEngine
}
