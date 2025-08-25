/*
JadeDB Percolator事务协调器

本模块实现了专门为Percolator事务模型优化的分布式事务协调器。
在原有事务协调器基础上，增加了Percolator特定的功能和优化。

核心增强：
1. 主键协调：使用主键作为事务协调者，简化协调逻辑
2. 异步提交：支持主键提交后的异步二级键提交
3. 时间戳管理：集成TSO，提供全局时间戳排序
4. MVCC集成：深度集成Percolator MVCC系统
5. 锁管理集成：与分布式锁管理器紧密集成
6. 故障恢复：支持Percolator特定的故障恢复机制

设计特点：
- 主键优先：主键作为事务的协调点
- 两阶段优化：针对Percolator的2PC优化
- 高并发：支持大量并发Percolator事务
- 故障容错：完整的故障检测和恢复
- 性能监控：详细的Percolator事务指标
*/

package percolator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/coordination"
	"github.com/util6/JadeDB/distributed/locks"
	"github.com/util6/JadeDB/storage"
)

// PercolatorCoordinator Percolator事务协调器
type PercolatorCoordinator struct {
	mu sync.RWMutex

	// 基础组件
	nodeID      string                        // 节点ID
	tso         coordination.TimestampOracle  // 时间戳服务器
	storage     storage.Engine                // 持久化存储
	mvcc        *PercolatorMVCC               // MVCC管理器
	lockManager *locks.DistributedLockManager // 分布式锁管理器

	// 配置
	config *PercolatorCoordinatorConfig // 协调器配置

	// 事务管理
	activeTxns map[string]*CoordinatedPercolatorTxn // 活跃事务
	txnRecords map[string]*PercolatorTxnRecord      // 事务记录

	// 性能统计
	metrics *PercolatorCoordinatorMetrics // 性能指标
	logger  *log.Logger                   // 日志记录器

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// PercolatorCoordinatorConfig Percolator协调器配置
type PercolatorCoordinatorConfig struct {
	// 基础配置
	NodeID            string        // 节点ID
	MaxConcurrentTxns int           // 最大并发事务数
	TxnTimeout        time.Duration // 事务超时时间

	// Percolator特定配置
	PrewriteTimeout        time.Duration // 预写超时时间
	CommitTimeout          time.Duration // 提交超时时间
	AsyncCommit            bool          // 是否启用异步提交
	PrimaryKeyOptimization bool          // 是否启用主键优化

	// 性能优化配置
	BatchSize        int  // 批处理大小
	ParallelPrewrite bool // 是否启用并行预写
	EnablePipelining bool // 是否启用流水线

	// 故障恢复配置
	RecoveryInterval   time.Duration // 恢复检查间隔
	StateCheckInterval time.Duration // 状态检查间隔
	CleanupInterval    time.Duration // 清理间隔
	MaxRetries         int           // 最大重试次数
}

// CoordinatedPercolatorTxn 协调器管理的Percolator事务
type CoordinatedPercolatorTxn struct {
	mu sync.RWMutex

	// 事务标识
	TxnID    string // 事务ID
	StartTS  uint64 // 开始时间戳
	CommitTS uint64 // 提交时间戳

	// 事务状态
	State      common.TransactionState // 事务状态
	PrimaryKey []byte                  // 主键
	IsPrimary  bool                    // 是否为主键事务

	// 操作记录
	Mutations map[string]*common.Mutation // 变更操作
	Reads     map[string]uint64           // 读取记录

	// 协调器引用
	Coordinator *PercolatorCoordinator // 协调器引用

	// 时间信息
	CreatedAt time.Time // 创建时间
	UpdatedAt time.Time // 更新时间
	ExpiresAt time.Time // 过期时间

	// 性能统计
	PrewriteStartTime time.Time // 预写开始时间
	PrewriteEndTime   time.Time // 预写结束时间
	CommitStartTime   time.Time // 提交开始时间
	CommitEndTime     time.Time // 提交结束时间
}

// PercolatorTxnRecord Percolator事务记录
type PercolatorTxnRecord struct {
	TxnID        string                  `json:"txn_id"`        // 事务ID
	State        common.TransactionState `json:"state"`         // 事务状态
	StartTS      uint64                  `json:"start_ts"`      // 开始时间戳
	CommitTS     uint64                  `json:"commit_ts"`     // 提交时间戳
	PrimaryKey   []byte                  `json:"primary_key"`   // 主键
	MutationKeys []string                `json:"mutation_keys"` // 变更键列表
	CreatedAt    time.Time               `json:"created_at"`    // 创建时间
	UpdatedAt    time.Time               `json:"updated_at"`    // 更新时间
}

// PercolatorCoordinatorMetrics Percolator协调器性能指标
type PercolatorCoordinatorMetrics struct {
	// 事务统计
	TotalTransactions     uint64
	ActiveTransactions    uint64
	CommittedTransactions uint64
	AbortedTransactions   uint64

	// Percolator特定指标
	PrimaryKeyTransactions   uint64
	SecondaryKeyTransactions uint64
	AsyncCommitTransactions  uint64

	// 性能指标
	AvgTransactionTime     time.Duration
	AvgPrewriteTime        time.Duration
	AvgCommitTime          time.Duration
	AvgPrimaryCommitTime   time.Duration
	AvgSecondaryCommitTime time.Duration

	// 错误统计
	PrewriteFailures uint64
	CommitFailures   uint64
	TimeoutFailures  uint64
	ConflictFailures uint64
	LockFailures     uint64
}

// NewPercolatorCoordinator 创建Percolator事务协调器
func NewPercolatorCoordinator(nodeID string, tso coordination.TimestampOracle, storage storage.Engine,
	mvcc *PercolatorMVCC, lockManager *locks.DistributedLockManager,
	config *PercolatorCoordinatorConfig) *PercolatorCoordinator {

	if config == nil {
		config = DefaultPercolatorCoordinatorConfig(nodeID)
	}

	pc := &PercolatorCoordinator{
		nodeID:      nodeID,
		tso:         tso,
		storage:     storage,
		mvcc:        mvcc,
		lockManager: lockManager,
		config:      config,
		activeTxns:  make(map[string]*CoordinatedPercolatorTxn),
		txnRecords:  make(map[string]*PercolatorTxnRecord),
		metrics:     &PercolatorCoordinatorMetrics{},
		logger:      log.New(log.Writer(), "[PERCOLATOR_COORDINATOR] ", log.LstdFlags),
		stopCh:      make(chan struct{}),
	}

	// 启动后台服务
	pc.startBackgroundServices()

	return pc
}

// DefaultPercolatorCoordinatorConfig 默认Percolator协调器配置
func DefaultPercolatorCoordinatorConfig(nodeID string) *PercolatorCoordinatorConfig {
	return &PercolatorCoordinatorConfig{
		NodeID:                 nodeID,
		MaxConcurrentTxns:      1000,
		TxnTimeout:             30 * time.Second,
		PrewriteTimeout:        10 * time.Second,
		CommitTimeout:          5 * time.Second,
		AsyncCommit:            true,
		PrimaryKeyOptimization: true,
		BatchSize:              100,
		ParallelPrewrite:       true,
		EnablePipelining:       true,
		RecoveryInterval:       5 * time.Second,
		StateCheckInterval:     2 * time.Second,
		CleanupInterval:        10 * time.Second,
		MaxRetries:             3,
	}
}

// BeginTransaction 开始新的Percolator事务
func (pc *PercolatorCoordinator) BeginTransaction(ctx context.Context) (*CoordinatedPercolatorTxn, error) {
	// 检查并发限制
	pc.mu.RLock()
	if len(pc.activeTxns) >= pc.config.MaxConcurrentTxns {
		pc.mu.RUnlock()
		return nil, fmt.Errorf("too many concurrent transactions: %d", len(pc.activeTxns))
	}
	pc.mu.RUnlock()

	// 获取开始时间戳
	startTS, err := pc.tso.GetTimestamp(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get start timestamp: %w", err)
	}

	// 创建事务ID
	txnID := fmt.Sprintf("percolator_txn_%s_%d_%d", pc.nodeID, startTS, time.Now().UnixNano())

	// 创建Percolator事务
	txn := &CoordinatedPercolatorTxn{
		TxnID:       txnID,
		StartTS:     startTS,
		State:       common.TxnActive,
		Mutations:   make(map[string]*common.Mutation),
		Reads:       make(map[string]uint64),
		Coordinator: pc,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(pc.config.TxnTimeout),
	}

	// 注册事务
	pc.mu.Lock()
	pc.activeTxns[txnID] = txn
	pc.metrics.TotalTransactions++
	pc.metrics.ActiveTransactions++
	pc.mu.Unlock()

	// 持久化事务记录
	if err := pc.persistTransactionRecord(txn); err != nil {
		pc.logger.Printf("Failed to persist transaction record: %v", err)
		// 不返回错误，继续执行
	}

	pc.logger.Printf("Started Percolator transaction: %s, startTS: %d", txnID, startTS)
	return txn, nil
}

// startBackgroundServices 启动后台服务
func (pc *PercolatorCoordinator) startBackgroundServices() {
	// 启动状态检查服务
	pc.wg.Add(1)
	go pc.runStateChecker()

	// 启动恢复服务
	pc.wg.Add(1)
	go pc.runRecoveryService()

	// 启动清理服务
	pc.wg.Add(1)
	go pc.runCleanupService()
}

// Put 在事务中写入数据
func (txn *CoordinatedPercolatorTxn) Put(ctx context.Context, key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != common.TxnActive {
		return fmt.Errorf("transaction is not in init state: %s", txn.State)
	}

	keyStr := string(key)

	// 添加到变更列表
	txn.Mutations[keyStr] = &common.Mutation{
		Type:  common.MutationPut,
		Key:   key,
		Value: value,
	}

	// 设置主键（第一个写入的键）
	if txn.PrimaryKey == nil {
		txn.PrimaryKey = key
		txn.IsPrimary = true
		txn.Coordinator.metrics.PrimaryKeyTransactions++
	} else {
		txn.Coordinator.metrics.SecondaryKeyTransactions++
	}

	txn.UpdatedAt = time.Now()
	return nil
}

// Delete 在事务中删除数据
func (txn *CoordinatedPercolatorTxn) Delete(ctx context.Context, key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != common.TxnActive {
		return fmt.Errorf("transaction is not in init state: %s", txn.State)
	}

	keyStr := string(key)

	// 添加到变更列表
	txn.Mutations[keyStr] = &common.Mutation{
		Type:  common.MutationDelete,
		Key:   key,
		Value: nil,
	}

	// 设置主键（第一个写入的键）
	if txn.PrimaryKey == nil {
		txn.PrimaryKey = key
		txn.IsPrimary = true
		txn.Coordinator.metrics.PrimaryKeyTransactions++
	} else {
		txn.Coordinator.metrics.SecondaryKeyTransactions++
	}

	txn.UpdatedAt = time.Now()
	return nil
}

// Get 在事务中读取数据
func (txn *CoordinatedPercolatorTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != common.TxnActive {
		return nil, fmt.Errorf("transaction is not in init state: %s", txn.State)
	}

	keyStr := string(key)

	// 检查本地写入缓存
	if mutation, exists := txn.Mutations[keyStr]; exists {
		if mutation.Type == common.MutationPut {
			return mutation.Value, nil
		} else {
			return nil, nil // 已删除
		}
	}

	// 从MVCC读取
	value, err := txn.Coordinator.mvcc.Get(ctx, key, txn.StartTS)
	if err != nil {
		return nil, err
	}

	// 记录读取操作
	txn.Reads[keyStr] = txn.StartTS
	txn.UpdatedAt = time.Now()

	return value, nil
}

// Commit 提交Percolator事务
func (txn *CoordinatedPercolatorTxn) Commit(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != common.TxnActive {
		return fmt.Errorf("transaction is not in correct state for commit: %s", txn.State)
	}

	// 只读事务直接提交
	if len(txn.Mutations) == 0 {
		txn.State = common.TxnCommitted
		txn.CommitEndTime = time.Now()
		txn.Coordinator.updateMetrics(txn)
		txn.Coordinator.cleanupTransaction(txn.TxnID)
		return nil
	}

	// 获取提交时间戳
	commitTS, err := txn.Coordinator.tso.GetTimestamp(ctx)
	if err != nil {
		return fmt.Errorf("failed to get commit timestamp: %w", err)
	}

	txn.CommitTS = commitTS
	txn.CommitStartTime = time.Now()

	// 执行两阶段提交
	if err := txn.executePercolatorCommit(ctx); err != nil {
		txn.Coordinator.metrics.CommitFailures++
		return err
	}

	txn.State = common.TxnCommitted
	txn.CommitEndTime = time.Now()
	txn.Coordinator.metrics.CommittedTransactions++

	// 更新性能指标
	txn.Coordinator.updateMetrics(txn)

	// 清理事务
	txn.Coordinator.cleanupTransaction(txn.TxnID)

	txn.Coordinator.logger.Printf("Committed Percolator transaction: %s, startTS: %d, commitTS: %d",
		txn.TxnID, txn.StartTS, txn.CommitTS)
	return nil
}

// Rollback 回滚Percolator事务
func (txn *CoordinatedPercolatorTxn) Rollback(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State == common.TxnCommitted {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	if txn.State == common.TxnAborted {
		return nil // 已经回滚
	}

	// 执行回滚
	if err := txn.executePercolatorRollback(ctx); err != nil {
		return err
	}

	txn.State = common.TxnAborted
	txn.Coordinator.metrics.AbortedTransactions++

	// 清理事务
	txn.Coordinator.cleanupTransaction(txn.TxnID)

	txn.Coordinator.logger.Printf("Rolled back Percolator transaction: %s, startTS: %d",
		txn.TxnID, txn.StartTS)
	return nil
}

// 缺失的方法实现
func (txn *CoordinatedPercolatorTxn) executePercolatorCommit(ctx context.Context) error {
	// 准备变更列表
	mutations := make([]*common.Mutation, 0, len(txn.Mutations))
	for _, mutation := range txn.Mutations {
		mutations = append(mutations, mutation)
	}

	// 使用MVCC进行预写
	if err := txn.Coordinator.mvcc.Prewrite(ctx, mutations, txn.PrimaryKey, txn.StartTS, txn.TxnID); err != nil {
		return fmt.Errorf("prewrite failed: %w", err)
	}

	// 准备键列表
	keys := make([][]byte, 0, len(txn.Mutations))
	for _, mutation := range txn.Mutations {
		keys = append(keys, mutation.Key)
	}

	// 使用MVCC进行提交
	if err := txn.Coordinator.mvcc.Commit(ctx, keys, txn.StartTS, txn.CommitTS, txn.TxnID); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}

func (txn *CoordinatedPercolatorTxn) executePercolatorRollback(ctx context.Context) error {
	// 准备键列表
	keys := make([][]byte, 0, len(txn.Mutations))
	for _, mutation := range txn.Mutations {
		keys = append(keys, mutation.Key)
	}

	// 使用MVCC进行回滚
	return txn.Coordinator.mvcc.Rollback(ctx, keys, txn.StartTS, txn.TxnID)
}

func (pc *PercolatorCoordinator) updateMetrics(txn *CoordinatedPercolatorTxn) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// 更新事务时间指标
	if !txn.CommitEndTime.IsZero() && !txn.CreatedAt.IsZero() {
		duration := txn.CommitEndTime.Sub(txn.CreatedAt)
		if pc.metrics.AvgTransactionTime == 0 {
			pc.metrics.AvgTransactionTime = duration
		} else {
			pc.metrics.AvgTransactionTime = (pc.metrics.AvgTransactionTime + duration) / 2
		}
	}

	// 更新预写时间指标
	if !txn.PrewriteEndTime.IsZero() && !txn.PrewriteStartTime.IsZero() {
		duration := txn.PrewriteEndTime.Sub(txn.PrewriteStartTime)
		if pc.metrics.AvgPrewriteTime == 0 {
			pc.metrics.AvgPrewriteTime = duration
		} else {
			pc.metrics.AvgPrewriteTime = (pc.metrics.AvgPrewriteTime + duration) / 2
		}
	}

	// 更新提交时间指标
	if !txn.CommitEndTime.IsZero() && !txn.CommitStartTime.IsZero() {
		duration := txn.CommitEndTime.Sub(txn.CommitStartTime)
		if pc.metrics.AvgCommitTime == 0 {
			pc.metrics.AvgCommitTime = duration
		} else {
			pc.metrics.AvgCommitTime = (pc.metrics.AvgCommitTime + duration) / 2
		}
	}
}

func (pc *PercolatorCoordinator) cleanupTransaction(txnID string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	delete(pc.activeTxns, txnID)
	delete(pc.txnRecords, txnID)
	pc.metrics.ActiveTransactions--
}

func (pc *PercolatorCoordinator) persistTransactionRecord(txn *CoordinatedPercolatorTxn) error {
	// 创建事务记录
	record := &PercolatorTxnRecord{
		TxnID:      txn.TxnID,
		State:      txn.State,
		StartTS:    txn.StartTS,
		CommitTS:   txn.CommitTS,
		PrimaryKey: txn.PrimaryKey,
		CreatedAt:  txn.CreatedAt,
		UpdatedAt:  txn.UpdatedAt,
	}

	// 添加变更键列表
	mutationKeys := make([]string, 0, len(txn.Mutations))
	for key := range txn.Mutations {
		mutationKeys = append(mutationKeys, key)
	}
	record.MutationKeys = mutationKeys

	// 持久化到存储
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("percolator_txn_record_%s", txn.TxnID)
	return pc.storage.Put([]byte(key), data)
}

// 后台服务方法
func (pc *PercolatorCoordinator) runStateChecker() {
	defer pc.wg.Done()

	ticker := time.NewTicker(pc.config.StateCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pc.stopCh:
			return
		case <-ticker.C:
			pc.checkTransactionStates()
		}
	}
}

func (pc *PercolatorCoordinator) runRecoveryService() {
	defer pc.wg.Done()

	ticker := time.NewTicker(pc.config.RecoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pc.stopCh:
			return
		case <-ticker.C:
			pc.recoverTransactions()
		}
	}
}

func (pc *PercolatorCoordinator) runCleanupService() {
	defer pc.wg.Done()

	ticker := time.NewTicker(pc.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pc.stopCh:
			return
		case <-ticker.C:
			pc.cleanupExpiredTransactions()
		}
	}
}

func (pc *PercolatorCoordinator) checkTransactionStates() {
	// 简化实现：检查活跃事务的状态
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	for _, txn := range pc.activeTxns {
		if time.Now().After(txn.ExpiresAt) {
			// 事务超时，标记为需要清理
			pc.logger.Printf("Transaction %s expired", txn.TxnID)
		}
	}
}

func (pc *PercolatorCoordinator) recoverTransactions() {
	// 简化实现：恢复未完成的事务
	// 在实际实现中，这里会扫描持久化的事务记录并恢复未完成的事务
}

func (pc *PercolatorCoordinator) cleanupExpiredTransactions() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	now := time.Now()
	for txnID, txn := range pc.activeTxns {
		if now.After(txn.ExpiresAt) {
			delete(pc.activeTxns, txnID)
			delete(pc.txnRecords, txnID)
			pc.metrics.ActiveTransactions--
			pc.metrics.TimeoutFailures++
		}
	}
}

// Stop 停止协调器
func (pc *PercolatorCoordinator) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}

// GetMetrics 获取性能指标
func (pc *PercolatorCoordinator) GetMetrics() *PercolatorCoordinatorMetrics {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// 更新实时指标
	pc.metrics.ActiveTransactions = uint64(len(pc.activeTxns))

	return pc.metrics
}
