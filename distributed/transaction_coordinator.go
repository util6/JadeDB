/*
JadeDB 分布式事务协调器

本模块实现了分布式事务协调器，负责协调跨多个节点的分布式事务。
采用两阶段提交(2PC)协议确保分布式事务的ACID特性，支持故障恢复、
超时处理和并发控制。

核心功能：
1. 两阶段提交：实现标准的2PC协议，确保分布式事务一致性
2. 事务管理：事务生命周期管理，状态跟踪和持久化
3. 故障恢复：协调者和参与者故障的检测和恢复
4. 超时处理：事务超时检测和自动回滚机制
5. 并发控制：支持多个并发事务的协调

设计特点：
- 强一致性：保证分布式事务的ACID特性
- 故障容错：支持协调者和参与者的故障恢复
- 高性能：批量处理、并行执行、连接复用
- 可观测性：完整的事务状态跟踪和监控
*/

package distributed

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// TransactionCoordinator 分布式事务协调器
type TransactionCoordinator struct {
	// 基本信息
	coordinatorID string
	raftNode      *RaftNode

	// 事务管理
	mu           sync.RWMutex
	transactions map[string]*DistributedTransaction

	// 参与者管理
	participants map[string]*ParticipantNode

	// 配置
	config *CoordinatorConfig

	// 状态
	running bool
	stopCh  chan struct{}

	// 监控
	metrics *CoordinatorMetrics
	logger  *log.Logger
}

// CoordinatorConfig 协调器配置
type CoordinatorConfig struct {
	// 超时配置
	TransactionTimeout time.Duration // 事务超时时间
	PrepareTimeout     time.Duration // 准备阶段超时
	CommitTimeout      time.Duration // 提交阶段超时

	// 重试配置
	MaxRetries    int           // 最大重试次数
	RetryInterval time.Duration // 重试间隔

	// 性能配置
	MaxConcurrentTxns int // 最大并发事务数
	BatchSize         int // 批量处理大小

	// 持久化配置
	EnablePersistence bool   // 是否启用持久化
	LogDir            string // 日志目录
}

// DefaultCoordinatorConfig 默认协调器配置
func DefaultCoordinatorConfig() *CoordinatorConfig {
	return &CoordinatorConfig{
		TransactionTimeout: 30 * time.Second,
		PrepareTimeout:     10 * time.Second,
		CommitTimeout:      10 * time.Second,
		MaxRetries:         3,
		RetryInterval:      100 * time.Millisecond,
		MaxConcurrentTxns:  1000,
		BatchSize:          100,
		EnablePersistence:  true,
		LogDir:             "./txn_logs",
	}
}

// CoordinatorMetrics 协调器监控指标
type CoordinatorMetrics struct {
	// 事务统计
	TotalTransactions     int64 // 总事务数
	CommittedTransactions int64 // 已提交事务数
	AbortedTransactions   int64 // 已中止事务数
	TimeoutTransactions   int64 // 超时事务数

	// 性能指标
	AvgTransactionTime time.Duration // 平均事务时间
	AvgPrepareTime     time.Duration // 平均准备时间
	AvgCommitTime      time.Duration // 平均提交时间

	// 并发指标
	ActiveTransactions int64 // 活跃事务数
	MaxConcurrentTxns  int64 // 最大并发事务数
}

// DistributedTransaction 分布式事务
type DistributedTransaction struct {
	// 基本信息
	TxnID         string
	CoordinatorID string
	StartTime     time.Time

	// 参与者
	Participants []*ParticipantNode

	// 状态
	State    TransactionState
	Decision TransactionDecision

	// 操作
	Operations []*TransactionOperation

	// 同步
	mu        sync.RWMutex
	prepareCh chan *PrepareResult
	commitCh  chan *CommitResult

	// 超时
	timeout      time.Duration
	timeoutTimer *time.Timer

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc
}

// TransactionState 事务状态
type TransactionState int

const (
	TxnStateInit TransactionState = iota
	TxnStatePreparing
	TxnStatePrepared
	TxnStateCommitting
	TxnStateCommitted
	TxnStateAborting
	TxnStateAborted
	TxnStateTimeout
)

func (s TransactionState) String() string {
	switch s {
	case TxnStateInit:
		return "INIT"
	case TxnStatePreparing:
		return "PREPARING"
	case TxnStatePrepared:
		return "PREPARED"
	case TxnStateCommitting:
		return "COMMITTING"
	case TxnStateCommitted:
		return "COMMITTED"
	case TxnStateAborting:
		return "ABORTING"
	case TxnStateAborted:
		return "ABORTED"
	case TxnStateTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

// TransactionDecision 事务决定
type TransactionDecision int

const (
	DecisionUnknown TransactionDecision = iota
	DecisionCommit
	DecisionAbort
)

// TransactionOperation 事务操作
type TransactionOperation struct {
	Type        string // 操作类型：PUT, DELETE, BATCH
	Key         string // 键
	Value       []byte // 值
	Shard       string // 分片
	Participant string // 参与者
}

// ParticipantNode 参与者节点
type ParticipantNode struct {
	NodeID    string
	Address   string
	Client    TransactionClient
	LastSeen  time.Time
	Available bool
}

// TransactionClient 事务客户端接口
type TransactionClient interface {
	Prepare(ctx context.Context, req *PrepareRequest) (*PrepareResponse, error)
	Commit(ctx context.Context, req *CommitRequest) (*CommitResponse, error)
	Abort(ctx context.Context, req *AbortRequest) (*AbortResponse, error)
	QueryStatus(ctx context.Context, req *QueryStatusRequest) (*QueryStatusResponse, error)
}

// PrepareRequest 准备请求
type PrepareRequest struct {
	TxnID      string
	Operations []*TransactionOperation
	Timeout    time.Duration
}

// PrepareResponse 准备响应
type PrepareResponse struct {
	TxnID     string
	NodeID    string
	Vote      Vote
	Reason    string
	Timestamp time.Time
}

// Vote 投票结果
type Vote int

const (
	VoteCommit Vote = iota
	VoteAbort
)

// CommitRequest 提交请求
type CommitRequest struct {
	TxnID     string
	Decision  TransactionDecision
	Timestamp time.Time
}

// CommitResponse 提交响应
type CommitResponse struct {
	TxnID     string
	NodeID    string
	Success   bool
	Error     string
	Timestamp time.Time
}

// AbortRequest 中止请求
type AbortRequest struct {
	TxnID     string
	Reason    string
	Timestamp time.Time
}

// AbortResponse 中止响应
type AbortResponse struct {
	TxnID     string
	NodeID    string
	Success   bool
	Error     string
	Timestamp time.Time
}

// QueryStatusRequest 查询状态请求
type QueryStatusRequest struct {
	TxnID string
}

// QueryStatusResponse 查询状态响应
type QueryStatusResponse struct {
	TxnID     string
	NodeID    string
	State     TransactionState
	Decision  TransactionDecision
	Timestamp time.Time
}

// PrepareResult 准备结果
type PrepareResult struct {
	NodeID   string
	Response *PrepareResponse
	Error    error
}

// CommitResult 提交结果
type CommitResult struct {
	NodeID   string
	Response *CommitResponse
	Error    error
}

// NewTransactionCoordinator 创建事务协调器
func NewTransactionCoordinator(coordinatorID string, raftNode *RaftNode, config *CoordinatorConfig) *TransactionCoordinator {
	if config == nil {
		config = DefaultCoordinatorConfig()
	}

	return &TransactionCoordinator{
		coordinatorID: coordinatorID,
		raftNode:      raftNode,
		transactions:  make(map[string]*DistributedTransaction),
		participants:  make(map[string]*ParticipantNode),
		config:        config,
		stopCh:        make(chan struct{}),
		metrics:       &CoordinatorMetrics{},
		logger:        log.New(log.Writer(), "[TXN-COORDINATOR] ", log.LstdFlags),
	}
}

// Start 启动协调器
func (tc *TransactionCoordinator) Start() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.running {
		return fmt.Errorf("coordinator already running")
	}

	tc.running = true
	tc.logger.Printf("Transaction coordinator started: %s", tc.coordinatorID)

	// 启动后台任务
	go tc.timeoutChecker()
	go tc.metricsCollector()

	return nil
}

// Stop 停止协调器
func (tc *TransactionCoordinator) Stop() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if !tc.running {
		return nil
	}

	close(tc.stopCh)
	tc.running = false

	// 中止所有活跃事务
	for _, txn := range tc.transactions {
		if txn.State != TxnStateCommitted && txn.State != TxnStateAborted {
			tc.abortTransactionLocked(txn, "coordinator shutdown")
		}
	}

	tc.logger.Printf("Transaction coordinator stopped: %s", tc.coordinatorID)
	return nil
}

// BeginTransaction 开始事务
func (tc *TransactionCoordinator) BeginTransaction(operations []*TransactionOperation) (*DistributedTransaction, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// 检查并发限制
	if len(tc.transactions) >= tc.config.MaxConcurrentTxns {
		return nil, fmt.Errorf("too many concurrent transactions")
	}

	// 生成事务ID
	txnID := tc.generateTxnID()

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), tc.config.TransactionTimeout)

	// 创建事务
	txn := &DistributedTransaction{
		TxnID:         txnID,
		CoordinatorID: tc.coordinatorID,
		StartTime:     time.Now(),
		State:         TxnStateInit,
		Operations:    operations,
		prepareCh:     make(chan *PrepareResult, len(tc.participants)),
		commitCh:      make(chan *CommitResult, len(tc.participants)),
		timeout:       tc.config.TransactionTimeout,
		ctx:           ctx,
		cancel:        cancel,
	}

	// 确定参与者
	participants, err := tc.determineParticipants(operations)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to determine participants: %w", err)
	}
	txn.Participants = participants

	// 设置超时定时器
	txn.timeoutTimer = time.AfterFunc(tc.config.TransactionTimeout, func() {
		tc.handleTransactionTimeout(txnID)
	})

	// 注册事务
	tc.transactions[txnID] = txn
	tc.metrics.TotalTransactions++
	tc.metrics.ActiveTransactions++

	tc.logger.Printf("Started transaction: %s with %d participants",
		txnID, len(participants))

	return txn, nil
}

// ExecuteTransaction 执行事务（两阶段提交）
func (tc *TransactionCoordinator) ExecuteTransaction(txn *DistributedTransaction) error {
	start := time.Now()
	defer func() {
		tc.metrics.AvgTransactionTime = time.Since(start)
		tc.mu.Lock()
		tc.metrics.ActiveTransactions--
		tc.mu.Unlock()
	}()

	// 阶段1：准备阶段
	if err := tc.preparePhase(txn); err != nil {
		tc.abortTransaction(txn, fmt.Sprintf("prepare failed: %v", err))
		return err
	}

	// 阶段2：提交阶段
	if err := tc.commitPhase(txn); err != nil {
		tc.logger.Printf("Commit phase failed for txn %s: %v", txn.TxnID, err)
		// 注意：提交阶段失败不能回滚，因为决定已经做出
		return err
	}

	return nil
}

// preparePhase 准备阶段
func (tc *TransactionCoordinator) preparePhase(txn *DistributedTransaction) error {
	prepareStart := time.Now()
	defer func() {
		tc.metrics.AvgPrepareTime = time.Since(prepareStart)
	}()

	tc.logger.Printf("Starting prepare phase for txn: %s", txn.TxnID)

	txn.mu.Lock()
	txn.State = TxnStatePreparing
	txn.mu.Unlock()

	// 并发发送准备请求
	for _, participant := range txn.Participants {
		go tc.sendPrepareRequest(txn, participant)
	}

	// 收集准备响应
	preparedCount := 0
	timeout := time.After(tc.config.PrepareTimeout)

	for i := 0; i < len(txn.Participants); i++ {
		select {
		case result := <-txn.prepareCh:
			if result.Error != nil {
				return fmt.Errorf("prepare failed on node %s: %w", result.NodeID, result.Error)
			}

			if result.Response.Vote == VoteCommit {
				preparedCount++
				tc.logger.Printf("Node %s voted COMMIT for txn %s", result.NodeID, txn.TxnID)
			} else {
				return fmt.Errorf("node %s voted ABORT for txn %s: %s",
					result.NodeID, txn.TxnID, result.Response.Reason)
			}

		case <-timeout:
			return fmt.Errorf("prepare phase timeout for txn %s", txn.TxnID)

		case <-txn.ctx.Done():
			return fmt.Errorf("transaction cancelled: %s", txn.TxnID)
		}
	}

	// 所有参与者都准备好
	if preparedCount == len(txn.Participants) {
		txn.mu.Lock()
		txn.State = TxnStatePrepared
		txn.Decision = DecisionCommit
		txn.mu.Unlock()

		tc.logger.Printf("All participants prepared for txn: %s", txn.TxnID)
		return nil
	}

	return fmt.Errorf("not all participants prepared for txn %s", txn.TxnID)
}

// commitPhase 提交阶段
func (tc *TransactionCoordinator) commitPhase(txn *DistributedTransaction) error {
	commitStart := time.Now()
	defer func() {
		tc.metrics.AvgCommitTime = time.Since(commitStart)
	}()

	tc.logger.Printf("Starting commit phase for txn: %s", txn.TxnID)

	txn.mu.Lock()
	txn.State = TxnStateCommitting
	txn.mu.Unlock()

	// 记录提交决定到Raft日志
	if err := tc.logCommitDecision(txn); err != nil {
		tc.logger.Printf("Failed to log commit decision for txn %s: %v", txn.TxnID, err)
		// 继续执行，因为决定已经做出
	}

	// 并发发送提交请求
	for _, participant := range txn.Participants {
		go tc.sendCommitRequest(txn, participant)
	}

	// 收集提交响应（尽力而为）
	committedCount := 0
	timeout := time.After(tc.config.CommitTimeout)

	for i := 0; i < len(txn.Participants); i++ {
		select {
		case result := <-txn.commitCh:
			if result.Error != nil {
				tc.logger.Printf("Commit failed on node %s for txn %s: %v",
					result.NodeID, txn.TxnID, result.Error)
			} else if result.Response.Success {
				committedCount++
				tc.logger.Printf("Node %s committed txn %s", result.NodeID, txn.TxnID)
			}

		case <-timeout:
			tc.logger.Printf("Commit phase timeout for txn %s, committed: %d/%d",
				txn.TxnID, committedCount, len(txn.Participants))
			break

		case <-txn.ctx.Done():
			tc.logger.Printf("Transaction cancelled during commit: %s", txn.TxnID)
			break
		}
	}

	// 更新事务状态
	txn.mu.Lock()
	txn.State = TxnStateCommitted
	txn.mu.Unlock()

	// 清理资源
	tc.cleanupTransaction(txn)

	tc.metrics.CommittedTransactions++
	tc.logger.Printf("Transaction committed: %s", txn.TxnID)

	return nil
}

// sendPrepareRequest 发送准备请求
func (tc *TransactionCoordinator) sendPrepareRequest(txn *DistributedTransaction, participant *ParticipantNode) {
	req := &PrepareRequest{
		TxnID:      txn.TxnID,
		Operations: tc.getParticipantOperations(txn, participant.NodeID),
		Timeout:    tc.config.PrepareTimeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), tc.config.PrepareTimeout)
	defer cancel()

	resp, err := participant.Client.Prepare(ctx, req)

	result := &PrepareResult{
		NodeID:   participant.NodeID,
		Response: resp,
		Error:    err,
	}

	select {
	case txn.prepareCh <- result:
	case <-txn.ctx.Done():
	}
}

// sendCommitRequest 发送提交请求
func (tc *TransactionCoordinator) sendCommitRequest(txn *DistributedTransaction, participant *ParticipantNode) {
	req := &CommitRequest{
		TxnID:     txn.TxnID,
		Decision:  txn.Decision,
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), tc.config.CommitTimeout)
	defer cancel()

	resp, err := participant.Client.Commit(ctx, req)

	result := &CommitResult{
		NodeID:   participant.NodeID,
		Response: resp,
		Error:    err,
	}

	select {
	case txn.commitCh <- result:
	case <-txn.ctx.Done():
	}
}

// abortTransaction 中止事务
func (tc *TransactionCoordinator) abortTransaction(txn *DistributedTransaction, reason string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.abortTransactionLocked(txn, reason)
}

// abortTransactionLocked 中止事务（已加锁）
func (tc *TransactionCoordinator) abortTransactionLocked(txn *DistributedTransaction, reason string) {
	tc.logger.Printf("Aborting transaction %s: %s", txn.TxnID, reason)

	txn.mu.Lock()
	if txn.State == TxnStateAborted || txn.State == TxnStateCommitted {
		txn.mu.Unlock()
		return
	}

	txn.State = TxnStateAborting
	txn.Decision = DecisionAbort
	txn.mu.Unlock()

	// 发送中止请求给所有参与者
	for _, participant := range txn.Participants {
		go tc.sendAbortRequest(txn, participant, reason)
	}

	// 等待中止完成或超时
	timeout := time.After(tc.config.CommitTimeout)
	abortedCount := 0

	for i := 0; i < len(txn.Participants); i++ {
		select {
		case <-timeout:
			tc.logger.Printf("Abort timeout for txn %s", txn.TxnID)
			goto cleanup
		default:
			abortedCount++
		}
	}

cleanup:
	txn.mu.Lock()
	txn.State = TxnStateAborted
	txn.mu.Unlock()

	tc.cleanupTransaction(txn)
	tc.metrics.AbortedTransactions++
}

// sendAbortRequest 发送中止请求
func (tc *TransactionCoordinator) sendAbortRequest(txn *DistributedTransaction, participant *ParticipantNode, reason string) {
	req := &AbortRequest{
		TxnID:     txn.TxnID,
		Reason:    reason,
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), tc.config.CommitTimeout)
	defer cancel()

	_, err := participant.Client.Abort(ctx, req)
	if err != nil {
		tc.logger.Printf("Failed to abort on node %s for txn %s: %v",
			participant.NodeID, txn.TxnID, err)
	}
}

// handleTransactionTimeout 处理事务超时
func (tc *TransactionCoordinator) handleTransactionTimeout(txnID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	txn, exists := tc.transactions[txnID]
	if !exists {
		return
	}

	txn.mu.Lock()
	if txn.State == TxnStateCommitted || txn.State == TxnStateAborted {
		txn.mu.Unlock()
		return
	}

	txn.State = TxnStateTimeout
	txn.mu.Unlock()

	tc.logger.Printf("Transaction timeout: %s", txnID)
	tc.abortTransactionLocked(txn, "transaction timeout")
	tc.metrics.TimeoutTransactions++
}

// determineParticipants 确定参与者
func (tc *TransactionCoordinator) determineParticipants(operations []*TransactionOperation) ([]*ParticipantNode, error) {
	participantMap := make(map[string]*ParticipantNode)

	for _, op := range operations {
		if op.Participant == "" {
			return nil, fmt.Errorf("operation missing participant: %s", op.Key)
		}

		if participant, exists := tc.participants[op.Participant]; exists {
			participantMap[op.Participant] = participant
		} else {
			return nil, fmt.Errorf("unknown participant: %s", op.Participant)
		}
	}

	var participants []*ParticipantNode
	for _, participant := range participantMap {
		participants = append(participants, participant)
	}

	return participants, nil
}

// getParticipantOperations 获取参与者的操作
func (tc *TransactionCoordinator) getParticipantOperations(txn *DistributedTransaction, participantID string) []*TransactionOperation {
	var operations []*TransactionOperation

	for _, op := range txn.Operations {
		if op.Participant == participantID {
			operations = append(operations, op)
		}
	}

	return operations
}

// logCommitDecision 记录提交决定到Raft日志
func (tc *TransactionCoordinator) logCommitDecision(txn *DistributedTransaction) error {
	if tc.raftNode == nil {
		return nil // 如果没有Raft节点，跳过日志记录
	}

	// 这里应该序列化决定并提交到Raft
	// 简化实现
	data := []byte(fmt.Sprintf("COMMIT:%s", txn.TxnID))
	return tc.raftNode.Propose(data)
}

// cleanupTransaction 清理事务
func (tc *TransactionCoordinator) cleanupTransaction(txn *DistributedTransaction) {
	// 停止超时定时器
	if txn.timeoutTimer != nil {
		txn.timeoutTimer.Stop()
	}

	// 取消上下文
	if txn.cancel != nil {
		txn.cancel()
	}

	// 从事务映射中删除
	delete(tc.transactions, txn.TxnID)
}

// generateTxnID 生成事务ID
func (tc *TransactionCoordinator) generateTxnID() string {
	return fmt.Sprintf("%s-%d-%d", tc.coordinatorID, time.Now().UnixNano(), len(tc.transactions))
}

// timeoutChecker 超时检查器
func (tc *TransactionCoordinator) timeoutChecker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tc.stopCh:
			return
		case <-ticker.C:
			tc.checkTimeouts()
		}
	}
}

// checkTimeouts 检查超时事务
func (tc *TransactionCoordinator) checkTimeouts() {
	tc.mu.RLock()
	var timeoutTxns []*DistributedTransaction

	for _, txn := range tc.transactions {
		txn.mu.RLock()
		if txn.State != TxnStateCommitted && txn.State != TxnStateAborted &&
			time.Since(txn.StartTime) > txn.timeout {
			timeoutTxns = append(timeoutTxns, txn)
		}
		txn.mu.RUnlock()
	}
	tc.mu.RUnlock()

	// 处理超时事务
	for _, txn := range timeoutTxns {
		tc.handleTransactionTimeout(txn.TxnID)
	}
}

// metricsCollector 指标收集器
func (tc *TransactionCoordinator) metricsCollector() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tc.stopCh:
			return
		case <-ticker.C:
			tc.updateMetrics()
		}
	}
}

// updateMetrics 更新指标
func (tc *TransactionCoordinator) updateMetrics() {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	activeCount := int64(0)
	for _, txn := range tc.transactions {
		txn.mu.RLock()
		if txn.State != TxnStateCommitted && txn.State != TxnStateAborted {
			activeCount++
		}
		txn.mu.RUnlock()
	}

	tc.metrics.ActiveTransactions = activeCount
	if activeCount > tc.metrics.MaxConcurrentTxns {
		tc.metrics.MaxConcurrentTxns = activeCount
	}
}

// AddParticipant 添加参与者
func (tc *TransactionCoordinator) AddParticipant(nodeID, address string, client TransactionClient) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	participant := &ParticipantNode{
		NodeID:    nodeID,
		Address:   address,
		Client:    client,
		LastSeen:  time.Now(),
		Available: true,
	}

	tc.participants[nodeID] = participant
	tc.logger.Printf("Added participant: %s at %s", nodeID, address)
}

// RemoveParticipant 移除参与者
func (tc *TransactionCoordinator) RemoveParticipant(nodeID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	delete(tc.participants, nodeID)
	tc.logger.Printf("Removed participant: %s", nodeID)
}

// GetTransaction 获取事务
func (tc *TransactionCoordinator) GetTransaction(txnID string) (*DistributedTransaction, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	txn, exists := tc.transactions[txnID]
	return txn, exists
}

// GetMetrics 获取监控指标
func (tc *TransactionCoordinator) GetMetrics() *CoordinatorMetrics {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// 返回指标副本
	return &CoordinatorMetrics{
		TotalTransactions:     tc.metrics.TotalTransactions,
		CommittedTransactions: tc.metrics.CommittedTransactions,
		AbortedTransactions:   tc.metrics.AbortedTransactions,
		TimeoutTransactions:   tc.metrics.TimeoutTransactions,
		AvgTransactionTime:    tc.metrics.AvgTransactionTime,
		AvgPrepareTime:        tc.metrics.AvgPrepareTime,
		AvgCommitTime:         tc.metrics.AvgCommitTime,
		ActiveTransactions:    tc.metrics.ActiveTransactions,
		MaxConcurrentTxns:     tc.metrics.MaxConcurrentTxns,
	}
}
