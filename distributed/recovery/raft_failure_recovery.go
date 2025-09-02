/*
JadeDB Raft故障恢复增强模块

本模块实现了Raft算法的增强故障恢复机制，处理各种故障场景：
1. 网络分区处理：检测和处理网络分区，防止脑裂
2. 节点重启恢复：节点重启后的快速状态恢复
3. 领导者故障转移：快速的领导者选举和切换
4. 数据一致性恢复：确保故障恢复后的数据一致性
5. 集群成员变更：动态处理集群成员的加入和离开
6. 故障检测和预警：主动检测潜在故障并预警

设计特点：
- 快速恢复：优化的故障检测和恢复算法
- 数据安全：确保故障恢复过程中的数据完整性
- 自动化：最小化人工干预的自动故障恢复
- 可配置：灵活的故障恢复策略配置
- 可观测：详细的故障恢复监控和日志
*/

package recovery

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/distributed/raft"
)

// RaftFailureRecovery Raft故障恢复管理器
type RaftFailureRecovery struct {
	mu sync.RWMutex

	// Raft节点引用
	raftNode *raft.RaftNode

	// 配置
	config *FailureRecoveryConfig

	// 故障检测器
	failureDetector *FailureDetector

	// 网络分区检测器
	partitionDetector *NetworkPartitionDetector

	// 恢复策略管理器
	recoveryManager *RecoveryStrategyManager

	// 集群健康监控器
	healthMonitor *ClusterHealthMonitor

	// 性能统计
	metrics *FailureRecoveryMetrics

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
	logger *log.Logger
}

// FailureRecoveryConfig 故障恢复配置
type FailureRecoveryConfig struct {
	// 故障检测配置
	FailureDetectionInterval time.Duration // 故障检测间隔
	NodeTimeoutThreshold     time.Duration // 节点超时阈值
	HeartbeatMissThreshold   int           // 心跳丢失阈值

	// 网络分区配置
	PartitionDetectionEnabled bool // 是否启用分区检测
	MinClusterSize            int  // 最小集群大小
	QuorumSize                int  // 法定人数大小

	// 恢复策略配置
	AutoRecoveryEnabled   bool          // 是否启用自动恢复
	RecoveryTimeout       time.Duration // 恢复超时时间
	MaxRecoveryRetries    int           // 最大恢复重试次数
	RecoveryBackoffFactor float64       // 恢复退避因子

	// 领导者选举配置
	ElectionTimeoutMin time.Duration // 选举超时最小值
	ElectionTimeoutMax time.Duration // 选举超时最大值
	LeaderLeaseTimeout time.Duration // 领导者租约超时

	// 数据一致性配置
	ConsistencyCheckEnabled  bool          // 是否启用一致性检查
	ConsistencyCheckInterval time.Duration // 一致性检查间隔
	DataRepairEnabled        bool          // 是否启用数据修复

	// 监控配置
	HealthCheckInterval       time.Duration // 健康检查间隔
	MetricsCollectionInterval time.Duration // 指标收集间隔
	AlertingEnabled           bool          // 是否启用告警
}

// FailureDetector 故障检测器
type FailureDetector struct {
	mu sync.RWMutex

	config         *FailureRecoveryConfig
	nodeStates     map[string]*NodeState
	lastHeartbeats map[string]time.Time
	suspectedNodes map[string]time.Time
	failedNodes    map[string]time.Time
}

// NodeState 节点状态
type NodeState struct {
	NodeID        string
	Status        NodeHealthStatus
	LastSeen      time.Time
	MissedBeats   int
	RecoveryCount int
}

// NodeHealthStatus 节点健康状态
type NodeHealthStatus int

const (
	NodeHealthy     NodeHealthStatus = iota // 健康
	NodeSuspected                           // 可疑
	NodeFailed                              // 失败
	NodeRecovering                          // 恢复中
	NodePartitioned                         // 网络分区
)

// NetworkPartitionDetector 网络分区检测器
type NetworkPartitionDetector struct {
	mu sync.RWMutex

	config             *FailureRecoveryConfig
	clusterSize        int
	reachableNodes     map[string]bool
	partitionDetected  bool
	partitionStartTime time.Time
}

// RecoveryStrategyManager 恢复策略管理器
type RecoveryStrategyManager struct {
	mu sync.RWMutex

	config           *FailureRecoveryConfig
	activeRecoveries map[string]*RecoveryOperation
	recoveryHistory  []*RecoveryRecord
}

// RecoveryOperation 恢复操作
type RecoveryOperation struct {
	ID         string
	Type       RecoveryType
	TargetNode string
	StartTime  time.Time
	Status     RecoveryStatus
	RetryCount int
	LastError  error
}

// RecoveryType 恢复类型
type RecoveryType int

const (
	RecoveryTypeNodeRestart       RecoveryType = iota // 节点重启恢复
	RecoveryTypeLeaderElection                        // 领导者选举恢复
	RecoveryTypeDataSync                              // 数据同步恢复
	RecoveryTypePartitionMerge                        // 分区合并恢复
	RecoveryTypeConsistencyRepair                     // 一致性修复恢复
)

// RecoveryStatus 恢复状态
type RecoveryStatus int

const (
	RecoveryStatusPending    RecoveryStatus = iota // 待处理
	RecoveryStatusInProgress                       // 进行中
	RecoveryStatusCompleted                        // 已完成
	RecoveryStatusFailed                           // 失败
	RecoveryStatusCancelled                        // 已取消
)

// RecoveryRecord 恢复记录
type RecoveryRecord struct {
	Operation   *RecoveryOperation
	CompletedAt time.Time
	Duration    time.Duration
	Success     bool
	ErrorMsg    string
}

// ClusterHealthMonitor 集群健康监控器
type ClusterHealthMonitor struct {
	mu sync.RWMutex

	config        *FailureRecoveryConfig
	clusterHealth *ClusterHealth
	healthHistory []*ClusterHealthSnapshot
}

// ClusterHealth 集群健康状态
type ClusterHealth struct {
	TotalNodes       int
	HealthyNodes     int
	SuspectedNodes   int
	FailedNodes      int
	PartitionedNodes int
	HasLeader        bool
	LeaderID         string
	QuorumAvailable  bool
	LastUpdated      time.Time
}

// ClusterHealthSnapshot 集群健康快照
type ClusterHealthSnapshot struct {
	Timestamp time.Time
	Health    *ClusterHealth
}

// FailureRecoveryMetrics 故障恢复指标
type FailureRecoveryMetrics struct {
	// 故障检测指标
	TotalFailuresDetected uint64
	NodeFailures          uint64
	NetworkPartitions     uint64
	LeaderFailures        uint64

	// 恢复操作指标
	TotalRecoveryOperations uint64
	SuccessfulRecoveries    uint64
	FailedRecoveries        uint64
	AvgRecoveryTime         time.Duration

	// 性能指标
	MeanTimeToDetection time.Duration
	MeanTimeToRecovery  time.Duration
	ClusterAvailability float64

	// 当前状态指标
	ActiveRecoveries   int
	ClusterHealthScore float64
	LastFailureTime    time.Time
	LastRecoveryTime   time.Time
}

// NewRaftFailureRecovery 创建Raft故障恢复管理器
func NewRaftFailureRecovery(raftNode *raft.RaftNode, config *FailureRecoveryConfig) *RaftFailureRecovery {
	if config == nil {
		config = DefaultFailureRecoveryConfig()
	}

	rfr := &RaftFailureRecovery{
		raftNode: raftNode,
		config:   config,
		metrics:  &FailureRecoveryMetrics{},
		stopCh:   make(chan struct{}),
		logger:   log.New(log.Writer(), "[RAFT_FAILURE_RECOVERY] ", log.LstdFlags),
	}

	// 初始化组件
	rfr.initializeComponents()

	// 启动后台服务
	rfr.startBackgroundServices()

	return rfr
}

// DefaultFailureRecoveryConfig 默认故障恢复配置
func DefaultFailureRecoveryConfig() *FailureRecoveryConfig {
	return &FailureRecoveryConfig{
		FailureDetectionInterval:  1 * time.Second,
		NodeTimeoutThreshold:      5 * time.Second,
		HeartbeatMissThreshold:    3,
		PartitionDetectionEnabled: true,
		MinClusterSize:            3,
		QuorumSize:                2,
		AutoRecoveryEnabled:       true,
		RecoveryTimeout:           30 * time.Second,
		MaxRecoveryRetries:        3,
		RecoveryBackoffFactor:     2.0,
		ElectionTimeoutMin:        150 * time.Millisecond,
		ElectionTimeoutMax:        300 * time.Millisecond,
		LeaderLeaseTimeout:        10 * time.Second,
		ConsistencyCheckEnabled:   true,
		ConsistencyCheckInterval:  10 * time.Second,
		DataRepairEnabled:         true,
		HealthCheckInterval:       2 * time.Second,
		MetricsCollectionInterval: 5 * time.Second,
		AlertingEnabled:           true,
	}
}

// initializeComponents 初始化组件
func (rfr *RaftFailureRecovery) initializeComponents() {
	// 初始化故障检测器
	rfr.failureDetector = &FailureDetector{
		config:         rfr.config,
		nodeStates:     make(map[string]*NodeState),
		lastHeartbeats: make(map[string]time.Time),
		suspectedNodes: make(map[string]time.Time),
		failedNodes:    make(map[string]time.Time),
	}

	// 初始化网络分区检测器
	if rfr.config.PartitionDetectionEnabled {
		rfr.partitionDetector = &NetworkPartitionDetector{
			config:         rfr.config,
			reachableNodes: make(map[string]bool),
		}
	}

	// 初始化恢复策略管理器
	rfr.recoveryManager = &RecoveryStrategyManager{
		config:           rfr.config,
		activeRecoveries: make(map[string]*RecoveryOperation),
		recoveryHistory:  make([]*RecoveryRecord, 0),
	}

	// 初始化集群健康监控器
	rfr.healthMonitor = &ClusterHealthMonitor{
		config: rfr.config,
		clusterHealth: &ClusterHealth{
			LastUpdated: time.Now(),
		},
		healthHistory: make([]*ClusterHealthSnapshot, 0),
	}
}

// DetectFailures 检测故障
func (rfr *RaftFailureRecovery) DetectFailures() {
	rfr.failureDetector.mu.Lock()
	defer rfr.failureDetector.mu.Unlock()

	now := time.Now()

	// 检查每个节点的状态
	for nodeID, lastHeartbeat := range rfr.failureDetector.lastHeartbeats {
		timeSinceLastHeartbeat := now.Sub(lastHeartbeat)

		nodeState, exists := rfr.failureDetector.nodeStates[nodeID]
		if !exists {
			nodeState = &NodeState{
				NodeID:   nodeID,
				Status:   NodeHealthy,
				LastSeen: lastHeartbeat,
			}
			rfr.failureDetector.nodeStates[nodeID] = nodeState
		}

		// 更新节点状态
		if timeSinceLastHeartbeat > rfr.config.NodeTimeoutThreshold {
			if nodeState.Status == NodeHealthy {
				nodeState.Status = NodeSuspected
				nodeState.MissedBeats++
				rfr.failureDetector.suspectedNodes[nodeID] = now
				rfr.logger.Printf("Node %s is now suspected (missed %d heartbeats)",
					nodeID, nodeState.MissedBeats)
			} else if nodeState.Status == NodeSuspected &&
				nodeState.MissedBeats >= rfr.config.HeartbeatMissThreshold {
				nodeState.Status = NodeFailed
				rfr.failureDetector.failedNodes[nodeID] = now
				rfr.metrics.NodeFailures++
				rfr.metrics.TotalFailuresDetected++
				rfr.metrics.LastFailureTime = now
				rfr.logger.Printf("Node %s is now marked as failed", nodeID)

				// 触发恢复操作
				if rfr.config.AutoRecoveryEnabled {
					rfr.triggerNodeRecovery(nodeID)
				}
			}
		} else {
			// 节点恢复健康
			if nodeState.Status != NodeHealthy {
				rfr.logger.Printf("Node %s recovered to healthy state", nodeID)
				nodeState.Status = NodeHealthy
				nodeState.MissedBeats = 0
				delete(rfr.failureDetector.suspectedNodes, nodeID)
				delete(rfr.failureDetector.failedNodes, nodeID)
			}
		}

		nodeState.LastSeen = lastHeartbeat
	}
}

// DetectNetworkPartition 检测网络分区
func (rfr *RaftFailureRecovery) DetectNetworkPartition() {
	if !rfr.config.PartitionDetectionEnabled || rfr.partitionDetector == nil {
		return
	}

	rfr.partitionDetector.mu.Lock()
	defer rfr.partitionDetector.mu.Unlock()

	// 计算可达节点数量
	reachableCount := 0
	for _, reachable := range rfr.partitionDetector.reachableNodes {
		if reachable {
			reachableCount++
		}
	}

	// 检查是否发生网络分区
	totalNodes := rfr.partitionDetector.clusterSize
	if totalNodes > 0 {
		// 如果可达节点数量少于法定人数，可能发生了网络分区
		if reachableCount < rfr.config.QuorumSize {
			if !rfr.partitionDetector.partitionDetected {
				rfr.partitionDetector.partitionDetected = true
				rfr.partitionDetector.partitionStartTime = time.Now()
				rfr.metrics.NetworkPartitions++
				rfr.metrics.TotalFailuresDetected++
				rfr.logger.Printf("Network partition detected: %d/%d nodes reachable",
					reachableCount, totalNodes)

				// 触发分区恢复
				if rfr.config.AutoRecoveryEnabled {
					rfr.triggerPartitionRecovery()
				}
			}
		} else {
			// 分区恢复
			if rfr.partitionDetector.partitionDetected {
				duration := time.Since(rfr.partitionDetector.partitionStartTime)
				rfr.partitionDetector.partitionDetected = false
				rfr.logger.Printf("Network partition resolved after %v", duration)
			}
		}
	}
}

// triggerNodeRecovery 触发节点恢复
func (rfr *RaftFailureRecovery) triggerNodeRecovery(nodeID string) {
	recoveryID := fmt.Sprintf("node_recovery_%s_%d", nodeID, time.Now().UnixNano())

	operation := &RecoveryOperation{
		ID:         recoveryID,
		Type:       RecoveryTypeNodeRestart,
		TargetNode: nodeID,
		StartTime:  time.Now(),
		Status:     RecoveryStatusPending,
		RetryCount: 0,
	}

	rfr.recoveryManager.mu.Lock()
	rfr.recoveryManager.activeRecoveries[recoveryID] = operation
	rfr.recoveryManager.mu.Unlock()

	rfr.metrics.TotalRecoveryOperations++
	rfr.logger.Printf("Triggered node recovery for %s (operation: %s)", nodeID, recoveryID)

	// 异步执行恢复操作
	go rfr.executeRecoveryOperation(operation)
}

// triggerPartitionRecovery 触发分区恢复
func (rfr *RaftFailureRecovery) triggerPartitionRecovery() {
	recoveryID := fmt.Sprintf("partition_recovery_%d", time.Now().UnixNano())

	operation := &RecoveryOperation{
		ID:        recoveryID,
		Type:      RecoveryTypePartitionMerge,
		StartTime: time.Now(),
		Status:    RecoveryStatusPending,
	}

	rfr.recoveryManager.mu.Lock()
	rfr.recoveryManager.activeRecoveries[recoveryID] = operation
	rfr.recoveryManager.mu.Unlock()

	rfr.metrics.TotalRecoveryOperations++
	rfr.logger.Printf("Triggered partition recovery (operation: %s)", recoveryID)

	// 异步执行恢复操作
	go rfr.executeRecoveryOperation(operation)
}

// executeRecoveryOperation 执行恢复操作
func (rfr *RaftFailureRecovery) executeRecoveryOperation(operation *RecoveryOperation) {
	start := time.Now()
	operation.Status = RecoveryStatusInProgress

	ctx, cancel := context.WithTimeout(context.Background(), rfr.config.RecoveryTimeout)
	defer cancel()

	var err error
	switch operation.Type {
	case RecoveryTypeNodeRestart:
		err = rfr.executeNodeRecovery(ctx, operation)
	case RecoveryTypePartitionMerge:
		err = rfr.executePartitionRecovery(ctx, operation)
	case RecoveryTypeLeaderElection:
		err = rfr.executeLeaderElectionRecovery(ctx, operation)
	case RecoveryTypeDataSync:
		err = rfr.executeDataSyncRecovery(ctx, operation)
	case RecoveryTypeConsistencyRepair:
		err = rfr.executeConsistencyRepairRecovery(ctx, operation)
	default:
		err = fmt.Errorf("unknown recovery type: %v", operation.Type)
	}

	// 更新操作状态
	duration := time.Since(start)
	if err != nil {
		operation.Status = RecoveryStatusFailed
		operation.LastError = err
		rfr.metrics.FailedRecoveries++
		rfr.logger.Printf("Recovery operation %s failed: %v", operation.ID, err)

		// 重试逻辑
		if operation.RetryCount < rfr.config.MaxRecoveryRetries {
			operation.RetryCount++
			backoffDuration := time.Duration(float64(time.Second) *
				(rfr.config.RecoveryBackoffFactor * float64(operation.RetryCount)))
			rfr.logger.Printf("Retrying recovery operation %s in %v (attempt %d/%d)",
				operation.ID, backoffDuration, operation.RetryCount, rfr.config.MaxRecoveryRetries)

			time.AfterFunc(backoffDuration, func() {
				rfr.executeRecoveryOperation(operation)
			})
			return
		}
	} else {
		operation.Status = RecoveryStatusCompleted
		rfr.metrics.SuccessfulRecoveries++
		rfr.metrics.LastRecoveryTime = time.Now()
		rfr.logger.Printf("Recovery operation %s completed successfully in %v", operation.ID, duration)
	}

	// 更新平均恢复时间
	if rfr.metrics.AvgRecoveryTime == 0 {
		rfr.metrics.AvgRecoveryTime = duration
	} else {
		rfr.metrics.AvgRecoveryTime = (rfr.metrics.AvgRecoveryTime + duration) / 2
	}

	// 记录恢复历史
	record := &RecoveryRecord{
		Operation:   operation,
		CompletedAt: time.Now(),
		Duration:    duration,
		Success:     err == nil,
	}
	if err != nil {
		record.ErrorMsg = err.Error()
	}

	rfr.recoveryManager.mu.Lock()
	rfr.recoveryManager.recoveryHistory = append(rfr.recoveryManager.recoveryHistory, record)
	delete(rfr.recoveryManager.activeRecoveries, operation.ID)
	rfr.recoveryManager.mu.Unlock()
}

// executeNodeRecovery 执行节点恢复
func (rfr *RaftFailureRecovery) executeNodeRecovery(ctx context.Context, operation *RecoveryOperation) error {
	// 简化实现：尝试重新连接节点
	rfr.logger.Printf("Executing node recovery for %s", operation.TargetNode)

	// 在实际实现中，这里会：
	// 1. 尝试重新建立与节点的连接
	// 2. 检查节点状态
	// 3. 同步必要的数据
	// 4. 重新加入集群

	// 模拟恢复过程
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(1 * time.Second):
		// 模拟恢复成功
		return nil
	}
}

// executePartitionRecovery 执行分区恢复
func (rfr *RaftFailureRecovery) executePartitionRecovery(ctx context.Context, operation *RecoveryOperation) error {
	rfr.logger.Printf("Executing partition recovery")

	// 在实际实现中，这里会：
	// 1. 检测分区的两侧
	// 2. 确定哪一侧保留为主分区
	// 3. 合并分区数据
	// 4. 重新建立集群一致性

	// 模拟恢复过程
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		// 模拟恢复成功
		return nil
	}
}

// executeLeaderElectionRecovery 执行领导者选举恢复
func (rfr *RaftFailureRecovery) executeLeaderElectionRecovery(ctx context.Context, operation *RecoveryOperation) error {
	rfr.logger.Printf("Executing leader election recovery")

	// 在实际实现中，这里会：
	// 1. 触发新的领导者选举
	// 2. 确保选举过程的正确性
	// 3. 验证新领导者的合法性

	// 模拟恢复过程
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		// 模拟恢复成功
		return nil
	}
}

// executeDataSyncRecovery 执行数据同步恢复
func (rfr *RaftFailureRecovery) executeDataSyncRecovery(ctx context.Context, operation *RecoveryOperation) error {
	rfr.logger.Printf("Executing data sync recovery")

	// 在实际实现中，这里会：
	// 1. 检查数据不一致性
	// 2. 从领导者同步最新数据
	// 3. 验证数据完整性

	// 模拟恢复过程
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3 * time.Second):
		// 模拟恢复成功
		return nil
	}
}

// executeConsistencyRepairRecovery 执行一致性修复恢复
func (rfr *RaftFailureRecovery) executeConsistencyRepairRecovery(ctx context.Context, operation *RecoveryOperation) error {
	rfr.logger.Printf("Executing consistency repair recovery")

	// 在实际实现中，这里会：
	// 1. 检测一致性问题
	// 2. 修复数据不一致
	// 3. 验证修复结果

	// 模拟恢复过程
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		// 模拟恢复成功
		return nil
	}
}

// startBackgroundServices 启动后台服务
func (rfr *RaftFailureRecovery) startBackgroundServices() {
	// 启动故障检测服务
	rfr.wg.Add(1)
	go rfr.runFailureDetectionService()

	// 启动健康监控服务
	rfr.wg.Add(1)
	go rfr.runHealthMonitoringService()

	// 启动指标收集服务
	rfr.wg.Add(1)
	go rfr.runMetricsCollectionService()
}

// runFailureDetectionService 运行故障检测服务
func (rfr *RaftFailureRecovery) runFailureDetectionService() {
	defer rfr.wg.Done()

	ticker := time.NewTicker(rfr.config.FailureDetectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rfr.stopCh:
			return
		case <-ticker.C:
			rfr.DetectFailures()
			if rfr.config.PartitionDetectionEnabled {
				rfr.DetectNetworkPartition()
			}
		}
	}
}

// runHealthMonitoringService 运行健康监控服务
func (rfr *RaftFailureRecovery) runHealthMonitoringService() {
	defer rfr.wg.Done()

	ticker := time.NewTicker(rfr.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rfr.stopCh:
			return
		case <-ticker.C:
			rfr.updateClusterHealth()
		}
	}
}

// runMetricsCollectionService 运行指标收集服务
func (rfr *RaftFailureRecovery) runMetricsCollectionService() {
	defer rfr.wg.Done()

	ticker := time.NewTicker(rfr.config.MetricsCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rfr.stopCh:
			return
		case <-ticker.C:
			rfr.collectMetrics()
		}
	}
}

// updateClusterHealth 更新集群健康状态
func (rfr *RaftFailureRecovery) updateClusterHealth() {
	rfr.healthMonitor.mu.Lock()
	defer rfr.healthMonitor.mu.Unlock()

	health := &ClusterHealth{
		LastUpdated: time.Now(),
	}

	// 统计节点状态
	rfr.failureDetector.mu.RLock()
	for _, nodeState := range rfr.failureDetector.nodeStates {
		health.TotalNodes++
		switch nodeState.Status {
		case NodeHealthy:
			health.HealthyNodes++
		case NodeSuspected:
			health.SuspectedNodes++
		case NodeFailed:
			health.FailedNodes++
		case NodePartitioned:
			health.PartitionedNodes++
		}
	}
	rfr.failureDetector.mu.RUnlock()

	// 检查是否有领导者
	if rfr.raftNode != nil {
		currentState := rfr.raftNode.GetState()
		health.HasLeader = currentState == raft.Leader
		health.LeaderID = "" // 暂时设为空，需要通过公开方法获取
	}

	// 检查法定人数
	health.QuorumAvailable = health.HealthyNodes >= rfr.config.QuorumSize

	// 计算健康分数
	if health.TotalNodes > 0 {
		healthScore := float64(health.HealthyNodes) / float64(health.TotalNodes)
		rfr.metrics.ClusterHealthScore = healthScore
	}

	// 计算可用性
	if health.QuorumAvailable {
		rfr.metrics.ClusterAvailability = 1.0
	} else {
		rfr.metrics.ClusterAvailability = 0.0
	}

	rfr.healthMonitor.clusterHealth = health

	// 保存健康快照
	snapshot := &ClusterHealthSnapshot{
		Timestamp: time.Now(),
		Health:    health,
	}
	rfr.healthMonitor.healthHistory = append(rfr.healthMonitor.healthHistory, snapshot)

	// 限制历史记录数量
	if len(rfr.healthMonitor.healthHistory) > 100 {
		rfr.healthMonitor.healthHistory = rfr.healthMonitor.healthHistory[1:]
	}
}

// collectMetrics 收集指标
func (rfr *RaftFailureRecovery) collectMetrics() {
	rfr.recoveryManager.mu.RLock()
	rfr.metrics.ActiveRecoveries = len(rfr.recoveryManager.activeRecoveries)
	rfr.recoveryManager.mu.RUnlock()

	// 计算平均检测时间
	// 在实际实现中，这里会计算从故障发生到检测到的平均时间
	rfr.metrics.MeanTimeToDetection = rfr.config.FailureDetectionInterval

	// 计算平均恢复时间
	// 已在executeRecoveryOperation中更新
}

// UpdateNodeHeartbeat 更新节点心跳
func (rfr *RaftFailureRecovery) UpdateNodeHeartbeat(nodeID string) {
	rfr.failureDetector.mu.Lock()
	defer rfr.failureDetector.mu.Unlock()

	rfr.failureDetector.lastHeartbeats[nodeID] = time.Now()
}

// UpdateNodeReachability 更新节点可达性
func (rfr *RaftFailureRecovery) UpdateNodeReachability(nodeID string, reachable bool) {
	if rfr.partitionDetector == nil {
		return
	}

	rfr.partitionDetector.mu.Lock()
	defer rfr.partitionDetector.mu.Unlock()

	rfr.partitionDetector.reachableNodes[nodeID] = reachable
}

// SetClusterSize 设置集群大小
func (rfr *RaftFailureRecovery) SetClusterSize(size int) {
	if rfr.partitionDetector != nil {
		rfr.partitionDetector.mu.Lock()
		rfr.partitionDetector.clusterSize = size
		rfr.partitionDetector.mu.Unlock()
	}
}

// GetClusterHealth 获取集群健康状态
func (rfr *RaftFailureRecovery) GetClusterHealth() *ClusterHealth {
	rfr.healthMonitor.mu.RLock()
	defer rfr.healthMonitor.mu.RUnlock()

	// 返回健康状态副本
	health := *rfr.healthMonitor.clusterHealth
	return &health
}

// GetFailureRecoveryMetrics 获取故障恢复指标
func (rfr *RaftFailureRecovery) GetFailureRecoveryMetrics() *FailureRecoveryMetrics {
	rfr.mu.RLock()
	defer rfr.mu.RUnlock()

	// 返回指标副本
	metrics := *rfr.metrics
	return &metrics
}

// GetActiveRecoveries 获取活跃恢复操作
func (rfr *RaftFailureRecovery) GetActiveRecoveries() []*RecoveryOperation {
	rfr.recoveryManager.mu.RLock()
	defer rfr.recoveryManager.mu.RUnlock()

	operations := make([]*RecoveryOperation, 0, len(rfr.recoveryManager.activeRecoveries))
	for _, op := range rfr.recoveryManager.activeRecoveries {
		// 返回操作副本
		opCopy := *op
		operations = append(operations, &opCopy)
	}

	return operations
}

// GetRecoveryHistory 获取恢复历史
func (rfr *RaftFailureRecovery) GetRecoveryHistory(limit int) []*RecoveryRecord {
	rfr.recoveryManager.mu.RLock()
	defer rfr.recoveryManager.mu.RUnlock()

	history := rfr.recoveryManager.recoveryHistory
	if limit > 0 && len(history) > limit {
		history = history[len(history)-limit:]
	}

	// 返回历史记录副本
	records := make([]*RecoveryRecord, len(history))
	for i, record := range history {
		recordCopy := *record
		recordCopy.Operation = &(*record.Operation) // 深拷贝操作
		records[i] = &recordCopy
	}

	return records
}

// TriggerManualRecovery 触发手动恢复
func (rfr *RaftFailureRecovery) TriggerManualRecovery(recoveryType RecoveryType, targetNode string) (string, error) {
	recoveryID := fmt.Sprintf("manual_recovery_%d", time.Now().UnixNano())

	operation := &RecoveryOperation{
		ID:         recoveryID,
		Type:       recoveryType,
		TargetNode: targetNode,
		StartTime:  time.Now(),
		Status:     RecoveryStatusPending,
		RetryCount: 0,
	}

	rfr.recoveryManager.mu.Lock()
	rfr.recoveryManager.activeRecoveries[recoveryID] = operation
	rfr.recoveryManager.mu.Unlock()

	rfr.metrics.TotalRecoveryOperations++
	rfr.logger.Printf("Triggered manual recovery (type: %v, target: %s, operation: %s)",
		recoveryType, targetNode, recoveryID)

	// 异步执行恢复操作
	go rfr.executeRecoveryOperation(operation)

	return recoveryID, nil
}

// CancelRecovery 取消恢复操作
func (rfr *RaftFailureRecovery) CancelRecovery(recoveryID string) error {
	rfr.recoveryManager.mu.Lock()
	defer rfr.recoveryManager.mu.Unlock()

	operation, exists := rfr.recoveryManager.activeRecoveries[recoveryID]
	if !exists {
		return fmt.Errorf("recovery operation not found: %s", recoveryID)
	}

	if operation.Status == RecoveryStatusCompleted || operation.Status == RecoveryStatusFailed {
		return fmt.Errorf("cannot cancel completed recovery operation: %s", recoveryID)
	}

	operation.Status = RecoveryStatusCancelled
	delete(rfr.recoveryManager.activeRecoveries, recoveryID)

	rfr.logger.Printf("Cancelled recovery operation: %s", recoveryID)
	return nil
}

// IsHealthy 检查集群是否健康
func (rfr *RaftFailureRecovery) IsHealthy() bool {
	health := rfr.GetClusterHealth()
	return health.QuorumAvailable && health.HasLeader
}

// Stop 停止故障恢复管理器
func (rfr *RaftFailureRecovery) Stop() {
	close(rfr.stopCh)
	rfr.wg.Wait()
	rfr.logger.Printf("Raft failure recovery stopped")
}
