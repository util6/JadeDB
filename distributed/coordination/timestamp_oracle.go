/*
JadeDB 时间戳服务器 (Timestamp Oracle - TSO)

本模块实现了Percolator事务模型的核心组件：全局时间戳服务器。
TSO为分布式事务提供全局唯一、单调递增的时间戳，确保事务的正确排序。

核心功能：
1. 全局时间戳生成：提供全局唯一的逻辑时间戳
2. 高可用性：基于Raft实现的高可用TSO服务
3. 批量分配：批量分配时间戳以提高性能
4. 持久化：时间戳状态的持久化存储
5. 监控：详细的性能指标和监控

设计特点：
- 单调性：严格保证时间戳的单调递增
- 高性能：批量分配减少网络开销
- 高可用：基于Raft的主备切换
- 可扩展：支持多个TSO实例的负载均衡
*/

package coordination

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/distributed/raft"
)

// TimestampOracle 时间戳服务器接口
type TimestampOracle interface {
	// GetTimestamp 获取单个时间戳
	GetTimestamp(ctx context.Context) (uint64, error)

	// GetTimestamps 批量获取时间戳
	GetTimestamps(ctx context.Context, count uint32) (uint64, uint64, error)

	// UpdateTimestamp 更新时间戳（用于恢复）
	UpdateTimestamp(ctx context.Context, timestamp uint64) error

	// GetCurrentTimestamp 获取当前时间戳（不分配）
	GetCurrentTimestamp() uint64

	// Start 启动TSO服务
	Start() error

	// Stop 停止TSO服务
	Stop() error

	// IsLeader 检查是否为主TSO
	IsLeader() bool

	// GetMetrics 获取监控指标
	GetMetrics() *TSOMetrics
}

// RaftTimestampOracle 基于Raft的时间戳服务器实现
type RaftTimestampOracle struct {
	mu sync.RWMutex

	// 基础配置
	nodeID string
	config *TSOConfig
	logger *log.Logger

	// Raft组件
	raftNode *raft.RaftNode

	// 时间戳状态
	currentTimestamp uint64 // 当前已分配的最大时间戳
	maxTimestamp     uint64 // 当前批次的最大时间戳
	batchSize        uint32 // 批量分配大小

	// 性能优化
	timestampCache  chan uint64 // 时间戳缓存
	allocationMutex sync.Mutex  // 分配锁
	lastAllocation  time.Time   // 上次分配时间

	// 监控指标
	metrics *TSOMetrics

	// 状态管理
	isRunning bool
	isLeader  bool
	stopCh    chan struct{}
	leaderCh  chan bool
}

// TSOConfig TSO配置
type TSOConfig struct {
	// 基础配置
	NodeID     string // 节点ID
	DataDir    string // 数据目录
	ListenAddr string // 监听地址

	// 时间戳配置
	BatchSize          uint32        // 批量分配大小
	CacheSize          int           // 缓存大小
	AllocationInterval time.Duration // 分配间隔

	// Raft配置
	RaftConfig *raft.RaftConfig // Raft配置

	// 性能配置
	MaxConcurrency int           // 最大并发数
	RequestTimeout time.Duration // 请求超时

	// 高可用配置
	LeaderLease     time.Duration // 领导者租约时间
	ElectionTimeout time.Duration // 选举超时
}

// TSOMetrics TSO监控指标
type TSOMetrics struct {
	// 请求统计
	TotalRequests   uint64
	SuccessRequests uint64
	FailedRequests  uint64
	BatchRequests   uint64

	// 性能指标
	AvgLatency time.Duration
	MaxLatency time.Duration
	MinLatency time.Duration
	Throughput float64

	// 时间戳统计
	CurrentTimestamp uint64
	AllocatedCount   uint64
	CachedCount      uint64

	// 状态指标
	IsLeader       bool
	LeaderChanges  uint64
	LastAllocation time.Time

	// 错误统计
	AllocationErrors uint64
	RaftErrors       uint64
	TimeoutErrors    uint64
}

// TSORequest TSO请求
type TSORequest struct {
	Type      string `json:"type"`       // 请求类型：GET_TS, GET_BATCH_TS, UPDATE_TS
	Count     uint32 `json:"count"`      // 批量请求数量
	Timestamp uint64 `json:"timestamp"`  // 更新时间戳值
	RequestID string `json:"request_id"` // 请求ID
}

// TSOResponse TSO响应
type TSOResponse struct {
	Success   bool   `json:"success"`
	StartTS   uint64 `json:"start_ts"`   // 起始时间戳
	EndTS     uint64 `json:"end_ts"`     // 结束时间戳
	CurrentTS uint64 `json:"current_ts"` // 当前时间戳
	Error     string `json:"error,omitempty"`
	LeaderID  string `json:"leader_id,omitempty"`
}

// NewRaftTimestampOracle 创建基于Raft的时间戳服务器
func NewRaftTimestampOracle(config *TSOConfig) *RaftTimestampOracle {
	if config == nil {
		config = DefaultTSOConfig()
	}

	tso := &RaftTimestampOracle{
		nodeID:         config.NodeID,
		config:         config,
		logger:         log.New(log.Writer(), "[TSO] ", log.LstdFlags),
		batchSize:      config.BatchSize,
		timestampCache: make(chan uint64, config.CacheSize),
		metrics:        &TSOMetrics{},
		stopCh:         make(chan struct{}),
		leaderCh:       make(chan bool, 1),
		isRunning:      false,
		isLeader:       false,
	}

	return tso
}

// DefaultTSOConfig 默认TSO配置
func DefaultTSOConfig() *TSOConfig {
	return &TSOConfig{
		NodeID:             "tso_node_1",
		DataDir:            "./data/tso",
		ListenAddr:         ":8081",
		BatchSize:          1000,
		CacheSize:          10000,
		AllocationInterval: 10 * time.Millisecond,
		MaxConcurrency:     1000,
		RequestTimeout:     5 * time.Second,
		LeaderLease:        10 * time.Second,
		ElectionTimeout:    3 * time.Second,
		RaftConfig: &raft.RaftConfig{
			ElectionTimeout:  150 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
		},
	}
}

// Start 启动TSO服务
func (tso *RaftTimestampOracle) Start() error {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	if tso.isRunning {
		return fmt.Errorf("TSO already running")
	}

	// 初始化时间戳（基于当前时间）
	now := time.Now().UnixNano()
	tso.currentTimestamp = uint64(now)
	tso.maxTimestamp = tso.currentTimestamp

	// 创建TSO状态机
	stateMachine := &TSOStateMachine{tso: tso}

	// 创建Raft节点
	transport := raft.NewHTTPRaftTransport()
	replicas := []string{tso.nodeID} // 单节点启动，实际应用中需要多个副本

	tso.raftNode = raft.NewRaftNode(tso.nodeID, replicas, tso.config.RaftConfig, stateMachine, transport)

	// 启动Raft节点
	if err := tso.raftNode.Start(); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}

	// 启动后台服务
	go tso.runLeaderElectionLoop()
	go tso.runTimestampAllocationLoop()
	go tso.runMetricsUpdateLoop()

	tso.isRunning = true
	tso.logger.Printf("TSO service started on %s", tso.config.ListenAddr)
	return nil
}

// Stop 停止TSO服务
func (tso *RaftTimestampOracle) Stop() error {
	tso.mu.Lock()
	defer tso.mu.Unlock()

	if !tso.isRunning {
		return nil
	}

	// 发送停止信号
	close(tso.stopCh)

	// 停止Raft节点
	if tso.raftNode != nil {
		if err := tso.raftNode.Stop(); err != nil {
			tso.logger.Printf("Failed to stop raft node: %v", err)
		}
	}

	tso.isRunning = false
	tso.isLeader = false
	tso.logger.Printf("TSO service stopped")
	return nil
}

// GetTimestamp 获取单个时间戳
func (tso *RaftTimestampOracle) GetTimestamp(ctx context.Context) (uint64, error) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		tso.updateLatencyMetrics(latency)
		atomic.AddUint64(&tso.metrics.TotalRequests, 1)
	}()

	// 检查是否为领导者
	if !tso.IsLeader() {
		atomic.AddUint64(&tso.metrics.FailedRequests, 1)
		return 0, fmt.Errorf("not leader, current leader: %s", tso.getLeaderID())
	}

	// 直接分配单个时间戳，避免缓存竞争
	startTS, _, err := tso.allocateTimestampBatch(ctx, 1)
	if err != nil {
		atomic.AddUint64(&tso.metrics.FailedRequests, 1)
		return 0, err
	}

	atomic.AddUint64(&tso.metrics.SuccessRequests, 1)
	return startTS, nil
}

// GetTimestamps 批量获取时间戳
func (tso *RaftTimestampOracle) GetTimestamps(ctx context.Context, count uint32) (uint64, uint64, error) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		tso.updateLatencyMetrics(latency)
		atomic.AddUint64(&tso.metrics.TotalRequests, 1)
		atomic.AddUint64(&tso.metrics.BatchRequests, 1)
	}()

	// 检查是否为领导者
	if !tso.IsLeader() {
		atomic.AddUint64(&tso.metrics.FailedRequests, 1)
		return 0, 0, fmt.Errorf("not leader, current leader: %s", tso.getLeaderID())
	}

	// 分配时间戳批次
	startTS, endTS, err := tso.allocateTimestampBatch(ctx, count)
	if err != nil {
		atomic.AddUint64(&tso.metrics.FailedRequests, 1)
		return 0, 0, err
	}

	atomic.AddUint64(&tso.metrics.SuccessRequests, 1)
	return startTS, endTS, nil
}

// allocateTimestampBatch 分配时间戳批次
func (tso *RaftTimestampOracle) allocateTimestampBatch(ctx context.Context, count uint32) (uint64, uint64, error) {
	tso.allocationMutex.Lock()
	defer tso.allocationMutex.Unlock()

	// 检查当前批次是否足够
	if tso.currentTimestamp+uint64(count) <= tso.maxTimestamp {
		startTS := tso.currentTimestamp + 1
		tso.currentTimestamp += uint64(count)
		endTS := tso.currentTimestamp

		return startTS, endTS, nil
	}

	// 需要通过Raft分配新的批次
	batchSize := tso.batchSize
	if count > batchSize {
		batchSize = count
	}

	request := &TSORequest{
		Type:      "ALLOCATE_BATCH",
		Count:     batchSize,
		RequestID: fmt.Sprintf("%s_%d", tso.nodeID, time.Now().UnixNano()),
	}

	// 通过Raft提议分配请求
	if err := tso.proposeAllocation(ctx, request); err != nil {
		atomic.AddUint64(&tso.metrics.AllocationErrors, 1)
		return 0, 0, fmt.Errorf("failed to allocate timestamp batch: %w", err)
	}

	// 等待Raft应用完成，然后重新检查
	// 简化实现：直接使用更新后的maxTimestamp
	if tso.currentTimestamp+uint64(count) <= tso.maxTimestamp {
		startTS := tso.currentTimestamp + 1
		tso.currentTimestamp += uint64(count)
		endTS := tso.currentTimestamp

		atomic.AddUint64(&tso.metrics.AllocatedCount, uint64(count))
		tso.lastAllocation = time.Now()

		return startTS, endTS, nil
	}

	return 0, 0, fmt.Errorf("failed to allocate timestamps after raft proposal")
}

// UpdateTimestamp 更新时间戳（用于恢复）
func (tso *RaftTimestampOracle) UpdateTimestamp(ctx context.Context, timestamp uint64) error {
	if !tso.IsLeader() {
		return fmt.Errorf("not leader")
	}

	request := &TSORequest{
		Type:      "UPDATE_TS",
		Timestamp: timestamp,
		RequestID: fmt.Sprintf("%s_%d", tso.nodeID, time.Now().UnixNano()),
	}

	return tso.proposeAllocation(ctx, request)
}

// GetCurrentTimestamp 获取当前时间戳（不分配）
func (tso *RaftTimestampOracle) GetCurrentTimestamp() uint64 {
	return atomic.LoadUint64(&tso.currentTimestamp)
}

// IsLeader 检查是否为主TSO
func (tso *RaftTimestampOracle) IsLeader() bool {
	if tso.raftNode == nil {
		return false
	}
	return tso.raftNode.GetState() == raft.Leader
}

// GetMetrics 获取监控指标
func (tso *RaftTimestampOracle) GetMetrics() *TSOMetrics {
	tso.mu.RLock()
	defer tso.mu.RUnlock()

	// 更新实时指标
	tso.metrics.CurrentTimestamp = tso.currentTimestamp
	tso.metrics.IsLeader = tso.IsLeader()
	tso.metrics.LastAllocation = tso.lastAllocation
	tso.metrics.CachedCount = uint64(len(tso.timestampCache))

	return tso.metrics
}

// proposeAllocation 通过Raft提议分配请求
func (tso *RaftTimestampOracle) proposeAllocation(ctx context.Context, request *TSORequest) error {
	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	return tso.raftNode.Propose(data)
}

// fillTimestampCache 填充时间戳缓存
func (tso *RaftTimestampOracle) fillTimestampCache(startTS, endTS uint64) {
	for ts := startTS; ts <= endTS; ts++ {
		select {
		case tso.timestampCache <- ts:
			// 成功添加到缓存
		default:
			// 缓存已满，停止填充
			return
		}
	}
}

// runLeaderElectionLoop 运行领导者选举循环
func (tso *RaftTimestampOracle) runLeaderElectionLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-tso.stopCh:
			return
		case <-ticker.C:
			newLeaderState := tso.IsLeader()
			if newLeaderState != tso.isLeader {
				tso.isLeader = newLeaderState
				if newLeaderState {
					tso.logger.Printf("Became TSO leader")
					atomic.AddUint64(&tso.metrics.LeaderChanges, 1)
				} else {
					tso.logger.Printf("Lost TSO leadership")
				}

				// 通知领导者状态变化
				select {
				case tso.leaderCh <- newLeaderState:
				default:
				}
			}
		}
	}
}

// runTimestampAllocationLoop 运行时间戳分配循环
func (tso *RaftTimestampOracle) runTimestampAllocationLoop() {
	ticker := time.NewTicker(tso.config.AllocationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tso.stopCh:
			return
		case <-ticker.C:
			// 预分配时间戳批次以提高性能
			if tso.IsLeader() && len(tso.timestampCache) < cap(tso.timestampCache)/2 {
				ctx, cancel := context.WithTimeout(context.Background(), tso.config.RequestTimeout)
				_, _, err := tso.allocateTimestampBatch(ctx, tso.batchSize)
				cancel()

				if err != nil {
					tso.logger.Printf("Failed to pre-allocate timestamps: %v", err)
				}
			}
		}
	}
}

// runMetricsUpdateLoop 运行指标更新循环
func (tso *RaftTimestampOracle) runMetricsUpdateLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastRequests uint64
	var lastTime time.Time = time.Now()

	for {
		select {
		case <-tso.stopCh:
			return
		case <-ticker.C:
			// 计算吞吐量
			currentRequests := atomic.LoadUint64(&tso.metrics.TotalRequests)
			currentTime := time.Now()

			if !lastTime.IsZero() {
				duration := currentTime.Sub(lastTime).Seconds()
				requestDiff := currentRequests - lastRequests
				tso.metrics.Throughput = float64(requestDiff) / duration
			}

			lastRequests = currentRequests
			lastTime = currentTime
		}
	}
}

// updateLatencyMetrics 更新延迟指标
func (tso *RaftTimestampOracle) updateLatencyMetrics(latency time.Duration) {
	// 更新平均延迟
	if tso.metrics.AvgLatency == 0 {
		tso.metrics.AvgLatency = latency
	} else {
		tso.metrics.AvgLatency = (tso.metrics.AvgLatency + latency) / 2
	}

	// 更新最大延迟
	if latency > tso.metrics.MaxLatency {
		tso.metrics.MaxLatency = latency
	}

	// 更新最小延迟
	if tso.metrics.MinLatency == 0 || latency < tso.metrics.MinLatency {
		tso.metrics.MinLatency = latency
	}
}

// getLeaderID 获取当前领导者ID
func (tso *RaftTimestampOracle) getLeaderID() string {
	if tso.raftNode == nil {
		return ""
	}
	// 这里需要从Raft节点获取领导者信息
	// 简化实现，返回当前节点ID如果是领导者
	if tso.IsLeader() {
		return tso.nodeID
	}
	return "unknown"
}

// TSOStateMachine TSO状态机实现
type TSOStateMachine struct {
	tso *RaftTimestampOracle
}

// Apply 应用TSO操作到状态机
func (sm *TSOStateMachine) Apply(entry raft.LogEntry) interface{} {
	var request TSORequest
	if err := json.Unmarshal(entry.Data, &request); err != nil {
		return &TSOResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal request: %v", err),
		}
	}

	switch request.Type {
	case "ALLOCATE_BATCH":
		return sm.handleAllocateBatch(&request)
	case "UPDATE_TS":
		return sm.handleUpdateTimestamp(&request)
	default:
		return &TSOResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown request type: %s", request.Type),
		}
	}
}

// handleAllocateBatch 处理批量分配请求
func (sm *TSOStateMachine) handleAllocateBatch(request *TSORequest) *TSOResponse {
	sm.tso.mu.Lock()
	defer sm.tso.mu.Unlock()

	// 分配新的时间戳批次
	startTS := sm.tso.maxTimestamp + 1
	endTS := startTS + uint64(request.Count) - 1

	// 更新最大时间戳和当前时间戳
	sm.tso.maxTimestamp = endTS
	// 注意：不要更新currentTimestamp，它应该由allocateTimestampBatch方法管理

	return &TSOResponse{
		Success:   true,
		StartTS:   startTS,
		EndTS:     endTS,
		CurrentTS: endTS,
		LeaderID:  sm.tso.nodeID,
	}
}

// handleUpdateTimestamp 处理时间戳更新请求
func (sm *TSOStateMachine) handleUpdateTimestamp(request *TSORequest) *TSOResponse {
	sm.tso.mu.Lock()
	defer sm.tso.mu.Unlock()

	// 更新时间戳（确保单调性）
	if request.Timestamp > sm.tso.maxTimestamp {
		sm.tso.maxTimestamp = request.Timestamp
		sm.tso.currentTimestamp = request.Timestamp
	}

	return &TSOResponse{
		Success:   true,
		CurrentTS: sm.tso.maxTimestamp,
		LeaderID:  sm.tso.nodeID,
	}
}

// Snapshot 创建TSO状态快照
func (sm *TSOStateMachine) Snapshot() ([]byte, error) {
	sm.tso.mu.RLock()
	defer sm.tso.mu.RUnlock()

	snapshot := map[string]interface{}{
		"current_timestamp": sm.tso.currentTimestamp,
		"max_timestamp":     sm.tso.maxTimestamp,
		"node_id":           sm.tso.nodeID,
		"timestamp":         time.Now().UnixNano(),
	}

	return json.Marshal(snapshot)
}

// Restore 从快照恢复TSO状态
func (sm *TSOStateMachine) Restore(data []byte) error {
	sm.tso.mu.Lock()
	defer sm.tso.mu.Unlock()

	var snapshot map[string]interface{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	if currentTS, ok := snapshot["current_timestamp"].(float64); ok {
		sm.tso.currentTimestamp = uint64(currentTS)
	}

	if maxTS, ok := snapshot["max_timestamp"].(float64); ok {
		sm.tso.maxTimestamp = uint64(maxTS)
	}

	return nil
}
