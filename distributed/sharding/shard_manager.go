/*
JadeDB 分片管理器

本模块实现了分布式数据库的分片管理功能，支持数据分片和Raft组管理。
每个分片对应一个独立的Raft组，实现数据的分布式存储和一致性保证。

核心功能：
1. 分片管理：创建、删除、迁移数据分片
2. Raft组管理：每个分片对应一个Raft组
3. 分片路由：根据键值路由到正确的分片
4. 负载均衡：动态调整分片分布
5. 故障恢复：分片级别的故障检测和恢复

设计特点：
- 水平扩展：支持动态添加和删除分片
- 高可用：每个分片都有多个副本
- 一致性：基于Raft算法保证强一致性
- 性能优化：智能路由和本地缓存
*/

package sharding

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/raft"
	"github.com/util6/JadeDB/storage"
)

// ShardManager 分片管理器
type ShardManager struct {
	mu sync.RWMutex

	// 分片映射
	shards map[string]*RaftShard // shardID -> RaftShard

	// 分片路由器
	router *ShardRouter

	// 负载均衡器
	balancer *ShardBalancer

	// 配置
	config *ShardManagerConfig

	// 监控指标
	metrics *ShardManagerMetrics

	// 日志记录器
	logger *log.Logger

	// 状态
	isRunning bool
}

// RaftShard Raft分片
type RaftShard struct {
	mu sync.RWMutex

	// 分片信息
	shardID  string
	shardKey []byte   // 分片键范围
	replicas []string // 副本节点列表

	// Raft组件
	raftNode     *raft.RaftNode
	stateMachine *raft.StorageEngineStateMachine

	// 存储引擎
	engine storage.Engine

	// 配置
	config *raft.RaftConfig

	// 状态
	isLeader    bool
	isRunning   bool
	lastApplied uint64

	// 监控
	metrics *ShardMetrics
	logger  *log.Logger
}

// ShardRouter 分片路由器
type ShardRouter struct {
	mu sync.RWMutex

	// 路由表：键范围 -> 分片ID
	routingTable map[string]string

	// 一致性哈希环
	hashRing *ConsistentHashRing

	// 配置
	config *ShardRouterConfig
}

// ShardBalancer 分片负载均衡器
type ShardBalancer struct {
	mu sync.RWMutex

	// 节点负载信息
	nodeLoads map[string]*NodeLoad

	// 分片分布信息
	shardDistribution map[string][]string // nodeID -> shardIDs

	// 配置
	config *ShardBalancerConfig

	// 重平衡状态
	rebalancing   bool
	lastRebalance time.Time
}

// 配置结构体
type ShardManagerConfig struct {
	// 分片配置
	DefaultShardCount   int     // 默认分片数量
	MaxShardSize        int64   // 最大分片大小
	MinShardSize        int64   // 最小分片大小
	ShardSplitThreshold float64 // 分片分裂阈值
	ShardMergeThreshold float64 // 分片合并阈值

	// 副本配置
	ReplicationFactor int // 副本因子
	MinReplicas       int // 最小副本数

	// 性能配置
	RoutingCacheSize    int           // 路由缓存大小
	RoutingCacheTTL     time.Duration // 路由缓存TTL
	HealthCheckInterval time.Duration // 健康检查间隔
	RebalanceInterval   time.Duration // 重平衡间隔
}

type ShardRouterConfig struct {
	HashFunction string        // 哈希函数类型
	VirtualNodes int           // 虚拟节点数量
	CacheSize    int           // 缓存大小
	CacheTTL     time.Duration // 缓存TTL
}

type ShardBalancerConfig struct {
	LoadThreshold      float64       // 负载阈值
	RebalanceThreshold float64       // 重平衡阈值
	MaxMigrations      int           // 最大并发迁移数
	MigrationTimeout   time.Duration // 迁移超时时间
}

// 监控指标结构体
type ShardManagerMetrics struct {
	// 分片统计
	TotalShards     int64
	ActiveShards    int64
	InactiveShards  int64
	MigratingShards int64

	// 操作统计
	RoutingRequests uint64
	RoutingHits     uint64
	RoutingMisses   uint64
	RebalanceCount  uint64

	// 性能指标
	AvgRoutingLatency   time.Duration
	AvgRebalanceLatency time.Duration
	AvgMigrationLatency time.Duration

	// 错误统计
	RoutingErrors   uint64
	RebalanceErrors uint64
	MigrationErrors uint64
}

type ShardMetrics struct {
	// 基本统计
	KeyCount     uint64
	DataSize     uint64
	RequestCount uint64
	ErrorCount   uint64

	// 性能指标
	AvgLatency   time.Duration
	Throughput   float64
	LastActivity time.Time

	// Raft统计
	RaftTerm      uint64
	RaftIndex     uint64
	IsLeader      bool
	FollowerCount int
}

type NodeLoad struct {
	NodeID       string
	CPUUsage     float64
	MemoryUsage  float64
	DiskUsage    float64
	NetworkUsage float64
	ShardCount   int
	RequestRate  float64
	LastUpdate   time.Time
}

// ConsistentHashRing 一致性哈希环
type ConsistentHashRing struct {
	mu           sync.RWMutex
	nodes        map[uint32]string // hash -> nodeID
	sortedHashes []uint32
	virtualNodes int
}

// NewShardManager 创建分片管理器
func NewShardManager(config *ShardManagerConfig) *ShardManager {
	if config == nil {
		config = DefaultShardManagerConfig()
	}

	sm := &ShardManager{
		shards:    make(map[string]*RaftShard),
		router:    NewShardRouter(DefaultShardRouterConfig()),
		balancer:  NewShardBalancer(DefaultShardBalancerConfig()),
		config:    config,
		metrics:   &ShardManagerMetrics{},
		logger:    log.New(log.Writer(), "[ShardManager] ", log.LstdFlags),
		isRunning: false,
	}

	return sm
}

// DefaultShardManagerConfig 默认分片管理器配置
func DefaultShardManagerConfig() *ShardManagerConfig {
	return &ShardManagerConfig{
		DefaultShardCount:   16,
		MaxShardSize:        100 * 1024 * 1024, // 100MB
		MinShardSize:        10 * 1024 * 1024,  // 10MB
		ShardSplitThreshold: 0.8,
		ShardMergeThreshold: 0.3,
		ReplicationFactor:   3,
		MinReplicas:         2,
		RoutingCacheSize:    1000,
		RoutingCacheTTL:     5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		RebalanceInterval:   10 * time.Minute,
	}
}

// DefaultShardRouterConfig 默认分片路由器配置
func DefaultShardRouterConfig() *ShardRouterConfig {
	return &ShardRouterConfig{
		HashFunction: "md5",
		VirtualNodes: 150,
		CacheSize:    1000,
		CacheTTL:     5 * time.Minute,
	}
}

// DefaultShardBalancerConfig 默认分片负载均衡器配置
func DefaultShardBalancerConfig() *ShardBalancerConfig {
	return &ShardBalancerConfig{
		LoadThreshold:      0.8,
		RebalanceThreshold: 0.3,
		MaxMigrations:      3,
		MigrationTimeout:   30 * time.Minute,
	}
}

// CreateShard 创建新分片
func (sm *ShardManager) CreateShard(shardID string, replicas []string, engine storage.Engine) (*RaftShard, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 检查分片是否已存在
	if _, exists := sm.shards[shardID]; exists {
		return nil, fmt.Errorf("shard %s already exists", shardID)
	}

	// 创建Raft配置
	raftConfig := &raft.RaftConfig{
		ElectionTimeout:  150 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	}

	// 创建存储引擎状态机
	stateMachine := raft.NewStorageEngineStateMachine(engine, raft.DefaultStorageStateMachineConfig())

	// 创建网络传输（这里使用HTTP传输）
	transport := raft.NewHTTPRaftTransport()

	// 创建Raft节点
	raftNode := raft.NewRaftNode(shardID, replicas, raftConfig, stateMachine, transport)

	// 创建分片
	shard := &RaftShard{
		shardID:      shardID,
		shardKey:     []byte(shardID),
		replicas:     replicas,
		raftNode:     raftNode,
		stateMachine: stateMachine,
		engine:       engine,
		config:       raftConfig,
		metrics:      &ShardMetrics{},
		logger:       log.New(log.Writer(), fmt.Sprintf("[Shard-%s] ", shardID), log.LstdFlags),
		isRunning:    false,
	}

	// 启动分片
	if err := shard.Start(); err != nil {
		return nil, fmt.Errorf("failed to start shard %s: %w", shardID, err)
	}

	// 添加到分片映射
	sm.shards[shardID] = shard

	// 更新路由表
	sm.router.AddShard(shardID, shard.shardKey)

	// 更新指标
	sm.metrics.TotalShards++
	sm.metrics.ActiveShards++

	sm.logger.Printf("Created shard %s with %d replicas", shardID, len(replicas))
	return shard, nil
}

// GetShard 获取分片
func (sm *ShardManager) GetShard(shardID string) (*RaftShard, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	shard, exists := sm.shards[shardID]
	return shard, exists
}

// RouteShard 路由键到分片
func (sm *ShardManager) RouteShard(key []byte) (string, error) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		sm.metrics.AvgRoutingLatency = (sm.metrics.AvgRoutingLatency + latency) / 2
		sm.metrics.RoutingRequests++
	}()

	return sm.router.Route(key)
}

// Start 启动分片管理器
func (sm *ShardManager) Start() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.isRunning {
		return fmt.Errorf("shard manager already running")
	}

	sm.isRunning = true
	sm.logger.Printf("Shard manager started with %d shards", len(sm.shards))
	return nil
}

// Stop 停止分片管理器
func (sm *ShardManager) Stop() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isRunning {
		return nil
	}

	// 停止所有分片
	for shardID, shard := range sm.shards {
		if err := shard.Stop(); err != nil {
			sm.logger.Printf("Failed to stop shard %s: %v", shardID, err)
		}
	}

	sm.isRunning = false
	sm.logger.Printf("Shard manager stopped")
	return nil
}

// RaftShard 方法实现

// Start 启动分片
func (shard *RaftShard) Start() error {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.isRunning {
		return fmt.Errorf("shard %s already running", shard.shardID)
	}

	// 启动Raft节点
	if err := shard.raftNode.Start(); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}

	shard.isRunning = true
	shard.logger.Printf("Shard started")
	return nil
}

// Stop 停止分片
func (shard *RaftShard) Stop() error {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if !shard.isRunning {
		return nil
	}

	// 停止Raft节点
	if err := shard.raftNode.Stop(); err != nil {
		shard.logger.Printf("Failed to stop raft node: %v", err)
	}

	// 关闭状态机
	if err := shard.stateMachine.Close(); err != nil {
		shard.logger.Printf("Failed to close state machine: %v", err)
	}

	// 关闭存储引擎
	if err := shard.engine.Close(); err != nil {
		shard.logger.Printf("Failed to close engine: %v", err)
	}

	shard.isRunning = false
	shard.logger.Printf("Shard stopped")
	return nil
}

// Put 写入键值对
func (shard *RaftShard) Put(key, value []byte) error {
	if !shard.isRunning {
		return fmt.Errorf("shard %s not running", shard.shardID)
	}

	// 创建存储操作
	operation := &common.StorageOperation{
		Type:      "PUT",
		Key:       key,
		Value:     value,
		Timestamp: uint64(time.Now().UnixNano()),
	}

	// 通过Raft提议操作
	return shard.proposeOperation(operation)
}

// Get 读取键值
func (shard *RaftShard) Get(key []byte) ([]byte, error) {
	if !shard.isRunning {
		return nil, fmt.Errorf("shard %s not running", shard.shardID)
	}

	// 直接从存储引擎读取（读操作不需要通过Raft）
	return shard.engine.Get(key)
}

// Delete 删除键
func (shard *RaftShard) Delete(key []byte) error {
	if !shard.isRunning {
		return fmt.Errorf("shard %s not running", shard.shardID)
	}

	// 创建存储操作
	operation := &common.StorageOperation{
		Type:      "DELETE",
		Key:       key,
		Timestamp: uint64(time.Now().UnixNano()),
	}

	// 通过Raft提议操作
	return shard.proposeOperation(operation)
}

// proposeOperation 通过Raft提议操作
func (shard *RaftShard) proposeOperation(operation *common.StorageOperation) error {
	// 序列化操作
	data, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	// 提议到Raft（使用Propose方法）
	return shard.raftNode.Propose(data)
}

// IsLeader 检查是否为领导者
func (shard *RaftShard) IsLeader() bool {
	return shard.raftNode.GetState() == raft.Leader
}

// GetMetrics 获取分片指标
func (shard *RaftShard) GetMetrics() *ShardMetrics {
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// 更新Raft统计
	shard.metrics.RaftTerm = 0  // 暂时设为0，需要通过公开方法获取
	shard.metrics.RaftIndex = 0 // 暂时设为0，需要通过公开方法获取
	shard.metrics.IsLeader = shard.IsLeader()

	// 更新存储统计
	if stats := shard.engine.GetStats(); stats != nil {
		shard.metrics.KeyCount = uint64(stats.KeyCount)
		shard.metrics.DataSize = uint64(stats.DataSize)
	}

	return shard.metrics
}

// ShardRouter 方法实现

// NewShardRouter 创建分片路由器
func NewShardRouter(config *ShardRouterConfig) *ShardRouter {
	return &ShardRouter{
		routingTable: make(map[string]string),
		hashRing:     NewConsistentHashRing(config.VirtualNodes),
		config:       config,
	}
}

// AddShard 添加分片到路由表
func (sr *ShardRouter) AddShard(shardID string, shardKey []byte) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	keyStr := string(shardKey)
	sr.routingTable[keyStr] = shardID
	sr.hashRing.AddNode(shardID)
}

// RemoveShard 从路由表移除分片
func (sr *ShardRouter) RemoveShard(shardID string, shardKey []byte) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	keyStr := string(shardKey)
	delete(sr.routingTable, keyStr)
	sr.hashRing.RemoveNode(shardID)
}

// Route 路由键到分片
func (sr *ShardRouter) Route(key []byte) (string, error) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	// 使用一致性哈希路由
	return sr.hashRing.GetNode(key), nil
}

// ConsistentHashRing 方法实现

// NewConsistentHashRing 创建一致性哈希环
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		nodes:        make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		virtualNodes: virtualNodes,
	}
}

// AddNode 添加节点到哈希环
func (chr *ConsistentHashRing) AddNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	for i := 0; i < chr.virtualNodes; i++ {
		hash := chr.hash(fmt.Sprintf("%s:%d", nodeID, i))
		chr.nodes[hash] = nodeID
		chr.sortedHashes = append(chr.sortedHashes, hash)
	}

	chr.sortHashes()
}

// RemoveNode 从哈希环移除节点
func (chr *ConsistentHashRing) RemoveNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()

	for i := 0; i < chr.virtualNodes; i++ {
		hash := chr.hash(fmt.Sprintf("%s:%d", nodeID, i))
		delete(chr.nodes, hash)

		// 从排序列表中移除
		for j, h := range chr.sortedHashes {
			if h == hash {
				chr.sortedHashes = append(chr.sortedHashes[:j], chr.sortedHashes[j+1:]...)
				break
			}
		}
	}
}

// GetNode 获取键对应的节点
func (chr *ConsistentHashRing) GetNode(key []byte) string {
	chr.mu.RLock()
	defer chr.mu.RUnlock()

	if len(chr.sortedHashes) == 0 {
		return ""
	}

	hash := chr.hash(string(key))

	// 找到第一个大于等于hash的节点
	for _, h := range chr.sortedHashes {
		if h >= hash {
			return chr.nodes[h]
		}
	}

	// 如果没找到，返回第一个节点（环形）
	return chr.nodes[chr.sortedHashes[0]]
}

// hash 计算哈希值
func (chr *ConsistentHashRing) hash(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// sortHashes 排序哈希值
func (chr *ConsistentHashRing) sortHashes() {
	// 简单的冒泡排序，实际应用中可以使用更高效的排序算法
	n := len(chr.sortedHashes)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if chr.sortedHashes[j] > chr.sortedHashes[j+1] {
				chr.sortedHashes[j], chr.sortedHashes[j+1] = chr.sortedHashes[j+1], chr.sortedHashes[j]
			}
		}
	}
}

// ShardBalancer 方法实现

// NewShardBalancer 创建分片负载均衡器
func NewShardBalancer(config *ShardBalancerConfig) *ShardBalancer {
	return &ShardBalancer{
		nodeLoads:         make(map[string]*NodeLoad),
		shardDistribution: make(map[string][]string),
		config:            config,
		rebalancing:       false,
	}
}
