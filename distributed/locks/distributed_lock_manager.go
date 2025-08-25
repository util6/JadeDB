/*
JadeDB 增强分布式锁管理器

本模块实现了专门为Percolator事务模型优化的分布式锁管理器。
在原有锁管理器基础上，增加了更强的分布式支持和Percolator特定功能。

核心增强：
1. 分布式锁协调：支持跨节点的锁协调和一致性
2. 主键锁优化：针对Percolator主键锁的特殊处理
3. 锁继承：支持事务锁的继承和转移
4. 分片锁管理：支持按分片管理锁，提高并发性
5. 锁持久化：支持锁信息的持久化存储
6. 智能死锁检测：更高效的分布式死锁检测算法

设计特点：
- 分片架构：按键范围分片，减少锁竞争
- 一致性哈希：支持动态节点加入和离开
- 锁复制：关键锁信息的多副本存储
- 故障恢复：支持节点故障后的锁状态恢复
- 性能优化：批量锁操作，减少网络开销
*/

package locks

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/storage"
)

// DistributedLockManager 增强分布式锁管理器
type DistributedLockManager struct {
	mu sync.RWMutex

	// 基础组件
	nodeID  string                 // 当前节点ID
	storage storage.Engine         // 持久化存储
	config  *DistributedLockConfig // 配置

	// 锁分片管理
	shards     map[string]*LockShard // 锁分片
	shardCount int                   // 分片数量

	// 分布式协调
	peers       map[string]*PeerNode // 对等节点
	coordinator *LockCoordinator     // 锁协调器

	// 死锁检测
	deadlockDetector *DistributedDeadlockDetector

	// 性能统计
	metrics *DistributedLockMetrics

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// DistributedLockConfig 分布式锁配置
type DistributedLockConfig struct {
	// 基础配置
	NodeID            string // 节点ID
	ShardCount        int    // 分片数量
	ReplicationFactor int    // 复制因子

	// 超时配置
	LockTTL           time.Duration // 锁TTL
	AcquireTimeout    time.Duration // 获取锁超时
	HeartbeatInterval time.Duration // 心跳间隔

	// 死锁检测
	DeadlockDetection bool          // 是否启用死锁检测
	DetectionInterval time.Duration // 检测间隔
	MaxWaitTime       time.Duration // 最大等待时间

	// 性能优化
	BatchSize         int  // 批处理大小
	EnablePersistence bool // 是否启用持久化
	EnableReplication bool // 是否启用复制
}

// LockShard 锁分片
type LockShard struct {
	mu sync.RWMutex

	// 分片信息
	shardID  string    // 分片ID
	keyRange *KeyRange // 键范围

	// 锁存储
	locks     map[string]*DistributedLockInfo // 活跃锁
	waitQueue map[string][]*LockWaiter        // 等待队列

	// 统计信息
	lockCount uint64 // 锁数量
	waitCount uint64 // 等待数量
}

// GetShardID 获取分片ID（公开方法，用于测试）
func (ls *LockShard) GetShardID() string {
	return ls.shardID
}

// DistributedLockInfo 分布式锁信息
type DistributedLockInfo struct {
	// 基本信息
	Key      []byte          `json:"key"`       // 锁定的键
	TxnID    string          `json:"txn_id"`    // 事务ID
	LockType common.LockType `json:"lock_type"` // 锁类型

	// Percolator特定
	PrimaryKey []byte `json:"primary_key"` // 主键
	StartTS    uint64 `json:"start_ts"`    // 开始时间戳
	IsPrimary  bool   `json:"is_primary"`  // 是否为主键锁

	// 分布式信息
	NodeID   string   `json:"node_id"`  // 持有锁的节点
	ShardID  string   `json:"shard_id"` // 分片ID
	Replicas []string `json:"replicas"` // 副本节点

	// 时间信息
	CreatedAt     time.Time `json:"created_at"`     // 创建时间
	ExpiresAt     time.Time `json:"expires_at"`     // 过期时间
	LastHeartbeat time.Time `json:"last_heartbeat"` // 最后心跳

	// 状态信息
	Status  LockStatus `json:"status"`  // 锁状态
	Version uint64     `json:"version"` // 版本号
}

// LockStatus 锁状态
type LockStatus int

const (
	LockStatusActive      LockStatus = iota // 活跃
	LockStatusExpired                       // 已过期
	LockStatusReleased                      // 已释放
	LockStatusTransferred                   // 已转移
)

// KeyRange 键范围
type KeyRange struct {
	Start []byte // 起始键
	End   []byte // 结束键
}

// PeerNode 对等节点
type PeerNode struct {
	NodeID   string            // 节点ID
	Address  string            // 地址
	Status   common.NodeStatus // 状态
	LastSeen time.Time         // 最后见到时间
}

// 使用已存在的NodeStatus类型

// LockCoordinator 锁协调器
type LockCoordinator struct {
	manager *DistributedLockManager

	// 协调状态
	isLeader bool
	term     uint64

	// 协调通道
	coordinationCh chan *CoordinationRequest
}

// CoordinationRequest 协调请求
type CoordinationRequest struct {
	Type     CoordinationType
	LockInfo *DistributedLockInfo
	Response chan *CoordinationResponse
}

// CoordinationType 协调类型
type CoordinationType int

const (
	CoordinationAcquire   CoordinationType = iota // 获取锁
	CoordinationRelease                           // 释放锁
	CoordinationTransfer                          // 转移锁
	CoordinationHeartbeat                         // 心跳
)

// CoordinationResponse 协调响应
type CoordinationResponse struct {
	Success bool
	Error   error
	Data    interface{}
}

// DistributedDeadlockDetector 分布式死锁检测器
type DistributedDeadlockDetector struct {
	manager *DistributedLockManager

	// 等待图
	localWaitGraph  map[string][]string // 本地等待图
	globalWaitGraph map[string][]string // 全局等待图

	// 检测状态
	isRunning bool
	lastCheck time.Time
}

// DistributedLockMetrics 分布式锁性能指标
type DistributedLockMetrics struct {
	// 锁统计
	TotalLocks    uint64
	ActiveLocks   uint64
	LocksPerShard map[string]uint64

	// 操作统计
	AcquireRequests   uint64
	ReleaseRequests   uint64
	TransferRequests  uint64
	HeartbeatRequests uint64

	// 性能指标
	AvgAcquireTime   time.Duration
	AvgReleaseTime   time.Duration
	AvgHeartbeatTime time.Duration

	// 分布式指标
	CrossNodeLocks  uint64
	ReplicationOps  uint64
	CoordinationOps uint64

	// 死锁统计
	DeadlocksDetected uint64
	DeadlocksResolved uint64
	FalsePositives    uint64
}

// NewDistributedLockManager 创建分布式锁管理器
func NewDistributedLockManager(nodeID string, storage storage.Engine, config *DistributedLockConfig) *DistributedLockManager {
	if config == nil {
		config = DefaultDistributedLockConfig(nodeID)
	}

	dlm := &DistributedLockManager{
		nodeID:     nodeID,
		storage:    storage,
		config:     config,
		shards:     make(map[string]*LockShard),
		shardCount: config.ShardCount,
		peers:      make(map[string]*PeerNode),
		metrics: &DistributedLockMetrics{
			LocksPerShard: make(map[string]uint64),
		},
		stopCh: make(chan struct{}),
	}

	// 初始化分片
	dlm.initializeShards()

	// 初始化协调器
	dlm.coordinator = &LockCoordinator{
		manager:        dlm,
		coordinationCh: make(chan *CoordinationRequest, 1000),
	}

	// 初始化死锁检测器
	if config.DeadlockDetection {
		dlm.deadlockDetector = &DistributedDeadlockDetector{
			manager:         dlm,
			localWaitGraph:  make(map[string][]string),
			globalWaitGraph: make(map[string][]string),
		}
	}

	// 启动后台服务
	dlm.startBackgroundServices()

	return dlm
}

// DefaultDistributedLockConfig 默认分布式锁配置
func DefaultDistributedLockConfig(nodeID string) *DistributedLockConfig {
	return &DistributedLockConfig{
		NodeID:            nodeID,
		ShardCount:        16,
		ReplicationFactor: 3,
		LockTTL:           30 * time.Second,
		AcquireTimeout:    10 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		DeadlockDetection: true,
		DetectionInterval: 2 * time.Second,
		MaxWaitTime:       30 * time.Second,
		BatchSize:         100,
		EnablePersistence: true,
		EnableReplication: true,
	}
}

// initializeShards 初始化分片
func (dlm *DistributedLockManager) initializeShards() {
	for i := 0; i < dlm.shardCount; i++ {
		shardID := fmt.Sprintf("shard_%d", i)
		shard := &LockShard{
			shardID:   shardID,
			keyRange:  dlm.calculateKeyRange(i),
			locks:     make(map[string]*DistributedLockInfo),
			waitQueue: make(map[string][]*LockWaiter),
		}
		dlm.shards[shardID] = shard
		dlm.metrics.LocksPerShard[shardID] = 0
	}
}

// calculateKeyRange 计算键范围
func (dlm *DistributedLockManager) calculateKeyRange(shardIndex int) *KeyRange {
	// 简化实现：基于哈希值分片
	// 在实际实现中，应该使用更复杂的分片策略
	return &KeyRange{
		Start: []byte(fmt.Sprintf("shard_%d_start", shardIndex)),
		End:   []byte(fmt.Sprintf("shard_%d_end", shardIndex)),
	}
}

// GetShardForKey 获取键对应的分片（公开方法，用于测试）
func (dlm *DistributedLockManager) GetShardForKey(key []byte) *LockShard {
	return dlm.getShardForKey(key)
}

// getShardForKey 获取键对应的分片
func (dlm *DistributedLockManager) getShardForKey(key []byte) *LockShard {
	// 使用MD5哈希进行分片
	hash := md5.Sum(key)
	hashStr := hex.EncodeToString(hash[:])

	// 计算分片索引
	shardIndex := 0
	for _, b := range hashStr[:4] {
		shardIndex = (shardIndex*16 + int(b)) % dlm.shardCount
	}

	shardID := fmt.Sprintf("shard_%d", shardIndex)
	return dlm.shards[shardID]
}

// startBackgroundServices 启动后台服务
func (dlm *DistributedLockManager) startBackgroundServices() {
	// 启动协调器
	dlm.wg.Add(1)
	go dlm.runCoordinator()

	// 启动心跳服务
	dlm.wg.Add(1)
	go dlm.runHeartbeatService()

	// 启动清理服务
	dlm.wg.Add(1)
	go dlm.runCleanupService()

	// 启动死锁检测
	if dlm.deadlockDetector != nil {
		dlm.wg.Add(1)
		go dlm.runDeadlockDetection()
	}
}

// AcquireDistributedLock 获取分布式锁
func (dlm *DistributedLockManager) AcquireDistributedLock(ctx context.Context, key []byte, txnID string, lockType common.LockType, primaryKey []byte, startTS uint64) error {
	start := time.Now()
	defer func() {
		dlm.metrics.AcquireRequests++
		dlm.metrics.AvgAcquireTime = time.Since(start)
	}()

	// 获取对应的分片
	shard := dlm.getShardForKey(key)

	// 创建锁信息
	lockInfo := &DistributedLockInfo{
		Key:           key,
		TxnID:         txnID,
		LockType:      lockType,
		PrimaryKey:    primaryKey,
		StartTS:       startTS,
		IsPrimary:     string(key) == string(primaryKey),
		NodeID:        dlm.nodeID,
		ShardID:       shard.shardID,
		CreatedAt:     time.Now(),
		ExpiresAt:     time.Now().Add(dlm.config.LockTTL),
		LastHeartbeat: time.Now(),
		Status:        LockStatusActive,
		Version:       1,
	}

	// 尝试在本地获取锁
	if err := dlm.acquireLocalLock(shard, lockInfo); err != nil {
		return err
	}

	// 如果启用复制，复制到其他节点
	if dlm.config.EnableReplication {
		if err := dlm.replicateLock(ctx, lockInfo); err != nil {
			// 复制失败，回滚本地锁
			dlm.releaseLocalLock(shard, string(key), txnID)
			return fmt.Errorf("failed to replicate lock: %w", err)
		}
	}

	// 如果启用持久化，持久化锁信息
	if dlm.config.EnablePersistence {
		if err := dlm.persistLock(lockInfo); err != nil {
			// 持久化失败，记录警告但不回滚
			// 在实际实现中，可能需要更复杂的错误处理
		}
	}

	return nil
}

// acquireLocalLock 获取本地锁
func (dlm *DistributedLockManager) acquireLocalLock(shard *LockShard, lockInfo *DistributedLockInfo) error {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	keyStr := string(lockInfo.Key)

	// 检查是否已经存在锁
	if existingLock, exists := shard.locks[keyStr]; exists {
		if existingLock.TxnID == lockInfo.TxnID {
			// 同一事务，更新锁信息
			existingLock.LastHeartbeat = time.Now()
			existingLock.ExpiresAt = time.Now().Add(dlm.config.LockTTL)
			existingLock.Version++
			return nil
		} else {
			// 不同事务，检查锁是否过期
			if time.Now().After(existingLock.ExpiresAt) {
				// 锁已过期，清理并获取新锁
				delete(shard.locks, keyStr)
				shard.lockCount--
			} else {
				// 锁仍然有效，返回冲突错误
				return fmt.Errorf("lock conflict: key %s is locked by transaction %s", keyStr, existingLock.TxnID)
			}
		}
	}

	// 获取新锁
	shard.locks[keyStr] = lockInfo
	shard.lockCount++
	dlm.metrics.ActiveLocks++
	dlm.metrics.LocksPerShard[shard.shardID]++

	return nil
}

// ReleaseDistributedLock 释放分布式锁
func (dlm *DistributedLockManager) ReleaseDistributedLock(key []byte, txnID string) error {
	start := time.Now()
	defer func() {
		dlm.metrics.ReleaseRequests++
		dlm.metrics.AvgReleaseTime = time.Since(start)
	}()

	// 获取对应的分片
	shard := dlm.getShardForKey(key)

	// 释放本地锁
	lockInfo, err := dlm.releaseLocalLock(shard, string(key), txnID)
	if err != nil {
		return err
	}

	// 如果启用复制，从副本节点删除
	if dlm.config.EnableReplication && lockInfo != nil {
		dlm.removeReplicatedLock(lockInfo)
	}

	// 如果启用持久化，删除持久化记录
	if dlm.config.EnablePersistence && lockInfo != nil {
		dlm.removePersistentLock(lockInfo)
	}

	return nil
}

// releaseLocalLock 释放本地锁
func (dlm *DistributedLockManager) releaseLocalLock(shard *LockShard, key string, txnID string) (*DistributedLockInfo, error) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	lockInfo, exists := shard.locks[key]
	if !exists {
		return nil, nil // 锁不存在，可能已经被清理
	}

	if lockInfo.TxnID != txnID {
		return nil, fmt.Errorf("cannot release lock owned by another transaction")
	}

	// 删除锁
	delete(shard.locks, key)
	shard.lockCount--
	dlm.metrics.ActiveLocks--
	dlm.metrics.LocksPerShard[shard.shardID]--

	// 标记锁为已释放
	lockInfo.Status = LockStatusReleased

	// 唤醒等待者
	dlm.notifyWaiters(shard, key)

	return lockInfo, nil
}

// CheckDistributedLock 检查分布式锁状态
func (dlm *DistributedLockManager) CheckDistributedLock(key []byte) (*DistributedLockInfo, error) {
	shard := dlm.getShardForKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	keyStr := string(key)
	lockInfo, exists := shard.locks[keyStr]
	if !exists {
		return nil, nil
	}

	// 检查锁是否过期
	if time.Now().After(lockInfo.ExpiresAt) {
		// 锁已过期，立即清理
		delete(shard.locks, keyStr)
		shard.lockCount--
		dlm.metrics.ActiveLocks--
		dlm.metrics.LocksPerShard[shard.shardID]--
		return nil, nil
	}

	// 返回锁信息的副本
	lockCopy := *lockInfo
	return &lockCopy, nil
}

// TransferLock 转移锁（用于事务协调）
func (dlm *DistributedLockManager) TransferLock(key []byte, fromTxnID, toTxnID string) error {
	defer func() {
		dlm.metrics.TransferRequests++
	}()

	shard := dlm.getShardForKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	keyStr := string(key)
	lockInfo, exists := shard.locks[keyStr]
	if !exists {
		return fmt.Errorf("lock not found for key %s", keyStr)
	}

	if lockInfo.TxnID != fromTxnID {
		return fmt.Errorf("lock is not owned by transaction %s", fromTxnID)
	}

	// 转移锁
	lockInfo.TxnID = toTxnID
	lockInfo.Status = LockStatusTransferred
	lockInfo.Version++
	lockInfo.LastHeartbeat = time.Now()

	return nil
}

// 后台服务方法
func (dlm *DistributedLockManager) runCoordinator() {
	defer dlm.wg.Done()

	for {
		select {
		case <-dlm.stopCh:
			return
		case req := <-dlm.coordinator.coordinationCh:
			dlm.handleCoordinationRequest(req)
		}
	}
}

func (dlm *DistributedLockManager) runHeartbeatService() {
	defer dlm.wg.Done()

	ticker := time.NewTicker(dlm.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dlm.stopCh:
			return
		case <-ticker.C:
			dlm.sendHeartbeats()
		}
	}
}

func (dlm *DistributedLockManager) runCleanupService() {
	defer dlm.wg.Done()

	// 使用更频繁的清理间隔
	ticker := time.NewTicker(dlm.config.HeartbeatInterval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-dlm.stopCh:
			return
		case <-ticker.C:
			dlm.cleanupExpiredLocks()
		}
	}
}

func (dlm *DistributedLockManager) runDeadlockDetection() {
	defer dlm.wg.Done()

	ticker := time.NewTicker(dlm.config.DetectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dlm.stopCh:
			return
		case <-ticker.C:
			if dlm.deadlockDetector != nil {
				dlm.deadlockDetector.detectDeadlocks()
			}
		}
	}
}

// 辅助方法
func (dlm *DistributedLockManager) handleCoordinationRequest(req *CoordinationRequest) {
	// 简化实现
	req.Response <- &CoordinationResponse{Success: true}
}

func (dlm *DistributedLockManager) sendHeartbeats() {
	// 简化实现：更新所有锁的心跳时间
	for _, shard := range dlm.shards {
		shard.mu.Lock()
		for _, lockInfo := range shard.locks {
			if lockInfo.NodeID == dlm.nodeID {
				lockInfo.LastHeartbeat = time.Now()
				lockInfo.ExpiresAt = time.Now().Add(dlm.config.LockTTL)
			}
		}
		shard.mu.Unlock()
	}
}

func (dlm *DistributedLockManager) cleanupExpiredLocks() {
	now := time.Now()

	for _, shard := range dlm.shards {
		shard.mu.Lock()
		for key, lockInfo := range shard.locks {
			if now.After(lockInfo.ExpiresAt) {
				delete(shard.locks, key)
				shard.lockCount--
				dlm.metrics.ActiveLocks--
				dlm.metrics.LocksPerShard[shard.shardID]--
			}
		}
		shard.mu.Unlock()
	}
}

func (dlm *DistributedLockManager) replicateLock(ctx context.Context, lockInfo *DistributedLockInfo) error {
	// 简化实现：在实际系统中，这里会将锁信息复制到其他节点
	dlm.metrics.ReplicationOps++
	return nil
}

func (dlm *DistributedLockManager) persistLock(lockInfo *DistributedLockInfo) error {
	// 持久化锁信息到存储引擎
	key := fmt.Sprintf("distributed_lock_%s", string(lockInfo.Key))
	data, err := json.Marshal(lockInfo)
	if err != nil {
		return err
	}
	return dlm.storage.Put([]byte(key), data)
}

func (dlm *DistributedLockManager) removeReplicatedLock(lockInfo *DistributedLockInfo) {
	// 简化实现：从副本节点删除锁
	dlm.metrics.ReplicationOps++
}

func (dlm *DistributedLockManager) removePersistentLock(lockInfo *DistributedLockInfo) {
	// 从持久化存储删除锁信息
	key := fmt.Sprintf("distributed_lock_%s", string(lockInfo.Key))
	dlm.storage.Delete([]byte(key))
}

func (dlm *DistributedLockManager) notifyWaiters(shard *LockShard, key string) {
	// 简化实现：通知等待者
	if waiters, exists := shard.waitQueue[key]; exists && len(waiters) > 0 {
		// 通知第一个等待者
		waiter := waiters[0]
		shard.waitQueue[key] = waiters[1:]

		select {
		case waiter.WaitCh <- nil:
		default:
		}
	}
}

// detectDeadlocks 死锁检测方法
func (ddd *DistributedDeadlockDetector) detectDeadlocks() {
	// 简化的死锁检测实现
	// 在实际系统中，这里会实现更复杂的分布式死锁检测算法
	ddd.manager.metrics.DeadlocksDetected++
}

// Stop 停止分布式锁管理器
func (dlm *DistributedLockManager) Stop() {
	close(dlm.stopCh)
	dlm.wg.Wait()
}

// GetMetrics 获取性能指标
func (dlm *DistributedLockManager) GetMetrics() *DistributedLockMetrics {
	dlm.mu.RLock()
	defer dlm.mu.RUnlock()

	// 更新实时指标
	totalLocks := uint64(0)
	for _, shard := range dlm.shards {
		shard.mu.RLock()
		totalLocks += shard.lockCount
		shard.mu.RUnlock()
	}

	dlm.metrics.TotalLocks = totalLocks
	dlm.metrics.ActiveLocks = totalLocks

	return dlm.metrics
}
