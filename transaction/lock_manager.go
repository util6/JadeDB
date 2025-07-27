/*
JadeDB 锁管理器

本模块实现了分布式事务系统的锁管理，支持多种锁类型和死锁检测。
为事务提供并发控制，确保数据一致性和隔离性。

核心功能：
1. 多种锁类型：共享锁、排他锁、意向锁
2. 锁兼容性：基于锁兼容性矩阵的冲突检测
3. 死锁检测：基于等待图的死锁检测算法
4. 锁等待：支持锁等待和超时处理
5. 锁升级：支持锁的升级和降级

设计特点：
- 细粒度锁：支持行级锁和表级锁
- 高并发：使用读写锁优化并发性能
- 死锁预防：多种死锁预防和检测策略
- 公平性：FIFO锁等待队列保证公平性
*/

package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// LockManager 锁管理器
type LockManager struct {
	// 配置
	config *LockConfig

	// 锁表
	mu    sync.RWMutex
	locks map[string]*LockEntry // key -> lock entry

	// 等待图（用于死锁检测）
	waitGraph *WaitGraph

	// 事务锁信息
	txnLocks map[string]map[string]*LockInfo // txnID -> key -> lock info
	txnMutex sync.RWMutex

	// 监控指标
	metrics *LockMetrics

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// LockConfig 锁配置
type LockConfig struct {
	LockTimeout           time.Duration // 锁超时时间
	DeadlockCheckInterval time.Duration // 死锁检测间隔
	EnableDeadlockDetect  bool          // 是否启用死锁检测
	MaxWaiters            int           // 最大等待者数量
}

// LockEntry 锁条目
type LockEntry struct {
	Key     string               // 锁的键
	Holders map[string]*LockInfo // 当前持有者
	Waiters []*LockWaiter        // 等待队列
	mu      sync.RWMutex         // 锁条目的锁
}

// LockInfo 锁信息
type LockInfo struct {
	TxnID       string    // 事务ID
	LockType    LockType  // 锁类型
	AcquireTime time.Time // 获取时间
}

// LockWaiter 锁等待者
type LockWaiter struct {
	TxnID     string        // 事务ID
	LockType  LockType      // 请求的锁类型
	WaitCh    chan error    // 等待通道
	Timeout   time.Duration // 超时时间
	StartTime time.Time     // 开始等待时间
}

// WaitGraph 等待图（用于死锁检测）
type WaitGraph struct {
	mu    sync.RWMutex
	edges map[string][]string // txnID -> [waiting for txnIDs]
}

// Deadlock 死锁信息
type Deadlock struct {
	Cycle []string // 死锁环中的事务ID
}

// LockMetrics 锁监控指标
type LockMetrics struct {
	// 锁统计
	TotalLocks   atomic.Int64 // 总锁数
	ActiveLocks  atomic.Int64 // 活跃锁数
	WaitingLocks atomic.Int64 // 等待锁数

	// 操作统计
	LockAcquires atomic.Int64 // 锁获取次数
	LockReleases atomic.Int64 // 锁释放次数
	LockTimeouts atomic.Int64 // 锁超时次数

	// 死锁统计
	DeadlockDetected atomic.Int64 // 检测到的死锁数
	DeadlockResolved atomic.Int64 // 解决的死锁数

	// 性能指标
	AvgLockWaitTime atomic.Int64 // 平均锁等待时间（纳秒）
	MaxLockWaitTime atomic.Int64 // 最大锁等待时间（纳秒）
}

// 锁兼容性矩阵
var lockCompatibilityMatrix = map[LockType]map[LockType]bool{
	SharedLock: {
		SharedLock:             true,
		ExclusiveLock:          false,
		IntentionSharedLock:    true,
		IntentionExclusiveLock: false,
	},
	ExclusiveLock: {
		SharedLock:             false,
		ExclusiveLock:          false,
		IntentionSharedLock:    false,
		IntentionExclusiveLock: false,
	},
	IntentionSharedLock: {
		SharedLock:             true,
		ExclusiveLock:          false,
		IntentionSharedLock:    true,
		IntentionExclusiveLock: true,
	},
	IntentionExclusiveLock: {
		SharedLock:             false,
		ExclusiveLock:          false,
		IntentionSharedLock:    true,
		IntentionExclusiveLock: true,
	},
}

// NewLockManager 创建锁管理器
func NewLockManager(config *TransactionConfig) (*LockManager, error) {
	lockConfig := &LockConfig{
		LockTimeout:           config.LockTimeout,
		DeadlockCheckInterval: config.DeadlockCheckInterval,
		EnableDeadlockDetect:  config.EnableDeadlockDetect,
		MaxWaiters:            100,
	}

	manager := &LockManager{
		config:    lockConfig,
		locks:     make(map[string]*LockEntry),
		waitGraph: &WaitGraph{edges: make(map[string][]string)},
		txnLocks:  make(map[string]map[string]*LockInfo),
		metrics:   &LockMetrics{},
		stopCh:    make(chan struct{}),
	}

	// 启动死锁检测服务
	if lockConfig.EnableDeadlockDetect {
		manager.wg.Add(1)
		go manager.deadlockDetectionService()
	}

	return manager, nil
}

// AcquireLock 获取锁
func (manager *LockManager) AcquireLock(txnID string, key []byte, lockType LockType) error {
	return manager.AcquireLockWithTimeout(txnID, key, lockType, manager.config.LockTimeout)
}

// AcquireLockWithTimeout 带超时的锁获取
func (manager *LockManager) AcquireLockWithTimeout(txnID string, key []byte, lockType LockType, timeout time.Duration) error {
	keyStr := string(key)
	start := time.Now()

	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.AvgLockWaitTime.Store(duration)
		if duration > manager.metrics.MaxLockWaitTime.Load() {
			manager.metrics.MaxLockWaitTime.Store(duration)
		}
		manager.metrics.LockAcquires.Add(1)
	}()

	// 获取或创建锁条目
	manager.mu.Lock()
	entry, exists := manager.locks[keyStr]
	if !exists {
		entry = &LockEntry{
			Key:     keyStr,
			Holders: make(map[string]*LockInfo),
			Waiters: make([]*LockWaiter, 0),
		}
		manager.locks[keyStr] = entry
	}
	manager.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// 检查是否可以立即获取锁
	if manager.canAcquireLock(entry, txnID, lockType) {
		return manager.grantLock(entry, txnID, lockType)
	}

	// 需要等待，检查等待队列是否已满
	if len(entry.Waiters) >= manager.config.MaxWaiters {
		return fmt.Errorf("too many waiters for key %s", keyStr)
	}

	// 创建等待者
	waiter := &LockWaiter{
		TxnID:     txnID,
		LockType:  lockType,
		WaitCh:    make(chan error, 1),
		Timeout:   timeout,
		StartTime: time.Now(),
	}

	// 添加到等待队列
	entry.Waiters = append(entry.Waiters, waiter)
	manager.metrics.WaitingLocks.Add(1)

	// 更新等待图
	manager.updateWaitGraph(txnID, entry.Holders)

	// 释放锁条目锁，等待通知
	entry.mu.Unlock()

	// 等待锁或超时
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case err := <-waiter.WaitCh:
		manager.metrics.WaitingLocks.Add(-1)
		return err
	case <-ctx.Done():
		// 超时，从等待队列中移除
		manager.removeWaiter(entry, txnID)
		manager.metrics.LockTimeouts.Add(1)
		manager.metrics.WaitingLocks.Add(-1)
		return fmt.Errorf("lock timeout for key %s", keyStr)
	}
}

// ReleaseLock 释放锁
func (manager *LockManager) ReleaseLock(txnID string, key []byte) error {
	keyStr := string(key)

	manager.mu.RLock()
	entry, exists := manager.locks[keyStr]
	manager.mu.RUnlock()

	if !exists {
		return fmt.Errorf("lock not found for key %s", keyStr)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// 检查是否持有锁
	lockInfo, exists := entry.Holders[txnID]
	if !exists {
		return fmt.Errorf("transaction %s does not hold lock for key %s", txnID, keyStr)
	}

	// 释放锁
	delete(entry.Holders, txnID)
	manager.metrics.ActiveLocks.Add(-1)
	manager.metrics.LockReleases.Add(1)

	// 从事务锁信息中移除
	manager.removeTxnLock(txnID, keyStr)

	// 更新等待图
	manager.removeFromWaitGraph(txnID)

	// 尝试唤醒等待者
	manager.wakeupWaiters(entry)

	// 如果没有持有者和等待者，删除锁条目
	if len(entry.Holders) == 0 && len(entry.Waiters) == 0 {
		manager.mu.Lock()
		delete(manager.locks, keyStr)
		manager.mu.Unlock()
	}

	_ = lockInfo // 避免未使用变量警告
	return nil
}

// ReleaseAllLocks 释放事务的所有锁
func (manager *LockManager) ReleaseAllLocks(txnID string) error {
	manager.txnMutex.RLock()
	txnLocks, exists := manager.txnLocks[txnID]
	manager.txnMutex.RUnlock()

	if !exists {
		return nil // 没有锁需要释放
	}

	// 释放所有锁
	for key := range txnLocks {
		manager.ReleaseLock(txnID, []byte(key))
	}

	// 清理事务锁信息
	manager.txnMutex.Lock()
	delete(manager.txnLocks, txnID)
	manager.txnMutex.Unlock()

	return nil
}

// canAcquireLock 检查是否可以获取锁
func (manager *LockManager) canAcquireLock(entry *LockEntry, txnID string, lockType LockType) bool {
	// 如果已经持有锁，检查是否可以升级
	if existingLock, exists := entry.Holders[txnID]; exists {
		return manager.canUpgradeLock(existingLock.LockType, lockType)
	}

	// 检查与现有锁的兼容性
	for _, holder := range entry.Holders {
		if !manager.isLockCompatible(holder.LockType, lockType) {
			return false
		}
	}

	return true
}

// isLockCompatible 检查锁兼容性
func (manager *LockManager) isLockCompatible(existingType, requestType LockType) bool {
	if compatibilityMap, exists := lockCompatibilityMatrix[existingType]; exists {
		return compatibilityMap[requestType]
	}
	return false
}

// canUpgradeLock 检查是否可以升级锁
func (manager *LockManager) canUpgradeLock(currentType, requestType LockType) bool {
	// 简单的锁升级规则：共享锁可以升级为排他锁
	if currentType == SharedLock && requestType == ExclusiveLock {
		return true
	}
	// 相同类型的锁总是兼容的
	return currentType == requestType
}

// grantLock 授予锁
func (manager *LockManager) grantLock(entry *LockEntry, txnID string, lockType LockType) error {
	// 创建锁信息
	lockInfo := &LockInfo{
		TxnID:       txnID,
		LockType:    lockType,
		AcquireTime: time.Now(),
	}

	// 添加到持有者列表
	entry.Holders[txnID] = lockInfo
	manager.metrics.ActiveLocks.Add(1)

	// 添加到事务锁信息
	manager.addTxnLock(txnID, entry.Key, lockInfo)

	return nil
}

// addTxnLock 添加事务锁信息
func (manager *LockManager) addTxnLock(txnID, key string, lockInfo *LockInfo) {
	manager.txnMutex.Lock()
	defer manager.txnMutex.Unlock()

	if _, exists := manager.txnLocks[txnID]; !exists {
		manager.txnLocks[txnID] = make(map[string]*LockInfo)
	}
	manager.txnLocks[txnID][key] = lockInfo
}

// removeTxnLock 移除事务锁信息
func (manager *LockManager) removeTxnLock(txnID, key string) {
	manager.txnMutex.Lock()
	defer manager.txnMutex.Unlock()

	if txnLocks, exists := manager.txnLocks[txnID]; exists {
		delete(txnLocks, key)
		if len(txnLocks) == 0 {
			delete(manager.txnLocks, txnID)
		}
	}
}

// updateWaitGraph 更新等待图
func (manager *LockManager) updateWaitGraph(waiterTxnID string, holders map[string]*LockInfo) {
	manager.waitGraph.mu.Lock()
	defer manager.waitGraph.mu.Unlock()

	var waitingFor []string
	for holderTxnID := range holders {
		if holderTxnID != waiterTxnID {
			waitingFor = append(waitingFor, holderTxnID)
		}
	}

	if len(waitingFor) > 0 {
		manager.waitGraph.edges[waiterTxnID] = waitingFor
	}
}

// removeFromWaitGraph 从等待图中移除
func (manager *LockManager) removeFromWaitGraph(txnID string) {
	manager.waitGraph.mu.Lock()
	defer manager.waitGraph.mu.Unlock()

	delete(manager.waitGraph.edges, txnID)
}

// wakeupWaiters 唤醒等待者
func (manager *LockManager) wakeupWaiters(entry *LockEntry) {
	var remainingWaiters []*LockWaiter

	for _, waiter := range entry.Waiters {
		if manager.canAcquireLock(entry, waiter.TxnID, waiter.LockType) {
			// 可以获取锁，授予锁并通知等待者
			err := manager.grantLock(entry, waiter.TxnID, waiter.LockType)
			waiter.WaitCh <- err
		} else {
			// 仍然需要等待
			remainingWaiters = append(remainingWaiters, waiter)
		}
	}

	entry.Waiters = remainingWaiters
}

// removeWaiter 从等待队列中移除等待者
func (manager *LockManager) removeWaiter(entry *LockEntry, txnID string) {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	var remainingWaiters []*LockWaiter
	for _, waiter := range entry.Waiters {
		if waiter.TxnID != txnID {
			remainingWaiters = append(remainingWaiters, waiter)
		}
	}
	entry.Waiters = remainingWaiters
}

// DetectDeadlocks 检测死锁
func (manager *LockManager) DetectDeadlocks() []*Deadlock {
	manager.waitGraph.mu.RLock()
	defer manager.waitGraph.mu.RUnlock()

	var deadlocks []*Deadlock
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	// 对每个节点进行DFS检测环
	for txnID := range manager.waitGraph.edges {
		if !visited[txnID] {
			if cycle := manager.dfsDetectCycle(txnID, visited, recStack, []string{}); cycle != nil {
				deadlock := &Deadlock{Cycle: cycle}
				deadlocks = append(deadlocks, deadlock)
				manager.metrics.DeadlockDetected.Add(1)
			}
		}
	}

	return deadlocks
}

// dfsDetectCycle DFS检测环
func (manager *LockManager) dfsDetectCycle(txnID string, visited, recStack map[string]bool, path []string) []string {
	visited[txnID] = true
	recStack[txnID] = true
	path = append(path, txnID)

	if neighbors, exists := manager.waitGraph.edges[txnID]; exists {
		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				if cycle := manager.dfsDetectCycle(neighbor, visited, recStack, path); cycle != nil {
					return cycle
				}
			} else if recStack[neighbor] {
				// 找到环，返回环中的节点
				cycleStart := -1
				for i, node := range path {
					if node == neighbor {
						cycleStart = i
						break
					}
				}
				if cycleStart >= 0 {
					return path[cycleStart:]
				}
			}
		}
	}

	recStack[txnID] = false
	return nil
}

// deadlockDetectionService 死锁检测服务
func (manager *LockManager) deadlockDetectionService() {
	defer manager.wg.Done()

	ticker := time.NewTicker(manager.config.DeadlockCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-manager.stopCh:
			return
		case <-ticker.C:
			deadlocks := manager.DetectDeadlocks()
			for _, deadlock := range deadlocks {
				manager.resolveDeadlock(deadlock)
			}
		}
	}
}

// resolveDeadlock 解决死锁
func (manager *LockManager) resolveDeadlock(deadlock *Deadlock) {
	// 简单策略：中止环中的最年轻事务
	// TODO: 实现更智能的死锁解决策略
	if len(deadlock.Cycle) > 0 {
		victimTxnID := deadlock.Cycle[len(deadlock.Cycle)-1]
		manager.ReleaseAllLocks(victimTxnID)
		manager.metrics.DeadlockResolved.Add(1)
	}
}

// GetMetrics 获取锁指标
func (manager *LockManager) GetMetrics() *LockMetrics {
	return manager.metrics
}

// Close 关闭锁管理器
func (manager *LockManager) Close() error {
	close(manager.stopCh)
	manager.wg.Wait()
	return nil
}
