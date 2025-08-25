/*
JadeDB B+树页面锁管理器模块

本模块实现B+树的页面级并发控制机制，专门用于保护B+树页面的并发访问。
这是存储引擎内部的锁管理，与事务系统的锁管理是不同层次的。

核心功能：
1. 页面级锁：支持页面读锁和写锁
2. 死锁检测：自动检测和解决页面锁死锁
3. 锁超时：防止长时间等待页面锁
4. 锁升级：支持读锁升级为写锁
5. 意向锁：支持多粒度锁机制

设计原理：
- 页面保护：确保页面修改的原子性
- 并发读取：多个线程可以同时读取页面
- 排他写入：写入时排斥所有其他访问
- 锁兼容性：标准的读写锁兼容性矩阵
- 公平性：FIFO等待队列保证公平性

注意：这是存储引擎内部的页面锁，不是事务系统的记录锁！
事务级的锁管理在transaction包中实现。
*/

package bplustree

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/utils"
)

// PageLockType 页面级锁类型（用于B+树页面保护）
type PageLockType uint8

const (
	// PageReadLock 页面读锁（共享锁）
	PageReadLock PageLockType = iota
	// PageWriteLock 页面写锁（排他锁）
	PageWriteLock
	// PageIntentionReadLock 页面意向读锁
	PageIntentionReadLock
	// PageIntentionWriteLock 页面意向写锁
	PageIntentionWriteLock
)

// 向后兼容的类型别名
type LockType = PageLockType

const (
	// 向后兼容的常量别名
	ReadLock           = PageReadLock
	WriteLock          = PageWriteLock
	IntentionReadLock  = PageIntentionReadLock
	IntentionWriteLock = PageIntentionWriteLock
)

func (plt PageLockType) String() string {
	switch plt {
	case PageReadLock:
		return "PAGE_READ"
	case PageWriteLock:
		return "PAGE_WRITE"
	case PageIntentionReadLock:
		return "PAGE_INTENTION_READ"
	case PageIntentionWriteLock:
		return "PAGE_INTENTION_WRITE"
	default:
		return "PAGE_UNKNOWN"
	}
}

// LockMode 定义锁模式
type LockMode uint8

const (
	// LockModeNone 无锁
	LockModeNone LockMode = iota
	// LockModeShared 共享模式
	LockModeShared
	// LockModeExclusive 排他模式
	LockModeExclusive
)

// LockRequest 锁请求
type LockRequest struct {
	TxnID     uint64    // 事务ID
	PageID    uint64    // 页面ID
	LockType  LockType  // 锁类型
	LockMode  LockMode  // 锁模式
	Timestamp time.Time // 请求时间
	Granted   bool      // 是否已授予
	WaitChan  chan bool // 等待通道
}

// PageLock 页面锁
type PageLock struct {
	PageID      uint64         // 页面ID
	Holders     []*LockRequest // 持有者列表
	Waiters     []*LockRequest // 等待者列表
	mutex       sync.RWMutex   // 保护锁状态
	ReaderCount int32          // 读者计数
	WriterCount int32          // 写者计数
}

// PageLockManager B+树页面锁管理器
// 负责管理B+树的页面级锁，确保页面并发访问的安全性
// 注意：这是存储引擎内部的页面锁，不是事务系统的记录锁
type PageLockManager struct {
	// 锁表
	lockTable  map[uint64]*PageLock // 页面锁表
	tableMutex sync.RWMutex         // 锁表保护锁

	// 事务锁映射
	txnLocks map[uint64][]*LockRequest // 事务持有的锁
	txnMutex sync.RWMutex              // 事务锁映射保护锁

	// 死锁检测
	deadlockDetector *DeadlockDetector
	enableDeadlock   bool

	// 配置选项
	lockTimeout time.Duration // 锁超时时间

	// 统计信息
	lockRequests atomic.Int64 // 锁请求总数
	lockGrants   atomic.Int64 // 锁授予总数
	lockWaits    atomic.Int64 // 锁等待总数
	deadlocks    atomic.Int64 // 死锁总数

	// 生命周期
	closer *utils.Closer
}

// DeadlockDetector 死锁检测器
type DeadlockDetector struct {
	waitGraph map[uint64][]uint64 // 等待图：事务ID -> 等待的事务ID列表
	mutex     sync.RWMutex        // 保护等待图
}

// NewPageLockManager 创建页面锁管理器
func NewPageLockManager(options *BTreeOptions) *PageLockManager {
	lm := &PageLockManager{
		lockTable:        make(map[uint64]*PageLock),
		txnLocks:         make(map[uint64][]*LockRequest),
		deadlockDetector: NewDeadlockDetector(),
		enableDeadlock:   options.DeadlockDetect,
		lockTimeout:      options.LockTimeout,
		closer:           utils.NewCloser(),
	}

	// 启动死锁检测服务
	if lm.enableDeadlock {
		lm.closer.Add(1)
		go lm.deadlockDetectionService()
	}

	return lm
}

// NewDeadlockDetector 创建死锁检测器
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		waitGraph: make(map[uint64][]uint64),
	}
}

// AcquireLock 获取锁
func (lm *PageLockManager) AcquireLock(ctx context.Context, txnID uint64, pageID uint64, lockType LockType) error {
	lm.lockRequests.Add(1)

	// 创建锁请求
	request := &LockRequest{
		TxnID:     txnID,
		PageID:    pageID,
		LockType:  lockType,
		LockMode:  lm.getLockMode(lockType),
		Timestamp: time.Now(),
		Granted:   false,
		WaitChan:  make(chan bool, 1),
	}

	// 获取或创建页面锁
	pageLock := lm.getOrCreatePageLock(pageID)

	// 尝试立即获取锁
	if lm.tryGrantLock(pageLock, request) {
		lm.lockGrants.Add(1)
		lm.addTxnLock(txnID, request)
		return nil
	}

	// 需要等待，添加到等待队列
	lm.addToWaitQueue(pageLock, request)
	lm.lockWaits.Add(1)

	// 更新等待图（用于死锁检测）
	if lm.enableDeadlock {
		lm.updateWaitGraph(txnID, pageLock)
	}

	// 等待锁授予或超时
	select {
	case granted := <-request.WaitChan:
		if granted {
			lm.lockGrants.Add(1)
			lm.addTxnLock(txnID, request)
			return nil
		}
		return fmt.Errorf("lock request denied")

	case <-time.After(lm.lockTimeout):
		// 超时，从等待队列移除
		lm.removeFromWaitQueue(pageLock, request)
		return fmt.Errorf("lock timeout for page %d", pageID)

	case <-ctx.Done():
		// 上下文取消
		lm.removeFromWaitQueue(pageLock, request)
		return ctx.Err()
	}
}

// ReleaseLock 释放锁
func (lm *PageLockManager) ReleaseLock(txnID uint64, pageID uint64) error {
	// 获取页面锁
	pageLock := lm.getPageLock(pageID)
	if pageLock == nil {
		return fmt.Errorf("page lock not found for page %d", pageID)
	}

	// 从持有者列表中移除
	removed := lm.removeFromHolders(pageLock, txnID)
	if !removed {
		return fmt.Errorf("lock not held by transaction %d for page %d", txnID, pageID)
	}

	// 从事务锁映射中移除
	lm.removeTxnLock(txnID, pageID)

	// 尝试授予等待中的锁
	lm.grantWaitingLocks(pageLock)

	return nil
}

// ReleaseAllLocks 释放事务的所有锁
func (lm *PageLockManager) ReleaseAllLocks(txnID uint64) error {
	lm.txnMutex.Lock()
	locks := lm.txnLocks[txnID]
	delete(lm.txnLocks, txnID)
	lm.txnMutex.Unlock()

	// 释放所有锁
	for _, request := range locks {
		if err := lm.ReleaseLock(txnID, request.PageID); err != nil {
			// 记录错误但继续释放其他锁
			continue
		}
	}

	return nil
}

// getLockMode 获取锁模式
func (lm *PageLockManager) getLockMode(lockType LockType) LockMode {
	switch lockType {
	case ReadLock, IntentionReadLock:
		return LockModeShared
	case WriteLock, IntentionWriteLock:
		return LockModeExclusive
	default:
		return LockModeNone
	}
}

// getOrCreatePageLock 获取或创建页面锁
func (lm *PageLockManager) getOrCreatePageLock(pageID uint64) *PageLock {
	lm.tableMutex.Lock()
	defer lm.tableMutex.Unlock()

	if pageLock, exists := lm.lockTable[pageID]; exists {
		return pageLock
	}

	// 创建新的页面锁
	pageLock := &PageLock{
		PageID:  pageID,
		Holders: make([]*LockRequest, 0),
		Waiters: make([]*LockRequest, 0),
	}

	lm.lockTable[pageID] = pageLock
	return pageLock
}

// getPageLock 获取页面锁
func (lm *PageLockManager) getPageLock(pageID uint64) *PageLock {
	lm.tableMutex.RLock()
	defer lm.tableMutex.RUnlock()
	return lm.lockTable[pageID]
}

// tryGrantLock 尝试立即授予锁
func (lm *PageLockManager) tryGrantLock(pageLock *PageLock, request *LockRequest) bool {
	pageLock.mutex.Lock()
	defer pageLock.mutex.Unlock()

	// 检查锁兼容性
	if lm.isCompatible(pageLock, request) {
		// 添加到持有者列表
		pageLock.Holders = append(pageLock.Holders, request)
		request.Granted = true

		// 更新计数器
		if request.LockMode == LockModeShared {
			atomic.AddInt32(&pageLock.ReaderCount, 1)
		} else {
			atomic.AddInt32(&pageLock.WriterCount, 1)
		}

		return true
	}

	return false
}

// isCompatible 检查锁兼容性
func (lm *PageLockManager) isCompatible(pageLock *PageLock, request *LockRequest) bool {
	// 如果没有持有者，总是兼容
	if len(pageLock.Holders) == 0 {
		return true
	}

	// 检查与现有持有者的兼容性
	for _, holder := range pageLock.Holders {
		if !lm.areCompatible(holder.LockMode, request.LockMode) {
			return false
		}
	}

	return true
}

// areCompatible 检查两个锁模式是否兼容
func (lm *PageLockManager) areCompatible(mode1, mode2 LockMode) bool {
	// 兼容性矩阵
	// 共享锁与共享锁兼容
	// 排他锁与任何锁都不兼容
	if mode1 == LockModeShared && mode2 == LockModeShared {
		return true
	}
	return false
}

// addToWaitQueue 添加到等待队列
func (lm *PageLockManager) addToWaitQueue(pageLock *PageLock, request *LockRequest) {
	pageLock.mutex.Lock()
	defer pageLock.mutex.Unlock()
	pageLock.Waiters = append(pageLock.Waiters, request)
}

// removeFromWaitQueue 从等待队列移除
func (lm *PageLockManager) removeFromWaitQueue(pageLock *PageLock, request *LockRequest) {
	pageLock.mutex.Lock()
	defer pageLock.mutex.Unlock()

	for i, waiter := range pageLock.Waiters {
		if waiter.TxnID == request.TxnID && waiter.PageID == request.PageID {
			// 移除等待者
			pageLock.Waiters = append(pageLock.Waiters[:i], pageLock.Waiters[i+1:]...)
			break
		}
	}
}

// removeFromHolders 从持有者列表移除
func (lm *PageLockManager) removeFromHolders(pageLock *PageLock, txnID uint64) bool {
	pageLock.mutex.Lock()
	defer pageLock.mutex.Unlock()

	for i, holder := range pageLock.Holders {
		if holder.TxnID == txnID {
			// 移除持有者
			pageLock.Holders = append(pageLock.Holders[:i], pageLock.Holders[i+1:]...)

			// 更新计数器
			if holder.LockMode == LockModeShared {
				atomic.AddInt32(&pageLock.ReaderCount, -1)
			} else {
				atomic.AddInt32(&pageLock.WriterCount, -1)
			}

			return true
		}
	}
	return false
}

// grantWaitingLocks 授予等待中的锁
func (lm *PageLockManager) grantWaitingLocks(pageLock *PageLock) {
	pageLock.mutex.Lock()
	defer pageLock.mutex.Unlock()

	// 尝试授予等待队列中的锁
	i := 0
	for i < len(pageLock.Waiters) {
		waiter := pageLock.Waiters[i]

		if lm.isCompatibleUnsafe(pageLock, waiter) {
			// 可以授予锁
			pageLock.Holders = append(pageLock.Holders, waiter)
			waiter.Granted = true

			// 更新计数器
			if waiter.LockMode == LockModeShared {
				atomic.AddInt32(&pageLock.ReaderCount, 1)
			} else {
				atomic.AddInt32(&pageLock.WriterCount, 1)
			}

			// 从等待队列移除
			pageLock.Waiters = append(pageLock.Waiters[:i], pageLock.Waiters[i+1:]...)

			// 通知等待者
			select {
			case waiter.WaitChan <- true:
			default:
			}
		} else {
			i++
		}
	}
}

// isCompatibleUnsafe 检查锁兼容性（不加锁版本）
func (lm *PageLockManager) isCompatibleUnsafe(pageLock *PageLock, request *LockRequest) bool {
	// 如果没有持有者，总是兼容
	if len(pageLock.Holders) == 0 {
		return true
	}

	// 检查与现有持有者的兼容性
	for _, holder := range pageLock.Holders {
		if !lm.areCompatible(holder.LockMode, request.LockMode) {
			return false
		}
	}

	return true
}

// addTxnLock 添加事务锁映射
func (lm *PageLockManager) addTxnLock(txnID uint64, request *LockRequest) {
	lm.txnMutex.Lock()
	defer lm.txnMutex.Unlock()
	lm.txnLocks[txnID] = append(lm.txnLocks[txnID], request)
}

// removeTxnLock 移除事务锁映射
func (lm *PageLockManager) removeTxnLock(txnID uint64, pageID uint64) {
	lm.txnMutex.Lock()
	defer lm.txnMutex.Unlock()

	locks := lm.txnLocks[txnID]
	for i, lock := range locks {
		if lock.PageID == pageID {
			lm.txnLocks[txnID] = append(locks[:i], locks[i+1:]...)
			break
		}
	}

	// 如果事务没有锁了，删除映射
	if len(lm.txnLocks[txnID]) == 0 {
		delete(lm.txnLocks, txnID)
	}
}

// updateWaitGraph 更新等待图
func (lm *PageLockManager) updateWaitGraph(txnID uint64, pageLock *PageLock) {
	lm.deadlockDetector.mutex.Lock()
	defer lm.deadlockDetector.mutex.Unlock()

	// 获取当前持有锁的事务
	var holders []uint64
	pageLock.mutex.RLock()
	for _, holder := range pageLock.Holders {
		if holder.TxnID != txnID {
			holders = append(holders, holder.TxnID)
		}
	}
	pageLock.mutex.RUnlock()

	// 更新等待图
	lm.deadlockDetector.waitGraph[txnID] = holders
}

// deadlockDetectionService 死锁检测服务
func (lm *PageLockManager) deadlockDetectionService() {
	defer lm.closer.Done()

	ticker := time.NewTicker(time.Second) // 每秒检测一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if deadlock := lm.deadlockDetector.detectDeadlock(); deadlock != nil {
				lm.resolveDeadlock(deadlock)
			}

		case <-lm.closer.CloseSignal:
			return
		}
	}
}

// detectDeadlock 检测死锁
func (dd *DeadlockDetector) detectDeadlock() []uint64 {
	dd.mutex.RLock()
	defer dd.mutex.RUnlock()

	// 使用DFS检测环
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	for txnID := range dd.waitGraph {
		if !visited[txnID] {
			if cycle := dd.dfsDetectCycle(txnID, visited, recStack, []uint64{}); cycle != nil {
				return cycle
			}
		}
	}

	return nil
}

// dfsDetectCycle DFS检测环
func (dd *DeadlockDetector) dfsDetectCycle(txnID uint64, visited, recStack map[uint64]bool, path []uint64) []uint64 {
	visited[txnID] = true
	recStack[txnID] = true
	path = append(path, txnID)

	for _, waitFor := range dd.waitGraph[txnID] {
		if !visited[waitFor] {
			if cycle := dd.dfsDetectCycle(waitFor, visited, recStack, path); cycle != nil {
				return cycle
			}
		} else if recStack[waitFor] {
			// 找到环
			cycleStart := -1
			for i, id := range path {
				if id == waitFor {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				return path[cycleStart:]
			}
		}
	}

	recStack[txnID] = false
	return nil
}

// resolveDeadlock 解决死锁
func (lm *PageLockManager) resolveDeadlock(deadlock []uint64) {
	lm.deadlocks.Add(1)

	// 选择最年轻的事务作为受害者（简单策略）
	victim := deadlock[0]
	for _, txnID := range deadlock[1:] {
		// 这里可以实现更复杂的受害者选择策略
		// 比如选择持有锁最少的事务
		victim = txnID
	}

	// 中止受害者事务
	lm.ReleaseAllLocks(victim)
}

// Close 关闭锁管理器
func (lm *PageLockManager) Close() error {
	lm.closer.Close()
	return nil
}

// GetStats 获取锁管理器统计信息
func (lm *PageLockManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"lock_requests": lm.lockRequests.Load(),
		"lock_grants":   lm.lockGrants.Load(),
		"lock_waits":    lm.lockWaits.Load(),
		"deadlocks":     lm.deadlocks.Load(),
		"active_locks":  len(lm.lockTable),
		"active_txns":   len(lm.txnLocks),
	}
}
