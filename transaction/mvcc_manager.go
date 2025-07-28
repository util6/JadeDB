/*
JadeDB MVCC管理器

本模块实现了多版本并发控制(MVCC)，为事务系统提供快照隔离和版本管理。
支持高并发读写，避免读写冲突，提供一致性的数据视图。

核心功能：
1. 版本管理：为每个键维护多个版本的数据
2. 快照隔离：为事务提供一致性的数据快照
3. 垃圾回收：清理不再需要的旧版本数据
4. 可见性判断：根据事务时间戳判断版本可见性
5. 冲突检测：检测写写冲突和读写冲突

设计特点：
- 时间戳排序：基于时间戳的版本排序和可见性判断
- 写时复制：写操作创建新版本，不影响正在读取的事务
- 延迟清理：延迟清理旧版本，确保所有事务完成后再清理
- 内存优化：使用高效的数据结构减少内存开销
*/

package transaction

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/txnwal"
)

// MVCCManager MVCC管理器
type MVCCManager struct {
	// 配置
	config *MVCCConfig

	// 版本存储
	mu       sync.RWMutex
	versions map[string]*VersionChain // key -> version chain

	// 垃圾回收
	gcWatermark atomic.Uint64 // 垃圾回收水位线
	gcQueue     chan *GCTask  // 垃圾回收任务队列

	// 活跃事务跟踪
	activeTxns map[string]*TxnInfo // 活跃事务信息
	txnMutex   sync.RWMutex        // 事务信息锁

	// 监控指标
	metrics *MVCCMetrics

	// WAL集成
	walManager txnwal.TxnWALManager

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// MVCCConfig MVCC配置
type MVCCConfig struct {
	MaxVersions       int           // 每个键的最大版本数
	GCInterval        time.Duration // 垃圾回收间隔
	GCBatchSize       int           // 垃圾回收批量大小
	VersionCacheSize  int           // 版本缓存大小
	EnableCompression bool          // 是否启用版本压缩
}

// DefaultMVCCConfig 默认MVCC配置
func DefaultMVCCConfig() *MVCCConfig {
	return &MVCCConfig{
		MaxVersions:       100,
		GCInterval:        time.Minute,
		GCBatchSize:       1000,
		VersionCacheSize:  10000,
		EnableCompression: false,
	}
}

// VersionChain 版本链
type VersionChain struct {
	Key      []byte       // 键
	Versions []*Version   // 版本列表（按时间戳降序排列）
	mu       sync.RWMutex // 版本链锁
}

// Version 数据版本
type Version struct {
	Timestamp uint64    // 版本时间戳
	Value     []byte    // 数据值
	TxnID     string    // 创建该版本的事务ID
	Deleted   bool      // 是否为删除标记
	CreatedAt time.Time // 创建时间
}

// TxnInfo 事务信息
type TxnInfo struct {
	TxnID     string    // 事务ID
	StartTs   uint64    // 开始时间戳
	CommitTs  uint64    // 提交时间戳（0表示未提交）
	ReadTs    uint64    // 读时间戳
	State     TxnState  // 事务状态
	StartTime time.Time // 开始时间
}

// TxnState 事务状态
type TxnState int

const (
	TxnStateActive TxnState = iota
	TxnStateCommitted
	TxnStateAborted
)

// GCTask 垃圾回收任务
type GCTask struct {
	Key       []byte
	Timestamp uint64
}

// MVCCMetrics MVCC监控指标
type MVCCMetrics struct {
	// 版本统计
	TotalVersions   atomic.Int64 // 总版本数
	ActiveVersions  atomic.Int64 // 活跃版本数
	DeletedVersions atomic.Int64 // 已删除版本数

	// 操作统计
	ReadOperations   atomic.Int64 // 读操作数
	WriteOperations  atomic.Int64 // 写操作数
	DeleteOperations atomic.Int64 // 删除操作数

	// 垃圾回收统计
	GCRuns     atomic.Int64 // 垃圾回收运行次数
	GCVersions atomic.Int64 // 垃圾回收的版本数
	GCDuration atomic.Int64 // 垃圾回收总耗时（纳秒）

	// 性能指标
	AvgReadTime     atomic.Int64 // 平均读取时间（纳秒）
	AvgWriteTime    atomic.Int64 // 平均写入时间（纳秒）
	VersionChainLen atomic.Int64 // 平均版本链长度
}

// NewMVCCManager 创建MVCC管理器
func NewMVCCManager(config *TransactionConfig) (*MVCCManager, error) {
	mvccConfig := &MVCCConfig{
		MaxVersions:       config.MaxVersions,
		GCInterval:        config.GCInterval,
		GCBatchSize:       1000,
		VersionCacheSize:  10000,
		EnableCompression: false,
	}

	manager := &MVCCManager{
		config:     mvccConfig,
		versions:   make(map[string]*VersionChain),
		activeTxns: make(map[string]*TxnInfo),
		gcQueue:    make(chan *GCTask, mvccConfig.GCBatchSize*2),
		metrics:    &MVCCMetrics{},
		stopCh:     make(chan struct{}),
	}

	// 启动垃圾回收服务
	manager.wg.Add(1)
	go manager.gcService()

	return manager, nil
}

// SetWALManager 设置WAL管理器
func (m *MVCCManager) SetWALManager(walManager txnwal.TxnWALManager) {
	m.walManager = walManager
}

// WriteVersionToWAL 将版本信息写入WAL
func (m *MVCCManager) WriteVersionToWAL(key string, version *Version, opType txnwal.TxnLogRecordType) error {
	if m.walManager == nil {
		return nil // WAL未配置，跳过
	}

	// 构造版本数据
	versionData := m.encodeVersionData(key, version)

	// 创建WAL记录
	record := &txnwal.TxnLogRecord{
		Type:  opType,
		TxnID: version.TxnID,
		Data:  versionData,
	}

	// 写入WAL
	ctx := context.Background()
	_, err := m.walManager.WriteRecord(ctx, record)
	return err
}

// encodeVersionData 编码版本数据
func (m *MVCCManager) encodeVersionData(key string, version *Version) []byte {
	// 简单的编码格式：[keyLen][key][valueLen][value][timestamp][txnID]
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	valueLen := len(version.Value)

	// 计算总大小
	totalSize := 4 + keyLen + 4 + valueLen + 8 + len(version.TxnID)
	data := make([]byte, totalSize)

	offset := 0

	// 编码key长度和key
	data[offset] = byte(keyLen >> 24)
	data[offset+1] = byte(keyLen >> 16)
	data[offset+2] = byte(keyLen >> 8)
	data[offset+3] = byte(keyLen)
	offset += 4
	copy(data[offset:], keyBytes)
	offset += keyLen

	// 编码value长度和value
	data[offset] = byte(valueLen >> 24)
	data[offset+1] = byte(valueLen >> 16)
	data[offset+2] = byte(valueLen >> 8)
	data[offset+3] = byte(valueLen)
	offset += 4
	copy(data[offset:], version.Value)
	offset += valueLen

	// 编码时间戳
	timestamp := uint64(version.Timestamp)
	data[offset] = byte(timestamp >> 56)
	data[offset+1] = byte(timestamp >> 48)
	data[offset+2] = byte(timestamp >> 40)
	data[offset+3] = byte(timestamp >> 32)
	data[offset+4] = byte(timestamp >> 24)
	data[offset+5] = byte(timestamp >> 16)
	data[offset+6] = byte(timestamp >> 8)
	data[offset+7] = byte(timestamp)
	offset += 8

	// 编码事务ID
	copy(data[offset:], []byte(version.TxnID))

	return data
}

// RegisterTransaction 注册事务
func (manager *MVCCManager) RegisterTransaction(txnID string, startTs uint64) {
	manager.txnMutex.Lock()
	defer manager.txnMutex.Unlock()

	manager.activeTxns[txnID] = &TxnInfo{
		TxnID:     txnID,
		StartTs:   startTs,
		ReadTs:    startTs,
		State:     TxnStateActive,
		StartTime: time.Now(),
	}
}

// CommitTransaction 提交事务
func (manager *MVCCManager) CommitTransaction(txnID string, commitTs uint64) {
	manager.txnMutex.Lock()
	defer manager.txnMutex.Unlock()

	if txnInfo, exists := manager.activeTxns[txnID]; exists {
		txnInfo.CommitTs = commitTs
		txnInfo.State = TxnStateCommitted
	}
}

// AbortTransaction 中止事务
func (manager *MVCCManager) AbortTransaction(txnID string) {
	manager.txnMutex.Lock()
	defer manager.txnMutex.Unlock()

	if txnInfo, exists := manager.activeTxns[txnID]; exists {
		txnInfo.State = TxnStateAborted
		delete(manager.activeTxns, txnID)
	}
}

// Put 写入数据版本
func (manager *MVCCManager) Put(key, value []byte, txnID string, timestamp uint64) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.AvgWriteTime.Store(duration)
		manager.metrics.WriteOperations.Add(1)
	}()

	keyStr := string(key)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	// 获取或创建版本链
	chain, exists := manager.versions[keyStr]
	if !exists {
		chain = &VersionChain{
			Key:      make([]byte, len(key)),
			Versions: make([]*Version, 0),
		}
		copy(chain.Key, key)
		manager.versions[keyStr] = chain
	}

	// 创建新版本
	version := &Version{
		Timestamp: timestamp,
		Value:     make([]byte, len(value)),
		TxnID:     txnID,
		Deleted:   false,
		CreatedAt: time.Now(),
	}
	copy(version.Value, value)

	// 插入版本（保持时间戳降序）
	chain.mu.Lock()
	chain.Versions = manager.insertVersion(chain.Versions, version)

	// 限制版本数量
	if len(chain.Versions) > manager.config.MaxVersions {
		// 将多余的版本加入垃圾回收队列
		for i := manager.config.MaxVersions; i < len(chain.Versions); i++ {
			select {
			case manager.gcQueue <- &GCTask{Key: key, Timestamp: chain.Versions[i].Timestamp}:
			default:
				// 队列满了，跳过
			}
		}
		chain.Versions = chain.Versions[:manager.config.MaxVersions]
	}
	chain.mu.Unlock()

	manager.metrics.TotalVersions.Add(1)
	manager.metrics.ActiveVersions.Add(1)

	return nil
}

// Get 读取数据版本
func (manager *MVCCManager) Get(key []byte, txnID string, readTs uint64) ([]byte, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.AvgReadTime.Store(duration)
		manager.metrics.ReadOperations.Add(1)
	}()

	keyStr := string(key)

	manager.mu.RLock()
	chain, exists := manager.versions[keyStr]
	manager.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	chain.mu.RLock()
	defer chain.mu.RUnlock()

	// 查找可见的版本
	for _, version := range chain.Versions {
		if manager.isVersionVisible(version, txnID, readTs) {
			if version.Deleted {
				return nil, fmt.Errorf("key deleted")
			}

			// 返回值的副本
			result := make([]byte, len(version.Value))
			copy(result, version.Value)
			return result, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// Delete 删除数据（创建删除标记版本）
func (manager *MVCCManager) Delete(key []byte, txnID string, timestamp uint64) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.AvgWriteTime.Store(duration)
		manager.metrics.DeleteOperations.Add(1)
	}()

	keyStr := string(key)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	// 获取或创建版本链
	chain, exists := manager.versions[keyStr]
	if !exists {
		chain = &VersionChain{
			Key:      make([]byte, len(key)),
			Versions: make([]*Version, 0),
		}
		copy(chain.Key, key)
		manager.versions[keyStr] = chain
	}

	// 创建删除标记版本
	version := &Version{
		Timestamp: timestamp,
		Value:     nil,
		TxnID:     txnID,
		Deleted:   true,
		CreatedAt: time.Now(),
	}

	// 插入版本
	chain.mu.Lock()
	chain.Versions = manager.insertVersion(chain.Versions, version)
	chain.mu.Unlock()

	manager.metrics.TotalVersions.Add(1)
	manager.metrics.DeletedVersions.Add(1)

	return nil
}

// insertVersion 插入版本（保持时间戳降序）
func (manager *MVCCManager) insertVersion(versions []*Version, newVersion *Version) []*Version {
	// 使用二分查找找到插入位置
	pos := sort.Search(len(versions), func(i int) bool {
		return versions[i].Timestamp <= newVersion.Timestamp
	})

	// 插入新版本
	versions = append(versions, nil)
	copy(versions[pos+1:], versions[pos:])
	versions[pos] = newVersion

	return versions
}

// isVersionVisible 判断版本是否对事务可见
func (manager *MVCCManager) isVersionVisible(version *Version, txnID string, readTs uint64) bool {
	// 如果是同一个事务创建的版本，总是可见
	if version.TxnID == txnID {
		return true
	}

	// 检查版本的创建事务是否已提交
	manager.txnMutex.RLock()
	txnInfo, exists := manager.activeTxns[version.TxnID]
	manager.txnMutex.RUnlock()

	if !exists {
		// 事务信息不存在，假设已提交且版本可见
		return version.Timestamp <= readTs
	}

	// 检查事务状态
	switch txnInfo.State {
	case TxnStateCommitted:
		// 已提交的事务，检查提交时间戳
		return txnInfo.CommitTs <= readTs
	case TxnStateAborted:
		// 已中止的事务，版本不可见
		return false
	case TxnStateActive:
		// 活跃事务，版本不可见（除非是同一个事务）
		return false
	default:
		return false
	}
}

// GarbageCollect 执行垃圾回收
func (manager *MVCCManager) GarbageCollect() {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.GCDuration.Add(duration)
		manager.metrics.GCRuns.Add(1)
	}()

	// 计算垃圾回收水位线
	watermark := manager.calculateGCWatermark()
	manager.gcWatermark.Store(watermark)

	// 收集需要清理的版本
	var gcCount int64
	manager.mu.Lock()
	for keyStr, chain := range manager.versions {
		chain.mu.Lock()

		// 找到可以清理的版本
		keepVersions := make([]*Version, 0, len(chain.Versions))
		for _, version := range chain.Versions {
			if version.Timestamp > watermark || manager.shouldKeepVersion(version) {
				keepVersions = append(keepVersions, version)
			} else {
				gcCount++
			}
		}

		// 更新版本链
		if len(keepVersions) == 0 {
			// 所有版本都被清理，删除整个版本链
			delete(manager.versions, keyStr)
		} else {
			chain.Versions = keepVersions
		}

		chain.mu.Unlock()
	}
	manager.mu.Unlock()

	// 更新指标
	manager.metrics.GCVersions.Add(gcCount)
	manager.metrics.ActiveVersions.Add(-gcCount)
}

// calculateGCWatermark 计算垃圾回收水位线
func (manager *MVCCManager) calculateGCWatermark() uint64 {
	manager.txnMutex.RLock()
	defer manager.txnMutex.RUnlock()

	// 找到最小的活跃事务开始时间戳
	var minStartTs uint64 = ^uint64(0) // 最大值

	for _, txnInfo := range manager.activeTxns {
		if txnInfo.State == TxnStateActive && txnInfo.StartTs < minStartTs {
			minStartTs = txnInfo.StartTs
		}
	}

	// 如果没有活跃事务，使用当前时间戳
	if minStartTs == ^uint64(0) {
		minStartTs = uint64(time.Now().UnixNano())
	}

	return minStartTs
}

// shouldKeepVersion 判断是否应该保留版本
// 实现智能的版本保留策略，考虑多种因素
func (manager *MVCCManager) shouldKeepVersion(version *Version) bool {
	// 1. 保留最近创建的版本（防止过于激进的清理）
	if time.Since(version.CreatedAt) < time.Hour {
		return true
	}

	// 2. 检查版本是否被某个未提交的事务创建
	manager.txnMutex.RLock()
	defer manager.txnMutex.RUnlock()

	if txnInfo, exists := manager.activeTxns[version.TxnID]; exists {
		// 如果创建这个版本的事务还活跃，必须保留版本
		if txnInfo.State == TxnStateActive {
			return true
		}
	}

	// 3. 检查是否有活跃事务可能需要这个版本
	for _, txnInfo := range manager.activeTxns {
		if txnInfo.State == TxnStateActive {
			// 如果事务的开始时间戳小于等于版本时间戳，
			// 说明这个事务可能需要读取这个版本
			if txnInfo.StartTs <= version.Timestamp {
				return true
			}

			// 如果事务的读时间戳大于等于版本时间戳，
			// 说明这个事务可能会读取这个版本
			if txnInfo.ReadTs >= version.Timestamp {
				return true
			}
		}
	}

	// 4. 对于已删除的版本，可以更激进地清理
	// 但仍需要保证可见性
	if version.Deleted {
		// 已删除的版本如果没有活跃事务依赖，可以清理
		return false
	}

	// 5. 默认不保留（可以被清理）
	return false
}

// gcService 垃圾回收服务
// 优化的垃圾回收服务，支持批量处理和智能调度
func (manager *MVCCManager) gcService() {
	defer manager.wg.Done()

	// 定时器：定期执行全量垃圾回收
	fullGCTicker := time.NewTicker(manager.config.GCInterval)
	defer fullGCTicker.Stop()

	// 定时器：定期处理垃圾回收队列
	batchGCTicker := time.NewTicker(manager.config.GCInterval / 10) // 更频繁的批量处理
	defer batchGCTicker.Stop()

	for {
		select {
		case <-manager.stopCh:
			// 关闭前处理剩余的垃圾回收任务
			manager.drainGCQueue()
			return

		case <-fullGCTicker.C:
			// 定期执行全量垃圾回收
			manager.GarbageCollect()

		case <-batchGCTicker.C:
			// 定期批量处理垃圾回收队列
			if len(manager.gcQueue) > 0 {
				manager.BatchGarbageCollect()
			}

		case task := <-manager.gcQueue:
			// 立即处理单个垃圾回收任务
			if task != nil {
				manager.processGCTask(task)
			}
		}
	}
}

// drainGCQueue 清空垃圾回收队列
// 在关闭服务前处理所有剩余的垃圾回收任务
func (manager *MVCCManager) drainGCQueue() {
	for {
		select {
		case task := <-manager.gcQueue:
			if task != nil {
				manager.processGCTask(task)
			}
		default:
			// 队列为空，退出
			return
		}
	}
}

// processGCTask 处理垃圾回收任务
// 处理单个键的特定版本垃圾回收任务
func (manager *MVCCManager) processGCTask(task *GCTask) {
	if task == nil {
		return
	}

	keyStr := string(task.Key)

	manager.mu.Lock()
	defer manager.mu.Unlock()

	// 获取版本链
	chain, exists := manager.versions[keyStr]
	if !exists {
		return // 版本链不存在，可能已经被清理
	}

	chain.mu.Lock()
	defer chain.mu.Unlock()

	// 检查是否可以安全清理这个版本
	watermark := manager.gcWatermark.Load()
	if task.Timestamp > watermark {
		return // 版本还不能被清理
	}

	// 查找并移除指定时间戳的版本
	newVersions := make([]*Version, 0, len(chain.Versions))
	var removedCount int64

	for _, version := range chain.Versions {
		if version.Timestamp == task.Timestamp {
			// 再次检查是否可以安全移除
			if manager.canRemoveVersion(version, watermark) {
				removedCount++
				continue // 跳过这个版本，即删除它
			}
		}
		newVersions = append(newVersions, version)
	}

	// 更新版本链
	if removedCount > 0 {
		chain.Versions = newVersions

		// 如果版本链为空，删除整个版本链
		if len(chain.Versions) == 0 {
			delete(manager.versions, keyStr)
		}

		// 更新统计信息
		manager.metrics.GCVersions.Add(removedCount)
		manager.metrics.ActiveVersions.Add(-removedCount)
	}
}

// canRemoveVersion 检查版本是否可以安全移除
func (manager *MVCCManager) canRemoveVersion(version *Version, watermark uint64) bool {
	// 版本时间戳必须小于等于水位线
	if version.Timestamp > watermark {
		return false
	}

	// 检查是否有活跃事务可能需要这个版本
	manager.txnMutex.RLock()
	defer manager.txnMutex.RUnlock()

	for _, txnInfo := range manager.activeTxns {
		// 如果有活跃事务的开始时间戳小于等于版本时间戳，
		// 说明这个事务可能需要读取这个版本
		if txnInfo.StartTs <= version.Timestamp {
			return false
		}
	}

	return true
}

// BatchGarbageCollect 批量垃圾回收
// 处理垃圾回收队列中的多个任务，提高效率
func (manager *MVCCManager) BatchGarbageCollect() {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Nanoseconds()
		manager.metrics.GCDuration.Add(duration)
		manager.metrics.GCRuns.Add(1)
	}()

	// 批量处理垃圾回收任务
	batchSize := manager.config.GCBatchSize
	tasks := make([]*GCTask, 0, batchSize)

	// 收集一批任务
	for i := 0; i < batchSize; i++ {
		select {
		case task := <-manager.gcQueue:
			if task != nil {
				tasks = append(tasks, task)
			}
		default:
			// 队列为空，停止收集
			break
		}
	}

	if len(tasks) == 0 {
		return
	}

	// 按键分组任务，减少锁竞争
	tasksByKey := make(map[string][]*GCTask)
	for _, task := range tasks {
		keyStr := string(task.Key)
		tasksByKey[keyStr] = append(tasksByKey[keyStr], task)
	}

	// 批量处理每个键的任务
	var totalRemoved int64
	for keyStr, keyTasks := range tasksByKey {
		removed := manager.batchProcessKeyTasks(keyStr, keyTasks)
		totalRemoved += removed
	}

	// 更新统计信息
	if totalRemoved > 0 {
		manager.metrics.GCVersions.Add(totalRemoved)
		manager.metrics.ActiveVersions.Add(-totalRemoved)
	}
}

// batchProcessKeyTasks 批量处理单个键的垃圾回收任务
func (manager *MVCCManager) batchProcessKeyTasks(keyStr string, tasks []*GCTask) int64 {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// 获取版本链
	chain, exists := manager.versions[keyStr]
	if !exists {
		return 0 // 版本链不存在
	}

	chain.mu.Lock()
	defer chain.mu.Unlock()

	// 获取当前水位线
	watermark := manager.gcWatermark.Load()

	// 创建要删除的时间戳集合
	timestampsToRemove := make(map[uint64]bool)
	for _, task := range tasks {
		if task.Timestamp <= watermark {
			timestampsToRemove[task.Timestamp] = true
		}
	}

	// 过滤版本链，移除可以清理的版本
	newVersions := make([]*Version, 0, len(chain.Versions))
	var removedCount int64

	for _, version := range chain.Versions {
		if timestampsToRemove[version.Timestamp] && manager.canRemoveVersion(version, watermark) {
			removedCount++
			continue // 跳过这个版本，即删除它
		}
		newVersions = append(newVersions, version)
	}

	// 更新版本链
	if removedCount > 0 {
		chain.Versions = newVersions

		// 如果版本链为空，删除整个版本链
		if len(chain.Versions) == 0 {
			delete(manager.versions, keyStr)
		}
	}

	return removedCount
}

// ForceGarbageCollect 强制垃圾回收
// 立即执行一次完整的垃圾回收，不考虑时间间隔
func (manager *MVCCManager) ForceGarbageCollect() {
	// 先处理队列中的任务
	manager.BatchGarbageCollect()

	// 再执行全量垃圾回收
	manager.GarbageCollect()
}

// GetGCStats 获取垃圾回收统计信息
func (manager *MVCCManager) GetGCStats() map[string]interface{} {
	return map[string]interface{}{
		"gc_runs":         manager.metrics.GCRuns.Load(),
		"gc_versions":     manager.metrics.GCVersions.Load(),
		"gc_duration_ns":  manager.metrics.GCDuration.Load(),
		"gc_watermark":    manager.gcWatermark.Load(),
		"gc_queue_size":   len(manager.gcQueue),
		"active_versions": manager.metrics.ActiveVersions.Load(),
		"total_versions":  manager.metrics.TotalVersions.Load(),
		"avg_gc_duration": manager.getAvgGCDuration(),
	}
}

// getAvgGCDuration 计算平均垃圾回收时间
func (manager *MVCCManager) getAvgGCDuration() int64 {
	runs := manager.metrics.GCRuns.Load()
	if runs == 0 {
		return 0
	}
	return manager.metrics.GCDuration.Load() / runs
}

// GetMetrics 获取MVCC指标
func (manager *MVCCManager) GetMetrics() *MVCCMetrics {
	return manager.metrics
}

// Close 关闭MVCC管理器
func (manager *MVCCManager) Close() error {
	close(manager.stopCh)
	manager.wg.Wait()
	close(manager.gcQueue)
	return nil
}
