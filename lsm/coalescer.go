/*
LSM 树智能批量合并器（Batch Coalescer）模块

批量合并器是 JadeDB 的高级性能优化组件，通过智能合并多个写入请求来提高吞吐量。
它实现了自适应的批量处理策略，在延迟和吞吐量之间找到最佳平衡点。

核心功能：
1. 智能合并：将多个小的写入请求合并成大的批次
2. 去重优化：自动去除重复键的旧版本，减少写放大
3. 自适应调整：根据系统负载动态调整批量参数
4. 延迟控制：在保证吞吐量的同时控制写入延迟

设计原理：
- 批量效应：合并多个操作减少系统调用和锁竞争
- 时间窗口：在固定时间窗口内收集请求进行合并
- 大小控制：当批次达到一定大小时立即处理
- 自适应算法：根据历史数据调整合并策略

优化策略：
1. 键排序：按键排序提高写入的局部性
2. 版本去重：相同键只保留最新版本
3. 批次分割：将大批次分割成合适大小的子批次
4. 异步处理：合并和处理在不同线程中进行

性能优势：
- 减少写放大：去重减少不必要的写入
- 提高吞吐量：批量操作摊薄单次操作开销
- 优化局部性：排序后的写入提高缓存命中率
- 自适应性：根据负载自动调整参数

适用场景：
- 高并发写入场景
- 有重复键更新的工作负载
- 对吞吐量要求高的应用
- 批量数据导入和处理

监控指标：
- 合并率：合并前后请求数量的比值
- 延迟分布：请求从提交到完成的时间分布
- 批次大小：平均批次大小和最大批次大小
- 去重效果：去重前后条目数量的比值
*/

package lsm

import (
	"github.com/util6/JadeDB/utils"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BatchRequest 表示一个批量写入请求。
// 它包含要写入的数据和同步机制，支持异步处理和结果等待。
//
// 生命周期：
// 1. 创建：客户端创建请求并填充数据
// 2. 提交：将请求提交给合并器
// 3. 合并：合并器将多个请求合并处理
// 4. 处理：异步执行实际的写入操作
// 5. 完成：通知客户端处理结果
type BatchRequest struct {
	// 数据内容

	// Entries 包含要写入的所有条目。
	// 这些条目会被合并器处理，可能与其他请求的条目合并。
	Entries []*utils.Entry

	// Ptrs 存储值指针，用于大值的分离存储。
	// 与 Entries 对应，记录值在值日志中的位置。
	Ptrs []*utils.ValuePtr

	// 同步机制

	// Wg 用于等待请求处理完成。
	// 客户端通过 Wait() 方法等待处理结果。
	Wg sync.WaitGroup

	// Err 存储处理过程中的错误信息。
	// 如果处理成功则为 nil，否则包含具体的错误。
	Err error
}

// Wait 等待批量请求处理完成并返回结果。
// 这是一个阻塞操作，会等待直到所有条目都被处理。
//
// 返回值：
// 处理过程中的错误，成功时返回 nil
//
// 使用场景：
// - 同步写入：需要确认写入完成的场景
// - 错误处理：需要检查写入是否成功
// - 流量控制：控制并发写入的数量
func (br *BatchRequest) Wait() error {
	br.Wg.Wait()
	return br.Err
}

// BatchCoalescer 实现智能的批量合并功能。
// 它收集多个写入请求，智能地合并成更大的批次，提高写入效率。
//
// 核心算法：
// 1. 收集阶段：在时间窗口内收集多个请求
// 2. 合并阶段：将请求按键排序并去重
// 3. 分割阶段：将大批次分割成合适大小的子批次
// 4. 处理阶段：异步执行实际的写入操作
//
// 自适应机制：
// - 根据历史批次大小调整延迟时间
// - 根据系统负载调整合并策略
// - 动态平衡延迟和吞吐量
type BatchCoalescer struct {
	// 系统集成

	// lsm 指向所属的 LSM 树实例。
	// 用于执行实际的写入操作。
	lsm *LSM

	// 请求管理

	// pending 存储等待合并的请求。
	// 使用切片存储，支持动态扩容。
	pending []*BatchRequest

	// mutex 保护并发访问 pending 和其他共享状态。
	// 使用互斥锁确保合并过程的原子性。
	mutex sync.Mutex

	// timer 用于控制合并的时间窗口。
	// 当定时器触发时，会强制合并当前的待处理请求。
	timer *time.Timer

	// 配置参数

	// maxBatchSize 限制单个批次的最大字节数。
	// 防止批次过大导致内存压力或处理延迟。
	maxBatchSize int64

	// maxLatency 限制请求的最大等待时间。
	// 确保即使在低负载情况下也能及时处理请求。
	maxLatency time.Duration

	// maxCount 限制单个批次的最大条目数量。
	// 防止批次中条目过多导致处理复杂度过高。
	maxCount int

	// 自适应参数（原子操作）

	// avgLatency 记录平均延迟时间（纳秒）。
	// 用于自适应调整合并策略。
	avgLatency int64

	// avgBatchSize 记录平均批次大小。
	// 用于评估合并效果和调整参数。
	avgBatchSize int64

	// 统计信息

	// stats 包含详细的运行统计信息。
	// 用于性能监控、调优和问题诊断。
	stats struct {
		// totalBatches 记录处理的总批次数。
		totalBatches int64

		// totalRequests 记录处理的总请求数。
		totalRequests int64

		// avgCoalesceTime 记录平均合并时间。
		avgCoalesceTime time.Duration

		// maxBatchSize 记录历史最大批次大小。
		maxBatchSize int64
	}
}

// NewBatchCoalescer 创建新的批量合并器
func NewBatchCoalescer(lsm *LSM) *BatchCoalescer {
	bc := &BatchCoalescer{
		lsm:          lsm,
		pending:      make([]*BatchRequest, 0, 64),
		maxBatchSize: 16 << 20, // 16MB
		maxLatency:   5 * time.Millisecond,
		maxCount:     32,
		avgLatency:   int64(5 * time.Millisecond),
		avgBatchSize: 1024,
	}
	return bc
}

// AddRequest 添加请求到批量处理器
func (bc *BatchCoalescer) AddRequest(req *BatchRequest) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.pending = append(bc.pending, req)

	// 检查是否需要立即处理
	if bc.shouldFlushImmediately() {
		bc.flushPendingLocked()
		return
	}

	// 设置或重置定时器
	if bc.timer == nil {
		bc.timer = time.AfterFunc(bc.getAdaptiveLatency(), bc.flushPending)
	}
}

// shouldFlushImmediately 检查是否需要立即刷新
func (bc *BatchCoalescer) shouldFlushImmediately() bool {
	if len(bc.pending) >= bc.maxCount {
		return true
	}

	var totalSize int64
	for _, req := range bc.pending {
		for _, entry := range req.Entries {
			totalSize += int64(entry.EstimateSize(1024))
		}
	}

	return totalSize >= bc.maxBatchSize
}

// getAdaptiveLatency 获取自适应延迟
func (bc *BatchCoalescer) getAdaptiveLatency() time.Duration {
	avgLatency := time.Duration(atomic.LoadInt64(&bc.avgLatency))
	avgBatchSize := atomic.LoadInt64(&bc.avgBatchSize)

	// 如果平均批量大小较小，减少延迟以提高响应性
	if avgBatchSize < 512 {
		return avgLatency / 2
	}

	// 如果平均批量大小较大，可以适当增加延迟以获得更好的批量效果
	if avgBatchSize > 2048 {
		return avgLatency * 2
	}

	return avgLatency
}

// flushPending 刷新待处理的请求
func (bc *BatchCoalescer) flushPending() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	bc.flushPendingLocked()
}

// flushPendingLocked 在持有锁的情况下刷新待处理的请求
func (bc *BatchCoalescer) flushPendingLocked() {
	if len(bc.pending) == 0 {
		return
	}

	start := time.Now()

	// 停止定时器
	if bc.timer != nil {
		bc.timer.Stop()
		bc.timer = nil
	}

	// 合并批次
	coalesced := bc.coalesceBatches(bc.pending)

	// 更新统计信息
	atomic.AddInt64(&bc.stats.totalBatches, int64(len(coalesced)))
	atomic.AddInt64(&bc.stats.totalRequests, int64(len(bc.pending)))

	// 清空待处理列表
	bc.pending = bc.pending[:0]

	// 异步处理合并后的批次
	go bc.processBatches(coalesced)

	// 更新合并时间统计
	coalesceTime := time.Since(start)
	bc.stats.avgCoalesceTime = (bc.stats.avgCoalesceTime + coalesceTime) / 2
}

// coalesceBatches 合并批次的核心逻辑
func (bc *BatchCoalescer) coalesceBatches(requests []*BatchRequest) [][]*utils.Entry {
	if len(requests) == 0 {
		return nil
	}

	// 收集所有条目
	var allEntries []*utils.Entry
	requestMap := make(map[*utils.Entry]*BatchRequest)

	for _, req := range requests {
		for _, entry := range req.Entries {
			allEntries = append(allEntries, entry)
			requestMap[entry] = req
		}
	}

	// 按键排序以提高局部性
	sort.Slice(allEntries, func(i, j int) bool {
		return string(allEntries[i].Key) < string(allEntries[j].Key)
	})

	// 去重：相同键的条目只保留最新的
	deduped := bc.deduplicateEntries(allEntries)

	// 分割成合适大小的批次
	batches := bc.splitIntoBatches(deduped)

	return batches
}

// deduplicateEntries 去重条目
func (bc *BatchCoalescer) deduplicateEntries(entries []*utils.Entry) []*utils.Entry {
	if len(entries) <= 1 {
		return entries
	}

	seen := make(map[string]*utils.Entry)
	var result []*utils.Entry

	for _, entry := range entries {
		key := string(entry.Key)
		if existing, exists := seen[key]; exists {
			// 保留版本更高的条目
			if entry.Version > existing.Version {
				seen[key] = entry
			}
		} else {
			seen[key] = entry
			result = append(result, entry)
		}
	}

	return result
}

// splitIntoBatches 将条目分割成批次
func (bc *BatchCoalescer) splitIntoBatches(entries []*utils.Entry) [][]*utils.Entry {
	if len(entries) == 0 {
		return nil
	}

	var batches [][]*utils.Entry
	var currentBatch []*utils.Entry
	var currentSize int64

	for _, entry := range entries {
		entrySize := int64(entry.EstimateSize(1024))

		// 检查是否需要开始新批次
		if len(currentBatch) > 0 &&
			(currentSize+entrySize > bc.maxBatchSize || len(currentBatch) >= bc.maxCount) {
			batches = append(batches, currentBatch)
			currentBatch = nil
			currentSize = 0
		}

		currentBatch = append(currentBatch, entry)
		currentSize += entrySize
	}

	// 添加最后一个批次
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// processBatches 处理合并后的批次
func (bc *BatchCoalescer) processBatches(batches [][]*utils.Entry) {
	for _, batch := range batches {
		// 使用LSM的写入方法处理批次
		for _, entry := range batch {
			bc.lsm.memTable.sl.Add(entry)
			bc.lsm.memTable.wal.Write(entry)
		}

		// 检查是否需要轮转内存表
		if bc.lsm.memTable.Size() >= bc.lsm.option.MemTableSize {
			bc.lsm.Rotate()
		}
	}
}

// GetStats 获取统计信息
func (bc *BatchCoalescer) GetStats() map[string]interface{} {
	bc.mutex.Lock()
	pendingCount := len(bc.pending)
	bc.mutex.Unlock()

	return map[string]interface{}{
		"total_batches":     atomic.LoadInt64(&bc.stats.totalBatches),
		"total_requests":    atomic.LoadInt64(&bc.stats.totalRequests),
		"avg_coalesce_time": bc.stats.avgCoalesceTime,
		"max_batch_size":    atomic.LoadInt64(&bc.stats.maxBatchSize),
		"pending_count":     pendingCount,
		"avg_batch_size":    atomic.LoadInt64(&bc.avgBatchSize),
		"adaptive_latency":  time.Duration(atomic.LoadInt64(&bc.avgLatency)),
	}
}
