/*
LSM 树提交管道（Commit Pipeline）模块

提交管道是 JadeDB 中的高性能写入优化组件，通过流水线处理提高写入吞吐量。
它将写入操作分解为多个阶段，每个阶段并行处理，显著提升写入性能。

核心设计：
1. 三阶段流水线：WAL写入 -> 内存表更新 -> 同步刷新
2. 异步处理：每个阶段在独立的 goroutine 中异步执行
3. 批量优化：将多个操作合并成批次，减少系统调用
4. 背压控制：通过通道缓冲区实现流量控制

设计原理：
- 流水线并行：多个阶段同时处理不同的批次
- 批量处理：减少系统调用和锁竞争
- 异步执行：提高 CPU 和 I/O 资源利用率
- 错误隔离：每个阶段独立处理错误

性能优势：
- 高吞吐量：流水线并行处理提高整体吞吐量
- 低延迟：异步处理减少写入操作的等待时间
- 资源优化：充分利用多核 CPU 和 I/O 并发
- 批量效应：批量操作摊薄单次操作的开销

适用场景：
- 写入密集型工作负载
- 需要高吞吐量的应用
- 批量数据导入
- 实时数据处理

流水线阶段：
1. WAL 阶段：将数据写入预写日志，保证持久性
2. MemTable 阶段：更新内存表，提供即时可见性
3. Sync 阶段：批量同步磁盘，确保数据安全

错误处理：
- 阶段隔离：每个阶段独立处理错误
- Promise 机制：异步错误传播
- 优雅降级：错误时停止后续处理
- 状态跟踪：详细的错误信息和状态
*/

package lsm

import (
	"github.com/util6/JadeDB/utils"
	"sync/atomic"
	"time"
)

// CommitBatch 表示一个提交批次，包含一组需要一起处理的写入操作。
// 批次是流水线处理的基本单位，通过批量处理提高效率。
//
// 生命周期：
// 1. 创建：收集一组写入操作创建批次
// 2. WAL阶段：写入预写日志
// 3. MemTable阶段：更新内存表
// 4. Sync阶段：同步到磁盘
// 5. 完成：通知所有等待者
//
// Promise 机制：
// 每个阶段都有对应的 Promise，用于异步通知阶段完成状态。
// 调用者可以选择等待特定阶段或全部阶段完成。
type CommitBatch struct {
	// 批次标识

	// id 是批次的唯一标识符。
	// 用于日志记录、调试和性能分析。
	// 通过原子递增生成，保证唯一性。
	id uint64

	// 数据内容

	// entries 包含批次中的所有写入条目。
	// 这些条目会按顺序处理，保持操作的原子性。
	entries []*utils.Entry

	// ptrs 存储值指针，用于大值的分离存储。
	// 与 entries 一一对应，记录值在值日志中的位置。
	ptrs []*utils.ValuePtr

	// 异步控制

	// walPromise 用于通知 WAL 写入阶段的完成状态。
	// 调用者可以等待此 Promise 确认数据已持久化到 WAL。
	walPromise *utils.Promise

	// memPromise 用于通知内存表更新阶段的完成状态。
	// 调用者可以等待此 Promise 确认数据已在内存中可见。
	memPromise *utils.Promise

	// syncPromise 用于通知同步阶段的完成状态。
	// 调用者可以等待此 Promise 确认数据已同步到磁盘。
	syncPromise *utils.Promise

	// 时间戳

	// timestamp 记录批次创建的时间。
	// 用于性能分析、延迟统计和调试。
	timestamp time.Time
}

// NewCommitBatch 创建一个新的提交批次。
// 这是批次的构造函数，初始化所有必要的字段和 Promise。
//
// 参数说明：
// entries: 要包含在批次中的写入条目
//
// 返回值：
// 初始化完成的提交批次
//
// 初始化过程：
// 1. 分配唯一的批次 ID
// 2. 存储写入条目
// 3. 创建值指针数组
// 4. 初始化各阶段的 Promise
// 5. 记录创建时间戳
func NewCommitBatch(entries []*utils.Entry) *CommitBatch {
	return &CommitBatch{
		id:          atomic.AddUint64(&batchIDCounter, 1),  // 原子递增生成唯一ID
		entries:     entries,                               // 存储写入条目
		ptrs:        make([]*utils.ValuePtr, len(entries)), // 预分配值指针数组
		walPromise:  utils.NewPromise(),                    // WAL阶段Promise
		memPromise:  utils.NewPromise(),                    // MemTable阶段Promise
		syncPromise: utils.NewPromise(),                    // Sync阶段Promise
		timestamp:   time.Now(),                            // 记录创建时间
	}
}

// batchIDCounter 是全局的批次ID计数器。
// 使用原子操作确保在并发环境下的唯一性。
var batchIDCounter uint64

// CommitPipeline 异步提交管道
type CommitPipeline struct {
	lsm       *LSM
	walStage  chan *CommitBatch
	memStage  chan *CommitBatch
	syncStage chan *CommitBatch
	closer    *utils.Closer

	// 统计信息
	stats struct {
		walBatches   int64
		memBatches   int64
		syncBatches  int64
		avgBatchSize int64
		avgLatency   time.Duration
	}
}

// NewCommitPipeline 创建新的提交管道
func NewCommitPipeline(lsm *LSM) *CommitPipeline {
	cp := &CommitPipeline{
		lsm:       lsm,
		walStage:  make(chan *CommitBatch, 64),
		memStage:  make(chan *CommitBatch, 64),
		syncStage: make(chan *CommitBatch, 64),
		closer:    utils.NewCloserInitial(3), // 使用NewCloserInitial初始化3个工作协程
	}

	// 启动管道工作协程
	go cp.walWorker()
	go cp.memWorker()
	go cp.syncWorker()

	return cp
}

// Submit 提交一个批次到管道
func (cp *CommitPipeline) Submit(entries []*utils.Entry) (*CommitBatch, error) {
	// 检查管道是否已关闭
	select {
	case <-cp.closer.HasBeenClosed():
		return nil, utils.ErrDBClosed
	default:
	}

	batch := NewCommitBatch(entries)

	select {
	case cp.walStage <- batch:
		return batch, nil
	case <-cp.closer.HasBeenClosed():
		return nil, utils.ErrDBClosed
	default:
		return nil, utils.ErrBlockedWrites
	}
}

// walWorker WAL写入工作协程
func (cp *CommitPipeline) walWorker() {
	defer cp.closer.Done()

	for {
		select {
		case batch := <-cp.walStage:
			err := cp.processWALStage(batch)
			batch.walPromise.Resolve(err)

			if err == nil {
				// 传递到下一阶段
				select {
				case cp.memStage <- batch:
				case <-cp.closer.HasBeenClosed():
					return
				}
			}

			atomic.AddInt64(&cp.stats.walBatches, 1)

		case <-cp.closer.HasBeenClosed():
			return
		}
	}
}

// memWorker 内存表更新工作协程
func (cp *CommitPipeline) memWorker() {
	defer cp.closer.Done()

	for {
		select {
		case batch := <-cp.memStage:
			err := cp.processMemStage(batch)
			batch.memPromise.Resolve(err)

			if err == nil {
				// 传递到下一阶段
				select {
				case cp.syncStage <- batch:
				case <-cp.closer.HasBeenClosed():
					return
				}
			}

			atomic.AddInt64(&cp.stats.memBatches, 1)

		case <-cp.closer.HasBeenClosed():
			return
		}
	}
}

// syncWorker 同步工作协程
func (cp *CommitPipeline) syncWorker() {
	defer cp.closer.Done()

	batches := make([]*CommitBatch, 0, 8)
	ticker := time.NewTicker(10 * time.Millisecond) // 10ms批量同步
	defer ticker.Stop()

	flushBatches := func() {
		if len(batches) == 0 {
			return
		}

		err := cp.processSyncStage(batches)
		for _, batch := range batches {
			batch.syncPromise.Resolve(err)
		}

		atomic.AddInt64(&cp.stats.syncBatches, int64(len(batches)))
		batches = batches[:0]
	}

	for {
		select {
		case batch := <-cp.syncStage:
			batches = append(batches, batch)

			// 达到批量大小或超时时刷新
			if len(batches) >= 8 {
				flushBatches()
			}

		case <-ticker.C:
			flushBatches()

		case <-cp.closer.HasBeenClosed():
			flushBatches()
			return
		}
	}
}

// processWALStage 处理WAL写入阶段
func (cp *CommitPipeline) processWALStage(batch *CommitBatch) error {
	// 写入WAL
	for _, entry := range batch.entries {
		if err := cp.lsm.memTable.wal.Write(entry); err != nil {
			return err
		}
	}
	return nil
}

// processMemStage 处理内存表更新阶段
func (cp *CommitPipeline) processMemStage(batch *CommitBatch) error {
	// 更新内存表
	for _, entry := range batch.entries {
		cp.lsm.memTable.sl.Add(entry)
	}

	// 检查是否需要轮转内存表
	if cp.shouldRotateMemTable() {
		return cp.lsm.Rotate()
	}

	return nil
}

// processSyncStage 处理同步阶段
// 负责将 WAL 文件的更改同步到磁盘，确保数据持久性
func (cp *CommitPipeline) processSyncStage(batches []*CommitBatch) error {
	// 批量同步WAL文件
	// 使用 WAL 的公共 Sync 方法进行同步操作
	if cp.lsm.memTable.wal != nil {
		return cp.lsm.memTable.wal.Sync()
	}
	return nil
}

// shouldRotateMemTable 检查是否需要轮转内存表
func (cp *CommitPipeline) shouldRotateMemTable() bool {
	return cp.lsm.memTable.Size() >= cp.lsm.option.MemTableSize
}

// Close 关闭管道
func (cp *CommitPipeline) Close() error {
	if cp.closer != nil {
		cp.closer.SignalAndWait()
	}
	return nil
}

// GetStats 获取统计信息
func (cp *CommitPipeline) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"wal_batches":    atomic.LoadInt64(&cp.stats.walBatches),
		"mem_batches":    atomic.LoadInt64(&cp.stats.memBatches),
		"sync_batches":   atomic.LoadInt64(&cp.stats.syncBatches),
		"avg_batch_size": atomic.LoadInt64(&cp.stats.avgBatchSize),
		"avg_latency":    cp.stats.avgLatency,
	}
}
