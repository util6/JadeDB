/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
JadeDB 是一个高性能的键值存储数据库，基于 LSM 树结构实现。

主要特性：
- 基于 LSM 树的存储引擎，支持高效的写入和范围查询
- 值日志（Value Log）分离存储，优化大值存储
- 支持事务操作和 MVCC（多版本并发控制）
- 内置压缩和垃圾回收机制
- 支持批量操作和迭代器
- 可配置的优化组件，包括管道处理和合并器

架构组件：
- LSM 树：负责索引和小值存储
- Value Log：负责大值存储，减少写放大
- MemTable：内存中的可变表，基于跳表实现
- SSTable：磁盘上的不可变表
- WAL：预写日志，保证数据持久性
*/

package JadeDB

import (
	"expvar"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/lsm"
	"github.com/util6/JadeDB/utils"

	"github.com/pkg/errors"
)

// 类型别名，避免循环依赖
type Options = lsm.Options
type Stats = lsm.Stats

// oracle 时间戳分配器（简化实现）
type oracle struct {
	nextTs uint64
}

func (o *oracle) newTimestamp() uint64 {
	return atomic.AddUint64(&o.nextTs, 1)
}

func newOracle() *oracle {
	return &oracle{nextTs: 1}
}

// newStats 创建统计信息（简化实现）
func newStats(opt *Options) *Stats {
	return &Stats{}
}

type (
	// CoreAPI 定义了 JadeDB 对外提供的核心功能接口。
	// 这个接口抽象了键值存储的基本操作，包括增删改查和迭代功能。
	// 通过接口设计，可以方便地进行单元测试和功能扩展。
	CoreAPI interface {
		// Set 存储一个键值对条目到数据库中。
		// 参数 data：包含键、值、过期时间等信息的条目
		// 返回值：如果操作失败，返回错误信息
		Set(data *utils.Entry) error

		// Get 根据键从数据库中检索对应的条目。
		// 参数 key：要查找的键
		// 返回值：找到的条目和可能的错误信息
		Get(key []byte) (*utils.Entry, error)

		// Del 从数据库中删除指定键的条目。
		// 参数 key：要删除的键
		// 返回值：如果操作失败，返回错误信息
		Del(key []byte) error

		// NewIterator 创建一个新的迭代器用于遍历数据库。
		// 参数 opt：迭代器的配置选项
		// 返回值：新创建的迭代器实例
		NewIterator(opt *utils.Options) utils.Iterator

		// Info 获取数据库的统计信息。
		// 返回值：包含各种统计数据的结构体
		Info() *Stats

		// Close 关闭数据库，释放所有资源。
		// 返回值：如果关闭过程中发生错误，返回错误信息
		Close() error
	}

	// DB 是 JadeDB 的核心结构体，代表一个数据库实例。
	// 它是全局唯一的，持有所有必要的资源句柄和配置信息。
	// 该结构体采用读写锁来保证并发安全，同时使用原子操作来管理状态。
	DB struct {
		sync.RWMutex // 读写锁，保护数据库的并发访问

		// 核心组件
		opt   *Options  // 数据库配置选项，包含各种参数设置
		lsm   *lsm.LSM  // LSM 树实例，负责索引管理和小值存储
		vlog  *valueLog // 值日志实例，负责大值的分离存储
		stats *Stats    // 统计信息收集器，用于监控数据库性能

		// 并发控制和通信
		flushChan   chan flushTask // 用于刷新内存表到磁盘的任务通道
		writeCh     chan *request  // 写入请求通道，用于批量处理写操作
		blockWrites int32          // 原子变量，控制是否阻塞写操作

		// 值日志相关
		vhead          *utils.ValuePtr // 值日志的头指针，用于垃圾回收
		logRotates     int32           // 日志轮转计数器
		valueThreshold int64           // 值大小阈值，决定是否使用值日志

		// 事务和状态管理
		orc      *oracle // 事务协调器，管理事务的时间戳和冲突检测
		isClosed int32   // 原子变量，标记数据库是否已关闭

		// 性能优化组件
		enableOptimization bool // 是否启用性能优化功能，包括管道处理和合并器
	}
)

var (
	// head 是一个特殊的键，用于存储值日志的头指针位置。
	// 这个键在数据库重启时用于恢复值日志的状态，确保数据的一致性。
	// 前缀 "!corekv!" 确保这个内部键不会与用户数据冲突。
	head = []byte("!corekv!head")
)

// Open 创建并打开一个新的 JadeDB 数据库实例。
// 这是数据库的主要入口点，负责初始化所有必要的组件和启动后台服务。
//
// 参数说明：
// opt: 数据库配置选项，包含工作目录、内存表大小、SSTable大小等参数
//
// 返回值：
// 返回初始化完成的数据库实例
//
// 初始化流程：
// 1. 创建数据库实例并设置基本配置
// 2. 初始化值日志（VLog）组件
// 3. 初始化 LSM 树组件
// 4. 启动各种后台服务（压缩、写入处理、统计收集）
//
// 注意：目前没有实现目录锁，可能需要添加以防止多个进程同时打开同一目录
func Open(opt *Options) *DB {
	// 创建资源管理器，用于优雅关闭
	c := utils.NewCloser()

	// 创建数据库实例并设置基本配置
	db := &DB{opt: opt}

	// 初始化值日志组件
	// 值日志用于存储大值，减少 LSM 树的写放大问题
	db.initVLog()

	// 初始化 LSM 树组件，配置各种参数
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,                         // 工作目录
		MemTableSize:        opt.MemTableSize,                    // 内存表大小
		SSTableMaxSz:        opt.SSTableMaxSz,                    // SSTable 最大大小
		BlockSize:           8 * 1024,                            // 块大小，固定为 8KB
		BloomFalsePositive:  0,                                   // 布隆过滤器假阳性率，0 表示禁用
		BaseLevelSize:       10 << 20,                            // 基础层大小，10MB
		LevelSizeMultiplier: 10,                                  // 层级大小倍数
		BaseTableSize:       5 << 20,                             // 基础表大小，5MB
		TableSizeMultiplier: 2,                                   // 表大小倍数
		NumLevelZeroTables:  15,                                  // 第0层表的最大数量
		MaxLevelNum:         7,                                   // 最大层级数
		NumCompactors:       1,                                   // 压缩器数量
		DiscardStatsCh:      &(db.vlog.lfDiscardStats.flushChan), // 丢弃统计通道
	})

	// 初始化统计信息收集器
	db.stats = newStats(opt)

	// 初始化性能优化组件
	// 默认启用优化功能，包括异步处理和合并优化
	db.enableOptimization = true
	if db.enableOptimization {
		db.lsm.EnableAsyncOptimization(true)
	}

	// 启动 SSTable 的合并压缩后台进程
	// 这个进程负责将多个小的 SSTable 合并成大的 SSTable，减少读放大
	go db.lsm.StartCompacter()

	// 准备值日志垃圾回收
	c.Add(1)

	// 初始化通信通道
	db.writeCh = make(chan *request)        // 写入请求通道
	db.flushChan = make(chan flushTask, 16) // 刷新任务通道，缓冲区大小为16

	// 启动写入处理后台协程
	// 这个协程负责批量处理写入请求，提高写入性能
	go db.doWrites(c)

	// 启动统计信息收集后台进程
	go db.stats.StartStats()

	return db
}

// Close 关闭数据库并释放所有相关资源。
// 这个方法会按顺序关闭各个组件，确保数据的完整性和一致性。
// 关闭顺序很重要：先关闭统计和丢弃统计，再关闭 LSM 和值日志。
//
// 返回值：
// 如果任何组件关闭失败，返回第一个遇到的错误
func (db *DB) Close() error {
	// 首先关闭值日志的丢弃统计收集器
	db.vlog.lfDiscardStats.closer.Close()

	// 关闭 LSM 树组件，这会停止压缩进程并刷新内存表
	if err := db.lsm.Close(); err != nil {
		return err
	}

	// 关闭值日志组件，确保所有待写入的数据都已持久化
	if err := db.vlog.close(); err != nil {
		return err
	}

	// 关闭统计信息收集器（暂时跳过）
	// if err := db.stats.close(); err != nil {
	//     return err
	// }

	return nil
}

// Del 删除指定键的条目。
// 删除操作通过写入一个墓碑标记（tombstone）来实现，而不是物理删除数据。
// 墓碑标记是一个值为 nil 的条目，在后续的压缩过程中会被真正清理。
//
// 参数说明：
// key: 要删除的键
//
// 返回值：
// 如果删除操作失败，返回错误信息
//
// 实现原理：
// LSM 树的删除操作是通过写入墓碑标记来实现的，这样可以保证删除操作的高性能，
// 真正的数据清理会在后续的压缩过程中进行。
func (db *DB) Del(key []byte) error {
	// 创建一个墓碑条目，值为 nil 表示删除
	return db.Set(&utils.Entry{
		Key:       key,
		Value:     nil, // nil 值表示这是一个删除操作
		ExpiresAt: 0,   // 不设置过期时间
	})
}

// Set 向数据库中存储一个键值对条目。
// 这是数据库的核心写入方法，会根据值的大小决定存储策略。
//
// 参数说明：
// data: 要存储的条目，包含键、值、过期时间等信息
//
// 返回值：
// 如果存储操作失败，返回错误信息
//
// 存储策略：
// 1. 小值直接存储在 LSM 树中
// 2. 大值存储在值日志中，LSM 树只存储指向值的指针
// 这种设计可以减少 LSM 树的写放大问题，提高大值存储的性能。
func (db *DB) Set(data *utils.Entry) error {
	// 参数验证：检查条目和键是否有效
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}

	// 声明变量用于值指针处理
	var (
		vp  *utils.ValuePtr // 值指针，用于大值存储
		err error
	)

	// 为键添加时间戳，实现 MVCC（多版本并发控制）
	// 使用 MaxUint32 作为时间戳，表示这是最新版本
	data.Key = utils.KeyWithTs(data.Key, math.MaxUint32)

	// 根据值的大小决定存储策略
	if !db.shouldWriteValueToLSM(data) {
		// 值太大，需要存储在值日志中
		// 首先在值日志中创建值指针
		if vp, err = db.vlog.newValuePtr(data); err != nil {
			return err
		}

		// 设置元数据标记，表示这是一个值指针
		data.Meta |= utils.BitValuePointer

		// 将值替换为编码后的指针
		data.Value = vp.Encode()
	}

	// 将条目存储到 LSM 树中
	// 如果是小值，直接存储值；如果是大值，存储值指针
	return db.lsm.Set(data)
}

// Get 根据键从数据库中检索对应的条目。
// 这是数据库的核心读取方法，支持从 LSM 树和值日志中读取数据。
//
// 参数说明：
// key: 要查找的键
//
// 返回值：
// 找到的条目和可能的错误信息
//
// 查找流程：
// 1. 首先从 LSM 树中查找条目
// 2. 如果条目包含值指针，则从值日志中读取实际值
// 3. 检查条目是否已删除或过期
// 4. 返回最终结果
//
// MVCC 支持：
// 通过为键添加时间戳来实现多版本并发控制，确保读取到正确版本的数据。
func (db *DB) Get(key []byte) (*utils.Entry, error) {
	// 参数验证：检查键是否为空
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}

	// 保存原始键，用于最终返回
	originKey := key
	var (
		entry *utils.Entry // 从 LSM 树中读取的条目
		err   error
	)

	// 为键添加时间戳，用于 MVCC 查找
	// 使用 MaxUint32 表示查找最新版本
	key = utils.KeyWithTs(key, math.MaxUint32)

	// 从 LSM 树中查询条目
	// 此时不确定条目的值是直接存储的还是值指针
	if entry, err = db.lsm.Get(key); err != nil {
		return entry, err
	}

	// 检查从 LSM 树获取的值是否是值指针
	if entry != nil && utils.IsValuePtr(entry) {
		// 如果是值指针，需要从值日志中读取实际值
		var vp utils.ValuePtr
		vp.Decode(entry.Value) // 解码值指针

		// 从值日志中读取实际值
		result, cb, err := db.vlog.read(&vp)
		defer utils.RunCallback(cb) // 确保回调函数被执行，用于资源清理

		if err != nil {
			return nil, err
		}

		// 安全复制读取到的值，避免内存引用问题
		entry.Value = utils.SafeCopy(nil, result)
	}

	// 检查条目是否已被删除或已过期
	if lsm.IsDeletedOrExpired(entry) {
		return nil, utils.ErrKeyNotFound
	}

	// 恢复原始键（去除时间戳）
	entry.Key = originKey
	return entry, nil
}

// Info 获取数据库的统计信息。
// 返回包含各种性能指标和状态信息的统计结构体。
//
// 返回值：
// 包含数据库统计信息的 Stats 结构体
//
// 统计信息包括：
// - 读写操作计数
// - 内存使用情况
// - 磁盘使用情况
// - 压缩统计
// - 性能指标等
func (db *DB) Info() *Stats {
	// 直接返回统计信息结构体
	// 统计信息由后台进程持续更新
	return db.stats
}

// SetOptimizationEnabled 设置是否启用优化
func (db *DB) SetOptimizationEnabled(enabled bool) {
	db.enableOptimization = enabled
	db.lsm.EnableAsyncOptimization(enabled)
}

// BatchSet 批量设置键值对
func (db *DB) BatchSet(entries []*utils.Entry) error {
	return db.lsm.BatchSet(entries)
}

// GetPipelineStats 获取管道统计信息
func (db *DB) GetPipelineStats() map[string]interface{} {
	return db.lsm.GetPipelineStats()
}

// GetCoalescerStats 获取合并器统计信息
func (db *DB) GetCoalescerStats() map[string]interface{} {
	return db.lsm.GetCoalescerStats()
}

// GetAllOptimizationStats 获取所有优化组件的统计信息
func (db *DB) GetAllOptimizationStats() map[string]interface{} {
	stats := make(map[string]interface{})

	if pipelineStats := db.GetPipelineStats(); pipelineStats != nil {
		stats["pipeline"] = pipelineStats
	}

	if coalescerStats := db.GetCoalescerStats(); coalescerStats != nil {
		stats["coalescer"] = coalescerStats
	}

	return stats
}

// RunValueLogGC triggers a value log garbage collection.
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	// Find head on disk
	headKey := utils.KeyWithTs(head, math.MaxUint64)
	val, err := db.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = &utils.Entry{
				Key:   headKey,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}

	// 内部key head 一定是value ptr 不需要检查内容
	var head utils.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return db.vlog.runGC(discardRatio, &head)
}

func (db *DB) shouldWriteValueToLSM(e *utils.Entry) bool {
	return int64(len(e.Value)) < db.opt.ValueThreshold
}

func (db *DB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var count, size int64
	for _, e := range entries {
		size += int64(e.EstimateSize(int(db.opt.ValueThreshold)))
		count++
	}
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	// TODO 尝试使用对象复用，后面entry对象也应该使用
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()     // for db write
	db.writeCh <- req // Handled in doWrites.
	return req, nil
}

// Check(kv.BatchSet(entries))
func (db *DB) batchSet(entries []*utils.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

func (db *DB) doWrites(lc *utils.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			utils.Err(fmt.Errorf("writeRequests: %v", err))
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-db.writeCh:
		case <-lc.CloseSignal:
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*utils.KVWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.CloseSignal:
				goto closedCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

// writeRequests is called serially by only one goroutine.
func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}
	err := db.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := db.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(b.Ptrs)
		db.Unlock()
	}
	done(nil)
	return nil
}

// writeToLSM 将请求中的数据写入 LSM 树。
// 该函数负责决定每个条目应如何处理并存储到 LSM 树中。

func (db *DB) writeToLSM(b *request) error {
	// 检查请求中的 Ptrs 和 Entries 数量是否匹配，如果不匹配则返回错误。
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs 和 Entries 不匹配: %+v", b)
	}

	// 遍历请求中的每个条目，决定其应如何处理和存储。
	for i, entry := range b.Entries {
		// 判断条目的值是否应该直接写入 LSM 树。
		if db.shouldWriteValueToLSM(entry) {
			// 清除条目元数据中的指针位，表示值是直接存储的。
			entry.Meta = entry.Meta &^ utils.BitValuePointer
		} else {
			// 设置条目元数据中的指针位，表示值是以指针形式存储的。
			entry.Meta = entry.Meta | utils.BitValuePointer
			// 编码并存储指向值位置的指针。
			entry.Value = b.Ptrs[i].Encode()
		}
		// 将处理后的条目存储到 LSM 树中。
		db.lsm.Set(entry)
	}
	// 如果所有条目都已成功处理并存储，则返回 nil。
	return nil
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}

// 结构体
type flushTask struct {
	mt           *utils.SkipList
	vptr         *utils.ValuePtr
	dropPrefixes [][]byte
}

func (db *DB) pushHead(ft flushTask) error {
	// Ensure we never push a zero valued head pointer.
	if ft.vptr.IsZero() {
		return errors.New("Head should not be zero")
	}

	fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()

	// Pick the max commit ts, so in case of crash, our read ts would be higher than all the
	// commits.
	headTs := utils.KeyWithTs(head, uint64(time.Now().Unix()/1e9))
	ft.mt.Add(&utils.Entry{
		Key:   headTs,
		Value: val,
	})
	return nil
}

// IsClosed 检查数据库是否已关闭
func (db *DB) IsClosed() bool {
	return atomic.LoadInt32(&db.isClosed) != 0
}
