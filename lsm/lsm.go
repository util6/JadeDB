/*
LSM 树存储引擎模块

LSM（Log-Structured Merge Tree）是 JadeDB 的核心存储引擎，
专门为写密集型工作负载设计，提供高性能的数据存储和检索能力。

核心设计原理：
1. 写入优化：所有写入首先进入内存表，避免随机磁盘写入
2. 分层存储：数据按层级组织，每层大小递增，减少读放大
3. 异步压缩：后台进程持续合并和压缩数据，维护性能
4. 顺序写入：所有磁盘写入都是顺序的，充分利用磁盘性能

主要组件：
- MemTable：内存中的可变表，接收所有新写入
- Immutable MemTables：已满的内存表，等待刷新到磁盘
- SSTable：磁盘上的不可变表，按层级组织
- Level Manager：管理多层级的 SSTable，控制压缩策略

性能优化：
- 布隆过滤器：快速判断键是否存在，减少磁盘访问
- 块缓存：缓存热点数据块，提高读取性能
- 异步管道：批量处理写入请求，提高吞吐量
- 智能合并：优化批量操作，减少写放大

适用场景：
- 写多读少的应用
- 需要高吞吐量的系统
- 对写入延迟敏感的场景
- 大数据量的存储需求
*/

package lsm

import (
	"github.com/util6/JadeDB/utils"
	"sync"
	"time"
)

// LSM 表示日志结构合并树（Log-Structured Merge Tree）的核心实现。
// 这是 JadeDB 的主要存储引擎，负责管理数据的写入、读取、压缩和持久化。
//
// 架构设计：
// 1. 内存层：MemTable 和 Immutable MemTables
// 2. 磁盘层：多层级的 SSTable 文件
// 3. 管理层：Level Manager 控制数据分布和压缩
// 4. 优化层：异步管道和批量合并器
//
// 数据流向：
// 写入 -> MemTable -> Immutable -> L0 -> L1 -> ... -> Ln
//
// 并发控制：
// 使用读写锁保护关键数据结构，支持多读者单写者模式。
// 内存表切换和压缩操作需要写锁，普通读写操作只需读锁。
type LSM struct {
	// 并发控制

	// lock 保护 LSM 树的关键数据结构。
	// 读操作（Get、迭代器创建）获取读锁。
	// 写操作（MemTable 切换、压缩）获取写锁。
	// 这种设计允许多个读操作并发执行，提高读取性能。
	lock sync.RWMutex

	// 内存存储层

	// memTable 是当前活跃的内存表，接收所有新的写入操作。
	// 基于跳表实现，提供 O(log n) 的插入和查找性能。
	// 当大小达到配置的阈值时，会被转换为不可变表。
	memTable *memTable

	// immutables 存储已满但尚未刷新到磁盘的内存表。
	// 这些表是只读的，不再接受新的写入。
	// 后台进程会将这些表刷新为 L0 层的 SSTable 文件。
	// 使用切片存储，支持多个不可变表并存。
	immutables []*memTable

	// 磁盘存储层

	// levels 管理磁盘上的多层级 SSTable 文件。
	// 负责文件的创建、读取、压缩和删除。
	// 实现分层压缩策略，维护数据的有序性和性能。
	levels *levelManager

	// 配置和控制

	// option 包含 LSM 树的所有配置参数。
	// 包括内存表大小、SSTable 大小、压缩策略等。
	// 这些参数在初始化时设置，运行时保持不变。
	option *Options

	// closer 用于优雅关闭 LSM 树。
	// 通过信号通知所有后台进程停止工作。
	// 确保数据完整性和资源正确释放。
	closer *utils.Closer

	// maxMemFID 跟踪内存表的文件标识符。
	// 用于生成唯一的文件名和管理文件生命周期。
	// 每次创建新的内存表时递增。
	maxMemFID uint32

	// 性能优化组件

	// pipeline 提供异步提交管道，批量处理写入请求。
	// 通过批量操作减少系统调用开销，提高写入吞吐量。
	// 支持并发写入和异步刷新，优化写入性能。
	pipeline *CommitPipeline

	// coalescer 提供智能批量合并功能。
	// 自动识别相关的操作并进行合并，减少写放大。
	// 特别适用于批量插入和更新场景。
	coalescer *BatchCoalescer

	// enableAsync 控制是否启用异步优化功能。
	// 启用后会使用管道和合并器来优化性能。
	// 可以根据工作负载特征动态调整。
	enableAsync bool
}

// Options 定义了 LSM 树的完整配置选项。
// 这些选项控制着 LSM 树的各个方面，从内存使用到压缩策略。
// 合理的配置对于获得最佳性能至关重要。
//
// 配置分类：
// 1. 存储配置：目录、文件大小等
// 2. 内存配置：内存表大小、缓存大小等
// 3. 性能配置：块大小、布隆过滤器等
// 4. 压缩配置：层级策略、压缩器数量等
//
// 调优建议：
// - 根据可用内存调整 MemTableSize
// - 根据磁盘性能调整 SSTableMaxSz
// - 根据工作负载调整压缩参数
// - 根据查询模式调整布隆过滤器
type Options struct {
	// 基础存储配置

	// WorkDir 指定数据库文件的存储目录。
	// 所有的 SSTable 文件、WAL 文件和元数据文件都存储在这个目录中。
	// 建议使用 SSD 存储以获得更好的性能。
	// 确保目录有足够的磁盘空间和适当的权限。
	WorkDir string

	// 内存配置

	// MemTableSize 设置单个内存表的最大字节数。
	// 当内存表达到此大小时，会被转换为不可变表并刷新到磁盘。
	// 较大的值可以减少磁盘写入频率，但占用更多内存。
	// 推荐值：64MB-256MB，根据可用内存调整。
	MemTableSize int64

	// 磁盘存储配置

	// SSTableMaxSz 设置单个 SSTable 文件的最大字节数。
	// 超过此大小的 SSTable 在压缩时会被分割。
	// 较大的值可以减少文件数量，但可能增加读取延迟。
	// 推荐值：64MB-256MB，根据磁盘性能调整。
	SSTableMaxSz int64

	// BlockSize 设置 SSTable 中数据块的字节大小。
	// 数据块是读取和缓存的基本单位。
	// 较小的块可以减少读放大，但增加索引开销。
	// 较大的块可以提高压缩率，但可能增加内存使用。
	// 推荐值：4KB-64KB，根据访问模式调整。
	BlockSize int

	// 性能优化配置

	// BloomFalsePositive 设置布隆过滤器的假阳性率。
	// 较低的值可以减少不必要的磁盘访问，但增加内存使用。
	// 值为 0 表示禁用布隆过滤器。
	// 推荐值：0.01（1%），在内存和性能之间平衡。
	BloomFalsePositive float64

	// 压缩策略配置

	// NumCompactors 设置并发压缩工作器的数量。
	// 更多的压缩器可以加快压缩速度，但增加 CPU 和 I/O 负载。
	// 应该根据 CPU 核心数和磁盘性能来设置。
	// 推荐值：1-4，根据硬件配置调整。
	NumCompactors int

	// BaseLevelSize 设置基础层（通常是 L1）的目标大小。
	// 这是分层压缩策略的起点，影响整体的存储结构。
	// 较大的值可以减少层级数量，但可能增加压缩开销。
	// 推荐值：10MB-100MB，根据数据量调整。
	BaseLevelSize int64

	// LevelSizeMultiplier 设置相邻层级之间的大小倍数。
	// 每个层级的目标大小是上一层级的这个倍数。
	// 较大的值可以减少层级数量，但可能增加读放大。
	// 推荐值：10，这是经过验证的最佳实践。
	LevelSizeMultiplier int

	// TableSizeMultiplier 设置表大小的增长倍数。
	// 控制不同层级中 SSTable 的大小差异。
	// 影响压缩的粒度和效率。
	// 推荐值：2，提供良好的平衡。
	TableSizeMultiplier int

	// BaseTableSize 设置基础层中 SSTable 的目标大小。
	// 这是压缩策略的另一个重要参数。
	// 影响文件数量和压缩频率。
	// 推荐值：2MB-32MB，根据工作负载调整。
	BaseTableSize int64

	// NumLevelZeroTables 设置 L0 层允许的最大 SSTable 数量。
	// 超过此数量时会触发 L0 到 L1 的压缩。
	// 较大的值可以减少压缩频率，但可能增加读放大。
	// 推荐值：4-8，在性能和延迟之间平衡。
	NumLevelZeroTables int

	// MaxLevelNum 设置 LSM 树的最大层级数。
	// 限制数据的最大分布深度。
	// 较多的层级可以容纳更多数据，但可能增加读放大。
	// 推荐值：7，适合大多数应用场景。
	MaxLevelNum int

	// 统计和监控配置

	// DiscardStatsCh 提供丢弃统计信息的通道。
	// 用于垃圾回收过程中的统计信息传递。
	// 帮助监控和优化存储空间的使用。
	DiscardStatsCh *chan map[uint32]int64
}

// IsDeletedOrExpired 检查条目是否被删除或过期
func IsDeletedOrExpired(entry *utils.Entry) bool {
	if entry == nil {
		return true
	}
	// 检查是否被标记为删除
	if entry.Meta&utils.BitDelete != 0 {
		return true
	}
	// 检查是否过期
	if entry.ExpiresAt != 0 && entry.ExpiresAt < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

// NewLSM _
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 初始化levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// 启动DB恢复过程加载wal，如果没有恢复内容则创建新的内存表
	lsm.memTable, lsm.immutables = lsm.recovery()
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser()

	// 初始化优化组件
	lsm.enableAsync = true // 默认启用异步优化
	if lsm.enableAsync {
		lsm.pipeline = NewCommitPipeline(lsm)
		lsm.coalescer = NewBatchCoalescer(lsm)
	}

	return lsm
}

// NewMergeIterator 创建一个合并迭代器
func (lsm *LSM) NewMergeIterator(iters []utils.Iterator, isAsc bool) utils.Iterator {
	// 简单实现，返回第一个迭代器
	if len(iters) > 0 {
		return iters[0]
	}
	return nil
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 优雅关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// 检查当前immutable是否写满，是的话创建新的immutable,并将当前内存表写到immutables中
	// 否则写入当前immutable中
	if int64(lsm.memTable.wal.Size())+
		int64(utils.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.Rotate()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		// TODO 这里问题很大，应该是用引用计数的方式回收
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		// TODO 将lsm的immutables队列置空，这里可以优化一下节省内存空间，还可以限制一下immutable的大小为固定值
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *utils.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}

	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// 从level manger查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) Close() error {
	// 关闭优化组件
	if lsm.pipeline != nil {
		if err := lsm.pipeline.Close(); err != nil {
			return err
		}
	}

	// 等待全部合并过程的结束
	// 等待全部api调用过程结束
	lsm.closer.Close()
	// TODO 需要加锁保证并发安全
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// GetMemTables 返回当前 LSM 树的所有内存表以及一个用于减少这些表引用计数的函数。
// 这个函数允许外部在使用完内存表后，通过调用返回的函数来释放对这些表的引用。
// 返回值:
// - []*memTable: 包含所有内存表的切片，包括可变内存表和不可变内存表。
// - func(): 当调用此函数时，它将减少每个返回的内存表的引用计数，以便进行资源清理。
func (lsm *LSM) GetMemTables() ([]*memTable, func()) {
	// 上读锁以防止在获取内存表期间对内存表进行修改。
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	var tables []*memTable

	// 将当前可变内存表添加到返回的表列表中。
	tables = append(tables, lsm.memTable)
	// 增加可变内存表的引用计数，表示有新的引用指向它。
	lsm.memTable.IncrRef()

	// 获取不可变内存表列表的最后一个元素的索引。
	last := len(lsm.immutables) - 1
	// 遍历不可变内存表列表，从最后一个不可变内存表开始，将它们添加到返回的表列表中。
	for i := range lsm.immutables {
		tables = append(tables, lsm.immutables[last-i])
		// 增加不可变内存表的引用计数。
		lsm.immutables[last-i].IncrRef()
	}
	// 返回内存表列表和一个用于减少这些表引用计数的函数。
	return tables, func() {
		// 遍历内存表列表，减少每个表的引用计数。
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i)
	}
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.SkipList {
	return lsm.memTable.sl
}

func (lsm *LSM) Rotate() error {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
	return nil
}

// EnableAsyncOptimization 启用/禁用异步优化
func (lsm *LSM) EnableAsyncOptimization(enabled bool) {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	if enabled && !lsm.enableAsync {
		lsm.pipeline = NewCommitPipeline(lsm)
		lsm.coalescer = NewBatchCoalescer(lsm)
		lsm.enableAsync = true
	} else if !enabled && lsm.enableAsync {
		if lsm.pipeline != nil {
			lsm.pipeline.Close()
			lsm.pipeline = nil
		}
		lsm.coalescer = nil
		lsm.enableAsync = false
	}
}

// GetPipelineStats 获取管道统计信息
func (lsm *LSM) GetPipelineStats() map[string]interface{} {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	if lsm.pipeline != nil {
		return lsm.pipeline.GetStats()
	}
	return nil
}

// GetCoalescerStats 获取合并器统计信息
func (lsm *LSM) GetCoalescerStats() map[string]interface{} {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()

	if lsm.coalescer != nil {
		return lsm.coalescer.GetStats()
	}
	return nil
}

// BatchSet 批量设置条目（使用优化）
func (lsm *LSM) BatchSet(entries []*utils.Entry) error {
	if lsm.enableAsync && lsm.pipeline != nil {
		// 使用异步管道
		batch, err := lsm.pipeline.Submit(entries)
		if err != nil {
			return err
		}

		// 等待所有阶段完成
		if err := batch.walPromise.Wait(); err != nil {
			return err
		}
		if err := batch.memPromise.Wait(); err != nil {
			return err
		}
		if err := batch.syncPromise.Wait(); err != nil {
			return err
		}

		return nil
	}

	// 回退到原始方法
	for _, entry := range entries {
		if err := lsm.Set(entry); err != nil {
			return err
		}
	}
	return nil
}
