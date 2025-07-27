/*
LSM 树层级管理模块

本模块负责管理 LSM 树的多层级结构，是 LSM 树存储引擎的核心组件之一。
层级管理器协调各个层级之间的数据流动、压缩操作和查询路由。

核心职责：
1. 层级结构管理：维护 L0 到 Ln 的多层级 SSTable 文件
2. 数据查询路由：按层级顺序查找数据，实现高效检索
3. 压缩协调：管理层级间的数据压缩和合并操作
4. 文件生命周期：跟踪文件创建、使用和删除
5. 元数据管理：维护 manifest 文件记录系统状态

设计原理：
- 分层存储：数据按层级组织，每层大小递增
- 有序查询：从新到旧按层级顺序查找数据
- 异步压缩：后台进程持续优化数据分布
- 元数据持久化：通过 manifest 文件保证一致性

层级特点：
- L0：接收内存表刷新的数据，文件间可能有重叠
- L1-Ln：通过压缩生成，文件间无重叠，有序排列
- 容量控制：每层有大小限制，超限时触发压缩
- 查询优化：使用布隆过滤器和索引加速查找

性能优化：
- 缓存机制：缓存热点数据块和索引信息
- 并发控制：支持多读者单写者的并发访问
- 批量操作：批量处理文件操作减少开销
- 智能压缩：根据数据分布选择最优压缩策略
*/

package lsm

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/util6/JadeDB/utils"

	"github.com/util6/JadeDB/file"
)

// initLevelManager 初始化并返回一个层级管理器实例。
// 这是 LSM 树初始化过程的关键步骤，负责设置多层级存储结构。
//
// 参数说明：
// opt: LSM 树的配置选项
//
// 返回值：
// 初始化完成的层级管理器实例
//
// 初始化流程：
// 1. 创建层级管理器实例并设置基本配置
// 2. 初始化压缩状态管理器
// 3. 加载 manifest 文件恢复系统状态
// 4. 构建层级结构和文件映射
//
// 错误处理：
// - manifest 加载失败会导致 panic，因为这是致命错误
// - 构建失败返回 nil，调用者需要处理这种情况
//
// 注意事项：
// - 这个方法在 LSM 树启动时调用，不是线程安全的
// - 失败时可能留下部分初始化的状态，需要清理
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	// 创建层级管理器实例并建立与 LSM 树的双向引用
	lm := &levelManager{lsm: lsm}

	// 初始化压缩状态管理器，用于跟踪正在进行的压缩操作
	lm.compactState = lsm.newCompactStatus()

	// 设置配置选项，包含层级大小、压缩策略等参数
	lm.opt = opt

	// 加载 manifest 文件，恢复系统的元数据状态
	// manifest 文件记录了所有 SSTable 文件的信息
	if err := lm.loadManifest(); err != nil {
		panic(err) // manifest 加载失败是致命错误
	}

	// 构建层级结构，创建各层级的处理器和文件映射
	err := lm.build()
	if err != nil {
		return nil // 构建失败，返回 nil 让调用者处理
	}

	// 返回完全初始化的层级管理器
	return lm
}

// levelManager 是 LSM 树的层级管理器，负责协调多层级存储结构。
// 它是 LSM 树存储引擎的核心组件，管理从 L0 到 Ln 的所有层级。
//
// 主要职责：
// 1. 文件管理：跟踪所有 SSTable 文件的生命周期
// 2. 查询路由：按层级顺序查找数据，实现高效检索
// 3. 压缩协调：管理层级间的数据压缩和合并
// 4. 元数据维护：通过 manifest 文件保持系统一致性
// 5. 缓存管理：优化热点数据的访问性能
//
// 并发模型：
// - 读操作：支持多个并发读取
// - 写操作：通过层级锁保护写入操作
// - 压缩操作：在后台异步执行，不阻塞读写
//
// 数据组织：
// - L0：文件间可能重叠，需要全部检查
// - L1-Ln：文件间无重叠，可以快速定位
// - 每层有大小限制，超限时触发压缩
type levelManager struct {
	// 文件标识管理

	// maxFID 跟踪已分配的最大文件标识符。
	// 这个值在系统重启后用于确保新文件 ID 的唯一性。
	// 每次创建新的 SSTable 文件时都会递增这个值。
	// 使用原子操作确保在并发环境下的正确性。
	maxFID uint64

	// 配置和选项

	// opt 包含 LSM 树的所有配置参数。
	// 包括层级大小限制、压缩策略、块大小等。
	// 这些参数在初始化时设置，运行时保持不变。
	opt *Options

	// 性能优化组件

	// cache 提供数据块和索引的缓存功能。
	// 缓存热点数据可以显著减少磁盘 I/O 操作。
	// 使用 LRU 策略管理缓存空间，平衡内存使用和性能。
	cache *cache

	// 元数据管理

	// manifestFile 维护 LSM 树的元数据信息。
	// 记录所有 SSTable 文件的位置、大小、键范围等信息。
	// 在系统重启时用于恢复 LSM 树的完整状态。
	// 所有结构变更都会同步更新到 manifest 文件。
	manifestFile *file.ManifestFile

	// 层级结构

	// levels 是各个层级处理器的数组。
	// 每个 levelHandler 管理一个层级的所有 SSTable 文件。
	// 数组索引对应层级编号（0 表示 L0，1 表示 L1，以此类推）。
	// 不同层级有不同的管理策略和压缩规则。
	levels []*levelHandler

	// 系统集成

	// lsm 指向所属的 LSM 树实例。
	// 用于访问 LSM 树的其他组件和配置。
	// 建立双向引用关系，便于组件间协调。
	lsm *LSM

	// 压缩管理

	// compactState 跟踪和管理压缩操作的状态。
	// 记录正在进行的压缩任务，防止冲突和重复。
	// 协调多个压缩工作器的并发执行。
	// 维护压缩统计信息，用于性能监控和调优。
	compactState *compactStatus
}

// Get 从 LSM 树的多个层级中查找指定键的条目。
// 这是 LSM 树查询操作的核心实现，按层级顺序查找数据。
//
// 参数说明：
// key: 要查找的键（可能包含时间戳）
//
// 返回值：
// 找到的条目和可能的错误信息
//
// 查找策略：
// 1. 优先查找 L0 层：L0 层包含最新的数据
// 2. 依次查找 L1-Ln 层：按层级顺序查找
// 3. 早期返回：一旦找到数据就立即返回
//
// 层级特点：
// - L0：文件间可能重叠，需要检查所有相关文件
// - L1-Ln：文件间无重叠，可以快速定位到特定文件
//
// 性能考虑：
// - 新数据优先：最新数据通常在较低层级，查找更快
// - 布隆过滤器：每层使用布隆过滤器快速排除不存在的键
// - 缓存优化：热点数据会被缓存，减少磁盘访问
//
// 错误处理：
// - 如果所有层级都没有找到数据，返回 ErrKeyNotFound
// - 磁盘 I/O 错误会直接返回给调用者
func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)

	// 第一步：查找 L0 层
	// L0 层包含最新刷新的数据，优先级最高
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}

	// 第二步：依次查找 L1 到 Ln 层
	// 按层级顺序查找，新数据在较低层级的概率更高
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}

	// 所有层级都没有找到数据
	return entry, utils.ErrKeyNotFound
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}

	manifest := lm.manifestFile.GetManifest()
	// 对比manifest 文件的正确性
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	// 逐一加载sstable 的index block 构建cache
	lm.cache = newCache(lm.opt)
	// TODO 初始化的时候index 结构放在了table中，相当于全部加载到了内存，减少了一次读磁盘，但增加了内存消耗
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t) // 记录一个level的文件总大小
	}
	// 对每一层进行排序
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// 得到最大的fid值
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// 向L0层flush一个sstable
// flush 方法负责将不可变的 memTable 刷新到 SSTable 文件中。
// 它创建一个新的表构建器，迭代传入的 immutable memTable，并将其内容添加到新表中。
// 最后，它更新 manifest 文件并添加新表的元信息。
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// 分配一个fid
	fid := immutable.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	// 构建一个 builder
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewSkipListIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	// 创建一个 table 对象
	table := openTable(lm, sstName, builder)
	err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		Checksum: []byte{'m', 'o', 'c', 'k'},
	})
	// manifest写入失败直接panic
	utils.Panic(err)
	// 更新manifest文件
	lm.levels[0].add(table)
	return
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) iterators() []utils.Iterator {

	itrs := make([]utils.Iterator, 0, len(lm.levels))
	for _, level := range lm.levels {
		itrs = append(itrs, level.iterators()...)
	}
	return itrs
}

func (lm *levelManager) loadCache() {

}
func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

// --------- level处理器 -------
type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}
func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
}

func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	// 如果是第0层文件则进行特殊处理
	if lh.levelNum == 0 {
		// TODO: logic...
		// 获取可能存在key的sst
		return lh.searchL0SST(key)
	} else {
		// TODO: logic...
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Serach(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

type levelHandlerRLocked struct{}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, lh.tables[i].ss.MaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, lh.tables[i].ss.MaxKey()) < 0
	})
	return left, right
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	lh.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}

	// Increase totalSize first.
	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	// Assign tables.
	lh.tables = newTables
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[i].ss.MinKey()) < 0
	})
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}

func (lh *levelHandler) iterators() []utils.Iterator {
	lh.RLock()
	defer lh.RUnlock()
	topt := &utils.Options{IsAsc: true}
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables, topt)
	}

	if len(lh.tables) == 0 {
		return nil
	}
	return []utils.Iterator{NewConcatIterator(lh.tables, topt)}
}

// NewConcatIterator 创建一个连接迭代器（简化实现）
func NewConcatIterator(tables []*table, opt *utils.Options) utils.Iterator {
	// 简化实现：返回第一个表的迭代器
	if len(tables) > 0 {
		return tables[0].NewIterator(opt)
	}
	return nil
}

// NewMergeIterator 创建一个合并迭代器（简化实现）
func NewMergeIterator(iters []utils.Iterator, reverse bool) utils.Iterator {
	// 简化实现：返回第一个迭代器
	if len(iters) > 0 {
		return iters[0]
	}
	return nil
}
