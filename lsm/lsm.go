package lsm

import (
	"github.com/rookieLiuyutao/corekv/utils"
	"sync"
)

// LSM _
// LSM 是一个结构体，代表一个日志结构合并树（Log-Structured Merge Tree）。
// 它主要用于数据库的底层存储引擎，以优化磁盘的读写效率。
// 该结构体包含了多个组件，每个组件在数据管理和检索过程中扮演着不同角色。
type LSM struct {
	// lock 用于并发控制，确保对LSM树的读写操作是线程安全的。
	lock sync.RWMutex
	// memTable 是一个内存中的数据结构，用于存储新写入的数据，直到其大小达到一定阈值。
	memTable *memTable
	// immutables 是一个指向已变为不可变的memTable的指针切片，这些表已经满了，需要被刷新到磁盘。
	immutables []*memTable
	// levels 是一个层级管理器，控制着数据在多个层级的SSD表中的分布和合并。
	levels *levelManager
	// option 包含了LSM树操作的配置选项，如压缩、缓存等。
	option *Options
	// closer 用于通知LSM树停止操作，比如当一个数据库被关闭时。
	closer *utils.Closer
	// maxMemFID 是一个用于管理内存中数据块的标识的变量。
	maxMemFID uint32
}

// Options _
// Options 是一个配置结构体，包含了数据库运行所需的所有可调整选项。
type Options struct {
	// WorkDir 指定了数据库文件存储的工作目录路径。
	WorkDir string
	// MemTableSize 设置了内存表（MemTable）的最大容量，单位为字节。
	MemTableSize int64
	// SSTableMaxSz 表示SSTable（磁盘存储数据表）的最大容量，单位为字节。
	SSTableMaxSz int64
	// BlockSize 定义了SSTable中每个数据块的大小，单位为字节。
	BlockSize int
	// BloomFalsePositive 设置布隆过滤器的误判概率。
	BloomFalsePositive float64

	// 以下为数据库压缩相关配置：
	NumCompactors       int   // 压缩工作器的数量。
	BaseLevelSize       int64 // 基础层级的大小限制。
	LevelSizeMultiplier int   // 各层级之间的预期大小比例因子。
	TableSizeMultiplier int   // 表大小增长的乘数因子。
	BaseTableSize       int64 // 初始层级的基本表大小。
	NumLevelZeroTables  int   // 层级0允许的最大表数量。
	MaxLevelNum         int   // 数据库最大层级数。

	// DiscardStatsCh 提供了一个通道，用于丢弃或统计特定操作的统计信息。
	DiscardStatsCh *chan map[uint32]int64
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
	return lsm
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

func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
}
