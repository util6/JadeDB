/*
MemTable 内存表模块

MemTable 是 LSM 树中的内存存储层，负责接收所有新的写入操作。
它是数据进入 LSM 树的第一站，提供高性能的内存访问。

核心特性：
1. 基于跳表的有序存储，支持高效的插入和查找
2. 预写日志（WAL）保证数据持久性，防止崩溃丢失
3. 支持 MVCC，通过版本号实现多版本并发控制
4. 内存使用可控，达到阈值后转为不可变表

工作流程：
1. 写入操作首先记录到 WAL 文件
2. 然后插入到内存中的跳表结构
3. 读取操作直接从跳表中查找
4. 达到大小限制后转为不可变表

数据安全：
- WAL 确保写入操作的持久性
- 崩溃恢复时可以从 WAL 重建内存表
- 支持原子操作，保证数据一致性

性能优化：
- 跳表提供 O(log n) 的插入和查找性能
- 内存访问避免磁盘 I/O 开销
- 批量操作减少系统调用次数
*/

package lsm

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/utils"
)

const (
	// walFileExt 定义 WAL 文件的扩展名。
	// WAL（Write-Ahead Log）文件用于记录所有写入操作，
	// 确保在系统崩溃时可以恢复数据。
	walFileExt string = ".wal"
)

// MemTable 是 memTable 的公开类型别名。
// 提供给外部包使用，隐藏内部实现细节。
type MemTable = memTable

// memTable 表示 LSM 树中的内存表。
// 这是数据写入的第一站，提供高性能的内存存储和访问。
//
// 设计原理：
// 1. 所有写入首先记录到 WAL，确保持久性
// 2. 数据存储在跳表中，保持有序性
// 3. 支持高效的插入、查找和范围查询
// 4. 达到大小限制后转为不可变表
//
// 并发安全：
// memTable 本身不提供并发保护，需要上层（LSM）提供锁机制。
// 跳表的底层实现是并发安全的。
//
// 内存管理：
// 通过跳表的 Arena 机制管理内存，避免频繁的内存分配。
type memTable struct {
	// lsm 指向所属的 LSM 树实例。
	// 用于访问配置选项和其他共享资源。
	// 提供与 LSM 树其他组件的交互接口。
	lsm *LSM

	// wal 是预写日志文件的句柄。
	// 所有写入操作都会先记录到这个文件中。
	// 确保在系统崩溃时可以恢复未持久化的数据。
	// WAL 文件按顺序写入，提供最佳的磁盘性能。
	wal *file.WalFile

	// sl 是底层的跳表数据结构。
	// 提供有序的键值存储，支持高效的操作。
	// 跳表的平均时间复杂度为 O(log n)。
	// 使用 Arena 内存管理，减少内存分配开销。
	sl *utils.SkipList

	// buf 是用于序列化操作的缓冲区。
	// 重用缓冲区可以减少内存分配和垃圾回收。
	// 主要用于将条目序列化为字节流。
	buf *bytes.Buffer

	// maxVersion 跟踪内存表中的最大版本号。
	// 用于 MVCC（多版本并发控制）。
	// 确保版本号的单调递增。
	maxVersion uint64
}

// NewMemtable 创建一个新的内存表实例。
// 这个方法在 LSM 树需要新的内存表时被调用，通常发生在：
// 1. 数据库初始化时
// 2. 当前内存表达到大小限制时
// 3. 系统恢复时
//
// 返回值：
// 返回初始化完成的内存表实例
//
// 初始化过程：
// 1. 生成唯一的文件标识符
// 2. 创建 WAL 文件用于持久化
// 3. 初始化跳表用于内存存储
// 4. 设置必要的引用关系
//
// 资源分配：
// - WAL 文件：用于崩溃恢复
// - 跳表：1MB 的初始 Arena 大小
// - 文件句柄：需要在关闭时释放
func (lsm *LSM) NewMemtable() *memTable {
	// 原子操作生成新的文件标识符
	// 确保在并发环境下文件 ID 的唯一性
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)

	// 配置 WAL 文件的选项
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,                     // 工作目录
		Flag:     os.O_CREATE | os.O_RDWR,                // 创建并读写模式
		MaxSz:    int(lsm.option.MemTableSize),           // WAL 文件最大大小
		FID:      newFid,                                 // 文件标识符
		FileName: mtFilePath(lsm.option.WorkDir, newFid), // 完整文件路径
	}

	// 创建并返回内存表实例
	return &memTable{
		wal: file.OpenWalFile(fileOpt),         // 打开 WAL 文件
		sl:  utils.NewSkipList(int64(1 << 20)), // 创建 1MB 的跳表
		lsm: lsm,                               // 设置 LSM 引用
	}
}

// close 关闭内存表并释放相关资源。
// 这个方法在内存表不再需要时被调用，确保资源的正确释放。
//
// 返回值：
// 如果关闭过程中发生错误，返回错误信息
//
// 清理过程：
// 1. 关闭 WAL 文件句柄
// 2. 释放文件系统资源
// 3. 跳表会通过垃圾回收自动清理
//
// 注意事项：
// - 关闭后的内存表不应再被使用
// - WAL 文件可能会被保留用于恢复
// - 跳表的内存会逐渐被垃圾回收
func (m *memTable) close() error {
	// 关闭 WAL 文件
	if err := m.wal.Close(); err != nil {
		return err
	}

	// 成功关闭所有资源
	return nil
}

// set 向内存表中插入一个键值对条目。
// 这是内存表的核心写入方法，确保数据的持久性和一致性。
//
// 参数说明：
// entry: 要插入的键值对条目
//
// 返回值：
// 如果插入过程中发生错误，返回错误信息
//
// 写入流程：
// 1. 首先写入 WAL 文件，确保持久性
// 2. 然后插入到内存跳表中，提供快速访问
//
// 错误处理：
// 如果 WAL 写入失败，整个操作失败，保证一致性。
// 如果跳表插入失败（内存不足等），也会返回错误。
//
// 性能考虑：
// - WAL 写入是顺序的，性能较好
// - 跳表插入的时间复杂度为 O(log n)
// - 批量操作可以提高整体性能
func (m *memTable) set(entry *utils.Entry) error {
	// 第一步：写入 WAL 文件，确保持久性
	// 如果系统崩溃，可以从 WAL 恢复这个写入操作
	if err := m.wal.Write(entry); err != nil {
		return err
	}

	// 第二步：插入到内存跳表中
	// 提供快速的内存访问，支持后续的读取操作
	m.sl.Add(entry)

	return nil
}

// Get 从内存表中检索指定键的条目。
// 这是内存表的核心读取方法，提供高性能的内存访问。
//
// 参数说明：
// key: 要查找的键
//
// 返回值：
// 找到的条目和可能的错误信息
//
// 查找过程：
// 1. 在跳表中搜索指定的键
// 2. 将找到的值结构转换为条目格式
// 3. 返回完整的条目信息
//
// 性能特点：
// - 跳表搜索的时间复杂度为 O(log n)
// - 纯内存操作，无磁盘 I/O 开销
// - 支持并发读取（跳表是并发安全的）
//
// 注意事项：
// - 如果键不存在，返回的条目值为空
// - 调用者需要检查返回的条目是否有效
// - 过期的条目仍然会被返回，需要上层检查
func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 从跳表中搜索指定的键
	// 跳表的搜索操作是 O(log n) 时间复杂度
	vs := m.sl.Search(key)

	// 将值结构转换为完整的条目格式
	e := &utils.Entry{
		Key:       key,          // 查询的键
		Value:     vs.Value,     // 找到的值
		ExpiresAt: vs.ExpiresAt, // 过期时间
		Meta:      vs.Meta,      // 元数据
		Version:   vs.Version,   // 版本号
	}

	return e, nil
}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}

// recovery 用于从工作目录中恢复LSM树的状态。
// 它会读取所有 wal 文件，并将这些文件中的数据加载到内存表中。

func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// 从工作目录中获取所有文件
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}

	var fids []uint64
	maxFid := lsm.levels.maxFID

	// 识别所有后缀为 .wal 的文件，并解析文件名中的 fid
	for _, fileInfo := range files {
		//检查文件名是否以特定扩展名 walFileExt 结尾
		if !strings.HasSuffix(fileInfo.Name(), walFileExt) {
			continue
		}
		filenameLen := len(fileInfo.Name())

		//文件名中提取一个数字ID，并将其解析为64位无符号整数
		//
		fid, err := strconv.ParseUint(fileInfo.Name()[:filenameLen-len(walFileExt)], 10, 64)
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}

	// 对文件 ID 进行排序
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	imms := []*memTable{}

	// 遍历文件 ID，打开对应的内存表并检查其大小
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			// 如果内存表为空，则跳过
			continue
		}
		// 将非空的内存表添加到结果列表中
		imms = append(imms, mt)
	}

	// 更新最终的最大文件 ID
	lsm.levels.maxFID = maxFid

	// 返回新创建的内存表和已加载的内存表切片
	return lsm.NewMemtable(), imms
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	s := utils.NewSkipList(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}
func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	// if endOff < m.wal.Size() {
	// 	return errors.WithMessage(utils.ErrTruncate, fmt.Sprintf("end offset: %d < size: %d", endOff, m.wal.Size()))
	// }
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}

// IncrRef increases the refcount
func (mt *memTable) IncrRef() {
	mt.sl.IncrRef()
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (mt *memTable) DecrRef() {
	mt.sl.DecrRef()
}
