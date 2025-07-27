// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
LSM 树表（Table）管理模块

Table 是 LSM 树中 SSTable 文件的内存表示，提供了对磁盘文件的抽象访问接口。
每个 Table 对应一个 SSTable 文件，包含有序的键值对数据。

核心功能：
1. 文件抽象：将 SSTable 文件抽象为内存对象
2. 数据访问：提供高效的键值查找和范围查询
3. 引用计数：管理文件的生命周期和垃圾回收
4. 缓存集成：与块缓存系统集成，优化读取性能

设计特点：
- 不可变性：SSTable 文件一旦创建就不再修改
- 有序存储：数据按键的字典序排列
- 块结构：文件内部按块组织，支持随机访问
- 索引优化：内置索引结构，加速键查找

文件结构：
[数据块1][数据块2]...[数据块N][索引块][布隆过滤器][元数据]

性能优化：
- 布隆过滤器：快速判断键是否存在
- 块索引：支持二分查找定位数据块
- 内存映射：使用 mmap 优化文件访问
- 引用计数：延迟删除，避免正在使用的文件被误删

生命周期：
1. 创建：从 MemTable 刷新或压缩操作产生
2. 打开：加载索引和元数据到内存
3. 使用：提供查询和迭代服务
4. 关闭：释放资源，可能删除文件

并发安全：
- 读操作：多线程并发安全
- 引用计数：使用原子操作保证正确性
- 文件删除：通过引用计数延迟删除
*/

package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
)

// table 表示 LSM 树中的一个 SSTable 文件。
// 它是 SSTable 文件在内存中的抽象表示，提供了访问文件内容的接口。
//
// 设计原理：
// 1. 文件抽象：封装底层的 SSTable 文件操作
// 2. 引用计数：管理文件的生命周期，支持安全的并发访问
// 3. 延迟删除：通过引用计数实现文件的延迟删除
// 4. 性能优化：集成缓存和索引，提供高效的数据访问
//
// 生命周期管理：
// - IncrRef()：增加引用计数，表示有新的使用者
// - DecrRef()：减少引用计数，可能触发文件删除
// - 引用计数为0时，文件可以被安全删除
//
// 并发安全：
// - 引用计数使用原子操作，保证并发安全
// - 文件内容是不可变的，支持并发读取
// - 删除操作通过引用计数延迟执行
type table struct {
	// 底层文件接口

	// ss 是底层的 SSTable 文件对象。
	// 提供对磁盘文件的直接访问接口。
	// 包含文件的索引、布隆过滤器等元数据。
	ss *file.SSTable

	// 管理器引用

	// lm 是所属的层级管理器。
	// 用于访问配置选项和其他管理功能。
	// 提供与 LSM 树其他组件的交互接口。
	lm *levelManager

	// 文件标识

	// fid 是文件的唯一标识符。
	// 用于文件命名、日志记录和调试。
	// 在整个数据库生命周期中保持唯一。
	fid uint64

	// 引用计数（原子操作）

	// ref 是文件的引用计数器。
	// 使用原子操作确保并发安全。
	// 当计数为0时，文件可以被垃圾回收。
	// 正数表示有活跃的使用者，负数表示文件已标记删除。
	ref int32
}

// openTable 函数用于打开指定名称的表，
// 如果提供了一个 builder，则将其内容刷新到磁盘；
// 否则，尝试打开一个已经存在的 sst 文件并初始化其内容。
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	// 获取sst文件的最大大小
	sstSize := int(lm.opt.SSTableMaxSz)
	// 如果builder存在，则获取builder的大小
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	// 获取表的fid
	fid := utils.FID(tableName)
	// 对builder存在的情况 把buf flush到磁盘
	if builder != nil {
		// 把builder中的数据flush到磁盘
		if t, err = builder.flush(lm, tableName); err != nil {
			// 打印错误信息
			utils.Err(err)
			return nil
		}
	} else {
		// 如果没有builder，则创建一个新的table，并尝试打开一个已经存在的sst文件
		t = &table{lm: lm, fid: fid}

		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// 先要引用一下，否则后面使用迭代器会导致引用状态错误
	t.IncrRef()
	//  初始化sst文件，把index加载进来
	if err := t.ss.Init(); err != nil {
		// 打印错误信息
		err := utils.Err(err)
		if err != nil {
			return nil
		}
		return nil
	}

	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&utils.Options{}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	// 如果迭代器无效，则打印错误信息
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	// 获取最大的key
	maxKey := itr.Item().Entry().Key
	// 设置sst文件的最大key
	t.ss.SetMaxKey(maxKey)

	return t
}

// Serach 从table中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	// 获取索引
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) indexKey() uint64 {
	return t.fid
}
func (t *table) getEntry(key, block []byte, idx int) (entry *utils.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 去加载sst对应的block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(idx < 0, fmt.Errorf("idx < 0"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
func (it *tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}

	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}
func (it *tableIterator) Valid() bool {
	return it.err != io.EOF // 如果没有的时候 则是EOF
}
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}
func (it *tableIterator) Item() utils.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek
// 二分法搜索 offsets
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key
// 否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中
// 如果都没有，则当前key不再此table
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return int64(t.ss.Size()) }

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}
func (t *table) Delete() error {
	return t.ss.Delete()
}

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}
func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
