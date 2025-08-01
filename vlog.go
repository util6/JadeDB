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
JadeDB 值日志（Value Log）模块

值日志是 JadeDB 实现值分离存储的核心组件，专门用于存储大值数据。
这种设计可以显著减少 LSM 树的写放大问题，提高整体性能。

核心设计原理：
1. 值分离：大于阈值的值存储在值日志中，LSM 树只存储指针
2. 顺序写入：所有值按顺序追加写入，充分利用磁盘顺序写性能
3. 垃圾回收：定期清理无效数据，回收磁盘空间
4. 并发安全：支持多线程并发读写，使用细粒度锁控制

主要组件：
- 日志文件：存储实际值数据的文件，按 FID 编号
- 文件管理：管理多个日志文件的生命周期
- 垃圾回收：清理过期和删除的数据
- 统计信息：跟踪空间使用和回收效果

性能优势：
- 减少写放大：大值不参与 LSM 树的压缩过程
- 提高缓存效率：LSM 树更小，缓存命中率更高
- 优化垃圾回收：批量回收，减少碎片化
- 支持大值：可以存储任意大小的值

适用场景：
- 存储大型对象（图片、文档、序列化数据）
- 写入密集型应用
- 需要高效垃圾回收的场景
- 对写放大敏感的系统
*/

package JadeDB

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/utils"
)

const (
	// discardStatsFlushThreshold 定义丢弃统计信息的刷新阈值。
	// 当累积的丢弃统计达到这个数量时，会触发批量刷新到磁盘。
	// 这种批量处理可以减少磁盘 I/O 次数，提高性能。
	discardStatsFlushThreshold = 100
)

var (
	// lfDiscardStatsKey 是存储丢弃统计信息的特殊键。
	// 使用特殊前缀确保不与用户数据冲突。
	// 这个键用于在 LSM 树中持久化垃圾回收的统计信息。
	lfDiscardStatsKey = []byte("!corekv!discard")
)

// valueLog 表示值日志的核心实现。
// 它负责管理大值的存储、读取、垃圾回收和文件生命周期。
//
// 设计原理：
// 1. 文件分片：使用多个日志文件，每个文件有唯一的 FID
// 2. 顺序写入：新数据总是追加到当前活跃文件的末尾
// 3. 随机读取：通过值指针快速定位和读取数据
// 4. 垃圾回收：定期清理无效数据，回收磁盘空间
//
// 并发模型：
// - 读操作：多个读操作可以并发执行
// - 写操作：写操作是串行的，但可以批量处理
// - 垃圾回收：在后台异步执行，不阻塞读写操作
//
// 文件格式：
// 每个条目在文件中的格式为：
// [Header][Key][Value][Checksum]
type valueLog struct {
	// 基础配置

	// dirPath 指定值日志文件的存储目录。
	// 所有的 .vlog 文件都存储在这个目录中。
	dirPath string

	// 文件管理（需要锁保护）

	// filesLock 保护文件相关的数据结构。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作（如创建、删除文件）需要写锁。
	filesLock sync.RWMutex

	// filesMap 维护所有活跃日志文件的映射。
	// 键是文件 ID（FID），值是对应的日志文件对象。
	// 用于快速查找和访问特定的日志文件。
	filesMap map[uint32]*file.LogFile

	// maxFid 跟踪当前最大的文件 ID。
	// 用于生成新文件的唯一标识符。
	// 每次创建新文件时递增。
	maxFid uint32

	// filesToBeDeleted 存储待删除的文件 ID 列表。
	// 这些文件已经被标记为删除，但可能还有活跃的迭代器在使用。
	// 当所有迭代器关闭后，这些文件会被物理删除。
	filesToBeDeleted []uint32

	// numActiveIterators 跟踪活跃迭代器的数量。
	// 使用原子操作确保并发安全。
	// 当这个计数为零时，可以安全删除标记的文件。
	numActiveIterators int32

	// 数据库集成

	// db 指向所属的数据库实例。
	// 用于访问数据库的配置和其他组件。
	db *DB

	// 写入状态（需要原子操作）

	// writableLogOffset 记录当前可写文件的写入偏移量。
	// 使用原子操作确保并发安全。
	// 读操作通过原子加载获取，写操作通过原子存储更新。
	writableLogOffset uint32

	// numEntriesWritten 记录已写入的条目数量。
	// 用于统计和监控目的。
	// 可以帮助决定何时轮转到新文件。
	numEntriesWritten uint32

	// 配置选项

	// opt 包含值日志的配置选项。
	// 包括文件大小限制、垃圾回收参数等。
	opt Options

	// 垃圾回收

	// garbageCh 用于触发垃圾回收操作的通道。
	// 后台 goroutine 监听这个通道，收到信号时执行垃圾回收。
	garbageCh chan struct{}

	// lfDiscardStats 管理丢弃统计信息。
	// 跟踪每个文件中无效数据的数量和大小。
	// 用于垃圾回收决策和空间回收优化。
	lfDiscardStats *lfDiscardStats
}

func (vlog *valueLog) newValuePtr(e *utils.Entry) (*utils.ValuePtr, error) {
	// TODO 尝试使用对象复用，后面entry对象也应该使用
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = []*utils.Entry{e}
	req.Wg.Add(1)
	req.IncrRef() // for db write
	defer req.DecrRef()
	err := vlog.write([]*request{req})
	return req.Ptrs[0], err
}
func (vlog *valueLog) open(db *DB, ptr *utils.ValuePtr, replayFn utils.LogEntry) error {
	vlog.lfDiscardStats.closer.Add(1)
	go vlog.flushDiscardStats()
	if err := vlog.populateFilesMap(); err != nil {
		return err
	}
	// If no files are found, then create a new file.
	if len(vlog.filesMap) == 0 {
		_, err := vlog.createVlogFile(0)
		return utils.WarpErr("Error while creating log file in valueLog.open", err)
	}
	fids := vlog.sortedFids()
	for _, fid := range fids {
		lf, ok := vlog.filesMap[fid]
		utils.CondPanic(!ok, fmt.Errorf("vlog.filesMap[fid] fid not found"))
		var err error
		if err = lf.Open(
			&file.Options{
				FID:      uint64(fid),
				FileName: vlog.fpath(fid),
				Dir:      vlog.dirPath,
				Path:     vlog.dirPath,
				MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
			}); err != nil {
			return errors.Wrapf(err, "Open existing file: %q", lf.FileName())
		}
		var offset uint32
		// 从head处开始重放vlog日志，而不是从第一条日志
		// head 相当于一个快照
		if fid == ptr.Fid {
			offset = ptr.Offset + ptr.Len
		}
		fmt.Printf("Replaying file id: %d at offset: %d\n", fid, offset)
		now := time.Now()
		// 重放日志
		if err := vlog.replayLog(lf, offset, replayFn); err != nil {
			// Log file is corrupted. Delete it.
			if err == utils.ErrDeleteVlogFile {
				delete(vlog.filesMap, fid)
				// Close the fd of the file before deleting the file otherwise windows complaints.
				if err := lf.Close(); err != nil {
					return errors.Wrapf(err, "failed to close vlog file %s", lf.FileName())
				}
				path := vlog.fpath(uint32(lf.FID))
				if err := os.Remove(path); err != nil {
					return errors.Wrapf(err, "failed to delete empty value log file: %q", path)
				}
				continue
			}
			return err
		}
		fmt.Printf("Replay took: %s\n", time.Since(now))

		if fid < vlog.maxFid {
			// This file has been replayed. It can now be mmapped.
			// For maxFid, the mmap would be done by the specially written code below.
			if err := lf.Init(); err != nil {
				return err
			}
		}
	}
	// Seek to the end to start writing.
	last, ok := vlog.filesMap[vlog.maxFid]
	utils.CondPanic(!ok, errors.New("vlog.filesMap[vlog.maxFid] not found"))
	lastOffset, err := last.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("file.Seek to end path:[%s]", last.FileName()))
	}
	vlog.writableLogOffset = uint32(lastOffset)

	// head的设计起到check point的作用
	vlog.db.vhead = &utils.ValuePtr{Fid: vlog.maxFid, Offset: uint32(lastOffset)}
	if err := vlog.populateDiscardStats(); err != nil {
		fmt.Errorf("Failed to populate discard stats: %s\n", err)
	}
	return nil
}

// Read reads the value log at a given location.
// TODO: Make this read private.
func (vlog *valueLog) read(vp *utils.ValuePtr) ([]byte, func(), error) {
	buf, lf, err := vlog.readValueBytes(vp)
	// log file is locked so, decide whether to lock immediately or let the caller to
	// unlock it, after caller uses it.
	cb := vlog.getUnlockCallback(lf)
	if err != nil {
		return nil, cb, err
	}

	if vlog.opt.VerifyValueChecksum {
		hash := crc32.New(utils.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			utils.RunCallback(cb)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		// Fetch checksum from the end of the buffer.
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != utils.BytesToU32(checksum) {
			utils.RunCallback(cb)
			return nil, nil, errors.Wrapf(utils.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}
	var h utils.Header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	if uint32(len(kv)) < h.KLen+h.VLen {
		fmt.Errorf("Invalid read: vp: %+v\n", vp)
		return nil, nil, errors.Errorf("Invalid read: Len: %d read at:[%d:%d]",
			len(kv), h.KLen, h.KLen+h.VLen)
	}
	return kv[h.KLen : h.KLen+h.VLen], cb, nil
}

// write 并不是并发安全的
func (vlog *valueLog) write(reqs []*request) error {
	//  需要检查是否能够正确写入
	if err := vlog.validateWrites(reqs); err != nil {
		return err
	}

	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	curlf := vlog.filesMap[maxFid]
	vlog.filesLock.RUnlock()

	var buf bytes.Buffer
	flushWrites := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes()
		offset := vlog.woffset()
		if err := curlf.Write(offset, data); err != nil {
			return errors.Wrapf(err, "Unable to write to value log file: %q", curlf.FileName())
		}
		buf.Reset()
		atomic.AddUint32(&vlog.writableLogOffset, uint32(len(data)))
		curlf.AddSize(vlog.writableLogOffset)
		return nil
	}
	toDisk := func() error {
		if err := flushWrites(); err != nil {
			return err
		}
		// 切分vlog文件
		if vlog.woffset() > uint32(vlog.opt.ValueLogFileSize) ||
			vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries {
			if err := curlf.DoneWriting(vlog.woffset()); err != nil {
				return err
			}

			newid := atomic.AddUint32(&vlog.maxFid, 1)
			utils.CondPanic(newid <= 0, fmt.Errorf("newid has overflown uint32: %v", newid))
			newlf, err := vlog.createVlogFile(newid)
			if err != nil {
				return err
			}
			curlf = newlf
			atomic.AddInt32(&vlog.db.logRotates, 1)
		}
		return nil
	}
	for i := range reqs {
		b := reqs[i]
		b.Ptrs = b.Ptrs[:0]
		var written int
		for j := range b.Entries {
			e := b.Entries[j]
			if vlog.db.shouldWriteValueToLSM(e) {
				b.Ptrs = append(b.Ptrs, &utils.ValuePtr{})
				continue
			}
			var p utils.ValuePtr

			p.Fid = uint32(curlf.FID)
			// Use the offset including buffer length so far.
			p.Offset = vlog.woffset() + uint32(buf.Len())
			plen, err := curlf.EncodeEntry(e, &buf, p.Offset) // Now encode the entry into buffer.
			if err != nil {
				return err
			}
			p.Len = uint32(plen)
			b.Ptrs = append(b.Ptrs, &p)
			written++

			if buf.Len() > vlog.db.opt.ValueLogFileSize {
				if err := flushWrites(); err != nil {
					return err
				}
			}
		}
		vlog.numEntriesWritten += uint32(written)
		// We write to disk here so that all entries that are part of the same transaction are
		// written to the same vlog file.
		writeNow :=
			vlog.woffset()+uint32(buf.Len()) > uint32(vlog.opt.ValueLogFileSize) ||
				vlog.numEntriesWritten > uint32(vlog.opt.ValueLogMaxEntries)
		if writeNow {
			if err := toDisk(); err != nil {
				return err
			}
		}
	}
	return toDisk()
}

func (vlog *valueLog) close() error {
	if vlog == nil || vlog.db == nil {
		return nil
	}
	// close flushDiscardStats.
	<-vlog.lfDiscardStats.closer.CloseSignal
	var err error
	for id, f := range vlog.filesMap {
		f.Lock() // We won’t release the lock.
		maxFid := vlog.maxFid
		if id == maxFid {
			// truncate writable log file to correct offset.
			if truncErr := f.Truncate(int64(vlog.woffset())); truncErr != nil && err == nil {
				err = truncErr
			}
		}
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		f.Unlock()
	}
	return err
}

func (vlog *valueLog) runGC(discardRatio float64, head *utils.ValuePtr) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		// Pick a log file for GC.
		defer func() {
			// 通过一个channel来控制一次仅运行一个GC任务
			<-vlog.garbageCh
		}()

		var err error
		files := vlog.pickLog(head)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}
		tried := make(map[uint32]bool)
		for _, lf := range files {
			//消重一下,防止随机策略和统计策略返回同一个fid
			if _, done := tried[uint32(lf.FID)]; done {
				continue
			}
			tried[uint32(lf.FID)] = true
			if err = vlog.doRunGC(lf, discardRatio); err == nil {
				return nil
			}
		}
		return err
	default:
		return utils.ErrRejected
	}
}

func (vlog *valueLog) doRunGC(lf *file.LogFile, discardRatio float64) (err error) {
	// 退出的时候把统计的discard清空
	defer func() {
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, uint32(lf.FID))
			vlog.lfDiscardStats.Unlock()
		}
	}()
	s := &sampler{
		lf:            lf,
		countRatio:    0.01, // 1% of num entries.
		sizeRatio:     0.1,  // 10% of the file as window.
		fromBeginning: false,
	}

	if _, err = vlog.sample(s, discardRatio); err != nil {
		return err
	}

	if err = vlog.rewrite(lf); err != nil {
		return err
	}
	return nil
}

// 重写
func (vlog *valueLog) rewrite(f *file.LogFile) error {
	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	vlog.filesLock.RUnlock()
	utils.CondPanic(uint32(f.FID) >= maxFid, fmt.Errorf("fid to move: %d. Current max fid: %d", f.FID, maxFid))

	wb := make([]*utils.Entry, 0, 1000)
	var size int64

	var count, moved int
	fe := func(e *utils.Entry) error {
		count++
		if count%100000 == 0 {
			fmt.Printf("Processing entry %d\n", count)
		}

		vs, err := vlog.db.lsm.Get(e.Key)
		if err != nil {
			return err
		}
		if utils.DiscardEntry(e, vs) {
			return nil
		}

		if len(vs.Value) == 0 {
			return errors.Errorf("Empty value: %+v", vs)
		}
		var vp utils.ValuePtr
		vp.Decode(vs.Value)

		if vp.Fid > uint32(f.FID) {
			return nil
		}
		if vp.Offset > e.Offset {
			return nil
		}
		// 如果从lsm和vlog的同一个位置读取带entry则重新写回，也有可能读取到旧的
		if vp.Fid == uint32(f.FID) && vp.Offset == e.Offset {
			moved++
			// This new entry only contains the key, and a pointer to the value.
			ne := new(utils.Entry)
			ne.Meta = 0 // Remove all bits. Different keyspace doesn't need these bits.
			ne.ExpiresAt = e.ExpiresAt
			ne.Key = append([]byte{}, e.Key...)
			ne.Value = append([]byte{}, e.Value...)
			es := int64(ne.EstimateSize(vlog.db.opt.ValueLogFileSize))
			// Consider size of value as well while considering the total size
			// of the batch. There have been reports of high memory usage in
			// rewrite because we don't consider the value size. See #1292.
			es += int64(len(e.Value))

			// Ensure length and size of wb is within transaction limits.
			if int64(len(wb)+1) >= vlog.opt.MaxBatchCount ||
				size+es >= vlog.opt.MaxBatchSize {
				if err := vlog.db.batchSet(wb); err != nil {
					return err
				}
				size = 0
				wb = wb[:0]
			}
			wb = append(wb, ne)
			size += es
		}
		return nil
	}

	_, err := vlog.iterate(f, 0, func(e *utils.Entry, vp *utils.ValuePtr) error {
		return fe(e)
	})
	if err != nil {
		return err
	}

	batchSize := 1024
	var loops int
	for i := 0; i < len(wb); {
		loops++
		if batchSize == 0 {
			return utils.ErrNoRewrite
		}
		end := i + batchSize
		if end > len(wb) {
			end = len(wb)
		}
		if err := vlog.db.batchSet(wb[i:end]); err != nil {
			if err == utils.ErrTxnTooBig {
				// Decrease the batch size to half.
				batchSize = batchSize / 2
				continue
			}
			return err
		}
		i += batchSize
	}
	var deleteFileNow bool
	// Entries written to LSM. Remove the older file now.
	{
		vlog.filesLock.Lock()
		// Just a sanity-check.
		if _, ok := vlog.filesMap[uint32(f.FID)]; !ok {
			vlog.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", f.FID)
		}
		if vlog.iteratorCount() == 0 {
			delete(vlog.filesMap, uint32(f.FID))
			//deleteFileNow = true
		} else {
			vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, uint32(f.FID))
		}
		vlog.filesLock.Unlock()
	}

	if deleteFileNow {
		if err := vlog.deleteLogFile(f); err != nil {
			return err
		}
	}

	return nil
}

func (vlog *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&vlog.numActiveIterators))
}

// TODO 在迭代器close时，需要调用此函数，关闭已经被判定需要移除的logfile
func (vlog *valueLog) decrIteratorCount() error {
	num := atomic.AddInt32(&vlog.numActiveIterators, -1)
	if num != 0 {
		return nil
	}

	vlog.filesLock.Lock()
	lfs := make([]*file.LogFile, 0, len(vlog.filesToBeDeleted))
	for _, id := range vlog.filesToBeDeleted {
		lfs = append(lfs, vlog.filesMap[id])
		delete(vlog.filesMap, id)
	}
	vlog.filesToBeDeleted = nil
	vlog.filesLock.Unlock()

	for _, lf := range lfs {
		if err := vlog.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) deleteLogFile(lf *file.LogFile) error {
	if lf == nil {
		return nil
	}
	lf.Lock()
	defer lf.Unlock()
	utils.Err(lf.Close())
	return os.Remove(lf.FileName())
}

// validateWrites  可以检查当前的req是否能写入vlog日志，一个vlog日志最大4GB
func (vlog *valueLog) validateWrites(reqs []*request) error {
	vlogOffset := uint64(vlog.woffset())
	for _, req := range reqs {
		// calculate size of the request.
		size := estimateRequestSize(req)
		estimatedVlogOffset := vlogOffset + size
		if estimatedVlogOffset > uint64(utils.MaxVlogFileSize) {
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d",
				estimatedVlogOffset, utils.MaxVlogFileSize)
		}

		if estimatedVlogOffset >= uint64(vlog.opt.ValueLogFileSize) {
			// We'll create a new vlog file if the estimated offset is greater or equal to
			// max vlog size. So, resetting the vlogOffset.
			vlogOffset = 0
			continue
		}
		// Estimated vlog offset will become current vlog offset if the vlog is not rotated.
		vlogOffset = estimatedVlogOffset
	}
	return nil
}

// estimateRequestSize returns the size that needed to be written for the given request.
func estimateRequestSize(req *request) uint64 {
	size := uint64(0)
	for _, e := range req.Entries {
		size += uint64(utils.MaxHeaderSize + len(e.Key) + len(e.Value) + crc32.Size)
	}
	return size
}

// getUnlockCallback will returns a function which unlock the logfile if the logfile is mmaped.
// otherwise, it unlock the logfile and return nil.
func (vlog *valueLog) getUnlockCallback(lf *file.LogFile) func() {
	if lf == nil {
		return nil
	}
	return lf.RUnlock
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (vlog *valueLog) readValueBytes(vp *utils.ValuePtr) ([]byte, *file.LogFile, error) {
	lf, err := vlog.getFileRLocked(vp)
	if err != nil {
		return nil, nil, err
	}

	buf, err := lf.ReadValuePtr(vp)
	return buf, lf, err
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (vlog *valueLog) getFileRLocked(vp *utils.ValuePtr) (*file.LogFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	ret, ok := vlog.filesMap[vp.Fid]
	if !ok {
		// log file has gone away, we can't do anything. Return.
		return nil, errors.Errorf("file with ID: %d not found", vp.Fid)
	}

	// Check for valid offset if we are reading from writable log.
	maxFid := vlog.maxFid
	if vp.Fid == maxFid {
		currentOffset := vlog.woffset()
		if vp.Offset >= currentOffset {
			return nil, errors.Errorf(
				"Invalid value pointer offset: %d greater than current offset: %d",
				vp.Offset, currentOffset)
		}
	}

	ret.RLock()
	return ret, nil
}

func (vlog *valueLog) woffset() uint32 {
	return atomic.LoadUint32(&vlog.writableLogOffset)
}

func (vlog *valueLog) populateFilesMap() error {
	vlog.filesMap = make(map[uint32]*file.LogFile)

	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return utils.WarpErr(fmt.Sprintf("Unable to open log dir. path[%s]", vlog.dirPath), err)
	}

	found := make(map[uint64]struct{})
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".vlog") {
			continue
		}
		fsz := len(f.Name())
		fid, err := strconv.ParseUint(f.Name()[:fsz-5], 10, 32)
		if err != nil {
			return utils.WarpErr(fmt.Sprintf("Unable to parse log id. name:[%s]", f.Name()), err)
		}
		if _, ok := found[fid]; ok {
			return utils.WarpErr(fmt.Sprintf("Duplicate file found. Please delete one. name:[%s]", f.Name()), err)
		}
		found[fid] = struct{}{}

		lf := &file.LogFile{
			FID: fid,
		}
		vlog.filesMap[uint32(fid)] = lf
		if vlog.maxFid < uint32(fid) {
			vlog.maxFid = uint32(fid)
		}
	}
	return nil
}

func (vlog *valueLog) createVlogFile(fid uint32) (*file.LogFile, error) {
	path := vlog.fpath(fid)

	lf := &file.LogFile{
		FID: uint64(fid),
	}

	var err error
	utils.Panic2(nil, lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      vlog.dirPath,
		Path:     vlog.dirPath,
		MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
	}))

	removeFile := func() {
		// 如果处理出错 则直接删除文件
		utils.Err(os.Remove(lf.FileName()))
	}

	if err = lf.Bootstrap(); err != nil {
		removeFile()
		return nil, err
	}

	if err = utils.SyncDir(vlog.dirPath); err != nil {
		removeFile()
		return nil, utils.WarpErr(fmt.Sprintf("Sync value log dir[%s]", vlog.dirPath), err)
	}
	vlog.filesLock.Lock()
	vlog.filesMap[fid] = lf
	vlog.maxFid = fid
	// 现在header才是0
	atomic.StoreUint32(&vlog.writableLogOffset, utils.VlogHeaderSize)
	vlog.numEntriesWritten = 0
	vlog.filesLock.Unlock()
	return lf, nil
}

// sortedFids returns the file id's not pending deletion, sorted.  Assumes we have shared access to
// filesMap.
func (vlog *valueLog) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range vlog.filesToBeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	ret := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if _, ok := toBeDeleted[fid]; !ok {
			ret = append(ret, fid)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func (vlog *valueLog) replayLog(lf *file.LogFile, offset uint32, replayFn utils.LogEntry) error {
	// Alright, let's iterate now.
	endOffset, err := vlog.iterate(lf, offset, replayFn)
	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile:[%s]", lf.FileName())
	}
	if int64(endOffset) == int64(lf.Size()) {
		return nil
	}

	// TODO: 如果vlog日志损坏怎么办? 当前默认是截断损坏的数据

	// The entire file should be truncated (i.e. it should be deleted).
	// If fid == maxFid then it's okay to truncate the entire file since it will be
	// used for future additions. Also, it's okay if the last file has size zero.
	// We mmap 2*opt.ValueLogSize for the last file. See vlog.Open() function
	// if endOffset <= vlogHeaderSize && lf.fid != vlog.maxFid {

	if endOffset <= utils.VlogHeaderSize {
		if uint32(lf.FID) != vlog.maxFid {
			return utils.ErrDeleteVlogFile
		}
		return lf.Bootstrap()
	}

	fmt.Printf("Truncating vlog file %s to offset: %d\n", lf.FileName(), endOffset)
	if err := lf.Truncate(int64(endOffset)); err != nil {
		return utils.WarpErr(
			fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}

// iterate iterates over log file. It doesn't not allocate new memory for every kv pair.
// Therefore, the kv pair is only valid for the duration of fn call.
func (vlog *valueLog) iterate(lf *file.LogFile, offset uint32, fn utils.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = utils.VlogHeaderSize
	}
	if int64(offset) == int64(lf.Size()) {
		// We're at the end of the file already. No need to do anything.
		return offset, nil
	}

	// We're not at the end of the file. Let's Seek to the offset and start reading.
	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to seek, name:%s", lf.FileName())
	}

	reader := bufio.NewReader(lf.FD())
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}

	var validEndOffset uint32 = offset

loop:
	for {
		e, err := read.Entry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e == nil:
			continue
		}

		var vp utils.ValuePtr
		vp.Len = uint32(int(e.Hlen) + len(e.Key) + len(e.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = e.Offset
		vp.Fid = uint32(lf.FID)
		validEndOffset = read.recordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, utils.WarpErr(fmt.Sprintf("Iteration function %s", lf.FileName()), err)
		}
	}
	return validEndOffset, nil
}

// 这个对象用来重放日志
type safeRead struct {
	k            []byte
	v            []byte
	recordOffset uint32
	lf           *file.LogFile
}

// Entry reads an entry from the provided reader. It also validates the checksum for every entry
// read. Returns error on failure.
func (r *safeRead) Entry(reader io.Reader) (*utils.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.Header
	hlen, err := h.DecodeFrom(tee)
	if err != nil {
		return nil, err
	}
	if h.KLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils.ErrTruncate
	}
	kl := int(h.KLen)
	if cap(r.k) < kl {
		r.k = make([]byte, 2*kl)
	}
	vl := int(h.VLen)
	if cap(r.v) < vl {
		r.v = make([]byte, 2*vl)
	}

	e := &utils.Entry{}
	e.Offset = r.recordOffset
	e.Hlen = hlen
	buf := make([]byte, h.KLen+h.VLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}

	e.Key = buf[:h.KLen]
	e.Value = buf[h.KLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.Meta = h.Meta
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}

// 统计脏数据
func (vlog *valueLog) populateDiscardStats() error {
	key := utils.KeyWithTs(lfDiscardStatsKey, math.MaxUint64)
	var statsMap map[uint32]int64
	vs, err := vlog.db.Get(key)
	if err != nil {
		return err
	}
	// Value doesn't exist.
	if vs.Meta == 0 && len(vs.Value) == 0 {
		return nil
	}
	val := vs.Value
	// Entry is not stored in the LSM tree.
	if utils.IsValuePtr(vs) {
		var vp utils.ValuePtr
		vp.Decode(val)
		// Read entry from the value log.
		result, cb, err := vlog.read(&vp)
		// Copy it before we release the read lock.
		val = utils.SafeCopy(nil, result)
		utils.RunCallback(cb)
		if err != nil {
			return err
		}
	}
	if len(val) == 0 {
		return nil
	}
	if err := json.Unmarshal(val, &statsMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	fmt.Printf("Value Log Discard stats: %v\n", statsMap)
	vlog.lfDiscardStats.flushChan <- statsMap
	return nil
}

func (vlog *valueLog) fpath(fid uint32) string {
	return utils.VlogFilePath(vlog.dirPath, fid)
}

// initVLog
func (db *DB) initVLog() {
	vp, _ := db.getHead()
	vlog := &valueLog{
		dirPath:          db.opt.WorkDir,
		filesToBeDeleted: make([]uint32, 0),
		lfDiscardStats: &lfDiscardStats{
			m:         make(map[uint32]int64),
			closer:    utils.NewCloser(),
			flushChan: make(chan map[uint32]int64, 16),
		},
	}
	vlog.db = db
	vlog.opt = *db.opt
	vlog.garbageCh = make(chan struct{}, 1)
	if err := vlog.open(db, vp, db.replayFunction()); err != nil {
		utils.Panic(err)
	}
	db.vlog = vlog
}

// getHead prints all the head pointer in the DB and return the max value.
func (db *DB) getHead() (*utils.ValuePtr, uint64) {
	var vptr utils.ValuePtr
	return &vptr, 0
}
func (db *DB) replayFunction() func(*utils.Entry, *utils.ValuePtr) error {
	toLSM := func(k []byte, vs utils.ValueStruct) {
		db.lsm.Set(&utils.Entry{
			Key:       k,
			Value:     vs.Value,
			ExpiresAt: vs.ExpiresAt,
			Meta:      vs.Meta,
		})
	}

	return func(e *utils.Entry, vp *utils.ValuePtr) error { // Function for replaying.
		nk := make([]byte, len(e.Key))
		copy(nk, e.Key)
		var nv []byte
		meta := e.Meta
		if db.shouldWriteValueToLSM(e) {
			nv = make([]byte, len(e.Value))
			copy(nv, e.Value)
		} else {
			nv = vp.Encode()
			meta = meta | utils.BitValuePointer
		}
		// Update vhead. If the crash happens while replay was in progess
		// and the head is not updated, we will end up replaying all the
		// files starting from file zero, again.
		db.updateHead([]*utils.ValuePtr{vp})

		v := utils.ValueStruct{
			Value:     nv,
			Meta:      meta,
			ExpiresAt: e.ExpiresAt,
		}
		// This entry is from a rewrite or via SetEntryAt(..).
		toLSM(nk, v)
		return nil
	}
}

// updateHead should not be called without the db.Lock() since db.vhead is used
// by the writer go routines and memtable flushing goroutine.
func (db *DB) updateHead(ptrs []*utils.ValuePtr) {
	var ptr *utils.ValuePtr
	for i := len(ptrs) - 1; i >= 0; i-- {
		p := ptrs[i]
		if !p.IsZero() {
			ptr = p
			break
		}
	}
	if ptr.IsZero() {
		return
	}

	utils.CondPanic(ptr.Less(db.vhead), fmt.Errorf("ptr.Less(db.vhead) is true"))
	db.vhead = ptr
}

// sync  同步一下，刷盘
func (vlog *valueLog) sync(fid uint32) error {

	vlog.filesLock.RLock()
	maxFid := vlog.maxFid
	// During replay it is possible to get sync call with fid less than maxFid.
	// Because older file has already been synced, we can return from here.
	if fid < maxFid || len(vlog.filesMap) == 0 {
		vlog.filesLock.RUnlock()
		return nil
	}
	curlf := vlog.filesMap[maxFid]
	// Sometimes it is possible that vlog.maxFid has been increased but file creation
	// with same id is still in progress and this function is called. In those cases
	// entry for the file might not be present in vlog.filesMap.
	if curlf == nil {
		vlog.filesLock.RUnlock()
		return nil
	}
	curlf.RLock()
	vlog.filesLock.RUnlock()

	err := curlf.Sync()
	curlf.RUnlock()
	return err
}

// Set
func (v *valueLog) set(entry *utils.Entry) error {
	return nil
}

func (v *valueLog) get(entry *utils.Entry) (*utils.Entry, error) {
	// valuePtr := utils.ValuePtrDecode(entry.Value)
	return nil, nil
}

// lfDiscardStats 记录丢弃key的数据
// lfDiscardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type lfDiscardStats struct {
	sync.RWMutex
	m                 map[uint32]int64
	flushChan         chan map[uint32]int64
	closer            *utils.Closer
	updatesSinceFlush int
}

func (vlog *valueLog) flushDiscardStats() {
	defer vlog.lfDiscardStats.closer.Done()

	mergeStats := func(stats map[uint32]int64) ([]byte, error) {
		vlog.lfDiscardStats.Lock()
		defer vlog.lfDiscardStats.Unlock()
		for fid, count := range stats {
			vlog.lfDiscardStats.m[fid] += count
			vlog.lfDiscardStats.updatesSinceFlush++
		}

		if vlog.lfDiscardStats.updatesSinceFlush > discardStatsFlushThreshold {
			encodedDS, err := json.Marshal(vlog.lfDiscardStats.m)
			if err != nil {
				return nil, err
			}
			vlog.lfDiscardStats.updatesSinceFlush = 0
			return encodedDS, nil
		}
		return nil, nil
	}

	process := func(stats map[uint32]int64) error {
		encodedDS, err := mergeStats(stats)
		if err != nil || encodedDS == nil {
			return err
		}

		entries := []*utils.Entry{{
			Key:   utils.KeyWithTs(lfDiscardStatsKey, 1),
			Value: encodedDS,
		}}
		req, err := vlog.db.sendToWriteCh(entries)
		// No special handling of ErrBlockedWrites is required as err is just logged in
		// for loop below.
		if err != nil {
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait()
	}

	closer := vlog.lfDiscardStats.closer
	for {
		select {
		case <-closer.CloseSignal:
			// For simplicity just return without processing already present in stats in flushChan.
			return
		case stats := <-vlog.lfDiscardStats.flushChan:
			if err := process(stats); err != nil {
				utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
		}
	}
}

// 请求池
var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

// request
type request struct {
	// Input values
	Entries []*utils.Entry
	// Output values and wait group stuff below
	Ptrs []*utils.ValuePtr
	Wg   sync.WaitGroup
	Err  error
	ref  int32
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref = 0
}

// GC 部分
// 选择需要gc的log文件
func (vlog *valueLog) pickLog(head *utils.ValuePtr) (files []*file.LogFile) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()
	fids := vlog.sortedFids()
	switch {
	// 只有一个log文件那不需要进行GC了
	case len(fids) <= 1:
		return nil
		// fid 是0说明是初次启动，更不需要gc了
		// TODO 先不处理head
		// case head.Fid == 0:
		// 	return nil
	}

	// 创建一个候选对象
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}
	// 加锁遍历fids，选择小于等于head fid的列表中discard统计最大的那个log文件
	// discard 就是在compact过程中统计的可丢弃key的数量
	vlog.lfDiscardStats.RLock()
	for _, fid := range fids {
		if fid >= head.Fid {
			break
		}
		if vlog.lfDiscardStats.m[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = vlog.lfDiscardStats.m[fid]
		}
	}
	vlog.lfDiscardStats.RUnlock()

	// 说明这是一个有效候选
	if candidate.fid != math.MaxUint32 { // Found a candidate
		files = append(files, vlog.filesMap[candidate.fid])
	}

	// 再补充一种随机选择的fid，比如应对初次执行时discard的统计不充分的情况
	var idxHead int
	for i, fid := range fids {
		if fid == head.Fid {
			idxHead = i
			break
		}
	}
	if idxHead == 0 { // Not found or first file
		idxHead = 1 // 开始对
	}
	idx := rand.Intn(idxHead) // Don’t include head.Fid. We pick a random file before it.
	if idx > 0 {
		idx = rand.Intn(idx + 1) // Another level of rand to favor smaller fids.
	}
	files = append(files, vlog.filesMap[fids[idx]])
	return files
}

// sampler 采样器
type sampler struct {
	lf            *file.LogFile
	sizeRatio     float64
	countRatio    float64
	fromBeginning bool
}

func (vlog *valueLog) sample(samp *sampler, discardRatio float64) (*reason, error) {
	sizePercent := samp.sizeRatio
	countPercent := samp.countRatio
	fileSize := samp.lf.Size()
	// Set up the sampling winxdow sizes.
	sizeWindow := float64(fileSize) * sizePercent
	sizeWindowM := sizeWindow / (1 << 20) // in MBs.
	countWindow := int(float64(vlog.opt.ValueLogMaxEntries) * countPercent)

	var skipFirstM float64
	var err error
	// Skip data only if fromBeginning is set to false. Pick a random start point.
	if !samp.fromBeginning {
		// Pick a random start point for the log.
		skipFirstM = float64(rand.Int63n(int64(fileSize))) // Pick a random starting location.
		skipFirstM -= sizeWindow                           // Avoid hitting EOF by moving back by window.
		skipFirstM /= float64(utils.Mi)                    // Convert to MBs.
	}
	var skipped float64

	var r reason
	start := time.Now()
	var numIterations int
	// 重放遍历vlog文件
	_, err = vlog.iterate(samp.lf, 0, func(e *utils.Entry, vp *utils.ValuePtr) error {
		numIterations++
		esz := float64(vp.Len) / (1 << 20) // in MBs.
		if skipped < skipFirstM {
			skipped += esz
			return nil
		}
		// Sample until we reach the window sizes or exceed 10 seconds.
		if r.count > countWindow {
			return utils.ErrStop
		}
		if r.total > sizeWindowM {
			return utils.ErrStop
		}
		if time.Since(start) > 10*time.Second {
			return utils.ErrStop
		}
		r.total += esz
		r.count++

		entry, err := vlog.db.Get(e.Key)
		if err != nil {
			return err
		}
		if utils.DiscardEntry(e, entry) {
			r.discard += esz
			return nil
		}

		// Value is still present in value log.
		utils.CondPanic(len(entry.Value) <= 0, fmt.Errorf("len(entry.Value) <= 0"))
		vp.Decode(entry.Value)

		if vp.Fid > uint32(samp.lf.FID) {
			// Value is present in a later log. Discard.
			r.discard += esz
			return nil
		}
		if vp.Offset > e.Offset {
			// Value is present in a later offset, but in the same log.
			r.discard += esz
			return nil
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	fmt.Printf("Fid: %d. Skipped: %5.2fMB Num iterations: %d. Data status=%+v\n",
		samp.lf.FID, skipped, numIterations, r)
	// If we couldn't sample at least a 1000 KV pairs or at least 75% of the window size,
	// and what we can discard is below the threshold, we should skip the rewrite.
	if (r.count < countWindow && r.total < sizeWindowM*0.75) || r.discard < discardRatio*r.total {
		fmt.Printf("Skipping GC on fid: %d", samp.lf.FID)
		return nil, utils.ErrNoRewrite
	}
	return &r, nil
}
func (vlog *valueLog) waitOnGC(lc *utils.Closer) {
	defer lc.Done()

	<-lc.CloseSignal // Wait for lc to be closed.

	// Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
	// the channel of size 1.
	vlog.garbageCh <- struct{}{}
}

type reason struct {
	total   float64
	discard float64
	count   int
}
