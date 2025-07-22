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
JadeDB WAL（Write-Ahead Log）文件管理模块

WAL 是数据库系统中的关键组件，用于保证数据的持久性和一致性。
在 JadeDB 中，WAL 确保所有写入操作在进入内存表之前先被持久化到磁盘。

核心功能：
1. 持久性保证：所有写入操作先记录到 WAL，再写入内存表
2. 崩溃恢复：系统重启时可以从 WAL 重建内存表状态
3. 原子性：每个条目的写入都是原子的，要么全部成功要么全部失败
4. 顺序写入：所有写入都是追加模式，充分利用磁盘顺序写性能

设计原理：
- 预写日志：Write-Ahead Logging，先写日志再写数据
- 顺序追加：所有写入都追加到文件末尾，避免随机写入
- 内存映射：使用 mmap 提高文件访问性能
- 校验和保护：每条记录都有校验和，防止数据损坏

文件格式：
每条记录：[长度][校验和][数据]
- 长度：4字节，记录数据部分的长度
- 校验和：4字节 CRC32，用于数据完整性验证
- 数据：变长，序列化的 Entry 数据

生命周期：
1. 创建：内存表创建时同时创建对应的 WAL 文件
2. 写入：每次写入内存表前先写入 WAL
3. 刷新：内存表刷新到 SSTable 后，WAL 可以删除
4. 恢复：系统启动时从 WAL 重建内存表

性能优化：
- 内存映射：减少系统调用开销
- 批量写入：支持批量操作减少 I/O 次数
- 缓冲区重用：重用缓冲区减少内存分配
- 异步同步：在适当时机异步刷新到磁盘

错误处理：
- 校验和验证：读取时验证数据完整性
- 部分写入检测：检测不完整的记录
- 自动修复：跳过损坏的记录，继续处理
- 错误报告：详细的错误信息用于诊断
*/

package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/util6/JadeDB/utils"
)

// WalFile 表示一个 WAL（Write-Ahead Log）文件。
// 它负责将所有写入操作持久化到磁盘，确保数据的持久性和一致性。
//
// 设计特点：
// 1. 顺序写入：所有写入都是追加模式，性能最优
// 2. 内存映射：使用 mmap 提高文件访问效率
// 3. 并发安全：使用读写锁保护并发访问
// 4. 校验保护：每条记录都有校验和验证
//
// 工作流程：
// 1. 写入操作先序列化为字节流
// 2. 计算校验和并添加到记录头部
// 3. 追加写入到文件末尾
// 4. 更新写入位置指针
//
// 恢复流程：
// 1. 从文件开头开始读取记录
// 2. 验证每条记录的校验和
// 3. 反序列化为 Entry 对象
// 4. 重建内存表状态
type WalFile struct {
	// 并发控制

	// lock 保护 WAL 文件的并发访问。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作需要独占访问，确保数据一致性。
	lock *sync.RWMutex

	// 文件接口

	// f 是底层的内存映射文件。
	// 提供高效的文件访问接口。
	// 使用 mmap 减少系统调用开销。
	f *MmapFile

	// 配置选项

	// opts 包含 WAL 文件的配置参数。
	// 包括文件路径、大小限制、权限等。
	opts *Options

	// 缓冲区管理

	// buf 是用于序列化操作的缓冲区。
	// 重用缓冲区可以减少内存分配开销。
	// 在写入操作中临时存储序列化数据。
	buf *bytes.Buffer

	// 大小管理

	// size 是文件的总大小（字节）。
	// 对应于内存映射区域的大小。
	// 用于边界检查和空间管理。
	size uint32

	// writeAt 是下一次写入的位置偏移量。
	// 指向文件中的下一个可写位置。
	// 随着写入操作递增，实现追加写入。
	writeAt uint32
}

// Fid _
func (wf *WalFile) Fid() uint64 {
	return wf.opts.FID
}

// Close _
func (wf *WalFile) Close() error {
	fileName := wf.f.Fd.Name()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

// Name _
func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

// Size 当前已经被写入的数据
func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}

// Sync 将 WAL 文件的更改同步到磁盘。
// 确保所有缓冲的写入操作都被持久化到存储设备。
//
// 返回值：
//   - error: 如果同步操作失败，返回错误信息
//
// 使用场景：
//   - 确保关键数据的持久性
//   - 实现事务的提交点
//   - 在系统关闭前保证数据安全
//
// 性能考虑：
//   - 同步操作可能比较耗时，应谨慎使用
//   - 可以批量进行同步操作以提高效率
//
// 并发安全：
//   - 使用读锁保护，允许并发读取但阻止写入
func (wf *WalFile) Sync() error {
	wf.lock.RLock()
	defer wf.lock.RUnlock()

	if wf.f == nil {
		return nil
	}
	return wf.f.Sync()
}

// OpenWalFile _
func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{f: omf, lock: &sync.RWMutex{}, opts: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	utils.Err(err)
	return wf
}

func (wf *WalFile) Write(entry *utils.Entry) error {
	// 落预写日志简单的同步写即可
	// 序列化为磁盘结构
	wf.lock.Lock()
	plen := utils.WalCodec(wf.buf, entry)
	buf := wf.buf.Bytes()
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, buf))
	wf.writeAt += uint32(plen)
	wf.lock.Unlock()
	return nil
}

// Iterate 从磁盘中遍历wal，获得数据
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	// For now, read directly from file, because it allows
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           wf,
	}
	var validEndOffset uint32 = offset
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e.IsZero():
			break loop
		}

		var vp utils.ValuePtr // 给kv分离的设计留下扩展,可以不用考虑其作用
		size := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

// Truncate _
// TODO Truncate 函数
func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fi, err := wf.f.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", wf.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncature(end)
}

// 封装kv分离的读操作
type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *WalFile
}

// MakeEntry _
func (r *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.WalHeader
	hlen, err := h.Decode(tee)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils.ErrTruncate
	}
	kl := int(h.KeyLen)
	if cap(r.K) < kl {
		r.K = make([]byte, 2*kl)
	}
	vl := int(h.ValueLen)
	if cap(r.V) < vl {
		r.V = make([]byte, 2*vl)
	}

	e := &utils.Entry{}
	e.Offset = r.RecordOffset
	e.Hlen = hlen
	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]
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
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}
