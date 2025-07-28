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
JadeDB Manifest 文件管理模块

Manifest 文件是 JadeDB 的元数据管理核心，记录了整个 LSM 树的结构信息。
它类似于数据库的系统目录，维护着所有 SSTable 文件的元信息。

核心功能：
1. 元数据持久化：记录所有 SSTable 文件的位置和属性
2. 系统恢复：在数据库重启时重建 LSM 树结构
3. 一致性保证：确保文件操作的原子性和一致性
4. 版本管理：跟踪文件的创建、删除和修改历史

设计原理：
- 追加写入：所有变更以追加方式写入，保证操作原子性
- 检查点机制：定期重写文件，清理历史记录
- 校验和保护：使用 CRC32 校验和防止数据损坏
- 实时同步：不使用内存映射，确保数据实时写入磁盘

文件格式：
每个记录包含：[长度][数据][校验和]
- 长度：4字节，记录数据部分的长度
- 数据：变长，使用 Protocol Buffers 序列化
- 校验和：4字节 CRC32，用于数据完整性验证

应用场景：
- 数据库启动时的状态恢复
- SSTable 文件的生命周期管理
- 压缩操作的元数据更新
- 系统一致性检查和修复

性能考虑：
- 顺序写入：所有操作都是追加写入，性能较好
- 批量操作：支持批量更新减少 I/O 次数
- 内存缓存：在内存中维护完整的元数据副本
- 定期重写：控制文件大小，避免过度增长
*/

package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
)

// ManifestFile 表示 JadeDB 的清单文件管理器。
// 清单文件是 LSM 树元数据的持久化存储，记录所有 SSTable 文件的信息。
//
// 设计特点：
// 1. 实时写入：不使用内存映射，确保数据立即写入磁盘
// 2. 追加模式：所有变更以追加方式记录，保证原子性
// 3. 校验保护：每条记录都有 CRC32 校验和
// 4. 定期重写：当删除记录过多时触发文件重写
//
// 并发安全：
// 使用互斥锁保护所有操作，确保多线程环境下的数据一致性。
//
// 文件格式：
// 每条记录：[4字节长度][变长数据][4字节CRC32]
type ManifestFile struct {
	// 配置和文件句柄

	// opt 存储清单文件的配置选项。
	// 包含文件路径、权限设置等基本配置信息。
	opt *Options

	// f 是底层的操作系统文件句柄。
	// 用于直接的文件读写操作，不使用缓冲或内存映射。
	// 确保数据能够立即写入磁盘，提供最强的持久性保证。
	f *os.File

	// 并发控制

	// lock 保护清单文件的所有操作。
	// 确保读写操作的原子性和一致性。
	// 虽然会影响并发性能，但对数据正确性至关重要。
	lock sync.Mutex

	// 维护策略

	// deletionsRewriteThreshold 定义触发文件重写的删除操作阈值。
	// 当累积的删除记录达到这个数量时，会重写整个文件。
	// 重写可以清理历史记录，减小文件大小，提高读取性能。
	deletionsRewriteThreshold int

	// 内存状态

	// manifest 是清单数据在内存中的完整副本。
	// 提供快速的查询和更新操作，避免频繁的磁盘访问。
	// 所有内存更新都会同步写入磁盘文件。
	manifest *Manifest
}

// Manifest 表示 LSM 树的完整元数据结构。
// 它是清单文件内容在内存中的表示，包含了重建 LSM 树所需的所有信息。
//
// 数据组织：
// 1. 按层级组织：每个层级维护自己的文件列表
// 2. 全局索引：通过文件 ID 快速查找文件信息
// 3. 统计信息：跟踪文件创建和删除的历史
//
// 一致性保证：
// 内存中的 Manifest 与磁盘文件保持严格同步。
type Manifest struct {
	// 层级结构

	// Levels 存储各个层级的文件组织信息。
	// 数组索引对应层级编号（0=L0, 1=L1, ...）。
	// 每个 levelManifest 维护该层级中所有文件的 ID 集合。
	Levels []levelManifest

	// 文件索引

	// Tables 是全局的文件信息映射表。
	// 键是文件的唯一标识符（FID），值是文件的详细信息。
	// 提供 O(1) 的文件信息查找能力。
	Tables map[uint64]TableManifest

	// 统计信息

	// Creations 记录自清单创建以来的文件创建总数。
	// 用于统计和监控，帮助了解系统的写入活动。
	Creations int

	// Deletions 记录自清单创建以来的文件删除总数。
	// 当删除数量过多时，会触发清单文件的重写操作。
	// 重写可以清理历史记录，优化文件大小和读取性能。
	Deletions int
}

// TableManifest 包含单个 SSTable 文件的基本元信息。
// 这些信息用于文件管理、完整性检查和系统恢复。
type TableManifest struct {
	// Level 指定文件所属的层级（0-N）。
	// L0 层的文件可能有重叠，其他层级的文件无重叠。
	// 层级信息用于查询路由和压缩策略。
	Level uint8

	// Checksum 存储文件内容的校验和。
	// 用于检测文件损坏和数据完整性验证。
	// 预留字段，方便未来扩展更多的校验算法。
	Checksum []byte
}

// levelManifest 维护特定层级中的文件集合。
// 使用集合结构快速判断文件是否属于某个层级。
type levelManifest struct {
	// Tables 存储该层级中所有文件的 ID 集合。
	// 使用 map[uint64]struct{} 实现高效的集合操作。
	// struct{} 不占用额外内存，只利用 map 的键来实现集合功能。
	Tables map[uint64]struct{}
}

// TableMeta 表示 SSTable 文件的元信息。
// 用于文件创建、管理和完整性验证。
type TableMeta struct {
	// ID 是 SSTable 文件的唯一标识符。
	// 通常用作文件名的一部分，确保文件名的唯一性。
	// 在整个数据库生命周期中保持唯一。
	ID uint64

	// Checksum 是文件内容的校验和。
	// 用于验证文件的完整性，检测潜在的数据损坏。
	// 支持多种校验算法，提供灵活的完整性保护。
	Checksum []byte
}

// OpenManifestFile 打开manifest文件
// 如果文件不存在，则创建一个新的manifest文件
// 该函数返回一个ManifestFile对象，用于操作manifest文件，或者返回错误
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	// 构造manifest文件的完整路径
	path := filepath.Join(opt.Dir, utils.ManifestFilename)

	// 初始化ManifestFile对象
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}

	// 尝试打开现有的manifest文件
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	// 如果打开失败 则尝试创建一个新的 manifest file
	if err != nil {
		// 如果错误不是因为文件不存在，则直接返回错误
		if !os.IsNotExist(err) {
			return mf, err
		}
		// 创建一个新的manifest对象
		m := createManifest()
		// 调用helpRewrite函数创建并重写manifest文件
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		// 如果重写失败，抛出异常
		utils.CondPanic(netCreations == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		// 如果有错误，返回
		if err != nil {
			return mf, err
		}
		// 设置ManifestFile对象的文件指针和manifest对象
		mf.f = fp
		f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开成功，则对manifest文件重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		// 关闭文件并返回错误
		_ = f.Close()
		return mf, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err := f.Truncate(truncOffset); err != nil {
		// 关闭文件并返回错误
		_ = f.Close()
		return mf, err
	}
	// 将文件指针移动到文件末尾
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		// 关闭文件并返回错误
		_ = f.Close()
		return mf, err
	}
	// 设置ManifestFile对象的文件指针和manifest对象
	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

// ReplayManifestFile 从给定的文件指针 fp 读取并解析manifest文件。
// 它返回解析后的 Manifest 结构体，文件中最后一个有效条目的偏移量 truncOffset，以及可能出现的错误。
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	// 使用 bufReader 包装文件指针，以支持缓冲读取和计数功能。
	r := &bufReader{reader: bufio.NewReader(fp)}

	// 读取文件头部的 magic bytes 以验证文件类型。
	var magicBuf [8]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		// 如果读取 magic bytes 时发生错误，返回错误信息。
		return &Manifest{}, 0, utils.ErrBadMagic
	}

	// 验证读取的 magic bytes 是否与预期值匹配。
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}

	// 从 magic bytes 中提取版本号，并验证其是否为支持的版本。
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}

	// 初始化一个空的 Manifest 结构体用于构建最终的 Manifest。
	build := createManifest()
	var offset int64
	for {
		// 在循环开始时记录当前读取位置。
		offset = r.count

		// 读取长度和CRC校验码。
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(r, lenCrcBuf[:])
		if err != nil {
			// 如果遇到 EOF 或者其他读取错误，根据错误类型决定是否退出循环或者返回错误。
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}

		// 解析长度，并根据该长度读取数据。
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			// 同样，遇到 EOF 或者其他读取错误时，决定是否退出循环或者返回错误。
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}

		// 校验读取的数据的CRC。
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		// 解析读取的数据为 ManifestChangeSet 结构体。
		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}

		// 应用解析的 ManifestChangeSet 到构建的 Manifest 中。
		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}

	// 返回构建的 Manifest，最后一个有效条目的偏移量，以及可能出现的错误。
	return build, offset, err
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func applyManifestChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			Checksum: append([]byte{}, tc.Checksum...),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64
}

func (r *bufReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.count += int64(n)
	return
}

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// Must be called while appendLock is held.
func (mf *ManifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = nextCreations
	mf.manifest.Deletions = 0
	mf.f = fp
	return nil
}

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	// We explicitly sync.
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// AddChanges 对外暴露的写比那更丰富
func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{Changes: changesParam}

	// 序列化操作移到锁外面，减少锁持有时间
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// 预计算CRC和长度信息，进一步减少锁内操作
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
	writeBuffer := append(lenCrcBuf[:], buf...)

	// 优化锁粒度：分离内存操作和I/O操作
	mf.lock.Lock()

	// 应用变更到内存中的manifest
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		mf.lock.Unlock()
		return err
	}

	// 检查是否需要重写manifest
	needRewrite := mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions)

	mf.lock.Unlock()

	// I/O操作在锁外进行
	if needRewrite {
		// 重写操作需要重新获取锁
		mf.lock.Lock()
		err := mf.rewrite()
		mf.lock.Unlock()
		if err != nil {
			return err
		}
	} else {
		// 写入操作不需要锁保护，因为文件写入是原子的
		if _, err := mf.f.Write(writeBuffer); err != nil {
			return err
		}
	}

	// 同步操作也在锁外进行
	return mf.f.Sync()
}

// AddTableMeta 存储level表到manifest的level中
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			filename := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// GetManifest manifest
func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
