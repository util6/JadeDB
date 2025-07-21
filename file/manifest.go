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

	"github.com/rookieLiuyutao/corekv/utils"

	"github.com/rookieLiuyutao/corekv/pb"

	"github.com/pkg/errors"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options   // opt 用于存储ManifestFile的配置选项
	f                         *os.File   // f 代表ManifestFile的底层操作系统文件句柄
	lock                      sync.Mutex // lock 用于确保对ManifestFile的操作是线程安全的
	deletionsRewriteThreshold int        // deletionsRewriteThreshold 表示触发重写操作的删除操作阈值
	manifest                  *Manifest  // manifest 用于存储和管理具体的manifest文件内容
}

// Manifest 是一个结构体，用于描述数据库或文件系统的manifest（清单）。
// 它包含了数据层次结构、表格信息以及创建和删除操作的计数。
type Manifest struct {
	// Levels 存储了按层次排列的levelManifest对象，每个levelManifest代表一个特定层次的数据信息。
	Levels []levelManifest

	// Tables 是一个映射，键是表格的唯一标识符（uint64类型），值是TableManifest对象，表示表格的详细信息。
	Tables map[uint64]TableManifest

	// Creations 记录了自manifest创建以来所创建的表格数量。
	Creations int

	// Deletions 记录了自manifest创建以来所删除的表格数量。
	Deletions int
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}

// levelManifest 结构体用于维护特定层级中的表ID集合。
// 主要用于跟踪某一层级中存在的表。
type levelManifest struct {
	Tables map[uint64]struct{} // 表ID集合
}

// TableMeta 表示 SST (Sorted String Table) 文件的元信息。
type TableMeta struct {
	ID       uint64 // - ID: SST 文件的唯一标识符。
	Checksum []byte // - Checksum: SST 文件内容的校验和，用于数据完整性验证。
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
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// TODO 锁粒度可以优化
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
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
