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
	"github.com/rookieLiuyutao/corekv/file"
	"github.com/rookieLiuyutao/corekv/utils"
)

const walFileExt string = ".wal"

type MemTable = memTable

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemtable _
func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize), //TODO wal 要设置多大比较合理？ 姑且跟sst一样大
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList(int64(1 << 20)), lsm: lsm}
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}

	return nil
}

func (m *memTable) set(entry *utils.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	m.sl.Add(entry)
	return nil
}

func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	vs := m.sl.Search(key)

	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
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
