//go:build !windows
// +build !windows

package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
	"sync"
	"time"
)

// SSTable 结构体表示可序列化跳表的数据结构，包含对数据的管理及索引信息。
type SSTable struct {
	lock           *sync.RWMutex  // 用于读写锁
	f              *MmapFile      // 内存映射文件
	maxKey         []byte         // 最大键值
	minKey         []byte         // 最小键值
	idxTables      *pb.TableIndex // 索引表
	hasBloomFilter bool           // 是否包含布隆过滤器
	idxLen         int            // 索引长度
	idxStart       int            // 索引起始位置
	fid            uint64         // 文件标识符
	createdAt      time.Time      // 创建时间
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)

	// Read checksum len from the last 4 bytes.
	readPos -= 4
	buf := ss.f.Data[readPos : readPos+4]
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// Skip checksum for now
	readPos -= checksumLen

	// Read index size from the footer.
	readPos -= 4
	buf = ss.f.Data[readPos : readPos+4]
	ss.idxLen = int(utils.BytesToU32(buf))

	// Read index.
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.f.Data[ss.idxStart : ss.idxStart+ss.idxLen]
	if err := proto.Unmarshal(data, ss.idxTables); err != nil {
		return nil, err
	}

	ss.hasBloomFilter = len(ss.idxTables.BloomFilter) > 0

	if len(ss.idxTables.GetOffsets()) > 0 {
		return ss.idxTables.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

func (ss *SSTable) read(bo *pb.BlockOffset) ([]byte, error) {
	return ss.f.Bytes(int(bo.GetOffset()), int(bo.GetLen()))
}

func (ss *SSTable) readIndex() error {
	return nil
}

func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) Close() error {
	return ss.f.Close()
}

func (ss *SSTable) Init() error {
	return nil
}

func (ss *SSTable) SetMaxKey(key []byte) {
	ss.maxKey = key
}

func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

func (ss *SSTable) FID() uint64 {
	return ss.fid
}

func (ss *SSTable) Size() int64 {
	return int64(len(ss.f.Data))
}

func (ss *SSTable) GetCreatedAt() *time.Time {
	return &ss.createdAt
}

func (ss *SSTable) Delete() error {
	return ss.f.Delete()
}

func (ss *SSTable) SetCreatedAt(t *time.Time) {
	ss.createdAt = *t
}

func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, opt.Flag, opt.MaxSz)
	utils.Panic(err)
	ss := &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
	ss.idxTables = &pb.TableIndex{}

	if _, err := ss.initTable(); err != nil {
		utils.Panic(err)
	}

	// TODO: 实现 TableIterator
	// it := utils.NewTableIterator(ss)
	// defer it.Close()
	// it.Rewind()
	// if it.Valid() {
	//	ss.minKey = it.Key()
	// }
	// it.SeekToLast()
	// if it.Valid() {
	//	ss.maxKey = it.Key()
	// }
	return ss
}
