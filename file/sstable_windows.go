package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
	"sync"
	"syscall"
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
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// Read checksum.
	readPos -= checksumLen
	expectedChk := ss.readCheckError(readPos, checksumLen)

	// Read index size from the footer.
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(utils.BytesToU32(buf))

	// Read index.
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTables = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// Init 初始化 SSTable 结构，包括加载文件创建时间和设置最小和最大键。
func (ss *SSTable) Init() error {
	// 初始化块偏移量
	var ko *pb.BlockOffset
	var err error

	// 调用内部函数初始化表结构
	if ko, err = ss.initTable(); err != nil {
		return err
	}

	// 从文件中获取创建时间
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Win32FileAttributeData)
	ss.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)

	// 初始化最小键
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey

	return nil
}
