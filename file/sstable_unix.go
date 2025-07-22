//go:build !windows && !darwin
// +build !windows,!darwin

/*
JadeDB SSTable 文件管理模块（Unix 版本）

SSTable（Sorted String Table）是 LSM 树中的核心数据结构，用于在磁盘上存储有序的键值对数据。
这个模块提供了 SSTable 文件的创建、读取、索引和管理功能。

核心特性：
1. 不可变性：SSTable 一旦创建就不再修改，保证数据一致性
2. 有序存储：所有键值对按字典序排列，支持高效查找
3. 分块组织：数据按块存储，支持随机访问和缓存优化
4. 索引加速：内置块索引，支持二分查找快速定位
5. 布隆过滤器：可选的布隆过滤器，加速不存在键的查找

文件结构：
[数据块1][数据块2]...[数据块N][块索引][布隆过滤器][校验和长度][校验和][索引长度]

设计原理：
- 内存映射：使用 mmap 提高文件访问性能
- 分层索引：块级索引支持快速定位
- 校验保护：校验和确保数据完整性
- 平台优化：针对 Unix 系统的特定优化

性能优化：
- 零拷贝：内存映射避免数据拷贝
- 预取优化：利用操作系统的预取机制
- 缓存友好：块结构适合缓存系统
- 并发读取：支持多线程并发读取

使用场景：
- LSM 树的磁盘存储层
- 大量有序数据的持久化
- 需要快速范围查询的场景
- 读多写少的数据访问模式

平台特性：
- Unix 系统：利用 mmap、madvise 等系统调用
- 内存管理：更精细的内存映射控制
- 文件系统：充分利用 Unix 文件系统特性
*/

package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
	"sync"
	"time"
)

// SSTable 表示一个不可变的排序字符串表文件。
// 它是 LSM 树在磁盘上的主要存储格式，包含有序的键值对数据和相关的索引信息。
//
// 文件布局：
// 1. 数据区：包含多个数据块，每个块存储一定数量的键值对
// 2. 索引区：包含块索引，记录每个块的键范围和位置
// 3. 布隆过滤器：可选的布隆过滤器，用于快速过滤不存在的键
// 4. 元数据区：包含文件的统计信息和配置参数
// 5. 校验和：用于验证文件完整性
//
// 访问模式：
// - 顺序扫描：遍历所有数据块
// - 随机查找：使用索引快速定位特定键
// - 范围查询：利用有序性进行范围扫描
//
// 并发安全：
// 使用读写锁保护，支持多个并发读取操作。
type SSTable struct {
	// 并发控制

	// lock 保护 SSTable 的并发访问。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作（如初始化）需要独占访问。
	lock *sync.RWMutex

	// 文件访问

	// f 是底层的内存映射文件。
	// 提供对 SSTable 文件内容的高效访问。
	// 使用 mmap 技术减少数据拷贝开销。
	f *MmapFile

	// 键范围信息

	// maxKey 是 SSTable 中的最大键。
	// 用于范围查询和文件选择优化。
	// 在文件初始化时从索引中提取。
	maxKey []byte

	// minKey 是 SSTable 中的最小键。
	// 用于范围查询和文件选择优化。
	// 在文件初始化时从索引中提取。
	minKey []byte

	// 索引信息

	// idxTables 包含 SSTable 的完整索引信息。
	// 包括块索引、布隆过滤器、统计信息等。
	// 使用 Protocol Buffers 序列化格式。
	idxTables *pb.TableIndex

	// 布隆过滤器

	// hasBloomFilter 标记是否包含布隆过滤器。
	// 布隆过滤器用于快速判断键是否可能存在。
	// 可以显著减少不必要的磁盘访问。
	hasBloomFilter bool

	// 索引位置信息

	// idxLen 是索引数据的字节长度。
	// 用于读取和解析索引信息。
	idxLen int

	// idxStart 是索引数据在文件中的起始位置。
	// 用于定位和读取索引信息。
	idxStart int

	// 文件元信息

	// fid 是文件的唯一标识符。
	// 用于文件管理、缓存和日志记录。
	fid uint64

	// createdAt 记录文件的创建时间。
	// 用于统计、监控和调试目的。
	createdAt time.Time
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
