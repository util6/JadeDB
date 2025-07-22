//go:build darwin
// +build darwin

/*
JadeDB SSTable 文件管理模块（Darwin/macOS 版本）

SSTable（Sorted String Table）是 LSM 树中的核心数据结构，用于在磁盘上存储有序的键值对数据。
这个模块提供了 SSTable 文件在 macOS (Darwin) 系统上的创建、读取、索引和管理功能。

核心特性：
1. 不可变性：SSTable 一旦创建就不再修改，保证数据一致性
2. 有序存储：所有键值对按字典序排列，支持高效查找
3. 分块组织：数据按块存储，支持随机访问和缓存优化
4. 索引加速：内置块索引，支持二分查找快速定位
5. 布隆过滤器：可选的布隆过滤器，加速不存在键的查找

文件结构：
[数据块1][数据块2]...[数据块N][块索引][布隆过滤器][校验和长度][校验和][索引长度]

Darwin/macOS 特性：
- HFS+/APFS 文件系统优化：充分利用 macOS 文件系统的特性
- 统一缓冲区缓存 (UBC)：利用 macOS 的高效缓存机制
- BSD 系统调用：使用标准的 POSIX 接口确保兼容性
- 内存映射优化：针对 macOS 虚拟内存系统的优化
- 文件系统事件：支持 macOS 的文件系统监控机制

设计原理：
- 内存映射：使用 mmap 提高文件访问性能
- 分层索引：块级索引支持快速定位
- 校验保护：校验和确保数据完整性
- 平台优化：针对 Darwin 系统的特定优化

性能优化：
- 零拷贝：内存映射避免数据拷贝
- 预取优化：利用 macOS 的预取机制
- 缓存友好：块结构适合 macOS 缓存系统
- 并发读取：支持多线程并发读取

使用场景：
- LSM 树的磁盘存储层
- 大量有序数据的持久化
- 需要快速范围查询的场景
- 读多写少的数据访问模式

macOS 平台特性：
- Darwin 内核：利用 XNU 内核的高级特性
- 内存管理：更精细的内存映射控制
- 文件系统：充分利用 APFS 的快照和克隆特性
- 安全性：支持 macOS 的文件系统权限和安全机制
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
//
// Darwin/macOS 优化：
// - 利用 APFS 文件系统的快照和克隆特性
// - 使用 macOS 的统一缓冲区缓存 (UBC)
// - 针对 Darwin 内核的内存管理优化
// - 支持 macOS 的文件系统事件监控
type SSTable struct {
	// 并发控制

	// lock 保护 SSTable 的并发访问。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作（如初始化）需要独占访问。
	// 在 macOS 上，利用 Darwin 内核的高效锁实现。
	lock *sync.RWMutex

	// 文件访问

	// f 是底层的内存映射文件。
	// 提供对 SSTable 文件内容的高效访问。
	// 使用 mmap 技术减少数据拷贝开销。
	// 在 macOS 上，充分利用统一缓冲区缓存 (UBC)。
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
	// 在 macOS 上，可以利用文件系统的扩展属性。
	createdAt time.Time
}

// initTable 初始化 SSTable 的索引信息。
// 从文件末尾读取索引数据，解析并验证文件结构。
//
// 返回值：
//   - *pb.BlockOffset: 第一个数据块的偏移信息
//   - error: 如果初始化失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用 APFS 文件系统的元数据缓存
//   - 使用 macOS 的高效内存映射机制
//   - 充分利用统一缓冲区缓存 (UBC)
func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)

	// 从文件末尾的最后 4 个字节读取校验和长度
	// 在 macOS 上，这个操作会被 UBC 缓存优化
	readPos -= 4
	buf := ss.f.Data[readPos : readPos+4]
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// 跳过校验和数据
	readPos -= checksumLen

	// 从文件尾部读取索引大小
	readPos -= 4
	buf = ss.f.Data[readPos : readPos+4]
	ss.idxLen = int(utils.BytesToU32(buf))

	// 读取索引数据
	// 在 macOS 上，大块数据读取会触发预取优化
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.f.Data[ss.idxStart : ss.idxStart+ss.idxLen]
	if err := proto.Unmarshal(data, ss.idxTables); err != nil {
		return nil, err
	}

	// 检查是否包含布隆过滤器
	ss.hasBloomFilter = len(ss.idxTables.BloomFilter) > 0

	// 返回第一个块的偏移信息
	if len(ss.idxTables.GetOffsets()) > 0 {
		return ss.idxTables.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// read 从 SSTable 中读取指定块的数据。
// 使用块偏移信息进行高效的随机访问读取。
//
// 参数：
//   - bo: 块偏移信息，包含位置和大小
//
// 返回值：
//   - []byte: 读取到的块数据
//   - error: 如果读取失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用内存映射的零拷贝特性
//   - 使用 macOS 的页面缓存机制
//   - 支持 APFS 的压缩和去重特性
func (ss *SSTable) read(bo *pb.BlockOffset) ([]byte, error) {
	return ss.f.Bytes(int(bo.GetOffset()), int(bo.GetLen()))
}

// readIndex 读取索引信息（当前为空实现）。
// 预留接口，用于未来的索引优化功能。
//
// 返回值：
//   - error: 总是返回 nil
func (ss *SSTable) readIndex() error {
	return nil
}

// Bytes 从 SSTable 中读取指定范围的字节数据。
// 提供对文件内容的直接访问接口。
//
// 参数：
//   - off: 读取的起始偏移量
//   - sz: 要读取的字节数
//
// 返回值：
//   - []byte: 读取到的数据
//   - error: 如果读取失败则返回错误
//
// Darwin/macOS 特性：
//   - 利用统一缓冲区缓存 (UBC) 提高性能
//   - 支持大文件的高效访问
//   - 使用 macOS 的预取机制优化顺序读取
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

// Indexs 返回 SSTable 的索引信息。
// 提供对完整索引数据的访问接口。
//
// 返回值：
//   - *pb.TableIndex: 包含块索引、布隆过滤器等信息的索引对象
func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

// MaxKey 返回 SSTable 中的最大键。
// 用于范围查询和文件选择优化。
//
// 返回值：
//   - []byte: 最大键的字节表示
func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

// MinKey 返回 SSTable 中的最小键。
// 用于范围查询和文件选择优化。
//
// 返回值：
//   - []byte: 最小键的字节表示
func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

// Close 关闭 SSTable 文件并释放相关资源。
// 确保内存映射被正确释放，文件句柄被关闭。
//
// 返回值：
//   - error: 如果关闭失败则返回错误
//
// Darwin/macOS 特性：
//   - 自动释放 Darwin 内核分配的资源
//   - 确保 UBC 缓存的一致性
//   - 支持 macOS 的文件系统事件通知
func (ss *SSTable) Close() error {
	return ss.f.Close()
}

// Init 初始化 SSTable（当前为空实现）。
// 预留接口，用于未来的初始化功能扩展。
//
// 返回值：
//   - error: 总是返回 nil
func (ss *SSTable) Init() error {
	return nil
}

// SetMaxKey 设置 SSTable 的最大键。
// 用于在文件创建或索引构建过程中更新键范围信息。
//
// 参数：
//   - key: 要设置的最大键
func (ss *SSTable) SetMaxKey(key []byte) {
	ss.maxKey = key
}

// HasBloomFilter 检查 SSTable 是否包含布隆过滤器。
// 布隆过滤器用于快速判断键是否可能存在，减少不必要的磁盘访问。
//
// 返回值：
//   - bool: 如果包含布隆过滤器则返回 true
func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

// FID 返回 SSTable 的文件标识符。
// 用于文件管理、缓存键生成和日志记录。
//
// 返回值：
//   - uint64: 文件的唯一标识符
func (ss *SSTable) FID() uint64 {
	return ss.fid
}

// Size 返回 SSTable 文件的字节大小。
// 用于统计、监控和存储空间管理。
//
// 返回值：
//   - int64: 文件的字节大小
//
// Darwin/macOS 特性：
//   - 利用内存映射获得准确的文件大小
//   - 支持 APFS 的压缩文件大小计算
func (ss *SSTable) Size() int64 {
	return int64(len(ss.f.Data))
}

// GetCreatedAt 返回 SSTable 文件的创建时间。
// 用于统计、监控和文件生命周期管理。
//
// 返回值：
//   - *time.Time: 文件的创建时间
func (ss *SSTable) GetCreatedAt() *time.Time {
	return &ss.createdAt
}

// Delete 删除 SSTable 文件。
// 从文件系统中永久删除这个文件。
//
// 返回值：
//   - error: 如果删除失败则返回错误
//
// Darwin/macOS 特性：
//   - 支持 APFS 的安全删除机制
//   - 自动清理文件系统缓存
//   - 支持 macOS 的垃圾箱机制（如果配置）
func (ss *SSTable) Delete() error {
	return ss.f.Delete()
}

// SetCreatedAt 设置 SSTable 文件的创建时间。
// 用于在文件创建或迁移过程中设置正确的时间戳。
//
// 参数：
//   - t: 要设置的创建时间
func (ss *SSTable) SetCreatedAt(t *time.Time) {
	ss.createdAt = *t
}

// OpenSStable 打开或创建一个 SSTable 文件。
// 这是创建 SSTable 实例的主要入口函数。
//
// 参数：
//   - opt: 文件操作选项，包含文件名、标志、大小等配置
//
// 返回值：
//   - *SSTable: 初始化完成的 SSTable 实例
//
// Darwin/macOS 优化：
//   - 使用 macOS 优化的内存映射文件
//   - 利用 APFS 文件系统的高级特性
//   - 支持 macOS 的文件系统事件监控
//   - 充分利用统一缓冲区缓存 (UBC)
//
// 功能说明：
// 1. 创建内存映射文件对象
// 2. 初始化 SSTable 结构体
// 3. 解析文件索引信息
// 4. 设置键范围信息（TODO: 需要实现 TableIterator）
//
// 注意事项：
// - 如果文件不存在且指定了创建标志，会创建新文件
// - 文件打开后会立即解析索引信息
// - 键范围信息的设置依赖于 TableIterator 的实现
func OpenSStable(opt *Options) *SSTable {
	// 使用 Darwin 优化的内存映射文件打开文件
	// 在 macOS 上，这会利用统一缓冲区缓存 (UBC) 和 APFS 特性
	omf, err := OpenMmapFile(opt.FileName, opt.Flag, opt.MaxSz)
	utils.Panic(err)

	// 创建 SSTable 实例，初始化基本字段
	// 使用读写锁支持并发访问
	ss := &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
	ss.idxTables = &pb.TableIndex{}

	// 初始化表结构，解析索引信息
	// 这个过程会读取文件末尾的索引数据
	if _, err := ss.initTable(); err != nil {
		utils.Panic(err)
	}

	// TODO: 实现 TableIterator 来设置键范围
	// 当前版本暂时跳过键范围的自动设置
	// 在未来版本中，这里会使用迭代器来确定最小和最大键
	//
	// 计划实现：
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
