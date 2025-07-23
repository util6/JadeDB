/*
JadeDB B+树页面管理模块

页面是B+树存储引擎的基本存储单位，参考InnoDB的设计实现固定大小的页面管理。
本模块提供页面的分配、回收、I/O操作和缓存管理功能。

核心设计原理：
1. 固定页面大小：16KB页面，与InnoDB保持一致，优化磁盘I/O
2. 页面类型管理：支持根页面、内部页面、叶子页面、溢出页面等
3. 空间管理：页面内空间分配，支持变长记录存储
4. 完整性保护：页面校验和，检测数据损坏
5. 缓存友好：页面结构优化，提高缓存命中率

页面结构设计：
- 页面头部：元数据信息（类型、校验和、LSN等）
- 目录区：记录偏移量数组，支持二分查找
- 记录区：实际数据存储区域
- 空闲空间：未使用的空间
- 页面尾部：校验信息

性能特性：
- O(log n) 查找：目录区支持二分查找
- 空间效率：紧凑的记录存储格式
- 并发友好：页面级别的锁机制
- 缓存优化：页面大小与系统页面对齐
*/

package bplustree

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"
)

// 页面大小和布局常量
const (
	// PageSize 页面大小（16KB），与InnoDB保持一致
	// 这个大小经过优化，既能充分利用磁盘I/O，又能适应内存缓存
	PageSize = 16 * 1024

	// PageHeaderSize 页面头部大小（56字节）
	// 包含页面类型、校验和、LSN、页面ID等元数据
	PageHeaderSize = 56

	// PageTrailerSize 页面尾部大小（8字节）
	// 包含校验和的副本，用于检测页面损坏
	PageTrailerSize = 8

	// MaxRecordSize 单条记录的最大大小
	// 确保页面能容纳至少2条记录
	MaxRecordSize = (PageSize - PageHeaderSize - PageTrailerSize) / 2

	// MinRecordsPerPage 每个页面的最小记录数
	// 用于触发页面合并操作
	MinRecordsPerPage = 2

	// MaxRecordsPerPage 每个页面的最大记录数（估算值）
	// 实际数量取决于记录大小
	MaxRecordsPerPage = 1000
)

// PageType 定义页面类型
type PageType uint16

const (
	// FreelistPage 空闲页面链表头
	// 管理已删除页面的回收和重用
	FreelistPage PageType = iota

	// RootPage 根页面
	// B+树的入口点，可能同时是内部页面或叶子页面
	RootPage

	// InternalPage 内部页面
	// 只存储键和指向子页面的指针，不存储实际数据
	InternalPage

	// LeafPage 叶子页面
	// 存储实际的键值对数据，通过链表连接
	LeafPage

	// OverflowPage 溢出页面
	// 用于存储超大的值，通过链表结构支持任意大小的值
	OverflowPage

	// MetaPage 元数据页面
	// 存储B+树的全局元数据信息
	MetaPage
)

// PageHeader 定义页面头部结构（56字节）
// 包含页面的所有元数据信息，用于页面管理和完整性检查
type PageHeader struct {
	// 基本信息（16字节）
	PageID   uint64   // 页面唯一标识符（8字节）
	PageType PageType // 页面类型（2字节）
	Level    uint16   // 在B+树中的层级（2字节）
	Flags    uint32   // 页面标志位（4字节）

	// 完整性保护（16字节）
	Checksum  uint32 // 页面校验和（4字节）
	LSN       uint64 // 日志序列号，用于恢复（8字节）
	Reserved1 uint32 // 保留字段（4字节）

	// 空间管理（16字节）
	RecordCount uint16 // 当前记录数量（2字节）
	FreeSpace   uint16 // 剩余空间大小（2字节）
	FreeOffset  uint16 // 空闲空间起始偏移（2字节）
	GarbageSize uint16 // 垃圾空间大小（2字节）
	FirstRecord uint16 // 第一条记录偏移（2字节）
	LastRecord  uint16 // 最后一条记录偏移（2字节）
	Reserved2   uint32 // 保留字段（4字节）

	// 链接信息（8字节）
	PrevPage uint64 // 前一个页面ID（仅叶子页面使用）
	NextPage uint64 // 下一个页面ID（仅叶子页面使用）
}

// PageTrailer 定义页面尾部结构（8字节）
// 用于检测页面损坏和验证数据完整性
type PageTrailer struct {
	ChecksumCopy uint32 // 校验和副本（4字节）
	Magic        uint32 // 魔数标识（4字节）
}

// Page 表示一个B+树页面
// 这是B+树存储的基本单位，包含页面数据和相关的操作方法
type Page struct {
	// 页面数据
	Data []byte // 页面原始数据（16KB）

	// 页面元数据
	ID       uint64    // 页面ID
	Type     PageType  // 页面类型
	Dirty    bool      // 是否已修改
	Pinned   int32     // 引用计数
	LastUsed time.Time // 最后使用时间

	// 并发控制
	mutex sync.RWMutex // 页面级读写锁

	// 缓存管理
	inBuffer bool        // 是否在缓冲池中
	lruNode  interface{} // LRU链表节点（不同模块使用不同类型）
}

// NewPage 创建一个新的页面
// 初始化页面数据结构和头部信息
func NewPage(pageID uint64, pageType PageType) *Page {
	page := &Page{
		Data:     make([]byte, PageSize),
		ID:       pageID,
		Type:     pageType,
		Dirty:    true,
		LastUsed: time.Now(),
	}

	// 初始化页面头部
	header := &PageHeader{
		PageID:      pageID,
		PageType:    pageType,
		Level:       0,
		Flags:       0,
		RecordCount: 0,
		FreeSpace:   PageSize - PageHeaderSize - PageTrailerSize,
		FreeOffset:  PageHeaderSize,
		GarbageSize: 0,
		FirstRecord: 0,
		LastRecord:  0,
		PrevPage:    0,
		NextPage:    0,
	}

	// 写入页面头部
	page.writeHeader(header)

	// 初始化页面尾部
	trailer := &PageTrailer{
		Magic: 0xDEADBEEF, // 魔数标识
	}
	page.writeTrailer(trailer)

	// 计算并设置校验和
	page.UpdateChecksum()

	return page
}

// GetHeader 获取页面头部信息
func (p *Page) GetHeader() *PageHeader {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	header := &PageHeader{}
	p.readHeader(header)
	return header
}

// SetHeader 设置页面头部信息
func (p *Page) SetHeader(header *PageHeader) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.writeHeader(header)
	p.Dirty = true
}

// readHeader 从页面数据中读取头部信息
func (p *Page) readHeader(header *PageHeader) {
	data := p.Data[:PageHeaderSize]

	header.PageID = binary.LittleEndian.Uint64(data[0:8])
	header.PageType = PageType(binary.LittleEndian.Uint16(data[8:10]))
	header.Level = binary.LittleEndian.Uint16(data[10:12])
	header.Flags = binary.LittleEndian.Uint32(data[12:16])

	header.Checksum = binary.LittleEndian.Uint32(data[16:20])
	header.LSN = binary.LittleEndian.Uint64(data[20:28])
	header.Reserved1 = binary.LittleEndian.Uint32(data[28:32])

	header.RecordCount = binary.LittleEndian.Uint16(data[32:34])
	header.FreeSpace = binary.LittleEndian.Uint16(data[34:36])
	header.FreeOffset = binary.LittleEndian.Uint16(data[36:38])
	header.GarbageSize = binary.LittleEndian.Uint16(data[38:40])
	header.FirstRecord = binary.LittleEndian.Uint16(data[40:42])
	header.LastRecord = binary.LittleEndian.Uint16(data[42:44])
	header.Reserved2 = binary.LittleEndian.Uint32(data[44:48])

	header.PrevPage = binary.LittleEndian.Uint64(data[48:56])
	header.NextPage = binary.LittleEndian.Uint64(data[56:64])
}

// writeHeader 将头部信息写入页面数据
func (p *Page) writeHeader(header *PageHeader) {
	data := p.Data[:PageHeaderSize]

	binary.LittleEndian.PutUint64(data[0:8], header.PageID)
	binary.LittleEndian.PutUint16(data[8:10], uint16(header.PageType))
	binary.LittleEndian.PutUint16(data[10:12], header.Level)
	binary.LittleEndian.PutUint32(data[12:16], header.Flags)

	binary.LittleEndian.PutUint32(data[16:20], header.Checksum)
	binary.LittleEndian.PutUint64(data[20:28], header.LSN)
	binary.LittleEndian.PutUint32(data[28:32], header.Reserved1)

	binary.LittleEndian.PutUint16(data[32:34], header.RecordCount)
	binary.LittleEndian.PutUint16(data[34:36], header.FreeSpace)
	binary.LittleEndian.PutUint16(data[36:38], header.FreeOffset)
	binary.LittleEndian.PutUint16(data[38:40], header.GarbageSize)
	binary.LittleEndian.PutUint16(data[40:42], header.FirstRecord)
	binary.LittleEndian.PutUint16(data[42:44], header.LastRecord)
	binary.LittleEndian.PutUint32(data[44:48], header.Reserved2)

	binary.LittleEndian.PutUint64(data[48:56], header.PrevPage)
	binary.LittleEndian.PutUint64(data[56:64], header.NextPage)
}

// writeTrailer 写入页面尾部信息
func (p *Page) writeTrailer(trailer *PageTrailer) {
	offset := PageSize - PageTrailerSize
	data := p.Data[offset:]

	binary.LittleEndian.PutUint32(data[0:4], trailer.ChecksumCopy)
	binary.LittleEndian.PutUint32(data[4:8], trailer.Magic)
}

// CalculateChecksum 计算页面校验和
// 使用CRC32算法计算页面数据的校验和，排除校验和字段本身
func (p *Page) CalculateChecksum() uint32 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// 计算校验和时排除头部的校验和字段（16-20字节）和尾部的校验和副本
	crc := crc32.NewIEEE()

	// 写入头部（排除校验和字段）
	crc.Write(p.Data[0:16])                          // PageID到Flags
	crc.Write(p.Data[20 : PageSize-PageTrailerSize]) // LSN到页面数据结束

	return crc.Sum32()
}

// UpdateChecksum 更新页面校验和
func (p *Page) UpdateChecksum() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	checksum := p.calculateChecksumUnsafe()

	// 更新头部校验和
	binary.LittleEndian.PutUint32(p.Data[16:20], checksum)

	// 更新尾部校验和副本
	offset := PageSize - PageTrailerSize
	binary.LittleEndian.PutUint32(p.Data[offset:offset+4], checksum)

	p.Dirty = true
}

// calculateChecksumUnsafe 计算校验和（不加锁版本）
func (p *Page) calculateChecksumUnsafe() uint32 {
	crc := crc32.NewIEEE()
	crc.Write(p.Data[0:16])                          // PageID到Flags
	crc.Write(p.Data[20 : PageSize-PageTrailerSize]) // LSN到页面数据结束
	return crc.Sum32()
}

// VerifyChecksum 验证页面校验和
func (p *Page) VerifyChecksum() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// 读取存储的校验和
	storedChecksum := binary.LittleEndian.Uint32(p.Data[16:20])
	storedChecksumCopy := binary.LittleEndian.Uint32(p.Data[PageSize-PageTrailerSize : PageSize-PageTrailerSize+4])

	// 校验和副本必须一致
	if storedChecksum != storedChecksumCopy {
		return false
	}

	// 计算当前校验和
	calculated := p.calculateChecksumUnsafe()

	return calculated == storedChecksum
}

// Pin 增加页面引用计数
func (p *Page) Pin() {
	atomic.AddInt32(&p.Pinned, 1)
	p.LastUsed = time.Now()
}

// Unpin 减少页面引用计数
func (p *Page) Unpin() {
	atomic.AddInt32(&p.Pinned, -1)
}

// IsPinned 检查页面是否被引用
func (p *Page) IsPinned() bool {
	return atomic.LoadInt32(&p.Pinned) > 0
}

// GetFreeSpace 获取页面剩余空间
func (p *Page) GetFreeSpace() uint16 {
	header := p.GetHeader()
	return header.FreeSpace
}

// GetRecordCount 获取页面记录数量
func (p *Page) GetRecordCount() uint16 {
	header := p.GetHeader()
	return header.RecordCount
}

// IsLeafPage 检查是否为叶子页面
func (p *Page) IsLeafPage() bool {
	return p.Type == LeafPage
}

// IsInternalPage 检查是否为内部页面
func (p *Page) IsInternalPage() bool {
	return p.Type == InternalPage
}

// GetSize 获取页面大小
func (p *Page) GetSize() int {
	return PageSize
}

// GetDataSize 获取页面数据区大小
func (p *Page) GetDataSize() int {
	return PageSize - PageHeaderSize - PageTrailerSize
}

// String 返回页面的字符串表示
func (p *Page) String() string {
	header := p.GetHeader()
	return fmt.Sprintf("Page{ID:%d, Type:%d, Records:%d, FreeSpace:%d, Dirty:%v, Pinned:%d}",
		p.ID, p.Type, header.RecordCount, header.FreeSpace, p.Dirty, p.Pinned)
}
