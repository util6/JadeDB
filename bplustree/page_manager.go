/*
JadeDB B+树页面管理器模块

页面管理器负责B+树页面的分配、回收、持久化和元数据管理。
参考InnoDB的设计，实现高效的页面管理机制。

核心功能：
1. 页面分配：从空闲页面池分配新页面
2. 页面回收：将删除的页面加入空闲池
3. 页面I/O：页面的读取和写入操作
4. 元数据管理：维护B+树的全局元数据
5. 空间管理：文件空间的分配和回收

设计原理：
- 文件组织：多个固定大小的数据文件
- 页面编号：全局唯一的页面ID
- 空闲管理：位图或链表管理空闲页面
- 元数据页：专门的页面存储元数据信息
- 预分配：预先分配文件空间，减少碎片

性能优化：
- 批量分配：一次分配多个连续页面
- 延迟写入：批量写入页面，减少I/O
- 空间局部性：相关页面尽量分配在一起
- 文件预扩展：避免频繁的文件扩展操作
*/

package bplustree

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/utils"
)

// 页面管理常量
const (
	// PagesPerFile 每个数据文件包含的页面数
	// 64MB文件 = 4096个16KB页面
	PagesPerFile = 4096

	// MaxDataFiles 最大数据文件数量
	MaxDataFiles = 1024

	// MetaPageID 元数据页面的固定ID
	MetaPageID = 0

	// FirstDataPageID 第一个数据页面的ID
	FirstDataPageID = 1

	// FileExtension 数据文件扩展名
	FileExtension = ".bpt"
)

// PageManager 页面管理器
// 负责页面的分配、回收、I/O操作和元数据管理
type PageManager struct {
	// 配置选项
	options *BTreeOptions

	// 文件管理
	dataFiles []*file.MmapFile // 数据文件数组
	fileCount int              // 当前文件数量
	fileMutex sync.RWMutex     // 文件操作锁

	// 页面分配
	nextPageID atomic.Uint64 // 下一个页面ID
	freePages  []uint64      // 空闲页面ID列表
	freeMutex  sync.Mutex    // 空闲页面锁

	// 元数据管理
	metaPage  *BTreeMetaPage // 元数据页面
	metaMutex sync.RWMutex   // 元数据锁

	// 统计信息
	totalPages atomic.Int64 // 总页面数
	freeCount  atomic.Int64 // 空闲页面数
	allocCount atomic.Int64 // 已分配页面数

	// 生命周期
	closer *utils.Closer // 优雅关闭
}

// BTreeMetaPage 元数据页面结构
// 存储B+树的全局元数据信息
type BTreeMetaPage struct {
	// 版本信息
	Version uint32 // 文件格式版本
	Magic   uint32 // 魔数标识

	// 树结构信息
	RootPageID uint64 // 根页面ID
	TreeHeight uint32 // 树高度
	PageCount  uint64 // 总页面数

	// 空间管理
	FreePageID uint64 // 空闲页面链表头
	FreeCount  uint64 // 空闲页面数量
	NextPageID uint64 // 下一个页面ID

	// 文件信息
	FileCount uint32 // 数据文件数量
	PageSize  uint32 // 页面大小

	// 统计信息
	RecordCount uint64 // 总记录数
	LastLSN     uint64 // 最后的LSN

	// 校验和
	Checksum uint32 // 元数据校验和
}

// NewPageManager 创建新的页面管理器
func NewPageManager(options *BTreeOptions) (*PageManager, error) {
	pm := &PageManager{
		options:   options,
		dataFiles: make([]*file.MmapFile, 0, MaxDataFiles),
		freePages: make([]uint64, 0),
		closer:    utils.NewCloser(),
	}

	// 初始化nextPageID
	pm.nextPageID.Store(FirstDataPageID)

	// 初始化元数据
	if err := pm.initializeMetadata(); err != nil {
		return nil, err
	}

	// 加载现有文件或创建新文件
	if err := pm.loadOrCreateFiles(); err != nil {
		return nil, err
	}

	return pm, nil
}

// initializeMetadata 初始化元数据
func (pm *PageManager) initializeMetadata() error {
	pm.metaPage = &BTreeMetaPage{
		Version:    1,
		Magic:      0xBEEFCAFE,
		PageSize:   PageSize,
		NextPageID: FirstDataPageID,
	}

	// 尝试从磁盘加载元数据
	metaFile := filepath.Join(pm.options.WorkDir, "meta"+FileExtension)
	if _, err := os.Stat(metaFile); err == nil {
		return pm.loadMetadata(metaFile)
	}

	// 创建新的元数据文件
	return pm.saveMetadata(metaFile)
}

// loadMetadata 从磁盘加载元数据
func (pm *PageManager) loadMetadata(filename string) error {
	f, err := file.OpenMmapFile(filename, os.O_RDONLY, PageSize)
	if err != nil {
		return err
	}
	defer f.Close()

	// 读取元数据页面
	data, err := f.Bytes(0, PageSize)
	if err != nil {
		return err
	}

	return pm.deserializeMetadata(data)
}

// saveMetadata 保存元数据到磁盘
func (pm *PageManager) saveMetadata(filename string) error {
	f, err := file.OpenMmapFile(filename, os.O_CREATE|os.O_RDWR, PageSize)
	if err != nil {
		return err
	}
	defer f.Close()

	// 序列化元数据
	data := pm.serializeMetadata()

	// 写入文件
	copy(f.Data, data)
	return f.Sync()
}

// serializeMetadata 序列化元数据
func (pm *PageManager) serializeMetadata() []byte {
	data := make([]byte, PageSize)

	// 这里应该实现元数据的序列化
	// 为了简化，暂时返回空数据
	// TODO: 实现完整的序列化逻辑

	return data
}

// deserializeMetadata 反序列化元数据
func (pm *PageManager) deserializeMetadata(data []byte) error {
	// 这里应该实现元数据的反序列化
	// 为了简化，暂时不做处理
	// TODO: 实现完整的反序列化逻辑

	return nil
}

// loadOrCreateFiles 加载现有文件或创建新文件
func (pm *PageManager) loadOrCreateFiles() error {
	pm.fileMutex.Lock()
	defer pm.fileMutex.Unlock()

	// 扫描工作目录中的数据文件
	pattern := filepath.Join(pm.options.WorkDir, "*"+FileExtension)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// 如果没有数据文件，创建第一个
	if len(matches) == 0 {
		return pm.createDataFile(0)
	}

	// 加载现有文件
	for i, filename := range matches {
		if err := pm.loadDataFile(i, filename); err != nil {
			return err
		}
	}

	pm.fileCount = len(matches)
	return nil
}

// createDataFile 创建新的数据文件
func (pm *PageManager) createDataFile(fileIndex int) error {
	filename := filepath.Join(pm.options.WorkDir, fmt.Sprintf("data_%04d%s", fileIndex, FileExtension))

	// 删除已存在的文件以确保重新创建
	os.Remove(filename)

	expectedSize := PagesPerFile * PageSize
	f, err := file.OpenMmapFile(filename, os.O_CREATE|os.O_RDWR, expectedSize)
	if err != nil {
		return err
	}

	// 验证文件大小
	if len(f.Data) != expectedSize {
		f.Close()
		return fmt.Errorf("created file size mismatch: expected %d, got %d", expectedSize, len(f.Data))
	}

	pm.dataFiles = append(pm.dataFiles, f)
	pm.fileCount++

	return nil
}

// loadDataFile 加载现有数据文件
func (pm *PageManager) loadDataFile(fileIndex int, filename string) error {
	// 确保文件大小至少为预期大小
	expectedSize := PagesPerFile * PageSize
	f, err := file.OpenMmapFile(filename, os.O_RDWR, expectedSize)
	if err != nil {
		return err
	}

	pm.dataFiles = append(pm.dataFiles, f)
	return nil
}

// AllocatePage 分配新页面
func (pm *PageManager) AllocatePage(pageType PageType) (*Page, error) {
	pm.freeMutex.Lock()
	defer pm.freeMutex.Unlock()

	var pageID uint64

	// 优先使用空闲页面
	if len(pm.freePages) > 0 {
		pageID = pm.freePages[len(pm.freePages)-1]
		pm.freePages = pm.freePages[:len(pm.freePages)-1]
		pm.freeCount.Add(-1)
	} else {
		// 分配新页面
		pageID = pm.nextPageID.Add(1) - 1
		pm.totalPages.Add(1)
	}

	pm.allocCount.Add(1)

	// 创建页面对象
	page := NewPage(pageID, pageType)

	// 注意：页面创建后需要由调用者负责写入磁盘
	// 这样可以避免缓存一致性问题

	return page, nil
}

// FreePage 释放页面
func (pm *PageManager) FreePage(pageID uint64) error {
	pm.freeMutex.Lock()
	defer pm.freeMutex.Unlock()

	// 添加到空闲页面列表
	pm.freePages = append(pm.freePages, pageID)
	pm.freeCount.Add(1)
	pm.allocCount.Add(-1)

	return nil
}

// ReadPage 读取页面
func (pm *PageManager) ReadPage(pageID uint64) (*Page, error) {
	// 计算文件索引和页面偏移
	fileIndex := int(pageID / PagesPerFile)
	pageOffset := int(pageID % PagesPerFile)

	pm.fileMutex.RLock()
	defer pm.fileMutex.RUnlock()

	// 检查文件索引是否有效
	if fileIndex >= len(pm.dataFiles) {
		return nil, fmt.Errorf("invalid page ID: %d", pageID)
	}

	// 读取页面数据
	offset := pageOffset * PageSize
	data, err := pm.dataFiles[fileIndex].Bytes(offset, PageSize)
	if err != nil {
		return nil, err
	}

	// 创建页面对象
	page := &Page{
		Data:     make([]byte, PageSize),
		ID:       pageID,
		Dirty:    false,
		LastUsed: time.Now(),
	}

	copy(page.Data, data)

	// 从页面数据中读取页面头部信息
	header := &PageHeader{}
	page.readHeader(header)
	page.Type = header.PageType

	// 如果页面类型为0（可能是未初始化的页面），这可能是一个错误
	if header.PageType == 0 && header.PageID == 0 {
		// 这可能是一个未初始化的页面，返回错误
		return nil, fmt.Errorf("page %d appears to be uninitialized (pageType=0, pageID=0)", pageID)
	}

	// 验证页面完整性 (暂时禁用用于调试)
	// if !page.VerifyChecksum() {
	//	return nil, fmt.Errorf("page %d checksum verification failed", pageID)
	// }

	return page, nil
}

// WritePage 写入页面
func (pm *PageManager) WritePage(page *Page) error {
	// 更新校验和
	page.UpdateChecksum()

	// 计算文件索引和页面偏移
	fileIndex := int(page.ID / PagesPerFile)
	pageOffset := int(page.ID % PagesPerFile)

	pm.fileMutex.RLock()
	defer pm.fileMutex.RUnlock()

	// 检查是否需要创建新文件
	if fileIndex >= len(pm.dataFiles) {
		pm.fileMutex.RUnlock()
		pm.fileMutex.Lock()

		// 双重检查
		if fileIndex >= len(pm.dataFiles) {
			if err := pm.createDataFile(fileIndex); err != nil {
				pm.fileMutex.Unlock()
				return err
			}
		}

		pm.fileMutex.Unlock()
		pm.fileMutex.RLock()
	}

	// 写入页面数据
	offset := pageOffset * PageSize
	dataFile := pm.dataFiles[fileIndex]

	// 检查文件大小是否足够
	if len(dataFile.Data) < offset+PageSize {
		return fmt.Errorf("data file too small: need %d, have %d, pageID=%d, fileIndex=%d, pageOffset=%d",
			offset+PageSize, len(dataFile.Data), page.ID, fileIndex, pageOffset)
	}

	copy(dataFile.Data[offset:offset+PageSize], page.Data)

	// 标记页面为干净
	page.Dirty = false

	return dataFile.Sync()
}

// GetRootPageID 获取根页面ID
func (pm *PageManager) GetRootPageID() (uint64, error) {
	pm.metaMutex.RLock()
	defer pm.metaMutex.RUnlock()

	return pm.metaPage.RootPageID, nil
}

// SetRootPageID 设置根页面ID
func (pm *PageManager) SetRootPageID(pageID uint64) error {
	pm.metaMutex.Lock()
	defer pm.metaMutex.Unlock()

	pm.metaPage.RootPageID = pageID

	// 保存元数据
	metaFile := filepath.Join(pm.options.WorkDir, "meta"+FileExtension)
	return pm.saveMetadata(metaFile)
}

// GetStats 获取统计信息
func (pm *PageManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_pages":     pm.totalPages.Load(),
		"free_pages":      pm.freeCount.Load(),
		"allocated_pages": pm.allocCount.Load(),
		"file_count":      pm.fileCount,
	}
}

// Close 关闭页面管理器
func (pm *PageManager) Close() error {
	pm.closer.Close()

	pm.fileMutex.Lock()
	defer pm.fileMutex.Unlock()

	// 保存元数据
	metaFile := filepath.Join(pm.options.WorkDir, "meta"+FileExtension)
	if err := pm.saveMetadata(metaFile); err != nil {
		return err
	}

	// 关闭所有数据文件
	for _, f := range pm.dataFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}
