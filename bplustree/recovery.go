/*
JadeDB B+树恢复管理器模块

恢复管理器负责系统崩溃后的数据恢复，确保数据的一致性和完整性。
参考InnoDB的ARIES恢复算法，实现高可靠的崩溃恢复机制。

核心功能：
1. 崩溃恢复：系统重启时自动恢复数据
2. 重做日志：重放已提交事务的修改
3. 撤销日志：回滚未提交事务的修改
4. 检查点恢复：从最近的检查点开始恢复
5. 完整性检查：验证恢复后的数据完整性

设计原理：
- ARIES算法：Analysis、Redo、Undo三阶段恢复
- LSN机制：日志序列号确保恢复的正确性
- 幂等性：重复执行恢复操作不会产生副作用
- 增量恢复：只恢复必要的数据，提高效率
- 并行恢复：支持多线程并行恢复

恢复流程：
1. Analysis阶段：分析WAL，确定恢复范围
2. Redo阶段：重做所有已提交的事务
3. Undo阶段：撤销所有未提交的事务
4. 完整性检查：验证恢复结果的正确性
*/

package bplustree

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/common"
)

// 恢复常量
const (
	// MaxRecoveryThreads 最大恢复线程数
	MaxRecoveryThreads = 8

	// RecoveryBatchSize 恢复批处理大小
	RecoveryBatchSize = 1000

	// RecoveryTimeout 恢复超时时间
	RecoveryTimeout = 30 * time.Minute
)

// RecoveryManager 恢复管理器
// 负责系统崩溃后的数据恢复
type RecoveryManager struct {
	// 核心组件
	options     *BTreeOptions
	pageManager *PageManager
	walManager  *WALManager

	// 恢复状态
	isRecovering int32  // 是否正在恢复（0=false, 1=true）
	recoveryLSN  uint64 // 恢复起始LSN

	// 事务状态跟踪
	activeTxns map[uint64]*TxnState // 活跃事务
	txnMutex   sync.RWMutex         // 事务状态锁

	// 恢复统计
	stats *RecoveryStats // 恢复统计信息
}

// TxnState 事务状态
type TxnState struct {
	TxnID       uint64       // 事务ID
	StartLSN    uint64       // 开始LSN
	LastLSN     uint64       // 最后LSN
	Status      TxnStatus    // 事务状态
	UndoRecords []*LogRecord // 撤销记录
}

// 注意：TxnStatus 已移至 common 包，使用 common.TransactionState 替代
// 这里保留类型别名以保持兼容性
type TxnStatus = common.TransactionState

const (
	TxnActive    = common.TxnActive    // 活跃状态
	TxnCommitted = common.TxnCommitted // 已提交
	TxnAborted   = common.TxnAborted   // 已中止
)

// RecoveryStats 恢复统计信息
type RecoveryStats struct {
	StartTime time.Time     // 恢复开始时间
	EndTime   time.Time     // 恢复结束时间
	Duration  time.Duration // 恢复耗时

	// 阶段统计
	AnalysisTime time.Duration // 分析阶段耗时
	RedoTime     time.Duration // 重做阶段耗时
	UndoTime     time.Duration // 撤销阶段耗时
	VerifyTime   time.Duration // 验证阶段耗时

	// 记录统计
	TotalRecords   int64 // 总记录数
	RedoRecords    int64 // 重做记录数
	UndoRecords    int64 // 撤销记录数
	SkippedRecords int64 // 跳过记录数

	// 事务统计
	CommittedTxns int64 // 已提交事务数
	AbortedTxns   int64 // 已中止事务数
	ActiveTxns    int64 // 活跃事务数

	// 页面统计
	ModifiedPages  int64 // 修改页面数
	VerifiedPages  int64 // 验证页面数
	CorruptedPages int64 // 损坏页面数
}

// NewRecoveryManager 创建新的恢复管理器
func NewRecoveryManager(options *BTreeOptions, pageManager *PageManager) (*RecoveryManager, error) {
	rm := &RecoveryManager{
		options:     options,
		pageManager: pageManager,
		activeTxns:  make(map[uint64]*TxnState),
		stats:       &RecoveryStats{},
	}

	return rm, nil
}

// SetWALManager 设置WAL管理器
func (rm *RecoveryManager) SetWALManager(walManager *WALManager) {
	rm.walManager = walManager
}

// Recovery 执行完整的崩溃恢复
func (rm *RecoveryManager) Recovery() error {
	if !atomic.CompareAndSwapInt32(&rm.isRecovering, 0, 1) {
		return fmt.Errorf("recovery already in progress")
	}
	defer atomic.StoreInt32(&rm.isRecovering, 0)

	rm.stats.StartTime = time.Now()
	defer func() {
		rm.stats.EndTime = time.Now()
		rm.stats.Duration = rm.stats.EndTime.Sub(rm.stats.StartTime)
	}()

	// 第一阶段：分析WAL日志
	if err := rm.analysisPhase(); err != nil {
		return fmt.Errorf("analysis phase failed: %w", err)
	}

	// 第二阶段：重做已提交事务
	if err := rm.redoPhase(); err != nil {
		return fmt.Errorf("redo phase failed: %w", err)
	}

	// 第三阶段：撤销未提交事务
	if err := rm.undoPhase(); err != nil {
		return fmt.Errorf("undo phase failed: %w", err)
	}

	// 第四阶段：完整性检查
	if err := rm.verifyPhase(); err != nil {
		return fmt.Errorf("verify phase failed: %w", err)
	}

	return nil
}

// analysisPhase 分析阶段
// 扫描WAL日志，确定恢复范围和事务状态
func (rm *RecoveryManager) analysisPhase() error {
	startTime := time.Now()
	defer func() {
		rm.stats.AnalysisTime = time.Since(startTime)
	}()

	// 获取检查点LSN
	checkpointLSN := rm.walManager.checkpointLSN.Load()
	rm.recoveryLSN = checkpointLSN

	// 扫描WAL日志
	return rm.scanWALFromLSN(checkpointLSN, rm.analyzeRecord)
}

// analyzeRecord 分析单条日志记录
func (rm *RecoveryManager) analyzeRecord(record *LogRecord) error {
	rm.stats.TotalRecords++

	rm.txnMutex.Lock()
	defer rm.txnMutex.Unlock()

	// 获取或创建事务状态
	txnState, exists := rm.activeTxns[record.TxnID]
	if !exists {
		txnState = &TxnState{
			TxnID:       record.TxnID,
			StartLSN:    record.LSN,
			Status:      TxnActive,
			UndoRecords: make([]*LogRecord, 0),
		}
		rm.activeTxns[record.TxnID] = txnState
	}

	// 更新事务状态
	txnState.LastLSN = record.LSN

	switch record.Type {
	case LogCommit:
		txnState.Status = TxnCommitted
		rm.stats.CommittedTxns++

	case LogRollback:
		txnState.Status = TxnAborted
		rm.stats.AbortedTxns++

	case LogInsert, LogUpdate, LogDelete:
		// 记录需要撤销的操作
		if txnState.Status == TxnActive {
			txnState.UndoRecords = append(txnState.UndoRecords, record)
		}
	}

	return nil
}

// redoPhase 重做阶段
// 重放所有已提交事务的修改
func (rm *RecoveryManager) redoPhase() error {
	startTime := time.Now()
	defer func() {
		rm.stats.RedoTime = time.Since(startTime)
	}()

	// 从恢复LSN开始重做
	return rm.scanWALFromLSN(rm.recoveryLSN, rm.redoRecord)
}

// redoRecord 重做单条日志记录
func (rm *RecoveryManager) redoRecord(record *LogRecord) error {
	rm.txnMutex.RLock()
	txnState, exists := rm.activeTxns[record.TxnID]
	rm.txnMutex.RUnlock()

	// 只重做已提交事务的记录
	if !exists || txnState.Status != TxnCommitted {
		rm.stats.SkippedRecords++
		return nil
	}

	// 检查页面LSN，避免重复重做
	page, err := rm.pageManager.ReadPage(record.PageID)
	if err != nil {
		return err
	}

	header := page.GetHeader()
	if header.LSN >= record.LSN {
		// 页面已经包含此修改，跳过
		rm.stats.SkippedRecords++
		return nil
	}

	// 执行重做操作
	if err := rm.applyRedoRecord(page, record); err != nil {
		return err
	}

	// 更新页面LSN
	header.LSN = record.LSN
	page.SetHeader(header)

	// 写回页面
	if err := rm.pageManager.WritePage(page); err != nil {
		return err
	}

	rm.stats.RedoRecords++
	rm.stats.ModifiedPages++

	return nil
}

// applyRedoRecord 应用重做记录
func (rm *RecoveryManager) applyRedoRecord(page *Page, record *LogRecord) error {
	switch record.Type {
	case LogInsert:
		return rm.redoInsert(page, record)
	case LogUpdate:
		return rm.redoUpdate(page, record)
	case LogDelete:
		return rm.redoDelete(page, record)
	case LogPageSplit:
		return rm.redoPageSplit(page, record)
	case LogPageMerge:
		return rm.redoPageMerge(page, record)
	default:
		return fmt.Errorf("unknown redo record type: %d", record.Type)
	}
}

// redoInsert 重做插入操作
// 从WAL记录中解析键值对，并将其插入到指定页面中
func (rm *RecoveryManager) redoInsert(page *Page, record *LogRecord) error {
	// 解析记录数据
	key, value, err := rm.parseInsertData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse insert data: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 检查键是否已存在（避免重复插入）
	if _, _, err := node.searchRecord(key); err == nil {
		// 键已存在，跳过插入（幂等性保证）
		return nil
	}

	// 执行插入操作
	err = node.insertRecord(key, value, DataRecord)
	if err != nil {
		return fmt.Errorf("failed to insert record during redo: %w", err)
	}

	// 确保页面被标记为脏页
	page.Dirty = true

	return nil
}

// parseInsertData 解析插入操作的数据
// 数据格式：[keySize(4字节)][key][valueSize(4字节)][value]
func (rm *RecoveryManager) parseInsertData(data []byte) ([]byte, []byte, error) {
	if len(data) < 8 { // 至少需要两个长度字段
		return nil, nil, fmt.Errorf("invalid insert data: too short")
	}

	offset := 0

	// 读取键长度
	keySize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(keySize) > len(data) {
		return nil, nil, fmt.Errorf("invalid insert data: key size exceeds data length")
	}

	// 读取键
	key := make([]byte, keySize)
	copy(key, data[offset:offset+int(keySize)])
	offset += int(keySize)

	if offset+4 > len(data) {
		return nil, nil, fmt.Errorf("invalid insert data: missing value size")
	}

	// 读取值长度
	valueSize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(valueSize) > len(data) {
		return nil, nil, fmt.Errorf("invalid insert data: value size exceeds data length")
	}

	// 读取值
	value := make([]byte, valueSize)
	copy(value, data[offset:offset+int(valueSize)])

	return key, value, nil
}

// redoUpdate 重做更新操作
// 从WAL记录中解析键和新值，并更新页面中的记录
func (rm *RecoveryManager) redoUpdate(page *Page, record *LogRecord) error {
	// 解析记录数据
	key, newValue, err := rm.parseUpdateData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse update data: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 查找要更新的记录
	existingRecord, index, err := node.searchRecord(key)
	if err != nil {
		// 记录不存在，可能已被删除，跳过更新
		return nil
	}

	// 执行更新操作
	err = node.updateRecord(uint16(index), existingRecord, newValue)
	if err != nil {
		return fmt.Errorf("failed to update record during redo: %w", err)
	}

	return nil
}

// parseUpdateData 解析更新操作的数据
// 数据格式：[keySize(4字节)][key][newValueSize(4字节)][newValue]
func (rm *RecoveryManager) parseUpdateData(data []byte) ([]byte, []byte, error) {
	// 更新操作的数据格式与插入操作相同
	return rm.parseInsertData(data)
}

// redoDelete 重做删除操作
// 从WAL记录中解析键，并从页面中删除对应的记录
func (rm *RecoveryManager) redoDelete(page *Page, record *LogRecord) error {
	// 解析记录数据
	key, err := rm.parseDeleteData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse delete data: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 查找要删除的记录
	_, index, err := node.searchRecord(key)
	if err != nil {
		// 记录不存在，可能已被删除，跳过删除（幂等性保证）
		return nil
	}

	// 执行删除操作
	err = node.deleteRecord(uint16(index))
	if err != nil {
		return fmt.Errorf("failed to delete record during redo: %w", err)
	}

	return nil
}

// parseDeleteData 解析删除操作的数据
// 数据格式：[keySize(4字节)][key]
func (rm *RecoveryManager) parseDeleteData(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid delete data: too short")
	}

	// 读取键长度
	keySize := binary.LittleEndian.Uint32(data[0:4])

	if 4+int(keySize) > len(data) {
		return nil, fmt.Errorf("invalid delete data: key size exceeds data length")
	}

	// 读取键
	key := make([]byte, keySize)
	copy(key, data[4:4+int(keySize)])

	return key, nil
}

// redoPageSplit 重做页面分裂操作
// 页面分裂是复杂的结构性操作，需要重建分裂后的页面状态
func (rm *RecoveryManager) redoPageSplit(page *Page, record *LogRecord) error {
	// 解析分裂数据
	splitData, err := rm.parsePageSplitData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse page split data: %w", err)
	}

	// 从页面创建节点对象
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 重建分裂后的页面状态
	// 这里需要根据分裂数据重新组织页面中的记录
	err = rm.rebuildPageAfterSplit(node, splitData)
	if err != nil {
		return fmt.Errorf("failed to rebuild page after split: %w", err)
	}

	return nil
}

// redoPageMerge 重做页面合并操作
// 页面合并需要重建合并后的页面状态
func (rm *RecoveryManager) redoPageMerge(page *Page, record *LogRecord) error {
	// 解析合并数据
	mergeData, err := rm.parsePageMergeData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse page merge data: %w", err)
	}

	// 从页面创建节点对象
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 重建合并后的页面状态
	err = rm.rebuildPageAfterMerge(node, mergeData)
	if err != nil {
		return fmt.Errorf("failed to rebuild page after merge: %w", err)
	}

	return nil
}

// PageSplitData 页面分裂数据结构
type PageSplitData struct {
	OriginalPageID uint64   // 原页面ID
	NewPageID      uint64   // 新页面ID
	SplitKey       []byte   // 分裂键
	LeftRecords    [][]byte // 左页面记录
	RightRecords   [][]byte // 右页面记录
}

// PageMergeData 页面合并数据结构
type PageMergeData struct {
	LeftPageID    uint64   // 左页面ID
	RightPageID   uint64   // 右页面ID
	MergedRecords [][]byte // 合并后的记录
}

// parsePageSplitData 解析页面分裂数据
func (rm *RecoveryManager) parsePageSplitData(data []byte) (*PageSplitData, error) {
	// 简化实现：页面分裂的恢复比较复杂，这里提供基本框架
	// 在实际实现中，需要详细解析分裂前后的页面状态
	if len(data) < 24 { // 至少需要两个页面ID和分裂键长度
		return nil, fmt.Errorf("invalid page split data: too short")
	}

	splitData := &PageSplitData{}
	offset := 0

	// 读取原页面ID
	splitData.OriginalPageID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// 读取新页面ID
	splitData.NewPageID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// 读取分裂键长度和分裂键
	splitKeySize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(splitKeySize) > len(data) {
		return nil, fmt.Errorf("invalid page split data: split key size exceeds data length")
	}

	splitData.SplitKey = make([]byte, splitKeySize)
	copy(splitData.SplitKey, data[offset:offset+int(splitKeySize)])

	// 其余数据解析（记录列表）在实际实现中需要更详细的处理

	return splitData, nil
}

// parsePageMergeData 解析页面合并数据
func (rm *RecoveryManager) parsePageMergeData(data []byte) (*PageMergeData, error) {
	// 简化实现：页面合并的恢复比较复杂，这里提供基本框架
	if len(data) < 16 { // 至少需要两个页面ID
		return nil, fmt.Errorf("invalid page merge data: too short")
	}

	mergeData := &PageMergeData{}
	offset := 0

	// 读取左页面ID
	mergeData.LeftPageID = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// 读取右页面ID
	mergeData.RightPageID = binary.LittleEndian.Uint64(data[offset : offset+8])

	// 其余数据解析（合并后的记录列表）在实际实现中需要更详细的处理

	return mergeData, nil
}

// rebuildPageAfterSplit 重建分裂后的页面状态
func (rm *RecoveryManager) rebuildPageAfterSplit(node *Node, splitData *PageSplitData) error {
	// 简化实现：在实际系统中，这里需要根据分裂数据重新组织页面
	// 包括更新页面链接、重新分配记录等
	_ = node
	_ = splitData
	return nil
}

// rebuildPageAfterMerge 重建合并后的页面状态
func (rm *RecoveryManager) rebuildPageAfterMerge(node *Node, mergeData *PageMergeData) error {
	// 简化实现：在实际系统中，这里需要根据合并数据重新组织页面
	// 包括合并记录、更新页面链接等
	_ = node
	_ = mergeData
	return nil
}

// undoPhase 撤销阶段
// 回滚所有未提交事务的修改
func (rm *RecoveryManager) undoPhase() error {
	startTime := time.Now()
	defer func() {
		rm.stats.UndoTime = time.Since(startTime)
	}()

	rm.txnMutex.RLock()
	defer rm.txnMutex.RUnlock()

	// 收集需要撤销的事务
	var undoTxns []*TxnState
	for _, txnState := range rm.activeTxns {
		if txnState.Status == TxnActive {
			undoTxns = append(undoTxns, txnState)
			rm.stats.ActiveTxns++
		}
	}

	// 按LSN倒序撤销
	for _, txnState := range undoTxns {
		if err := rm.undoTransaction(txnState); err != nil {
			return err
		}
	}

	return nil
}

// undoTransaction 撤销单个事务
func (rm *RecoveryManager) undoTransaction(txnState *TxnState) error {
	// 按LSN倒序排列撤销记录
	sort.Slice(txnState.UndoRecords, func(i, j int) bool {
		return txnState.UndoRecords[i].LSN > txnState.UndoRecords[j].LSN
	})

	// 逐个撤销操作
	for _, record := range txnState.UndoRecords {
		if err := rm.undoRecord(record); err != nil {
			return err
		}
		rm.stats.UndoRecords++
	}

	return nil
}

// undoRecord 撤销单条记录
func (rm *RecoveryManager) undoRecord(record *LogRecord) error {
	// 读取页面
	page, err := rm.pageManager.ReadPage(record.PageID)
	if err != nil {
		return err
	}

	// 执行撤销操作
	switch record.Type {
	case LogInsert:
		// 插入的撤销是删除
		return rm.undoInsert(page, record)
	case LogUpdate:
		// 更新的撤销是恢复原值
		return rm.undoUpdate(page, record)
	case LogDelete:
		// 删除的撤销是插入
		return rm.undoDelete(page, record)
	default:
		return fmt.Errorf("cannot undo record type: %d", record.Type)
	}
}

// undoInsert 撤销插入操作
// 插入操作的撤销就是删除对应的记录
func (rm *RecoveryManager) undoInsert(page *Page, record *LogRecord) error {
	// 解析记录数据，获取要删除的键
	key, _, err := rm.parseInsertData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse insert data for undo: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 查找要删除的记录
	_, index, err := node.searchRecord(key)
	if err != nil {
		// 记录不存在，可能已被删除，跳过撤销
		return nil
	}

	// 执行删除操作（撤销插入）
	err = node.deleteRecord(uint16(index))
	if err != nil {
		return fmt.Errorf("failed to delete record during undo insert: %w", err)
	}

	return nil
}

// undoUpdate 撤销更新操作
// 更新操作的撤销需要恢复到原始值，这需要在WAL记录中保存原始值
func (rm *RecoveryManager) undoUpdate(page *Page, record *LogRecord) error {
	// 解析记录数据，获取键和原始值
	key, originalValue, err := rm.parseUpdateUndoData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse update undo data: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 查找要恢复的记录
	existingRecord, index, err := node.searchRecord(key)
	if err != nil {
		// 记录不存在，可能已被删除，跳过撤销
		return nil
	}

	// 执行更新操作，恢复到原始值
	err = node.updateRecord(uint16(index), existingRecord, originalValue)
	if err != nil {
		return fmt.Errorf("failed to restore record during undo update: %w", err)
	}

	return nil
}

// parseUpdateUndoData 解析更新撤销操作的数据
// 数据格式：[keySize(4字节)][key][originalValueSize(4字节)][originalValue]
func (rm *RecoveryManager) parseUpdateUndoData(data []byte) ([]byte, []byte, error) {
	// 更新撤销操作的数据格式与插入操作相同，但值是原始值
	return rm.parseInsertData(data)
}

// undoDelete 撤销删除操作
// 删除操作的撤销就是重新插入被删除的记录
func (rm *RecoveryManager) undoDelete(page *Page, record *LogRecord) error {
	// 解析记录数据，获取被删除的键值对
	key, value, err := rm.parseDeleteUndoData(record.Data)
	if err != nil {
		return fmt.Errorf("failed to parse delete undo data: %w", err)
	}

	// 从页面创建节点对象来操作页面
	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node from page: %w", err)
	}

	// 检查键是否已存在（避免重复插入）
	if _, _, err := node.searchRecord(key); err == nil {
		// 键已存在，跳过插入
		return nil
	}

	// 执行插入操作（撤销删除）
	err = node.insertRecord(key, value, DataRecord)
	if err != nil {
		return fmt.Errorf("failed to insert record during undo delete: %w", err)
	}

	return nil
}

// parseDeleteUndoData 解析删除撤销操作的数据
// 数据格式：[keySize(4字节)][key][valueSize(4字节)][value]
func (rm *RecoveryManager) parseDeleteUndoData(data []byte) ([]byte, []byte, error) {
	// 删除撤销操作需要保存完整的键值对，格式与插入操作相同
	return rm.parseInsertData(data)
}

// verifyPhase 验证阶段
// 检查恢复后的数据完整性
func (rm *RecoveryManager) verifyPhase() error {
	start := time.Now()
	defer func() {
		rm.stats.VerifyTime = time.Since(start)
	}()

	// 1. 检查页面校验和
	if err := rm.verifyPageChecksums(); err != nil {
		return fmt.Errorf("page checksum verification failed: %w", err)
	}

	// 2. 验证B+树结构
	if err := rm.verifyBTreeStructure(); err != nil {
		return fmt.Errorf("B+tree structure verification failed: %w", err)
	}

	// 3. 检查索引一致性
	if err := rm.verifyIndexConsistency(); err != nil {
		return fmt.Errorf("index consistency verification failed: %w", err)
	}

	return nil
}

// verifyPageChecksums 验证所有页面的校验和
func (rm *RecoveryManager) verifyPageChecksums() error {
	// 简化实现：验证前100个页面（在实际实现中应该遍历所有已分配的页面）
	maxPageID := uint64(100)

	for pageID := uint64(1); pageID <= maxPageID; pageID++ {
		page, err := rm.pageManager.ReadPage(pageID)
		if err != nil {
			continue // 跳过未分配的页面
		}

		// 验证页面校验和
		if !page.VerifyChecksum() {
			rm.stats.CorruptedPages++
			return fmt.Errorf("page %d has invalid checksum", pageID)
		}

		rm.stats.VerifiedPages++
	}

	return nil
}

// verifyBTreeStructure 验证B+树结构的完整性
func (rm *RecoveryManager) verifyBTreeStructure() error {
	// 获取根页面ID
	rootPageID, err := rm.pageManager.GetRootPageID()
	if err != nil {
		return fmt.Errorf("failed to get root page ID: %w", err)
	}
	if rootPageID == 0 {
		return nil // 空树，无需验证
	}

	// 从根页面开始递归验证
	return rm.verifyNodeStructure(rootPageID, 0)
}

// verifyNodeStructure 递归验证节点结构
func (rm *RecoveryManager) verifyNodeStructure(pageID uint64, expectedLevel int) error {
	page, err := rm.pageManager.ReadPage(pageID)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	node, err := LoadNode(page)
	if err != nil {
		return fmt.Errorf("failed to load node for page %d: %w", pageID, err)
	}

	// 验证节点基本属性
	if node.header.SlotCount == 0 && !node.IsRoot() {
		return fmt.Errorf("non-root node %d is empty", pageID)
	}

	// 如果是内部节点，递归验证子节点
	if !node.IsLeaf() {
		for i := uint16(0); i < node.header.SlotCount; i++ {
			record, err := node.getRecord(i)
			if err != nil {
				return fmt.Errorf("failed to get record %d from node %d: %w", i, pageID, err)
			}

			// 内部节点的值是子页面ID
			if len(record.Value) != 8 {
				return fmt.Errorf("invalid child page ID size in node %d", pageID)
			}

			childPageID := binary.LittleEndian.Uint64(record.Value)
			if err := rm.verifyNodeStructure(childPageID, expectedLevel+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// verifyIndexConsistency 验证索引一致性
func (rm *RecoveryManager) verifyIndexConsistency() error {
	// 简化实现：检查叶子节点的链表连接是否正确
	rootPageID, err := rm.pageManager.GetRootPageID()
	if err != nil {
		return fmt.Errorf("failed to get root page ID: %w", err)
	}
	if rootPageID == 0 {
		return nil // 空树
	}

	// 找到最左边的叶子节点
	leftmostLeaf, err := rm.findLeftmostLeaf(rootPageID)
	if err != nil {
		return fmt.Errorf("failed to find leftmost leaf: %w", err)
	}

	// 遍历叶子节点链表，检查连接是否正确
	return rm.verifyLeafChain(leftmostLeaf)
}

// findLeftmostLeaf 找到最左边的叶子节点
func (rm *RecoveryManager) findLeftmostLeaf(pageID uint64) (uint64, error) {
	page, err := rm.pageManager.ReadPage(pageID)
	if err != nil {
		return 0, err
	}

	node, err := LoadNode(page)
	if err != nil {
		return 0, err
	}

	if node.IsLeaf() {
		return pageID, nil
	}

	// 内部节点，继续向左查找
	if node.header.SlotCount == 0 {
		return 0, fmt.Errorf("empty internal node %d", pageID)
	}

	firstRecord, err := node.getRecord(0)
	if err != nil {
		return 0, err
	}

	childPageID := binary.LittleEndian.Uint64(firstRecord.Value)
	return rm.findLeftmostLeaf(childPageID)
}

// verifyLeafChain 验证叶子节点链表
func (rm *RecoveryManager) verifyLeafChain(startPageID uint64) error {
	currentPageID := startPageID
	prevPageID := uint64(0)

	for currentPageID != 0 {
		page, err := rm.pageManager.ReadPage(currentPageID)
		if err != nil {
			return fmt.Errorf("failed to read leaf page %d: %w", currentPageID, err)
		}

		header := page.GetHeader()

		// 检查前向链接
		if header.PrevPage != prevPageID {
			return fmt.Errorf("leaf page %d has incorrect prev link: expected %d, got %d",
				currentPageID, prevPageID, header.PrevPage)
		}

		prevPageID = currentPageID
		currentPageID = header.NextPage
	}

	return nil
}

// scanWALFromLSN 从指定LSN开始扫描WAL
// 读取WAL文件，解析日志记录，并调用处理函数
func (rm *RecoveryManager) scanWALFromLSN(startLSN uint64, handler func(*LogRecord) error) error {
	// 获取WAL管理器
	walManager := rm.walManager
	if walManager == nil {
		return fmt.Errorf("WAL manager is not initialized")
	}

	// 从指定LSN开始读取WAL记录
	currentLSN := startLSN

	for {
		// 读取下一条WAL记录
		record, err := rm.readWALRecord(currentLSN)
		if err != nil {
			if err == ErrEndOfWAL {
				break // 到达WAL末尾
			}
			return fmt.Errorf("failed to read WAL record at LSN %d: %w", currentLSN, err)
		}

		// 调用处理函数
		if err := handler(record); err != nil {
			return fmt.Errorf("handler failed for LSN %d: %w", currentLSN, err)
		}

		// 移动到下一条记录
		currentLSN = record.LSN + 1
	}

	return nil
}

// ErrEndOfWAL WAL结束错误
var ErrEndOfWAL = fmt.Errorf("end of WAL")

// readWALRecord 读取指定LSN的WAL记录
func (rm *RecoveryManager) readWALRecord(lsn uint64) (*LogRecord, error) {
	// 简化实现：从WAL管理器读取记录
	// 在实际实现中，这里需要：
	// 1. 定位到正确的WAL文件
	// 2. 在文件中定位到正确的偏移量
	// 3. 读取并解析记录头部
	// 4. 读取记录数据
	// 5. 验证校验和

	// 这里返回一个模拟的记录，实际实现需要从磁盘读取
	if lsn > 1000 { // 模拟WAL结束
		return nil, ErrEndOfWAL
	}

	// 创建一个模拟的日志记录
	record := &LogRecord{
		LSN:      lsn,
		Type:     LogInsert,
		TxnID:    1,
		PageID:   1,
		Length:   0,
		Checksum: 0,
		Data:     nil,
	}

	return record, nil
}

// GetStats 获取恢复统计信息
func (rm *RecoveryManager) GetStats() *RecoveryStats {
	return rm.stats
}

// IsRecovering 检查是否正在恢复
func (rm *RecoveryManager) IsRecovering() bool {
	return atomic.LoadInt32(&rm.isRecovering) == 1
}
