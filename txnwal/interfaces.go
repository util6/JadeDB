/*
JadeDB 事务WAL（Transaction Write-Ahead Log）接口定义

本模块定义了事务级WAL系统的核心接口，专门负责事务的持久性、一致性和可恢复性。
与存储引擎内部的页面级WAL不同，事务WAL关注事务的ACID特性保证。

设计原则：
1. 接口分离：将WAL的不同职责分离到不同接口
2. 层次清晰：区分存储级WAL和事务级WAL
3. 可扩展性：支持不同的WAL实现策略
4. 高性能：支持批量操作和异步写入

核心接口：
- WALWriter: WAL写入接口
- WALReader: WAL读取接口
- TxnWALManager: WAL管理接口
- RecoveryManager: 恢复管理接口
*/

package txnwal

import (
	"context"
	"fmt"
	"time"
)

// TxnLSN 事务WAL日志序列号
type TxnLSN uint64

// TxnLogRecordType 事务日志记录类型
type TxnLogRecordType uint8

const (
	// 事务级日志类型
	LogTransactionBegin TxnLogRecordType = iota
	LogTransactionCommit
	LogTransactionRollback
	LogTransactionSavepoint

	// 存储级日志类型
	LogDataInsert
	LogDataUpdate
	LogDataDelete
	LogPageSplit
	LogPageMerge
	LogPageAllocation
	LogPageDeallocation

	// 系统级日志类型
	LogCheckpoint
	LogSystemShutdown
	LogSystemStartup
)

// TxnLogRecord WAL日志记录
type TxnLogRecord struct {
	LSN       TxnLSN           // 日志序列号
	Type      TxnLogRecordType // 日志类型
	TxnID     string           // 事务ID（如果适用）
	Timestamp time.Time        // 时间戳
	Data      []byte           // 日志数据
	Checksum  uint32           // 校验和
}

// TxnWALWriter 事务WAL写入接口
type TxnWALWriter interface {
	// WriteRecord 写入单条日志记录
	WriteRecord(ctx context.Context, record *TxnLogRecord) (TxnLSN, error)

	// WriteBatch 批量写入日志记录
	WriteBatch(ctx context.Context, records []*TxnLogRecord) ([]TxnLSN, error)

	// Flush 强制刷新缓冲区到持久存储
	Flush(ctx context.Context) error

	// Sync 同步到磁盘
	Sync(ctx context.Context) error

	// Close 关闭写入器
	Close() error
}

// TxnWALReader 事务WAL读取接口
type TxnWALReader interface {
	// ReadRecord 读取指定LSN的日志记录
	ReadRecord(ctx context.Context, lsn TxnLSN) (*TxnLogRecord, error)

	// ReadRange 读取指定范围的日志记录
	ReadRange(ctx context.Context, startLSN, endLSN TxnLSN) ([]*TxnLogRecord, error)

	// ReadFrom 从指定LSN开始读取
	ReadFrom(ctx context.Context, startLSN TxnLSN) (TxnWALIterator, error)

	// GetLatestLSN 获取最新的LSN
	GetLatestLSN() TxnLSN

	// Close 关闭读取器
	Close() error
}

// TxnWALIterator WAL迭代器接口
type TxnWALIterator interface {
	// Next 移动到下一条记录
	Next() bool

	// Record 获取当前记录
	Record() *TxnLogRecord

	// Error 获取迭代过程中的错误
	Error() error

	// Close 关闭迭代器
	Close() error
}

// TxnWALManager 事务WAL管理接口
type TxnWALManager interface {
	TxnWALWriter
	TxnWALReader

	// CreateCheckpoint 创建检查点
	CreateCheckpoint(ctx context.Context) (TxnLSN, error)

	// Truncate 截断WAL到指定LSN
	Truncate(ctx context.Context, lsn TxnLSN) error

	// GetStatistics 获取WAL统计信息
	GetStatistics() TxnWALStatistics

	// SetOptions 设置WAL选项
	SetOptions(options *TxnWALOptions) error
}

// RecoveryManager 恢复管理接口
type RecoveryManager interface {
	// Recovery 执行崩溃恢复
	Recovery(ctx context.Context) error

	// AnalysisPhase 分析阶段：确定恢复范围
	AnalysisPhase(ctx context.Context) (*RecoveryInfo, error)

	// RedoPhase 重做阶段：重放已提交事务
	RedoPhase(ctx context.Context, info *RecoveryInfo) error

	// UndoPhase 撤销阶段：回滚未提交事务
	UndoPhase(ctx context.Context, info *RecoveryInfo) error

	// VerifyIntegrity 验证数据完整性
	VerifyIntegrity(ctx context.Context) error
}

// TxnWALOptions WAL配置选项
type TxnWALOptions struct {
	// 文件配置
	Directory string // WAL文件目录
	FileSize  int64  // 单个WAL文件大小
	MaxFiles  int    // 最大文件数量

	// 性能配置
	BufferSize    int           // 写入缓冲区大小
	FlushInterval time.Duration // 自动刷新间隔
	SyncMode      SyncMode      // 同步模式

	// 压缩配置
	EnableCompression bool // 是否启用压缩
	CompressionLevel  int  // 压缩级别

	// 校验配置
	EnableChecksum    bool   // 是否启用校验和
	ChecksumAlgorithm string // 校验和算法
}

// SyncMode 同步模式
type SyncMode int

const (
	SyncNone   SyncMode = iota // 不同步
	SyncNormal                 // 正常同步
	SyncForce                  // 强制同步
)

// TxnWALStatistics WAL统计信息
type TxnWALStatistics struct {
	TotalRecords      int64         // 总记录数
	TotalBytes        int64         // 总字节数
	WriteRate         float64       // 写入速率（记录/秒）
	FlushCount        int64         // 刷新次数
	SyncCount         int64         // 同步次数
	AverageLatency    time.Duration // 平均延迟
	BufferUtilization float64       // 缓冲区利用率
}

// RecoveryInfo 恢复信息
type RecoveryInfo struct {
	StartLSN      TxnLSN            // 恢复起始LSN
	EndLSN        TxnLSN            // 恢复结束LSN
	CheckpointLSN TxnLSN            // 检查点LSN
	ActiveTxns    map[string]TxnLSN // 活跃事务
	CommittedTxns map[string]TxnLSN // 已提交事务
	AbortedTxns   map[string]TxnLSN // 已中止事务
}

// WALFactory WAL工厂接口
type WALFactory interface {
	// CreateTxnWALManager 创建WAL管理器
	CreateTxnWALManager(options *TxnWALOptions) (TxnWALManager, error)

	// CreateRecoveryManager 创建恢复管理器
	CreateRecoveryManager(walManager TxnWALManager, options *TxnWALOptions) (RecoveryManager, error)

	// GetSupportedTypes 获取支持的WAL类型
	GetSupportedTypes() []string
}

// WALError WAL错误类型
type WALError struct {
	Code    int
	Message string
	Cause   error
}

func (e *WALError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("WAL error [%d]: %s, caused by: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("WAL error [%d]: %s", e.Code, e.Message)
}

func (e *WALError) Unwrap() error {
	return e.Cause
}

// WAL错误代码常量
const (
	ErrWALNotFound       = 2001
	ErrWALCorrupted      = 2002
	ErrWALFull           = 2003
	ErrWALReadOnly       = 2004
	ErrWALInvalidLSN     = 2005
	ErrWALChecksumFailed = 2006
	ErrWALRecoveryFailed = 2007
)

// 工具函数

// NewWALError 创建WAL错误
func NewWALError(code int, message string, cause error) *WALError {
	return &WALError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// IsWALError 检查是否为WAL错误
func IsWALError(err error) bool {
	_, ok := err.(*WALError)
	return ok
}

// GetWALErrorCode 获取WAL错误代码
func GetWALErrorCode(err error) int {
	if walErr, ok := err.(*WALError); ok {
		return walErr.Code
	}
	return 0
}
