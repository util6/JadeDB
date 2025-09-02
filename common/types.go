/*
JadeDB 公共类型定义

本模块定义了JadeDB项目中各个包共享的基础类型和常量。
通过提取公共类型，避免包之间的循环依赖，建立清晰的依赖关系。

设计原则：
1. 只包含基础类型定义，不包含业务逻辑
2. 避免依赖其他业务包
3. 为所有包提供共享的基础类型
4. 保持类型定义的稳定性

包含的类型：
- 锁类型定义
- 事务状态定义
- 基础数据结构
- 常量定义
*/

package common

import (
	"time"
)

// TxnLockType 事务级锁类型（用于事务并发控制）
type TxnLockType int

const (
	// 基于操作的锁类型
	TxnLockPut    TxnLockType = iota // 写锁（PUT操作）
	TxnLockDelete                    // 删除锁（DELETE操作）
	TxnLockRead                      // 读锁（SELECT操作）

	// 基于共享性的锁类型
	TxnLockShared             // 共享锁
	TxnLockExclusive          // 排他锁
	TxnLockIntentionShared    // 意向共享锁
	TxnLockIntentionExclusive // 意向排他锁
)

// LockType 保持向后兼容性的类型别名
type LockType = TxnLockType

const (
	// 向后兼容的常量别名
	LockTypePut            = TxnLockPut
	LockTypeDelete         = TxnLockDelete
	LockTypeRead           = TxnLockRead
	SharedLock             = TxnLockShared
	ExclusiveLock          = TxnLockExclusive
	IntentionSharedLock    = TxnLockIntentionShared
	IntentionExclusiveLock = TxnLockIntentionExclusive
)

func (lt TxnLockType) String() string {
	switch lt {
	case TxnLockPut:
		return "TXN_PUT"
	case TxnLockDelete:
		return "TXN_DELETE"
	case TxnLockRead:
		return "TXN_READ"
	case TxnLockShared:
		return "TXN_SHARED"
	case TxnLockExclusive:
		return "TXN_EXCLUSIVE"
	case TxnLockIntentionShared:
		return "TXN_INTENTION_SHARED"
	case TxnLockIntentionExclusive:
		return "TXN_INTENTION_EXCLUSIVE"
	default:
		return "TXN_UNKNOWN"
	}
}

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TxnStatusActive    TransactionStatus = iota // 活跃状态
	TxnStatusCommitted                          // 已提交
	TxnStatusAborted                            // 已中止
	TxnStatusPrepared                           // 已准备（2PC中间状态）
)

func (ts TransactionStatus) String() string {
	switch ts {
	case TxnStatusActive:
		return "ACTIVE"
	case TxnStatusCommitted:
		return "COMMITTED"
	case TxnStatusAborted:
		return "ABORTED"
	case TxnStatusPrepared:
		return "PREPARED"
	default:
		return "UNKNOWN"
	}
}

// MutationType 变更操作类型
type MutationType int

const (
	MutationPut    MutationType = iota // 写入操作
	MutationDelete                     // 删除操作
)

func (mt MutationType) String() string {
	switch mt {
	case MutationPut:
		return "PUT"
	case MutationDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Mutation 变更操作
type Mutation struct {
	Type  MutationType // 操作类型
	Key   []byte       // 键
	Value []byte       // 值（删除操作时为空）
}

// WriteType 写入记录类型
type WriteType int

const (
	WriteTypePut      WriteType = iota // 写入
	WriteTypeDelete                    // 删除
	WriteTypeRollback                  // 回滚
)

func (wt WriteType) String() string {
	switch wt {
	case WriteTypePut:
		return "PUT"
	case WriteTypeDelete:
		return "DELETE"
	case WriteTypeRollback:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// IsolationLevel 事务隔离级别
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota // 读未提交
	ReadCommitted                         // 读已提交
	RepeatableRead                        // 可重复读
	Serializable                          // 串行化
)

func (il IsolationLevel) String() string {
	switch il {
	case ReadUncommitted:
		return "READ-uncommitted"
	case ReadCommitted:
		return "read-committed"
	case RepeatableRead:
		return "repeatable-read"
	case Serializable:
		return "serializable"
	default:
		return "unknown"
	}
}

// TransactionState 事务状态（统一定义，支持完整的事务生命周期）
type TransactionState int

const (
	TxnActive     TransactionState = iota // 活跃状态
	TxnPreparing                          // 准备中（分布式事务）
	TxnPrepared                           // 已准备（分布式事务）
	TxnCommitting                         // 提交中
	TxnCommitted                          // 已提交
	TxnAborting                           // 中止中
	TxnAborted                            // 已中止
)

func (ts TransactionState) String() string {
	switch ts {
	case TxnActive:
		return "ACTIVE"
	case TxnPreparing:
		return "PREPARING"
	case TxnPrepared:
		return "PREPARED"
	case TxnCommitting:
		return "COMMITTING"
	case TxnCommitted:
		return "COMMITTED"
	case TxnAborting:
		return "ABORTING"
	case TxnAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// KVPair 键值对
type KVPair struct {
	Key   []byte
	Value []byte
}

// TimestampPair 时间戳对
type TimestampPair struct {
	StartTS  uint64 // 开始时间戳
	CommitTS uint64 // 提交时间戳
}

// NodeHealthStatus 节点健康状态
type NodeHealthStatus int

const (
	NodeHealthy     NodeHealthStatus = iota // 健康
	NodeSuspected                           // 疑似故障
	NodeFailed                              // 故障
	NodePartitioned                         // 网络分区
)

func (nhs NodeHealthStatus) String() string {
	switch nhs {
	case NodeHealthy:
		return "HEALTHY"
	case NodeSuspected:
		return "SUSPECTED"
	case NodeFailed:
		return "FAILED"
	case NodePartitioned:
		return "PARTITIONED"
	default:
		return "UNKNOWN"
	}
}

// NodeStatus 节点状态类型别名，用于兼容性
type NodeStatus = NodeHealthStatus

// RecoveryType 恢复类型
type RecoveryType int

const (
	RecoveryTypeNodeRestart       RecoveryType = iota // 节点重启
	RecoveryTypeLeaderElection                        // 领导者选举
	RecoveryTypeDataSync                              // 数据同步
	RecoveryTypePartitionMerge                        // 分区合并
	RecoveryTypeConsistencyRepair                     // 一致性修复
)

func (rt RecoveryType) String() string {
	switch rt {
	case RecoveryTypeNodeRestart:
		return "NODE_RESTART"
	case RecoveryTypeLeaderElection:
		return "LEADER_ELECTION"
	case RecoveryTypeDataSync:
		return "DATA_SYNC"
	case RecoveryTypePartitionMerge:
		return "PARTITION_MERGE"
	case RecoveryTypeConsistencyRepair:
		return "CONSISTENCY_REPAIR"
	default:
		return "UNKNOWN"
	}
}

// RecoveryStatus 恢复状态
type RecoveryStatus int

const (
	RecoveryStatusPending    RecoveryStatus = iota // 等待中
	RecoveryStatusInProgress                       // 进行中
	RecoveryStatusCompleted                        // 已完成
	RecoveryStatusFailed                           // 失败
	RecoveryStatusCancelled                        // 已取消
)

func (rs RecoveryStatus) String() string {
	switch rs {
	case RecoveryStatusPending:
		return "PENDING"
	case RecoveryStatusInProgress:
		return "IN_PROGRESS"
	case RecoveryStatusCompleted:
		return "COMPLETED"
	case RecoveryStatusFailed:
		return "FAILED"
	case RecoveryStatusCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

// PercolatorLockRecord Percolator锁记录
type PercolatorLockRecord struct {
	TxnID      string    `json:"txn_id"`      // 事务ID
	StartTS    uint64    `json:"start_ts"`    // 开始时间戳
	PrimaryKey []byte    `json:"primary_key"` // 主键
	LockType   LockType  `json:"lock_type"`   // 锁类型
	TTL        uint64    `json:"ttl"`         // 生存时间（秒）
	CreatedAt  time.Time `json:"created_at"`  // 创建时间
	ValueSize  int       `json:"value_size"`  // 值大小
}

// PercolatorWriteRecord Percolator写入记录
type PercolatorWriteRecord struct {
	StartTS   uint64    `json:"start_ts"`   // 开始时间戳
	CommitTS  uint64    `json:"commit_ts"`  // 提交时间戳
	WriteType WriteType `json:"write_type"` // 写入类型
	TxnID     string    `json:"txn_id"`     // 事务ID
	CreatedAt time.Time `json:"created_at"` // 创建时间
}

// PercolatorDataRecord Percolator数据记录
type PercolatorDataRecord struct {
	Value     []byte    `json:"value"`      // 实际数据值
	StartTS   uint64    `json:"start_ts"`   // 开始时间戳
	TxnID     string    `json:"txn_id"`     // 事务ID
	CreatedAt time.Time `json:"created_at"` // 创建时间
}

// TransactionConfig 事务配置
type TransactionConfig struct {
	// 基础配置
	MaxVersions    int           `json:"max_versions"`     // 最大版本数
	GCInterval     time.Duration `json:"gc_interval"`      // 垃圾回收间隔
	MaxTxnDuration time.Duration `json:"max_txn_duration"` // 最大事务持续时间

	// 锁配置
	LockTTL           time.Duration `json:"lock_ttl"`           // 锁TTL
	DeadlockDetection bool          `json:"deadlock_detection"` // 是否启用死锁检测

	// 性能配置
	BatchSize         int `json:"batch_size"`          // 批量大小
	MaxConcurrentTxns int `json:"max_concurrent_txns"` // 最大并发事务数

	// Percolator配置
	EnablePercolator bool          `json:"enable_percolator"` // 是否启用Percolator模式
	CleanupInterval  time.Duration `json:"cleanup_interval"`  // 清理间隔
}

// TransactionOptions 事务选项
type TransactionOptions struct {
	IsolationLevel IsolationLevel `json:"isolation_level"` // 隔离级别
	ReadOnly       bool           `json:"read_only"`       // 是否只读
	Timeout        time.Duration  `json:"timeout"`         // 超时时间
	RetryCount     int            `json:"retry_count"`     // 重试次数
}

// Deadlock 死锁信息
type Deadlock struct {
	TxnIDs     []string  `json:"txn_ids"`     // 涉及的事务ID
	Keys       [][]byte  `json:"keys"`        // 涉及的键
	DetectedAt time.Time `json:"detected_at"` // 检测时间
	Message    string    `json:"message"`     // 错误消息
}

// PercolatorTxnState Percolator事务状态
type PercolatorTxnState int

const (
	PercolatorTxnStateActive PercolatorTxnState = iota
	PercolatorTxnStatePrepared
	PercolatorTxnStateCommitted
	PercolatorTxnStateAborted
	PercolatorTxnStateRolledBack
)

// PercolatorTxnInfo Percolator事务信息
type PercolatorTxnInfo struct {
	TxnID      string             `json:"txn_id"`      // 事务ID
	StartTS    uint64             `json:"start_ts"`    // 开始时间戳
	CommitTS   uint64             `json:"commit_ts"`   // 提交时间戳
	PrimaryKey []byte             `json:"primary_key"` // 主键
	State      PercolatorTxnState `json:"state"`       // 事务状态
	CreatedAt  time.Time          `json:"created_at"`  // 创建时间
	UpdatedAt  time.Time          `json:"updated_at"`  // 更新时间
}

// StorageOperation 存储操作
type StorageOperation struct {
	Type      string `json:"type"`      // 操作类型：PUT, DELETE, GET
	Key       []byte `json:"key"`       // 键
	Value     []byte `json:"value"`     // 值（DELETE操作时为空）
	Timestamp uint64 `json:"timestamp"` // 时间戳
}
