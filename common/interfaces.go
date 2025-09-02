/*
JadeDB 公共接口定义

本模块定义了JadeDB项目中各个包共享的接口。
通过接口抽象，实现依赖倒置，避免包之间的直接依赖。

设计原则：
1. 接口定义在使用方
2. 实现在提供方
3. 通过依赖注入连接
4. 保持接口的稳定性

包含的接口：
- 存储引擎接口
- 事务管理接口
- 时间戳服务接口
- 分布式协调接口
*/

package common

import (
	"context"
	"time"
)

// StorageEngine 存储引擎接口
// 定义了存储引擎的基本操作，供事务层使用
type StorageEngine interface {
	// 基本操作
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Exists(key []byte) (bool, error)

	// 批量操作
	BatchGet(keys [][]byte) ([][]byte, error)
	BatchPut(batch []KVPair) error
	BatchDelete(keys [][]byte) error

	// 迭代器
	NewIterator(options *IteratorOptions) (Iterator, error)
	Scan(startKey, endKey []byte, limit int) ([]KVPair, error)

	// 事务支持
	BeginTransaction() (StorageTransaction, error)

	// 生命周期
	Open() error
	Close() error
	Sync() error

	// 统计信息
	GetStats() map[string]interface{}
}

// StorageTransaction 存储事务接口
type StorageTransaction interface {
	// 基本操作
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	// 事务控制
	Commit() error
	Rollback() error

	// 状态查询
	GetTxnID() string
}

// Iterator 迭代器接口
type Iterator interface {
	// 迭代控制
	Next() bool
	Prev() bool
	Seek(key []byte) bool
	SeekToFirst() bool
	SeekToLast() bool

	// 数据访问
	Key() []byte
	Value() []byte
	Valid() bool

	// 生命周期
	Close() error
}

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	StartKey   []byte // 起始键
	EndKey     []byte // 结束键
	Reverse    bool   // 是否反向迭代
	KeyOnly    bool   // 是否只返回键
	Snapshot   uint64 // 快照时间戳
	PrefixScan bool   // 是否前缀扫描
}

// TimestampOracle 时间戳服务接口
// 为事务系统提供全局唯一的时间戳
type TimestampOracle interface {
	// 时间戳分配
	GetTimestamp() (uint64, error)
	GetTimestampBatch(count int) ([]uint64, error)

	// 时间戳管理
	UpdateTimestamp(ts uint64) error
	GetCurrentTimestamp() uint64

	// 生命周期
	Start() error
	Stop() error
}

// LockManager 锁管理器接口
// 提供细粒度的锁管理功能
type LockManager interface {
	// 锁操作
	AcquireLock(txnID string, key []byte, lockType LockType, timeout time.Duration) error
	ReleaseLock(txnID string, key []byte) error
	ReleaseAllLocks(txnID string) error

	// 锁查询
	IsLocked(key []byte) (bool, error)
	GetLockInfo(key []byte) (*LockInfo, error)
	GetLocksForTransaction(txnID string) ([]*LockInfo, error)

	// 死锁检测
	DetectDeadlock() ([]string, error)
	EnableDeadlockDetection(enable bool)
}

// LockInfo 锁信息
type LockInfo struct {
	// 基本信息
	Key       []byte    // 锁定的键
	TxnID     string    // 持有锁的事务ID
	LockType  LockType  // 锁类型
	CreatedAt time.Time // 锁创建时间
	ExpiresAt time.Time // 锁过期时间

	// 扩展信息（用于分布式锁和Percolator）
	StartTS    uint64 // 开始时间戳
	PrimaryKey []byte // 主键（Percolator使用）
	TTL        uint64 // 生存时间(秒)
	ValueSize  int    // 值大小
	IsExpired  bool   // 是否过期
}

// MVCCManager MVCC管理器接口
// 提供多版本并发控制功能
type MVCCManager interface {
	// 版本操作
	GetVersion(key []byte, ts uint64) ([]byte, error)
	PutVersion(key, value []byte, ts uint64) error
	DeleteVersion(key []byte, ts uint64) error

	// 事务支持
	RegisterTransaction(txnID string, startTs uint64) error
	UnregisterTransaction(txnID string) error

	// 垃圾回收
	GC(beforeTs uint64) error
	GetOldestActiveTransaction() uint64

	// 统计信息
	GetVersionCount(key []byte) (int, error)
	GetStats() map[string]interface{}
}

// TransactionManager 事务管理器接口
// 提供事务的生命周期管理功能
type TransactionManager interface {
	// 事务操作
	BeginTransaction(options *TransactionOptions) (string, error) // 开始事务，返回事务ID
	CommitTransaction(txnID string) error                         // 提交事务
	AbortTransaction(txnID string) error                          // 中止事务

	// 事务状态
	GetTransactionState(txnID string) (TransactionState, error)
	IsTransactionActive(txnID string) bool

	// 事务数据操作
	Get(txnID string, key []byte) ([]byte, error)
	Put(txnID string, key, value []byte) error
	Delete(txnID string, key []byte) error

	// 批量操作
	BatchGet(txnID string, keys [][]byte) ([][]byte, error)
	BatchPut(txnID string, pairs []KVPair) error
	BatchDelete(txnID string, keys [][]byte) error

	// 生命周期
	Start() error
	Stop() error

	// 统计信息
	GetStats() map[string]interface{}
}

// ConsensusEngine 共识引擎接口
// 为分布式系统提供共识功能
type ConsensusEngine interface {
	// 提案操作
	Propose(data []byte) error
	ProposeWithCallback(data []byte, callback func(error)) error

	// 状态查询
	GetState() ConsensusState
	GetLeader() string
	GetTerm() uint64

	// 节点管理
	AddNode(nodeID string, address string) error
	RemoveNode(nodeID string) error
	GetNodes() []NodeInfo

	// 生命周期
	Start(ctx context.Context) error
	Stop() error
}

// ConsensusState 共识状态
type ConsensusState int

const (
	ConsensusFollower  ConsensusState = iota // 跟随者
	ConsensusCandidate                       // 候选者
	ConsensusLeader                          // 领导者
)

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID  string           // 节点ID
	Address string           // 节点地址
	Status  NodeHealthStatus // 节点状态
}

// DistributedTransactionCoordinator 分布式事务协调器接口
// 为分布式事务提供协调功能
type DistributedTransactionCoordinator interface {
	// 事务协调
	BeginDistributedTransaction(ctx context.Context, txnID string, participants []string) error
	PrepareTransaction(ctx context.Context, txnID string) (bool, error)
	CommitTransaction(ctx context.Context, txnID string) error
	AbortTransaction(ctx context.Context, txnID string) error

	// 状态查询
	GetTransactionStatus(txnID string) (TransactionStatus, error)
	ListActiveTransactions() []string

	// 参与者管理
	RegisterParticipant(nodeID string, endpoint string) error
	UnregisterParticipant(nodeID string) error

	// 生命周期
	Start() error
	Stop() error
}

// ParticipantInfo 参与者信息
type ParticipantInfo struct {
	NodeID   string           // 节点ID
	Endpoint string           // 节点端点
	Status   NodeHealthStatus // 节点状态
}

// FailureDetector 故障检测器接口
// 提供节点故障检测功能
type FailureDetector interface {
	// 故障检测
	DetectFailures() []string
	IsNodeFailed(nodeID string) bool
	GetNodeStatus(nodeID string) NodeHealthStatus

	// 心跳管理
	UpdateHeartbeat(nodeID string) error
	GetLastHeartbeat(nodeID string) time.Time

	// 配置
	SetTimeout(timeout time.Duration)
	SetHeartbeatInterval(interval time.Duration)
}

// HealthMonitor 健康监控器接口
// 提供集群健康监控功能
type HealthMonitor interface {
	// 健康检查
	CheckHealth() *ClusterHealth
	IsHealthy() bool
	GetHealthScore() float64

	// 监控配置
	SetCheckInterval(interval time.Duration)
	EnableAutoRecovery(enable bool)
}

// ClusterHealth 集群健康状态
type ClusterHealth struct {
	TotalNodes       int       // 总节点数
	HealthyNodes     int       // 健康节点数
	SuspectedNodes   int       // 疑似故障节点数
	FailedNodes      int       // 故障节点数
	PartitionedNodes int       // 分区节点数
	HasLeader        bool      // 是否有领导者
	LeaderID         string    // 领导者ID
	QuorumAvailable  bool      // 法定人数是否可用
	LastUpdated      time.Time // 最后更新时间
}

// MetricsCollector 指标收集器接口
// 提供系统指标收集功能
type MetricsCollector interface {
	// 指标收集
	CollectMetrics() map[string]interface{}
	GetMetric(name string) interface{}
	SetMetric(name string, value interface{})

	// 指标导出
	ExportMetrics() ([]byte, error)
	ResetMetrics()
}

// EventBus 事件总线接口
// 提供事件驱动的解耦通信
type EventBus interface {
	// 事件发布
	Publish(event Event) error
	PublishAsync(event Event)

	// 事件订阅
	Subscribe(eventType string, handler EventHandler) error
	Unsubscribe(eventType string, handler EventHandler) error

	// 生命周期
	Start() error
	Stop() error
}

// Event 事件接口
type Event interface {
	Type() string
	Data() interface{}
	Timestamp() time.Time
}

// EventHandler 事件处理器
type EventHandler func(event Event) error
