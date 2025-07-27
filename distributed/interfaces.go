/*
JadeDB 分布式层接口定义

本模块定义了分布式层对外提供的接口，供上层（transaction、storage）使用。
通过接口隔离，避免循环依赖，实现清晰的层次结构。

层次关系：
distributed (底层) → storage (中层) → transaction (上层)

设计原则：
1. 接口隔离：分布式层只暴露必要的接口
2. 依赖倒置：上层依赖接口，不依赖具体实现
3. 单向依赖：避免循环依赖
4. 职责清晰：每层只负责自己的核心功能
*/

package distributed

import (
	"context"
	"time"
)

// DistributedTransactionCoordinator 分布式事务协调器接口
// 这是分布式层对外提供的核心接口
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
	GetParticipants() []ParticipantInfo

	// 生命周期
	Start() error
	Stop() error
	IsRunning() bool
}

// DistributedConsensus 分布式共识接口
// 提供Raft共识服务
type DistributedConsensus interface {
	// 共识操作
	Propose(ctx context.Context, data []byte) error
	Read(ctx context.Context, key []byte) ([]byte, error)

	// 集群管理
	AddNode(nodeID string, address string) error
	RemoveNode(nodeID string) error
	GetLeader() (string, error)
	GetNodes() []NodeInfo

	// 状态查询
	IsLeader() bool
	GetTerm() uint64
	GetCommitIndex() uint64

	// 生命周期
	Start() error
	Stop() error
}

// DistributedStorage 分布式存储接口
// 提供分布式存储服务
type DistributedStorage interface {
	// 存储操作
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error

	// 批量操作
	BatchPut(ctx context.Context, batch []KVPair) error
	BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error)
	BatchDelete(ctx context.Context, keys [][]byte) error

	// 范围操作
	Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KVPair, error)

	// 一致性保证
	ReadWithConsistency(ctx context.Context, key []byte, level ConsistencyLevel) ([]byte, error)
	WriteWithConsistency(ctx context.Context, key, value []byte, level ConsistencyLevel) error
}

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TxnStatusUnknown TransactionStatus = iota
	TxnStatusActive
	TxnStatusPreparing
	TxnStatusPrepared
	TxnStatusCommitting
	TxnStatusCommitted
	TxnStatusAborting
	TxnStatusAborted
)

func (s TransactionStatus) String() string {
	switch s {
	case TxnStatusActive:
		return "Active"
	case TxnStatusPreparing:
		return "Preparing"
	case TxnStatusPrepared:
		return "Prepared"
	case TxnStatusCommitting:
		return "Committing"
	case TxnStatusCommitted:
		return "Committed"
	case TxnStatusAborting:
		return "Aborting"
	case TxnStatusAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// ParticipantInfo 参与者信息
type ParticipantInfo struct {
	NodeID   string
	Endpoint string
	Status   ParticipantStatus
	LastSeen time.Time
}

// ParticipantStatus 参与者状态
type ParticipantStatus int

const (
	ParticipantStatusUnknown ParticipantStatus = iota
	ParticipantStatusActive
	ParticipantStatusInactive
	ParticipantStatusFailed
)

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID  string
	Address string
	Role    NodeRole
	Status  NodeStatus
}

// NodeRole 节点角色
type NodeRole int

const (
	NodeRoleFollower NodeRole = iota
	NodeRoleCandidate
	NodeRoleLeader
)

// NodeStatus 节点状态
type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusActive
	NodeStatusInactive
	NodeStatusFailed
)

// KVPair 键值对
type KVPair struct {
	Key   []byte
	Value []byte
}

// ConsistencyLevel 一致性级别
type ConsistencyLevel int

const (
	ConsistencyEventual     ConsistencyLevel = iota // 最终一致性
	ConsistencyStrong                               // 强一致性
	ConsistencyLinearizable                         // 线性一致性
)

// DistributedConfig 分布式配置
type DistributedConfig struct {
	// 节点配置
	NodeID      string
	BindAddress string
	DataDir     string

	// Raft配置
	RaftConfig *RaftConfig

	// 事务配置
	TransactionTimeout time.Duration
	PrepareTimeout     time.Duration
	CommitTimeout      time.Duration

	// 网络配置
	MaxConnections    int
	ConnectionTimeout time.Duration
	HeartbeatInterval time.Duration

	// 存储配置
	StorageEngine string
	StorageConfig interface{}
}

// RaftConfig Raft配置
type RaftConfig struct {
	ElectionTimeout    time.Duration
	HeartbeatTimeout   time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotInterval   time.Duration
	LogRetention       int
}

// DefaultDistributedConfig 默认分布式配置
func DefaultDistributedConfig() *DistributedConfig {
	return &DistributedConfig{
		NodeID:      "node-1",
		BindAddress: "127.0.0.1:8080",
		DataDir:     "/tmp/jadedb",
		RaftConfig: &RaftConfig{
			ElectionTimeout:    150 * time.Millisecond,
			HeartbeatTimeout:   50 * time.Millisecond,
			LeaderLeaseTimeout: 500 * time.Millisecond,
			SnapshotInterval:   1000,
			LogRetention:       10000,
		},
		TransactionTimeout: 30 * time.Second,
		PrepareTimeout:     5 * time.Second,
		CommitTimeout:      5 * time.Second,
		MaxConnections:     100,
		ConnectionTimeout:  10 * time.Second,
		HeartbeatInterval:  1 * time.Second,
		StorageEngine:      "btree",
		StorageConfig:      nil,
	}
}

// DistributedService 分布式服务接口
// 这是分布式层的总入口
type DistributedService interface {
	// 获取各个服务组件
	GetTransactionCoordinator() DistributedTransactionCoordinator
	GetConsensus() DistributedConsensus
	GetStorage() DistributedStorage

	// 服务管理
	Start() error
	Stop() error
	IsRunning() bool

	// 配置管理
	GetConfig() *DistributedConfig
	UpdateConfig(config *DistributedConfig) error

	// 健康检查
	HealthCheck() error
	GetMetrics() map[string]interface{}
}

// NewDistributedService 创建分布式服务
// 这是分布式层对外的唯一创建函数
func NewDistributedService(config *DistributedConfig) (DistributedService, error) {
	if config == nil {
		config = DefaultDistributedConfig()
	}

	return newDistributedServiceImpl(config)
}

// 内部实现接口，避免暴露实现细节
func newDistributedServiceImpl(config *DistributedConfig) (DistributedService, error) {
	// 这里会在具体实现文件中定义
	return nil, nil
}
