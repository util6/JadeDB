# JadeDB 分布式SQL系统架构设计

## 文档概述

本文档详细设计了基于JadeDB现有存储引擎的分布式SQL数据库系统架构。该系统将在现有的LSM树和B+树存储引擎基础上，构建完整的分布式SQL数据库，支持标准SQL语法、分布式事务、数据分片和高可用性。

## 技术基础分析

### 现有技术栈优势

**存储引擎层**：
- ✅ **双引擎架构**：LSM树(写优化) + B+树(读优化) + 混合引擎
- ✅ **ACID事务**：完整的MVCC事务支持，快照隔离
- ✅ **并发控制**：页面级锁管理器，死锁检测
- ✅ **崩溃恢复**：WAL + ARIES三阶段恢复
- ✅ **性能监控**：热点检测，趋势分析，异常检测
- ✅ **统一接口**：StorageEngine抽象，支持引擎切换

**数据管理层**：
- ✅ **内存管理**：LRU缓冲池，对象池复用
- ✅ **数据完整性**：CRC32校验，双写缓冲区
- ✅ **批量操作**：高效的批量读写接口
- ✅ **迭代器系统**：支持范围查询和双向遍历

**性能优化层**：
- ✅ **异步处理**：Promise/Future模式，管道处理
- ✅ **智能缓存**：自适应哈希索引，智能预读
- ✅ **压缩优化**：多级压缩策略，空间回收

### 需要扩展的能力

**SQL处理层**：
- ❌ SQL词法和语法分析器
- ❌ 抽象语法树(AST)构建
- ❌ 查询优化器和执行计划
- ❌ 标准SQL语法支持

**分布式协调层**：
- ❌ 集群管理和节点发现
- ❌ 数据分片和路由策略
- ❌ 分布式事务协调
- ❌ 故障检测和自动恢复

**查询执行层**：
- ❌ 分布式查询执行引擎
- ❌ 跨节点JOIN和聚合
- ❌ 并行查询处理
- ❌ 结果集合并和排序

---

## 系统架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    JadeDB 分布式SQL系统                      │
├─────────────────────────────────────────────────────────────┤
│                      SQL接口层                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │ SQL Gateway │ │ Connection  │ │ Protocol    │           │
│  │             │ │ Manager     │ │ Handler     │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                      计算层                                  │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │ SQL Parser  │ │ Query       │ │ Execution   │           │
│  │             │ │ Optimizer   │ │ Engine      │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                      协调层                                  │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │ Cluster     │ │ Transaction │ │ Shard       │           │
│  │ Manager     │ │ Coordinator │ │ Router      │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                      存储层 (现有)                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │ LSM Engine  │ │ B+Tree      │ │ Hybrid      │           │
│  │             │ │ Engine      │ │ Engine      │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### 分层职责定义

#### 1. SQL接口层 (SQL Interface Layer)

**职责**：
- 处理客户端连接和协议解析
- 提供标准SQL接口(MySQL/PostgreSQL兼容)
- 连接池管理和负载均衡
- 认证和权限控制

**核心组件**：
```go
type SQLGateway struct {
    connectionManager *ConnectionManager
    protocolHandler   *ProtocolHandler
    authManager       *AuthManager
    loadBalancer      *LoadBalancer
}

type ConnectionManager struct {
    connections map[string]*Connection
    pool        *ConnectionPool
    maxConn     int
    timeout     time.Duration
}
```

#### 2. 计算层 (Compute Layer)

**职责**：
- SQL语句解析和AST构建
- 查询优化和执行计划生成
- 分布式查询执行协调
- 结果集处理和返回

**核心组件**：
```go
type SQLParser struct {
    lexer     *Lexer
    parser    *Parser
    astBuilder *ASTBuilder
}

type QueryOptimizer struct {
    costModel     *CostModel
    ruleOptimizer *RuleOptimizer
    planGenerator *PlanGenerator
}

type ExecutionEngine struct {
    planExecutor   *PlanExecutor
    joinProcessor  *JoinProcessor
    aggregator     *Aggregator
    resultMerger   *ResultMerger
}
```

#### 3. 协调层 (Coordination Layer)

**职责**：
- 集群节点管理和服务发现
- 数据分片策略和路由决策
- 分布式事务协调和一致性保证
- 故障检测和自动恢复

**核心组件**：
```go
type ClusterManager struct {
    nodeRegistry   *NodeRegistry
    healthChecker  *HealthChecker
    failureDetector *FailureDetector
    rebalancer     *Rebalancer
}

type TransactionCoordinator struct {
    twoPhaseCommit *TwoPhaseCommit
    raftConsensus  *RaftConsensus
    lockManager    *DistributedLockManager
}

type ShardRouter struct {
    shardStrategy  ShardStrategy
    routingTable   *RoutingTable
    loadBalancer   *ShardLoadBalancer
}
```

#### 4. 存储层 (Storage Layer) - 现有

**职责**：
- 数据持久化和检索
- 本地事务管理
- 存储引擎选择和优化
- 数据完整性保证

**现有组件**：
- LSM树引擎：写优化存储
- B+树引擎：读优化存储  
- 混合引擎：自适应选择
- 统一存储接口：StorageEngine

---

## 核心设计原则

### 1. 计算存储分离

**设计理念**：
- 计算节点无状态，可水平扩展
- 存储节点有状态，数据分片存储
- 通过网络协议进行计算存储交互
- 支持计算和存储独立扩缩容

**架构优势**：
- 弹性扩展：计算和存储可独立扩展
- 资源优化：不同类型节点使用不同硬件配置
- 故障隔离：计算故障不影响数据安全
- 成本控制：按需分配计算和存储资源

### 2. 多租户架构

**租户隔离**：
- 数据库级别隔离：每个租户独立数据库
- 表级别隔离：共享数据库，独立表空间
- 行级别隔离：共享表，通过租户ID隔离
- 资源配额：CPU、内存、存储配额管理

**安全控制**：
- 认证授权：基于角色的访问控制(RBAC)
- 数据加密：传输加密和存储加密
- 审计日志：完整的操作审计记录
- 网络隔离：VPC和安全组隔离

### 3. 高可用设计

**数据可用性**：
- 多副本：每个分片至少3个副本
- 跨机房：副本分布在不同可用区
- 自动故障转移：主副本故障自动切换
- 数据一致性：强一致性和最终一致性选择

**服务可用性**：
- 无单点故障：所有组件都有冗余
- 健康检查：实时监控节点健康状态
- 自动恢复：故障节点自动重启和替换
- 降级策略：部分功能降级保证核心可用

### 4. 性能优化

**查询优化**：
- 基于成本的优化器(CBO)
- 统计信息收集和更新
- 索引推荐和自动创建
- 查询缓存和结果缓存

**执行优化**：
- 并行查询执行
- 向量化执行引擎
- 列式存储支持
- 智能预读和缓存

**网络优化**：
- 数据压缩传输
- 批量操作合并
- 连接池复用
- 异步I/O处理

---

## 数据模型设计

### 1. 逻辑数据模型

**层次结构**：
```
Cluster (集群)
├── Database (数据库)
│   ├── Schema (模式)
│   │   ├── Table (表)
│   │   │   ├── Column (列)
│   │   │   ├── Index (索引)
│   │   │   └── Constraint (约束)
│   │   ├── View (视图)
│   │   └── Procedure (存储过程)
│   └── User (用户)
└── Node (节点)
    ├── Shard (分片)
    └── Replica (副本)
```

**元数据管理**：
- 系统表：存储集群、数据库、表等元数据
- 版本控制：元数据变更的版本管理
- 分布式一致性：元数据在所有节点保持一致
- 缓存策略：元数据缓存提高访问性能

### 2. 物理数据模型

**分片策略**：
```go
type ShardStrategy interface {
    Shard(key []byte) uint64
    GetShardCount() uint64
    GetShardNodes(shardID uint64) []NodeID
}

// 范围分片
type RangeShardStrategy struct {
    ranges []ShardRange
}

// 哈希分片  
type HashShardStrategy struct {
    shardCount uint64
    hashFunc   hash.Hash64
}

// 目录分片
type DirectoryShardStrategy struct {
    directory map[string]uint64
}
```

**副本管理**：
- 主副本：处理读写请求
- 从副本：处理只读请求，异步复制
- 副本一致性：Raft协议保证强一致性
- 副本分布：跨机房分布提高可用性

---

## 接口设计

### 1. SQL接口

**标准SQL支持**：
```sql
-- DDL (数据定义语言)
CREATE DATABASE test_db;
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_users_email ON users(email);

-- DML (数据操作语言)
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users WHERE email = 'alice@example.com';
UPDATE users SET name = 'Alice Smith' WHERE id = 1;
DELETE FROM users WHERE id = 1;

-- 分布式查询
SELECT u.name, COUNT(o.id) as order_count
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= '2024-01-01'
GROUP BY u.id, u.name
ORDER BY order_count DESC
LIMIT 100;
```

### 2. 管理接口

**集群管理API**：
```go
type ClusterAPI interface {
    // 节点管理
    AddNode(node *NodeInfo) error
    RemoveNode(nodeID string) error
    ListNodes() ([]*NodeInfo, error)
    
    // 分片管理
    CreateShard(shardConfig *ShardConfig) error
    MoveShard(shardID uint64, targetNode string) error
    SplitShard(shardID uint64) error
    
    // 副本管理
    AddReplica(shardID uint64, nodeID string) error
    RemoveReplica(shardID uint64, nodeID string) error
    PromoteReplica(shardID uint64, nodeID string) error
}
```

### 3. 监控接口

**性能监控API**：
```go
type MonitoringAPI interface {
    // 集群指标
    GetClusterMetrics() (*ClusterMetrics, error)
    GetNodeMetrics(nodeID string) (*NodeMetrics, error)
    GetShardMetrics(shardID uint64) (*ShardMetrics, error)
    
    // 查询分析
    GetSlowQueries(limit int) ([]*SlowQuery, error)
    GetQueryStats() (*QueryStats, error)
    
    // 告警管理
    SetAlert(alert *AlertRule) error
    GetAlerts() ([]*Alert, error)
}
```

---

## 技术选型

### 1. 编程语言和框架

**Go语言优势**：
- 高并发：goroutine和channel天然支持并发
- 高性能：编译型语言，接近C/C++性能
- 简洁性：语法简单，易于维护
- 生态系统：丰富的网络和数据库库
- 现有代码：JadeDB已有Go代码基础

**核心框架**：
- 网络框架：基于net/http和gRPC
- 序列化：Protocol Buffers
- 配置管理：Viper
- 日志系统：Zap
- 测试框架：Testify

### 2. 通信协议

**内部通信**：
- gRPC：高性能RPC框架
- Protocol Buffers：高效序列化
- HTTP/2：多路复用和流控制
- TLS：传输层安全

**客户端协议**：
- MySQL协议：兼容MySQL客户端
- PostgreSQL协议：兼容PostgreSQL客户端
- HTTP REST API：Web应用集成
- WebSocket：实时数据推送

### 3. 一致性算法

**Raft共识算法**：
- 强一致性：保证数据一致性
- 分区容错：网络分区时保持可用
- 简单实现：相比Paxos更易理解和实现
- 成熟生态：有成熟的Go实现

**两阶段提交(2PC)**：
- 分布式事务：跨分片事务一致性
- 原子性保证：事务要么全部成功要么全部失败
- 超时处理：避免无限等待
- 故障恢复：协调者故障后的恢复机制

---

## 部署架构

### 1. 节点类型

**计算节点(Compute Node)**：
- 无状态设计，可水平扩展
- 处理SQL解析、优化、执行
- 内存密集型，需要大内存配置
- 可根据查询负载动态扩缩容

**存储节点(Storage Node)**：
- 有状态设计，存储数据分片
- 运行JadeDB存储引擎
- I/O密集型，需要高性能存储
- 数据迁移时需要谨慎操作

**协调节点(Coordinator Node)**：
- 管理集群元数据和配置
- 运行Raft共识算法
- 通常3-5个节点组成
- 高可用部署，跨机房分布

### 2. 网络拓扑

```
                    ┌─────────────────┐
                    │   Load Balancer │
                    └─────────────────┘
                             │
                    ┌─────────────────┐
                    │   SQL Gateway   │
                    └─────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Compute Node  │   │ Compute Node  │   │ Compute Node  │
│      1        │   │      2        │   │      3        │
└───────────────┘   └───────────────┘   └───────────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Storage Node  │   │ Storage Node  │   │ Storage Node  │
│      1        │   │      2        │   │      3        │
└───────────────┘   └───────────────┘   └───────────────┘
```

---

## 总结

基于JadeDB现有的强大存储引擎基础，分布式SQL系统的架构设计具有以下优势：

1. **技术基础扎实**：完整的存储引擎、事务管理、并发控制
2. **架构清晰**：分层设计，职责明确，易于开发和维护
3. **扩展性强**：计算存储分离，支持水平扩展
4. **可用性高**：多副本、故障转移、自动恢复
5. **性能优异**：基于成本的优化、并行执行、智能缓存

下一步将详细设计各个子系统的实现方案。
