# JadeDB Phase 6 - TiDB架构实现详细计划

## 文档概述

本文档详细规划了JadeDB向TiDB架构演进的完整实施方案，包括Percolator事务模型实现和TiDB SQL层完整复制。这是项目进入分布式SQL数据库阶段的关键里程碑。

## 总体目标

### 核心目标
1. **完全采用TiDB SQL层方案**：词法分析、语法分析、查询优化、执行引擎
2. **实现Percolator事务模型**：基于时间戳的分布式事务，支持快照隔离
3. **保持存储引擎兼容**：LSM和B+树双引擎继续支持
4. **分布式架构就绪**：为后续分布式扩展奠定基础

### 设计原则
- **渐进式演进**：基于现有架构逐步演进，降低风险
- **TiDB兼容**：最大程度复用TiDB的成熟方案
- **模块化设计**：各组件独立开发，便于测试和维护
- **性能优先**：在兼容性基础上追求最佳性能

---

## Phase 6.1 - Raft共识层完善 (优先级最高)

### 6.1.1 Raft核心功能完善 (2周)

**目标**：完善Raft算法的生产级功能

**当前状态分析**：
- ✅ 已有Raft基础实现（领导者选举、日志复制、状态机）
- ✅ 已有网络通信层和测试框架
- ❌ 快照机制不完整（关键TODO项）
- ❌ 缺少持久化存储
- ❌ 缺少与存储引擎的集成
- ❌ 网络分区和故障恢复不完善

**实现任务**：
```go
// 1. 完善Raft快照机制
type RaftSnapshot struct {
    // 快照元数据
    lastIncludedIndex uint64
    lastIncludedTerm  uint64

    // 状态机快照数据
    stateMachineData []byte

    // 配置信息
    configuration []byte
}

// 2. 实现持久化存储
type RaftPersistence struct {
    // 持久化Raft状态
    currentTerm uint64
    votedFor    string
    log         []LogEntry

    // 存储引擎集成
    storageEngine storage.Engine
}

// 3. 增强故障恢复
func (rn *RaftNode) RecoverFromFailure() error {
    // 从持久化存储恢复状态
    // 重建内存状态
    // 重新加入集群
}
```

**验收标准**：
- [ ] 快照机制完整实现
- [ ] 持久化存储集成完成
- [ ] 故障恢复测试通过
- [ ] 网络分区处理正确

### 6.1.2 Raft与存储引擎集成 (2周)

**目标**：实现Raft与LSM/B+树存储引擎的深度集成

**集成架构设计**：
```go
// 1. 存储引擎状态机适配器
type StorageEngineStateMachine struct {
    engine storage.Engine
    raftNode *RaftNode

    // 操作日志
    operationLog *OperationLog

    // 快照管理
    snapshotManager *SnapshotManager
}

// 2. 分片管理器
type ShardManager struct {
    // 每个分片对应一个Raft组
    shards map[string]*RaftShard

    // 分片路由
    router *ShardRouter

    // 负载均衡
    balancer *ShardBalancer
}

// 3. Raft分片
type RaftShard struct {
    shardID   string
    raftNode  *RaftNode
    engine    storage.Engine
    stateMachine *StorageEngineStateMachine
}

func (shard *RaftShard) Put(key, value []byte) error {
    // 通过Raft提议操作
    operation := &Operation{Type: "PUT", Key: key, Value: value}
    return shard.raftNode.Propose(operation)
}
```

**关键特性实现**：
- **状态机集成**：Raft状态机直接操作存储引擎
- **分片管理**：支持数据分片和Raft组管理
- **快照优化**：基于存储引擎的高效快照
- **故障恢复**：存储引擎级别的故障恢复

**验收标准**：
- [ ] 存储引擎状态机正确实现
- [ ] 分片管理功能完整
- [ ] 快照机制高效运行
- [ ] 集成测试全部通过

## Phase 6.2 - Percolator事务模型实现 (4周)

### 6.2.1 时间戳服务完善 (1.5周)

**目标**：实现TiDB风格的全局时间戳服务

**实现任务**：
```go
// 1. 增强TimestampOracle，支持TiDB风格的TSO
type TSOService struct {
    // 物理时间戳部分（毫秒级）
    physicalTS atomic.Uint64
    // 逻辑时间戳部分
    logicalTS atomic.Uint64
    // 时间戳批量分配
    batchAllocator *BatchTSAllocator
    // 分布式一致性（基于Raft）
    raftNode *RaftNode
}

// 2. 实现混合时间戳生成
func (tso *TSOService) GenerateTS() (uint64, error) {
    // 高44位：物理时间戳（毫秒）
    // 低20位：逻辑时间戳
    physical := uint64(time.Now().UnixMilli())
    logical := tso.logicalTS.Add(1)
    return (physical << 20) | (logical & 0xFFFFF), nil
}
```

**验收标准**：
- [ ] 支持TiDB兼容的混合时间戳格式
- [ ] 批量分配性能达到10万QPS
- [ ] 分布式环境下时间戳全局唯一性

### 6.2.2 Percolator事务核心实现 (2周)

**目标**：实现完整的Percolator事务模型

**核心组件设计**：
```go
// 1. Percolator事务管理器
type PercolatorTxnManager struct {
    tsoService    *TSOService
    lockManager   *PercolatorLockManager
    mvccManager   *PercolatorMVCCManager
    raftShards    map[string]*RaftShard // 与Raft集成
}

// 2. 两阶段提交实现
func (txn *PercolatorTransaction) Prewrite() error {
    // 1. 选择Primary Key
    // 2. 通过Raft并发Prewrite所有keys
    // 3. 检查冲突和锁
    // 4. 写入锁信息到Raft状态机
}
```

**验收标准**：
- [ ] 通过TPC-C事务测试
- [ ] 支持快照隔离级别
- [ ] 与Raft层正确集成

### 6.2.3 MVCC三列存储模型 (0.5周)

**目标**：实现Percolator的三列存储模型

**验收标准**：
- [ ] 三列存储模型正确实现
- [ ] 读写性能满足要求

---

## Phase 6.3 - TiDB SQL层架构实现 (3周)

### 6.3.1 SQL解析器实现 (1.5周)

**目标**：完全复制TiDB的SQL解析器架构

**实现策略**：
1. **直接移植TiDB parser包**：使用TiDB的开源parser
2. **适配JadeDB接口**：创建适配层连接parser和执行引擎
3. **扩展支持**：根据需要扩展特定语法

```go
// 1. TiDB Parser集成
import (
    "github.com/pingcap/tidb/parser"
    "github.com/pingcap/tidb/parser/ast"
)

type JadeDBSQLParser struct {
    parser *parser.Parser
    // 适配器组件
    astConverter *ASTConverter
    planBuilder  *PlanBuilder
}

// 2. AST转换适配器
type ASTConverter struct {
    // 将TiDB AST转换为JadeDB内部表示
}

// 3. 执行计划构建器
type PlanBuilder struct {
    // 基于AST构建执行计划
    storageEngines map[string]storage.Engine
    txnManager     *PercolatorTxnManager
}
```

**验收标准**：
- [ ] 支持完整的MySQL语法
- [ ] AST构建正确性验证
- [ ] 语法错误处理完善
- [ ] 性能满足要求

### 6.2.2 查询优化器实现 (1.5周)

**目标**：实现TiDB风格的查询优化器

```go
// 1. 优化器框架
type JadeDBOptimizer struct {
    // 逻辑优化器
    logicalOptimizer *LogicalOptimizer
    // 物理优化器  
    physicalOptimizer *PhysicalOptimizer
    // 统计信息
    statsManager *StatisticsManager
}

// 2. 优化规则集
var optimizationRules = []OptimizationRule{
    &PredicatePushDownRule{},
    &ColumnPruningRule{},
    &JoinReorderRule{},
    &IndexSelectionRule{},
    &PartitionPruningRule{},
}

// 3. 成本模型
type CostModel struct {
    // 存储引擎成本参数
    lsmCostParams    *LSMCostParams
    btreeCostParams  *BTreeCostParams
    // 网络成本参数
    networkCostParams *NetworkCostParams
}
```

### 6.2.3 执行引擎实现 (0.5周)

**目标**：实现向量化执行引擎

```go
// 1. 执行器框架
type ExecutionEngine struct {
    // 执行器工厂
    executorFactory *ExecutorFactory
    // 向量化支持
    vectorizedEngine *VectorizedEngine
    // 并行执行
    parallelExecutor *ParallelExecutor
}

// 2. 基础执行器
type BaseExecutor interface {
    Open(ctx context.Context) error
    Next(ctx context.Context, chunk *Chunk) error
    Close() error
}

// 3. 存储引擎适配
type StorageExecutorAdapter struct {
    engine storage.Engine
    txn    *PercolatorTransaction
}
```

---

## Phase 6.3 - 系统集成和测试 (2周)

### 6.3.1 组件集成 (1周)

**集成任务**：
1. **事务系统集成**：Percolator事务与存储引擎集成
2. **SQL引擎集成**：SQL解析器与执行引擎集成
3. **监控系统集成**：性能监控和指标收集
4. **配置管理集成**：统一配置管理系统

### 6.3.2 兼容性测试 (1周)

**测试内容**：
1. **MySQL兼容性测试**：使用MySQL测试套件
2. **TiDB兼容性测试**：使用TiDB测试用例
3. **性能基准测试**：与原有实现性能对比
4. **稳定性测试**：长时间运行和压力测试

---

## 实施时间线

### 总体时间安排 (10周)
```
Week 1-2:  Raft核心功能完善
Week 3-4:  Raft与存储引擎集成
Week 5-6:  时间戳服务完善 + Percolator事务核心
Week 7:    MVCC三列存储模型
Week 8-9:  TiDB SQL层架构实现
Week 10:   系统集成和测试
```

### 里程碑检查点
- **Week 2**: Raft快照和持久化完成
- **Week 4**: Raft与存储引擎集成完成
- **Week 6**: Percolator事务基本功能完成
- **Week 7**: MVCC三列存储模型完成
- **Week 9**: SQL层基本功能完成
- **Week 10**: 完整系统通过集成测试

---

## 风险评估和缓解策略

### 主要风险
1. **复杂度风险**：Percolator模型实现复杂
2. **性能风险**：新架构可能影响性能
3. **兼容性风险**：与现有代码集成困难
4. **时间风险**：开发时间可能超出预期

### 缓解策略
1. **分阶段实施**：每个阶段独立验证
2. **性能基准**：每个阶段都有性能基准测试
3. **向后兼容**：保持现有接口兼容性
4. **并行开发**：部分组件可以并行开发

---

## 成功标准

### 功能标准
- [ ] 完整的Percolator事务模型实现
- [ ] TiDB兼容的SQL解析和执行
- [ ] 双存储引擎继续支持
- [ ] 分布式架构基础就绪

### 性能标准  
- [ ] 事务吞吐量不低于现有实现的80%
- [ ] SQL查询性能达到预期目标
- [ ] 内存使用控制在合理范围
- [ ] 并发性能满足要求

### 质量标准
- [ ] 代码覆盖率达到80%以上
- [ ] 通过完整的测试套件
- [ ] 文档完整且准确
- [ ] 代码质量达到生产级别

这个计划将使JadeDB成为一个真正的分布式SQL数据库，为后续的分布式扩展奠定坚实基础。
