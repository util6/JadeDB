# Transaction包 - 事务管理系统

## 📋 包概述

Transaction包实现了JadeDB的完整事务管理系统，提供从单机到分布式的事务支持。采用分层设计，为SQL层提供完整的ACID事务保证。

## 🏗️ 架构设计

```text
事务接口层 (transaction.go)
    ↓
事务协调层 (local_transaction.go)
    ↓
并发控制层 (mvcc_manager.go, lock_manager.go)
    ↓
存储引擎层 (storage_adapter.go)
```

## 📁 文件组织

### 🔄 MVCC (多版本并发控制)
负责多版本数据管理和快照隔离

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `mvcc_manager.go` | MVCC管理器核心实现 | 版本管理、快照隔离、可见性判断 |
| `mvcc_manager_percolator_test.go` | MVCC Percolator集成测试 | 测试MVCC与Percolator的集成 |
| `mvcc_gc_test.go` | MVCC垃圾回收测试 | 测试版本清理和内存管理 |

**主要特性：**
- 时间戳排序的版本管理
- 快照隔离级别支持
- 自动垃圾回收机制
- Percolator事务模型支持

### 🔒 锁管理 (Locks)
提供细粒度的锁控制和死锁检测

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `lock_manager.go` | 锁管理器实现 | 锁获取、释放、升级、死锁检测 |
| `deadlock_test.go` | 死锁检测测试 | 测试死锁检测算法和恢复机制 |

**主要特性：**
- 行级锁定支持
- 锁升级机制
- 死锁检测和解决
- 锁等待超时处理

### 🔄 Percolator事务模型
实现Google Percolator分布式事务模型

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `percolator_types.go` | Percolator类型定义 | 锁记录、写入记录、数据记录类型 |
| `percolator_integration_test.go` | Percolator集成测试 | 测试两阶段提交和事务恢复 |

**主要特性：**
- 两阶段提交协议
- 乐观并发控制
- 分布式锁机制
- 故障恢复支持

### 📝 WAL (Write-Ahead Logging)
事务日志记录和恢复

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `simple_wal_test.go` | 简单WAL测试 | 测试基础WAL功能 |
| `debug_wal_test.go` | WAL调试测试 | 测试WAL调试和诊断功能 |
| `txn_wal_test.go` | 事务WAL测试 | 测试事务级别的WAL操作 |

**主要特性：**
- 事务操作日志记录
- 崩溃恢复支持
- 日志压缩和清理
- 性能优化

### 🎯 核心事务功能 (Core)
事务系统的核心接口和实现

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `transaction.go` | 统一事务接口定义 | 事务API、管理器、配置 |
| `local_transaction.go` | 本地事务实现 | 单机事务、保存点、迭代器 |
| `timestamp_oracle.go` | 时间戳服务 | 全局时间戳分配和管理 |
| `transaction_test.go` | 事务基础测试 | 测试事务基本功能 |
| `local_transaction_scan_test.go` | 本地事务扫描测试 | 测试范围查询和迭代器 |
| `complete_txn_test.go` | 完整事务测试 | 测试完整的事务生命周期 |
| `debug_txn_test.go` | 事务调试测试 | 测试事务调试和诊断功能 |

**主要特性：**
- 统一的事务API
- 多种隔离级别支持
- 保存点和嵌套事务
- 全局时间戳服务

### 🔌 适配器 (Adapters)
存储引擎集成适配器

| 文件 | 职责 | 核心功能 |
|------|------|----------|
| `storage_adapter.go` | 存储引擎事务适配器 | 存储引擎集成、事务隔离 |

**主要特性：**
- 存储引擎抽象
- 事务隔离实现
- 批量操作支持
- 性能优化

## 🔗 依赖关系

```text
transaction包依赖：
├── common (公共类型和接口)
├── storage (存储引擎接口)
└── txnwal (事务WAL)

被依赖方：
├── SQL层 (未来)
├── API层
└── 测试代码
```

## 🚀 使用示例

### 基本事务操作
```go
// 创建事务管理器
manager := NewTransactionManager(config)

// 开始事务
txn, err := manager.BeginTransaction(&TransactionOptions{
    Isolation: ReadCommitted,
    Timeout:   30 * time.Second,
})

// 执行操作
err = txn.Put([]byte("key"), []byte("value"))
value, err := txn.Get([]byte("key"))

// 提交事务
err = txn.Commit()
```

### MVCC操作
```go
// 获取MVCC管理器
mvcc := manager.GetMVCCManager()

// 注册事务
mvcc.RegisterTransaction("txn1", startTs)

// 写入版本
mvcc.Put(key, value, "txn1", timestamp)

// 读取版本
value, err := mvcc.Get(key, readTs)
```

### 锁管理
```go
// 获取锁管理器
locks := manager.GetLockManager()

// 获取锁
err := locks.AcquireLock("txn1", key, LockTypePut, timeout)

// 释放锁
err = locks.ReleaseLock("txn1", key)
```

## 📊 性能特性

- **高并发**：基于MVCC的无锁读取
- **低延迟**：优化的锁管理和时间戳分配
- **高吞吐**：批量操作和异步提交支持
- **内存优化**：高效的版本管理和垃圾回收

## 🔧 配置选项

```go
type TransactionConfig struct {
    MaxConcurrentTxns    int           // 最大并发事务数
    DefaultIsolation     IsolationLevel // 默认隔离级别
    LockTimeout          time.Duration  // 锁超时时间
    EnableDeadlockDetect bool          // 启用死锁检测
    GCInterval           time.Duration  // 垃圾回收间隔
    EnablePercolator     bool          // 启用Percolator模式
}
```

## 🧪 测试覆盖

- **单元测试**：每个组件的独立测试
- **集成测试**：组件间的集成测试
- **性能测试**：并发和压力测试
- **故障测试**：异常情况和恢复测试

## 📚 相关文档

- [Go语言循环依赖问题详解](../docs/Go语言循环依赖问题详解.md)
- [JadeDB循环依赖重构报告](../docs/JadeDB循环依赖重构报告.md)
- [文件重组实施指南](../docs/文件重组实施指南.md)

## 🤝 贡献指南

1. 遵循现有的代码风格和注释规范
2. 为新功能添加完整的测试用例
3. 更新相关文档和注释
4. 确保所有测试通过
5. 提交前运行性能测试
