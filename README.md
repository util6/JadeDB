# JadeDB

JadeDB 是一个高性能的键值数据库，支持双存储引擎架构（LSM树 + B+树），专为不同工作负载优化。

## 🚀 项目特性

### 双存储引擎架构
- **LSM树引擎**：写优化，适合写密集型工作负载
- **B+树引擎**：读优化，适合读密集型工作负载，达到InnoDB完整性标准

### 核心功能
- ✅ **ACID事务支持**：完整的事务隔离和一致性保证
- ✅ **崩溃恢复**：基于WAL的完整恢复机制
- ✅ **并发控制**：支持多线程并发访问
- ✅ **数据完整性**：多层校验和保护
- ✅ **性能优化**：自适应哈希索引、智能预读
- ✅ **内存管理**：LRU缓冲池、对象池复用

## 📁 项目结构

```
JadeDB/
├── lsm/                    # LSM树存储引擎
├── bplustree/              # B+树存储引擎 (新增)
│   ├── bplustree.go        # 主控制器
│   ├── page.go             # 页面管理
│   ├── page_manager.go     # 页面分配器
│   ├── buffer_pool.go      # 缓冲池
│   ├── wal.go              # WAL日志系统
│   ├── recovery.go         # 崩溃恢复
│   ├── adaptive_hash.go    # 自适应哈希索引
│   └── prefetcher.go       # 智能预读器
├── file/                   # 文件系统抽象
├── utils/                  # 工具库
├── examples/               # 示例程序
└── docs/                   # 项目文档
```

## 🎯 B+树引擎特性

### InnoDB级别的完整性
- **16KB固定页面**：与MySQL InnoDB保持一致
- **ARIES恢复算法**：工业标准的三阶段恢复
- **多版本并发控制**：支持快照隔离
- **完整性校验**：CRC32校验和保护

### 高性能设计
- **分区缓冲池**：16个分区，减少锁竞争
- **自适应哈希**：热点数据O(1)访问
- **智能预读**：访问模式检测和预测
- **批量I/O**：减少系统调用开销

### 性能基准 (Apple M4)
```
页面校验和计算:     ~3μs
页面校验和验证:     ~3μs
缓冲池访问:        ~123ns
```

## 🚧 开发状态

### ✅ 已完成 (Phase 1-3.0)
- [x] LSM树存储引擎
- [x] PebbleDB优化策略
- [x] B+树基础设施
  - [x] 页面管理系统
  - [x] 缓冲池管理
  - [x] WAL日志系统
  - [x] 崩溃恢复机制
  - [x] 性能优化组件

### 🚧 进行中 (Phase 3.1)
- [ ] B+树核心数据结构
- [ ] 页面分裂与合并算法
- [ ] 聚簇索引实现
- [ ] 范围查询支持

### 📋 待实现 (Phase 3.2-3.3)
- [ ] 锁机制与并发控制
- [ ] 辅助索引支持
- [ ] 双引擎兼容层
- [ ] 查询优化器

## 📖 文档

- [项目开发文档](docs/项目开发文档-JadeDB.md) - 开发过程记录
- [B+树引擎详细记录](docs/B+树引擎开发详细记录.md) - 实现细节和问题解决
- [B+树引擎README](bplustree/README.md) - 使用指南

## 🧪 测试

```bash
# 运行所有测试
go test ./...

# 运行B+树引擎测试
go test ./bplustree -v

# 运行性能基准测试
go test ./bplustree -bench=.
```

## 📊 测试覆盖

```
B+树引擎测试结果:
✅ TestPageBasicOperations     - 页面基本操作
✅ TestPageManager            - 页面管理器
✅ TestBufferPool             - 缓冲池管理
✅ TestWALManager             - WAL日志系统
✅ TestRecoveryManager        - 崩溃恢复
✅ TestBPlusTreeBasic         - B+树基础功能
✅ TestAdaptiveHashIndex      - 自适应哈希索引
✅ TestPrefetcher             - 智能预读器

总计: 8个测试全部通过
```

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目。

## 📄 许可证

本项目采用MIT许可证。