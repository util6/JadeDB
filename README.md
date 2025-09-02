# JadeDB

JadeDB 是一个高性能的键值数据库，支持双存储引擎架构（LSM树 + B+树），专为不同工作负载优化。

**🎉 B+树存储引擎已全面完成！** 实现了工业级数据库存储引擎的所有核心功能。

## 🚀 项目特性

### 双存储引擎架构

- **LSM树引擎**：写优化，适合写密集型工作负载 ✅
- **B+树引擎**：读优化，适合读密集型工作负载，达到InnoDB完整性标准 ✅
- **混合引擎**：根据工作负载自动切换存储引擎 ✅

### 📖 文档

### 主要文档

- [项目开发文档](docs/项目开发文档-JadeDB.md) - 主项目开发文档
- [B+树引擎开发详细记录](docs/B+树引擎开发详细记录.md) - 开发过程记录
- [B+树引擎完成总结](docs/B+树引擎完成总结.md) - 项目完成总结

### 技术文档

- [B+树高级特性实现指南](docs/B+树高级特性实现指南.md) - 高级特性实现
- [存储引擎接口设计文档](docs/存储引擎接口设计文档.md) - 接口设计说明
- [性能监控系统文档](docs/性能监控系统文档.md) - 监控系统文档

### 使用指南

- [B+树引擎README](bplustree/README.md) - 使用指南
- [优化功能使用指南](docs/优化功能使用指南.md) - LSM优化功能

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
