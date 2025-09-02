# JadeDB PebbleDB优化使用指南

## 概述

本指南介绍如何使用JadeDB中新实现的PebbleDB优化策略，包括异步提交管道、智能批量处理等功能。

## 快速开始

### 1. 启用管道优化

```go
package main

import (
    "github.com/util6/JadeDB"
    "github.com/util6/JadeDB/utils"
)

func main() {
    // 创建数据库选项
    opt := &JadeDB.Options{
        WorkDir:          "./data",
        SSTableMaxSz:     64 << 20, // 64MB
        MemTableSize:     16 << 20, // 16MB
        ValueLogFileSize: 128 << 20, // 128MB
        ValueThreshold:   1024,
        MaxBatchCount:    1000,
        MaxBatchSize:     16 << 20, // 16MB
    }
    
    // 打开数据库（默认启用管道优化）
    db := JadeDB.Open(opt)
    defer db.Close()
    
    // 或者手动控制管道优化
    db.SetPipelineEnabled(true)
    
    // 正常使用数据库
    entry := utils.NewEntry([]byte("key1"), []byte("value1"))
    err := db.Set(entry)
    if err != nil {
        panic(err)
    }
}
```

### 2. 批量写入优化

```go
// 批量写入多个键值对
entries := make([]*utils.Entry, 100)
for i := 0; i < 100; i++ {
    key := fmt.Sprintf("batch_key_%d", i)
    value := fmt.Sprintf("batch_value_%d", i)
    entries[i] = utils.NewEntry([]byte(key), []byte(value))
}

// 使用批量写入API
err := db.BatchSet(entries)
if err != nil {
    panic(err)
}
```

### 3. 性能监控

```go
// 获取管道统计信息
pipelineStats := db.GetPipelineStats()
if pipelineStats != nil {
    fmt.Printf("管道统计: %+v\n", pipelineStats)
}

// 获取批量合并器统计信息
coalescerStats := db.GetCoalescerStats()
if coalescerStats != nil {
    fmt.Printf("合并器统计: %+v\n", coalescerStats)
}
```

## 性能测试

### 运行基准测试

```bash
# 运行所有基准测试
go test -bench=. -benchmem

# 运行特定的基准测试
go test -bench=BenchmarkWriteThroughput -benchmem
go test -bench=BenchmarkWriteLatency -benchmem
go test -bench=BenchmarkBatchWrite -benchmem

# 运行性能对比测试
go test -bench=BenchmarkWriteThroughput/Pipeline -benchmem
go test -bench=BenchmarkWriteThroughput/Original -benchmem
```

### 运行演示程序

```bash
# 运行管道优化演示
go run examples/pipeline_demo.go
```

## 配置参数

### 数据库选项

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `MemTableSize` | 内存表大小 | 16MB-64MB |
| `SSTableMaxSz` | SSTable最大大小 | 64MB-256MB |
| `ValueThreshold` | Value Log阈值 | 1KB-4KB |
| `MaxBatchCount` | 最大批量数量 | 100-1000 |
| `MaxBatchSize` | 最大批量大小 | 8MB-32MB |

### 管道优化参数

管道优化参数在代码中自动调整，但可以通过以下方式影响：

```go
// 在创建数据库时设置较大的批量参数以获得更好的批量效果
opt := &JadeDB.Options{
    MaxBatchCount: 1000,    // 增加批量数量
    MaxBatchSize:  32 << 20, // 增加批量大小
}
```

## 性能优化建议

### 1. 写入优化

- **使用批量写入**: 对于大量数据写入，使用`BatchSet`而不是多次`Set`
- **合理设置批量大小**: 根据数据大小调整`MaxBatchCount`和`MaxBatchSize`
- **启用管道优化**: 确保`enablePipeline`为true

### 2. 内存优化

- **合理设置内存表大小**: 根据可用内存调整`MemTableSize`
- **监控内存使用**: 定期检查内存使用情况，避免内存泄漏

### 3. 磁盘I/O优化

- **使用SSD**: 管道优化在SSD上效果更明显
- **调整同步策略**: 根据数据重要性调整同步频率

## 监控指标

### 管道统计指标

- `wal_batches`: WAL处理的批次数
- `mem_batches`: 内存表处理的批次数  
- `sync_batches`: 同步处理的批次数
- `avg_batch_size`: 平均批次大小
- `avg_latency`: 平均延迟

### 合并器统计指标

- `total_batches`: 总批次数
- `total_requests`: 总请求数
- `avg_coalesce_time`: 平均合并时间
- `max_batch_size`: 最大批次大小
- `pending_count`: 待处理请求数

## 故障排除

### 常见问题

1. **性能没有提升**
   - 检查是否启用了管道优化
   - 确认批量大小设置合理
   - 验证磁盘I/O不是瓶颈

2. **内存使用过高**
   - 减少`MemTableSize`
   - 降低`MaxBatchSize`
   - 检查是否有内存泄漏

3. **延迟增加**
   - 检查批量合并器的延迟设置
   - 确认系统负载不过高
   - 验证磁盘性能

### 调试方法

```go
// 启用详细日志
import "log"

// 在关键路径添加日志
log.Printf("Pipeline stats: %+v", db.GetPipelineStats())
log.Printf("Coalescer stats: %+v", db.GetCoalescerStats())
```

## 最佳实践

### 1. 生产环境配置

```go
opt := &JadeDB.Options{
    WorkDir:          "/data/jadedb",
    SSTableMaxSz:     128 << 20, // 128MB
    MemTableSize:     32 << 20,  // 32MB
    ValueLogFileSize: 512 << 20, // 512MB
    ValueThreshold:   2048,      // 2KB
    MaxBatchCount:    500,
    MaxBatchSize:     16 << 20,  // 16MB
}
```

### 2. 高吞吐量场景

```go
opt := &JadeDB.Options{
    WorkDir:          "/data/jadedb",
    SSTableMaxSz:     256 << 20, // 256MB
    MemTableSize:     64 << 20,  // 64MB
    ValueLogFileSize: 1024 << 20, // 1GB
    ValueThreshold:   4096,       // 4KB
    MaxBatchCount:    1000,
    MaxBatchSize:     32 << 20,   // 32MB
}
```

### 3. 低延迟场景

```go
opt := &JadeDB.Options{
    WorkDir:          "/data/jadedb",
    SSTableMaxSz:     32 << 20,  // 32MB
    MemTableSize:     8 << 20,   // 8MB
    ValueLogFileSize: 128 << 20, // 128MB
    ValueThreshold:   1024,      // 1KB
    MaxBatchCount:    100,
    MaxBatchSize:     4 << 20,   // 4MB
}
```

## 版本兼容性

- 管道优化功能向后兼容
- 可以通过`SetPipelineEnabled(false)`禁用优化
- 数据格式完全兼容原版本

## 贡献指南

如果您想为PebbleDB优化功能贡献代码：

1. Fork项目仓库
2. 创建功能分支
3. 添加测试用例
4. 提交Pull Request

详细信息请参考项目根目录的CONTRIBUTING.md文件。
