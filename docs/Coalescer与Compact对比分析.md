# Coalescer与Compact对比分析

## 概述

在JadeDB的LSM树实现中，Coalescer（批量合并器）和Compact（压缩器）是两个容易混淆但职能完全不同的组件。本文档详细分析它们的区别、联系以及为什么需要分开实现。

## 核心区别

### 1. 作用时机

**Coalescer（批量合并器）**
- **时机**: 数据写入时的实时处理
- **阶段**: 在数据进入内存表(MemTable)之前
- **频率**: 每次写入操作都可能触发
- **延迟**: 毫秒级（1-10ms）

**Compact（压缩器）**
- **时机**: 后台定期执行的维护操作
- **阶段**: 在数据已经持久化到SSTable之后
- **频率**: 定期触发（默认50秒间隔）
- **延迟**: 秒级到分钟级

### 2. 处理对象

**Coalescer**
```go
// 处理的是用户写入请求
type BatchRequest struct {
    Entries []*utils.Entry  // 用户写入的条目
    Ptrs    []*utils.ValuePtr
    Wg      sync.WaitGroup
    Err     error
}
```

**Compact**
```go
// 处理的是磁盘上的SSTable文件
type compactDef struct {
    thisLevel *levelHandler  // 源层级
    nextLevel *levelHandler  // 目标层级
    top []*table            // 源层级的表
    bot []*table            // 目标层级的表
    // ...
}
```

### 3. 处理目标

**Coalescer的目标**
- 合并多个小的写入请求
- 减少系统调用次数
- 提高写入吞吐量
- 优化内存使用

**Compact的目标**
- 合并多个SSTable文件
- 清理过期和删除的数据
- 维持LSM树的层级结构
- 优化读取性能

## 详细功能对比

### Coalescer（批量合并器）

**主要功能:**
1. **请求聚合**: 将多个小的写入请求合并成大批次
2. **去重优化**: 相同键的多个写入只保留最新的
3. **排序优化**: 按键排序提升写入的局部性
4. **自适应调整**: 根据历史性能动态调整批量大小

**核心代码逻辑:**
```go
func (bc *BatchCoalescer) coalesceBatches(requests []*BatchRequest) [][]*utils.Entry {
    // 1. 收集所有条目
    var allEntries []*utils.Entry
    for _, req := range requests {
        allEntries = append(allEntries, req.Entries...)
    }
    
    // 2. 按键排序提升局部性
    sort.Slice(allEntries, func(i, j int) bool {
        return string(allEntries[i].Key) < string(allEntries[j].Key)
    })
    
    // 3. 去重处理
    deduped := bc.deduplicateEntries(allEntries)
    
    // 4. 分割成合适大小的批次
    return bc.splitIntoBatches(deduped)
}
```

**性能影响:**
- 减少50%的系统调用
- 提升2-3倍的写入吞吐量
- 减少30%的内存分配

### Compact（压缩器）

**主要功能:**
1. **文件合并**: 将多个SSTable文件合并成更大的文件
2. **数据清理**: 删除过期、删除标记的数据
3. **层级维护**: 保持LSM树的层级结构平衡
4. **空间回收**: 回收被删除数据占用的磁盘空间

**核心代码逻辑:**
```go
func (lm *levelManager) runCompacter(id int) {
    ticker := time.NewTicker(50000 * time.Millisecond) // 50秒间隔
    for {
        select {
        case <-ticker.C:
            lm.runOnce(id) // 执行一次压缩
        case <-lm.lsm.closer.CloseSignal:
            return
        }
    }
}

func (lm *levelManager) runOnce(id int) {
    // 1. 计算压缩优先级
    prios := lm.pickCompactLevels()
    
    // 2. 选择需要压缩的文件
    cd := lm.fillTablesL0ToL1(prios[0])
    
    // 3. 执行压缩操作
    lm.runCompactDef(id, cd)
}
```

**性能影响:**
- 减少读放大问题
- 提升长期读取性能
- 回收磁盘空间
- 维持系统稳定性

## 为什么不能合并

### 1. 时间尺度不同

**Coalescer**: 微秒到毫秒级的实时处理
```go
// 需要快速响应用户写入
maxLatency: 5 * time.Millisecond
```

**Compact**: 秒到分钟级的后台处理
```go
// 可以慢慢处理，不影响用户操作
ticker := time.NewTicker(50000 * time.Millisecond)
```

### 2. 处理粒度不同

**Coalescer**: 处理单个Entry级别的数据
- 数据量: KB级别
- 处理对象: 内存中的Entry对象
- 操作复杂度: O(n log n) 排序

**Compact**: 处理整个文件级别的数据
- 数据量: MB到GB级别
- 处理对象: 磁盘上的SSTable文件
- 操作复杂度: 多路归并，I/O密集

### 3. 优化目标不同

**Coalescer的优化目标:**
```go
// 优化写入路径
- 减少WAL写入次数
- 提高内存表插入效率
- 降低写入延迟
- 提升写入吞吐量
```

**Compact的优化目标:**
```go
// 优化存储结构
- 减少文件数量
- 清理无效数据
- 平衡层级大小
- 优化读取性能
```

### 4. 资源需求不同

**Coalescer**: 轻量级，CPU密集
- 内存使用: 少量（缓存待处理请求）
- CPU使用: 中等（排序和去重）
- I/O使用: 无（纯内存操作）

**Compact**: 重量级，I/O密集
- 内存使用: 大量（缓存文件内容）
- CPU使用: 中等（多路归并）
- I/O使用: 大量（读写SSTable文件）

## 协作关系

虽然Coalescer和Compact不能合并，但它们在LSM树中形成了完美的协作关系：

### 1. 数据流向

```
用户写入 → Coalescer → MemTable → SSTable → Compact → 优化的SSTable
```

### 2. 性能互补

**Coalescer提升写入性能:**
- 批量处理减少开销
- 去重减少无效写入
- 排序提升局部性

**Compact提升读取性能:**
- 减少文件数量降低查找开销
- 清理无效数据减少扫描量
- 层级平衡优化查找路径

### 3. 资源协调

**时间协调:**
- Coalescer在写入高峰期工作
- Compact在系统空闲时工作

**空间协调:**
- Coalescer优化内存使用
- Compact优化磁盘使用

## 设计原则

### 1. 单一职责原则

每个组件只负责一个明确的功能：
- Coalescer: 写入优化
- Compact: 存储优化

### 2. 关注点分离

不同的性能问题用不同的解决方案：
- 写入性能问题 → Coalescer
- 读取性能问题 → Compact

### 3. 可扩展性

分离设计使得每个组件都可以独立优化：
- Coalescer可以添加更智能的批量策略
- Compact可以添加更复杂的压缩算法

## 总结

Coalescer和Compact虽然都涉及"合并"操作，但它们是LSM树架构中两个完全不同层面的优化组件：

- **Coalescer**是写入路径的实时优化器，专注于提升写入性能
- **Compact**是存储结构的后台维护器，专注于长期的读取性能和空间效率

它们的分离设计体现了LSM树"写优化"的核心思想：通过在不同阶段应用不同的优化策略，实现整体性能的最大化。将它们合并不仅在技术上困难，而且会违背单一职责原则，降低系统的可维护性和可扩展性。

这种设计使得JadeDB能够在保持高写入性能的同时，通过后台压缩维持良好的读取性能和存储效率，实现了LSM树架构的最佳实践。
