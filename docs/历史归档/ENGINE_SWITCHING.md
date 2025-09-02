# JadeDB 多引擎支持与无感切换

JadeDB 现在支持多种存储引擎，并提供了无感切换功能，允许在运行时动态切换存储引擎而不影响数据访问。

## 支持的存储引擎

### 1. LSM树引擎 (LSMEngine)
- **优势**: 高写入吞吐量，适合写密集型工作负载
- **适用场景**: 时序数据、日志数据、高吞吐量OLTP
- **特性**: 
  - 高效的写入性能
  - 可配置的压缩策略
  - 布隆过滤器优化
  - 多级存储

### 2. B+树引擎 (BTreeEngine)
- **优势**: 快速范围查询，适合读密集型工作负载
- **适用场景**: 范围查询、OLAP工作负载、分析查询
- **特性**:
  - 快速范围查询
  - 高效的点查询
  - 页级缓存
  - 可配置填充因子

## 使用方式

### 传统模式 (LegacyMode)

传统模式保持向后兼容，直接使用LSM引擎：

```go
import "github.com/util6/JadeDB"

// 创建传统模式数据库实例
opt := &JadeDB.Options{
    WorkDir:      "./data",
    MemTableSize: 64 << 20, // 64MB
    SSTableMaxSz: 64 << 20, // 64MB
}

db := JadeDB.Open(opt) // 默认使用传统模式
defer db.Close()

// 正常的数据库操作
db.Set(&utils.Entry{Key: []byte("key"), Value: []byte("value")})
entry, err := db.Get([]byte("key"))
```

### 适配器模式 (AdapterMode)

适配器模式支持多引擎和动态切换：

```go
// 创建适配器模式数据库实例，指定初始引擎
db := JadeDB.OpenWithEngine(opt, JadeDB.AdapterMode, JadeDB.LSMEngine)
defer db.Close()

// 查看当前引擎
fmt.Printf("当前引擎: %v\n", db.GetCurrentEngineType())

// 切换到B+树引擎
err := db.SwitchEngine(JadeDB.BTreeEngine)
if err != nil {
    log.Fatal(err)
}

// 数据会自动迁移，API保持不变
entry, err := db.Get([]byte("key")) // 数据仍然可访问
```

## 引擎切换功能

### 无感切换
- 切换过程中API保持不变
- 自动数据迁移
- 支持热切换，无需停机

### 数据迁移
- 自动将数据从源引擎迁移到目标引擎
- 批量迁移优化性能
- 迁移过程中保证数据一致性

### 性能监控
```go
// 获取引擎统计信息
stats := db.GetEngineStats()
fmt.Printf("切换次数: %v\n", stats["switch_count"])
fmt.Printf("平均延迟: %v\n", stats["avg_latency"])

// 列出可用引擎
engines := db.ListAvailableEngines()
for _, engine := range engines {
    info, _ := db.GetEngineInfo(engine)
    fmt.Printf("%v: %s\n", engine, info.Description)
}
```

## API参考

### 核心方法

```go
// 创建数据库实例
func Open(opt *Options) *DB                                    // 传统模式
func OpenWithEngine(opt *Options, mode EngineMode, engineType EngineType) *DB // 适配器模式

// 引擎管理
func (db *DB) SwitchEngine(targetType EngineType) error        // 切换引擎
func (db *DB) GetCurrentEngineType() EngineType                // 获取当前引擎类型
func (db *DB) GetEngineMode() EngineMode                       // 获取引擎模式

// 统计信息
func (db *DB) GetEngineStats() map[string]interface{}          // 获取引擎统计
func (db *DB) ListAvailableEngines() []EngineType              // 列出可用引擎
func (db *DB) GetEngineInfo(engineType EngineType) (*storage.EngineInfo, error) // 获取引擎信息
```

### 数据操作 (API保持不变)

```go
// 基本操作
func (db *DB) Set(data *utils.Entry) error                     // 写入数据
func (db *DB) Get(key []byte) (*utils.Entry, error)            // 读取数据
func (db *DB) Del(key []byte) error                            // 删除数据

// 迭代器
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator   // 创建迭代器

// 生命周期
func (db *DB) Close() error                                     // 关闭数据库
```

## 配置选项

### 引擎类型
```go
const (
    LSMEngine    = storage.LSMTreeEngine     // LSM树引擎
    BTreeEngine  = storage.BPlusTreeEngine   // B+树引擎
    HashEngine   = storage.HashTableEngine   // 哈希表引擎
    ColumnEngine = storage.ColumnStoreEngine // 列存储引擎
)
```

### 引擎模式
```go
const (
    LegacyMode   EngineMode = iota // 传统模式
    AdapterMode                    // 适配器模式
)
```

## 性能考虑

### LSM引擎适用场景
- 写密集型工作负载
- 时序数据存储
- 日志和事件数据
- 需要高写入吞吐量的应用

### B+树引擎适用场景
- 读密集型工作负载
- 需要频繁范围查询
- OLAP和分析工作负载
- 需要快速点查询的应用

### 切换时机建议
- 工作负载模式发生变化时
- 性能瓶颈出现时
- 数据访问模式改变时
- 定期性能优化时

## 示例代码

完整的使用示例请参考 `examples/engine_switching_example.go`，包括：
- 传统模式使用
- 适配器模式使用
- 引擎切换演示
- 性能对比测试

## 注意事项

1. **数据迁移**: 引擎切换会触发数据迁移，大数据量时可能需要较长时间
2. **内存使用**: 迁移过程中可能会增加内存使用
3. **并发安全**: 切换过程中会暂时阻塞写操作
4. **配置兼容**: 不同引擎的配置参数可能不同
5. **性能影响**: 切换过程中可能会有短暂的性能影响

## 扩展性

JadeDB的引擎架构支持轻松添加新的存储引擎：

1. 实现 `storage.Engine` 接口
2. 创建引擎创建器实现 `storage.EngineCreator` 接口
3. 注册到引擎工厂
4. 即可通过统一API使用新引擎

这种设计使得JadeDB具有良好的扩展性，可以根据不同的使用场景选择最适合的存储引擎。