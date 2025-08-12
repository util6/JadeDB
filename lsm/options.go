// Copyright 2021 hardcore-o Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
JadeDB 配置选项模块

本模块定义了 JadeDB 数据库的所有配置选项，包括：
- 存储相关配置：工作目录、表大小、值阈值等
- 性能相关配置：批处理大小、内存表大小等
- 功能相关配置：校验和验证、冲突检测等

配置选项的设计原则：
1. 提供合理的默认值，开箱即用
2. 支持根据不同场景进行调优
3. 参数之间保持一致性和兼容性
4. 便于理解和维护
*/

package lsm

import "github.com/util6/JadeDB/utils"

// Options 定义了 JadeDB 数据库的完整配置选项。
// 这个结构体包含了数据库运行所需的所有参数，从存储配置到性能调优。
// 通过合理配置这些选项，可以针对不同的使用场景优化数据库性能。
type Options struct {
	// 值存储相关配置

	// ValueThreshold 决定值是直接存储在 LSM 树中还是存储在值日志中的阈值（字节）。
	// 小于此阈值的值直接存储在 LSM 树中，大于此阈值的值存储在值日志中。
	// 较小的阈值可以减少 LSM 树的大小但增加值日志的访问；
	// 较大的阈值可以减少值日志的访问但增加 LSM 树的大小。
	// 推荐值：64-1024 字节，根据实际值大小分布调整。
	ValueThreshold int64

	// 存储路径配置

	// WorkDir 指定数据库文件的存储目录。
	// 这个目录将包含所有的 SSTable 文件、值日志文件、WAL 文件和元数据文件。
	// 确保该目录有足够的磁盘空间和适当的读写权限。
	// 建议使用 SSD 存储以获得更好的性能。
	WorkDir string

	// 内存和表大小配置

	// MemTableSize 指定内存表（MemTable）的最大大小（字节）。
	// 当内存表达到此大小时，会被刷新到磁盘成为 SSTable。
	// 较大的内存表可以减少磁盘写入频率但占用更多内存；
	// 较小的内存表占用内存少但增加磁盘写入频率。
	// 推荐值：64MB-256MB，根据可用内存调整。
	MemTableSize int64

	// SSTableMaxSz 指定单个 SSTable 文件的最大大小（字节）。
	// 当 SSTable 超过此大小时，会在压缩过程中被分割。
	// 较大的 SSTable 可以减少文件数量但增加读取延迟；
	// 较小的 SSTable 可以提高读取性能但增加文件管理开销。
	// 推荐值：64MB-256MB。
	SSTableMaxSz int64

	// 批处理配置

	// MaxBatchCount 指定单个批处理操作中的最大条目数量。
	// 超过此数量的批处理操作会被拒绝，返回错误。
	// 这个限制有助于控制内存使用和操作延迟。
	// 推荐值：1000-10000，根据条目大小调整。
	MaxBatchCount int64

	// MaxBatchSize 指定单个批处理操作的最大总大小（字节）。
	// 超过此大小的批处理操作会被拒绝，返回错误。
	// 这个限制有助于控制内存使用和网络传输。
	// 推荐值：1MB-16MB。
	MaxBatchSize int64

	// 值日志配置

	// ValueLogFileSize 指定单个值日志文件的大小（字节）。
	// 当值日志文件达到此大小时，会创建新的日志文件。
	// 较大的日志文件可以减少文件数量但增加垃圾回收时间；
	// 较小的日志文件便于管理但增加文件操作开销。
	// 推荐值：1GB-4GB。
	ValueLogFileSize int

	// VerifyValueChecksum 指定是否对值日志中的数据进行校验和验证。
	// 启用此选项可以检测数据损坏，但会增加读取开销。
	// 在数据完整性要求高的场景下建议启用。
	VerifyValueChecksum bool

	// ValueLogMaxEntries 指定单个值日志文件中的最大条目数量。
	// 这是一个额外的限制，与文件大小限制配合使用。
	// 有助于控制单个文件的复杂度和垃圾回收效率。
	ValueLogMaxEntries uint32

	// 日志轮转配置

	// LogRotatesToFlush 指定在强制刷新内存表之前允许的日志轮转次数。
	// 较大的值可以减少磁盘写入但增加恢复时间；
	// 较小的值可以加快恢复但增加磁盘写入频率。
	// 推荐值：2-5。
	LogRotatesToFlush int32

	// 表大小限制

	// MaxTableSize 指定表的最大大小限制（字节）。
	// 这是一个全局限制，用于控制整体存储使用。
	// 当接近此限制时，数据库可能会触发更积极的压缩策略。
	MaxTableSize int64

	// 功能开关

	// DetectConflicts 指定是否启用事务冲突检测。
	// 启用此选项可以检测并发事务之间的冲突，但会增加处理开销。
	// 在需要严格事务隔离的场景下建议启用。
	DetectConflicts bool

	// LSM特定字段 - 从lsm.go中移过来的

	// BlockSize 设置 SSTable 中数据块的字节大小。
	BlockSize int

	// BloomFalsePositive 设置布隆过滤器的假阳性率。
	BloomFalsePositive float64

	// NumCompactors 设置并发压缩工作器的数量。
	NumCompactors int

	// BaseLevelSize 设置基础层（通常是 L1）的目标大小。
	BaseLevelSize int64

	// LevelSizeMultiplier 设置相邻层级之间的大小倍数。
	LevelSizeMultiplier int

	// TableSizeMultiplier 设置表大小的增长倍数。
	TableSizeMultiplier int

	// BaseTableSize 设置基础层中 SSTable 的目标大小。
	BaseTableSize int64

	// NumLevelZeroTables 设置 L0 层允许的最大 SSTable 数量。
	NumLevelZeroTables int

	// MaxLevelNum 设置 LSM 树的最大层级数。
	MaxLevelNum int

	// MaxImmutableTables 设置最大不可变内存表数量。
	// 当不可变内存表数量超过此限制时，会强制刷盘。
	// 较小的值可以减少内存使用但增加磁盘写入频率；
	// 较大的值可以减少磁盘写入但占用更多内存。
	// 推荐值：4-8。
	MaxImmutableTables int

	// DiscardStatsCh 提供丢弃统计信息的通道。
	DiscardStatsCh *chan map[uint32]int64
}

// NewDefaultOptions 创建并返回一个包含默认配置的 Options 实例。
// 这些默认值适用于大多数常见使用场景，提供了性能和资源使用的平衡。
//
// 默认配置说明：
// - WorkDir: "./work_test" - 使用当前目录下的测试目录
// - MemTableSize: 1024 字节 - 较小的内存表，适合测试
// - SSTableMaxSz: 1GB - 较大的 SSTable，适合生产环境
// - ValueThreshold: 使用工具包中的默认值
//
// 返回值：
// 配置了默认值的 Options 实例
//
// 使用建议：
// 1. 生产环境中应根据实际需求调整这些默认值
// 2. 特别注意 WorkDir、MemTableSize 和 ValueThreshold 的设置
// 3. 根据硬件配置和数据特征进行性能调优
func NewDefaultOptions() *Options {
	// 创建基础配置实例
	opt := &Options{
		WorkDir:      "./work_test", // 默认工作目录，生产环境应修改
		MemTableSize: 1024,          // 1KB，测试用的小内存表
		SSTableMaxSz: 1 << 30,       // 1GB，生产环境的合理大小

		// LSM特定字段的默认值
		BlockSize:           4096,     // 4KB
		BloomFalsePositive:  0.01,     // 1%
		NumCompactors:       2,        // 2个压缩器
		BaseLevelSize:       10 << 20, // 10MB
		LevelSizeMultiplier: 10,       // 10倍增长
		TableSizeMultiplier: 2,        // 2倍增长
		BaseTableSize:       2 << 20,  // 2MB
		NumLevelZeroTables:  4,        // L0层最多4个表
		MaxLevelNum:         7,        // 最多7层
		MaxImmutableTables:  6,        // 最多6个不可变内存表
	}

	// 设置值阈值为工具包中定义的默认值
	// 这个值经过优化，适合大多数使用场景
	opt.ValueThreshold = utils.DefaultValueThreshold

	return opt
}
