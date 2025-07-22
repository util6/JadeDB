/*
JadeDB Count-Min Sketch 频率估计器

Count-Min Sketch 是一种概率型数据结构，用于高效地估计数据流中元素的频率。
在 JadeDB 的缓存系统中，它用于跟踪数据的访问频率，支持智能的缓存决策。

核心特性：
1. 空间高效：使用固定大小的二维数组，空间复杂度为 O(w*d)
2. 时间高效：插入和查询操作都是 O(d) 时间复杂度
3. 近似准确：提供频率的上界估计，误差可控
4. 流式处理：支持在线处理大规模数据流

工作原理：
- 二维数组：d 行 w 列的计数器数组
- 多重哈希：每行使用不同的哈希函数
- 插入操作：对每行对应位置的计数器加 1
- 查询操作：返回所有行中对应位置的最小值

数学基础：
- 误差界限：ε = e/w，其中 e 是自然常数，w 是列数
- 置信度：δ = (1/2)^d，其中 d 是行数
- 空间复杂度：O(log(1/δ) * 1/ε)

在缓存系统中的应用：
- 频率统计：跟踪数据的访问频率
- 准入控制：基于频率决定是否缓存数据
- 晋升决策：高频数据从低级缓存晋升到高级缓存
- 淘汰策略：结合 LRU 实现更智能的淘汰

优化特性：
- 4位计数器：每个计数器使用 4 位，节省内存
- 饱和计数：计数器达到最大值时不再增加
- 快速哈希：使用高效的哈希函数族
- 缓存友好：紧凑的内存布局
*/

package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	// cmDepth 定义 Count-Min Sketch 的深度（行数）。
	// 深度为 4 提供了良好的准确性和性能平衡：
	// - 置信度：(1/2)^4 = 6.25%，即 93.75% 的置信度
	// - 计算开销：每次操作需要 4 次哈希计算
	// - 内存开销：相对较小的存储空间
	cmDepth = 4
)

// cmRow 表示 Count-Min Sketch 中的一行计数器。
// 每个字节包含两个 4 位计数器，最大计数值为 15。
//
// 存储格式：
// - 高 4 位：第一个计数器
// - 低 4 位：第二个计数器
// - 饱和计数：达到 15 时不再增加
//
// 设计优势：
// - 内存紧凑：每个计数器只占用 4 位
// - 访问高效：通过位运算快速访问和更新
// - 饱和保护：防止计数器溢出
type cmRow []byte

// newCmRow 创建一个新的计数器行。
// 参数 numCounters 指定计数器的数量，实际分配的字节数为 numCounters/2。
//
// 参数说明：
// numCounters: 计数器的总数量
//
// 返回值：
// 初始化的计数器行
//
// 内存布局：
// 由于每个字节包含两个计数器，所以字节数 = 计数器数 / 2
func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// cmSketch 实现了 Count-Min Sketch 数据结构。
// 它使用多行计数器和不同的哈希函数来估计元素的频率。
//
// 数据结构：
// - 二维计数器数组：d 行 w 列
// - 哈希函数族：每行使用不同的哈希种子
// - 位掩码：用于快速计算哈希位置
//
// 算法特点：
// - 插入：对每行对应位置的计数器递增
// - 查询：返回所有行中对应位置的最小值
// - 重置：支持周期性重置，适应访问模式变化
//
// 误差分析：
// - 过估计：可能高估频率，但不会低估
// - 误差界限：与数据流大小和数组宽度相关
// - 置信度：与数组深度相关
type cmSketch struct {
	// 计数器数组

	// rows 是二维计数器数组的行集合。
	// 每行使用不同的哈希函数，提供独立的频率估计。
	// 固定深度为 cmDepth，确保一致的性能特征。
	rows [cmDepth]cmRow

	// 哈希参数

	// seed 存储每行使用的哈希种子。
	// 不同的种子确保每行使用不同的哈希函数。
	// 随机生成的种子提供良好的哈希分布。
	seed [cmDepth]uint64

	// 位掩码

	// mask 用于将哈希值映射到有效的计数器位置。
	// 通常为 (numCounters - 1)，支持快速的模运算。
	// 要求计数器数量为 2 的幂，以实现高效的位运算。
	mask uint64
}

// newCmSketch 函数用于创建一个新的 Count-Min Sketch 数据结构实例。
// 参数 numCounters 指定了计数器的数量。 该函数会检查 numCounters 是否为零，如果是则抛出异常。
// 然后，它会确保 numCounters 是2的幂，并初始化 cmSketch 结构体的各个字段，包括 mask、seed 和 rows。
// 参数 numCounters 指定计数器的总数，必须是一个正整数。 如果 numCounters 为 0，将触发 panic，因为这表示输入参数无效。
func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}

	// 将 numCounters 调整为最接近的更大或相等的二次幂。
	// 这是因为 Count-Min Sketch 的效率在计数器数量为二次幂时最佳。
	numCounters = next2Power(numCounters)

	// 创建 cmSketch 实例。 mask 用于高效计算哈希函数的索引，通过对 numCounters 减 1 得到。
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	// 初始化一个随机数源，用于哈希函数的种子。
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 对于每一行，初始化一个随机数种子和一个计数器数组。
	// 这里的循环运行 cmDepth 次，是 Count-Min Sketch 的一个固定深度参数。
	for i := 0; i < cmDepth; i++ {
		// 为每一行生成一个随机数种子。
		sketch.seed[i] = source.Uint64()
		// 初始化该行的计数器数组。
		sketch.rows[i] = newCmRow(numCounters)
	}

	// 最后，返回初始化完成的 cmSketch 实例。
	return sketch
}

// Increment 增加给定哈希值在计数器中的计数。
// 该方法首先对每一行使用不同的种子进行处理。
// 它通过异或操作和应用掩码来更新每一行的计数。
func (s *cmSketch) Increment(hashed uint64) {
	// 遍历所有行并执行相同的操作
	for i := range s.rows {
		// 使用当前行的种子对哈希值进行调整，并使用掩码进行过滤后增加计数
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// Estimate 估算给定哈希值的计数
// 该方法利用计数迷糊(CM Sketch)数据结构来估算某个元素的出现频率
// 参数: hashed: 待估算元素的哈希值
// 返回值: int64: 估算的计数值
func (s *cmSketch) Estimate(hashed uint64) int64 {
	// 初始化最小值为255，这是一个较高的起始值，确保在比较中会被更新
	min := byte(255)

	// 遍历每一行，使用不同的种子来获取哈希值，并以此查询CM Sketch中的计数值
	for i := range s.rows {
		// 使用异或操作和当前行的种子调整哈希值，然后与掩码进行与操作以确定索引
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		// 如果当前行的计数值小于已知的最小值，则更新最小值
		if val < min {
			min = val
		}
	}

	// 返回最小计数值作为估算结果
	return int64(min)
}

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

func (r cmRow) increment(n uint64) {
	i := n / 2
	s := (n & 1) * 4
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += 1 << s
	}
}

func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

// cmRow.string 方法以字符串形式展示r的详细信息。
// 该方法遍历r中的每个元素，并将每个元素的每一位按照特定格式转换为十六进制表示。
// 参数: 无
// 返回值: 字符串，表示r的详细信息。
func (r cmRow) string() string {
	// 初始化字符串s为空。
	s := ""
	// 遍历r中的每个元素两次，以便处理每个元素的两位。
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		// 使用位操作获取每个元素的每一位，并将其转换为两位的十六进制字符串。
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	// 移除最后添加的空格。
	s = s[:len(s)-1]
	// 返回格式化后的字符串。
	return s
}
