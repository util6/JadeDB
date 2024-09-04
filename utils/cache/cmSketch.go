package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// cmSketch 结构体代表一个计数明文布隆过滤器，用于快速判断一个元素是否在一个集合中。
// 它通过少量的哈希函数和位数组来实现高效的查询操作，尽管会存在一定的误判率。
type cmSketch struct {
	// rows 数组存储了布隆过滤器的位数组行，每一行都是一个 cmRow 结构体，包含了一行的位信息。
	rows [cmDepth]cmRow
	// seed 数组存储了用于哈希计算的种子，每个种子对应一个哈希函数，用于确定元素在位数组中的位置。
	seed [cmDepth]uint64
	// mask 用于在哈希计算时快速定位到位数组的有效范围，通常是一个位掩码，确保哈希值落在正确的区间内。
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
