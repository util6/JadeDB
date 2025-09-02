/*
JadeDB 条目和数据结构模块

本模块定义了 JadeDB 中的核心数据结构，包括：
- Entry: 键值对条目，是数据库中数据的基本单位
- Header: 数据块头部信息，用于序列化和反序列化
- ValueStruct: 值结构体，用于存储值的详细信息

这些结构体支持：
- 高效的序列化和反序列化
- 过期时间管理
- 版本控制和 MVCC 支持
- 元数据存储
- 内存优化的编码格式

设计原则：
1. 紧凑的内存布局，减少内存占用
2. 高效的编码解码，支持变长编码
3. 灵活的元数据支持，便于扩展
4. 完整的生命周期管理，包括过期处理
*/

package utils

import (
	"encoding/binary"
	"math/bits"
	"time"
)

// Entry 表示数据库中的一个键值对条目。
// 这是 JadeDB 中数据的基本存储单位，包含了键、值以及相关的元数据信息。
//
// 设计考虑：
// 1. 支持 MVCC：通过 Version 字段实现多版本并发控制
// 2. 支持过期：通过 ExpiresAt 字段实现 TTL 功能
// 3. 支持元数据：通过 Meta 字段存储额外的标记信息
// 4. 支持大值优化：通过 ValThreshold 决定值的存储策略
//
// 内存布局优化：
// 字段按照访问频率和对齐要求排列，减少内存占用和提高访问效率。
type Entry struct {
	// 时间相关字段（8字节对齐）

	// ExpiresAt 指定条目的过期时间（Unix 时间戳，秒）。
	// 值为 0 表示永不过期。
	// 过期的条目在读取时会被视为不存在，在压缩时会被清理。
	// 这个字段支持 TTL（Time To Live）功能。
	ExpiresAt uint64

	// Version 是条目的版本号，用于 MVCC（多版本并发控制）。
	// 每次更新条目时，版本号会递增。
	// 在并发环境下，通过版本号可以检测冲突和实现快照隔离。
	Version uint64

	// 配置相关字段

	// Hlen 表示哈希头部的长度（字节）。
	// 这个字段在某些特殊的存储格式中使用，通常应该大于 0。
	// 主要用于兼容特定的数据格式或协议。
	Hlen int

	// ValThreshold 是值大小的阈值（字节）。
	// 小于此阈值的值直接存储在 LSM 树中。
	// 大于此阈值的值存储在值日志中，LSM 树只存储指针。
	// 这个设计可以减少 LSM 树的写放大问题。
	ValThreshold int64

	// 数据字段

	// Key 是条目的键，用于唯一标识一个条目。
	// 键的设计应该考虑排序性能和存储效率。
	// 在 LSM 树中，键用于索引和范围查询。
	Key []byte

	// Value 是条目的值，存储实际的数据内容。
	// 根据 ValThreshold 的设置，值可能直接存储或通过指针引用。
	// 支持任意二进制数据，包括序列化的对象。
	Value []byte

	// 元数据字段

	// Meta 存储条目的元数据标记（1字节）。
	// 可以包含删除标记、值指针标记、压缩标记等。
	// 通过位操作可以存储多个布尔标记。
	Meta byte

	// Offset 表示条目在存储文件中的偏移量。
	// 主要用于值日志中的定位和垃圾回收。
	// 在内存中的条目，这个字段可能不使用。
	Offset uint32
}

// Header 定义了数据块的头部信息结构。
// 在序列化和反序列化过程中，Header 提供了解析数据块所需的元信息。
//
// 用途：
// 1. 在值日志中标识每个条目的结构
// 2. 在网络传输中描述数据包格式
// 3. 在文件存储中提供数据块的索引信息
//
// 编码格式：
// +------+------------+--------------+-----------+
// | Meta | Key Length | Value Length | ExpiresAt |
// +------+------------+--------------+-----------+
// |  1B  |   Varint   |    Varint    |  Varint   |
// +------+------------+--------------+-----------+
//
// 使用变长编码（Varint）可以节省空间，特别是对于小的长度值。
type Header struct {
	// KLen 指定键的字节长度。
	// 使用 uint32 可以支持最大 4GB 的键，在实际应用中足够使用。
	// 大多数情况下键都比较短，使用变长编码可以节省空间。
	KLen uint32

	// VLen 指定值的字节长度。
	// 对于存储在值日志中的大值，这个字段记录了实际的值大小。
	// 支持最大 4GB 的值，满足大多数应用需求。
	VLen uint32

	// ExpiresAt 指定数据块的过期时间（Unix 时间戳，秒）。
	// 值为 0 表示永不过期。
	// 在读取时会检查这个时间戳，过期的数据会被忽略。
	// 在垃圾回收时，过期的数据会被清理。
	ExpiresAt uint64

	// Meta 存储数据块的元数据标记。
	// 可以包含删除标记、压缩标记、加密标记等信息。
	// 通过位操作可以在一个字节中存储多个布尔标记。
	// 常见的标记包括：
	// - 删除标记：表示这是一个墓碑记录
	// - 值指针标记：表示值存储在值日志中
	// - 压缩标记：表示值已被压缩
	Meta byte
}

// ValueStruct 表示一个值的完整结构。
// 这个结构体用于在内存中表示值的所有相关信息，包括数据和元数据。
//
// 设计考虑：
// 1. 紧凑的内存布局，减少内存占用
// 2. 支持序列化和反序列化
// 3. 包含完整的生命周期信息
// 4. 支持版本控制和并发访问
//
// 序列化格式：
// +------+-----------+-------+
// | Meta | ExpiresAt | Value |
// +------+-----------+-------+
// |  1B  |  Varint   |  Data |
// +------+-----------+-------+
//
// 注意：Version 字段不会被序列化，只用于内存中的版本跟踪。
type ValueStruct struct {
	// Meta 存储值的元数据标记。
	// 与 Entry 和 Header 中的 Meta 字段含义相同。
	// 用于标记值的特殊属性，如删除状态、压缩状态等。
	Meta byte

	// Value 存储实际的值数据。
	// 支持任意二进制数据，包括文本、序列化对象、图片等。
	// 长度由外部管理，通常在 Header 或 Entry 中记录。
	Value []byte

	// ExpiresAt 指定值的过期时间（Unix 时间戳，秒）。
	// 值为 0 表示永不过期。
	// 这个字段会被序列化，确保过期信息的持久化。
	ExpiresAt uint64

	// Version 用于内部版本跟踪，支持 MVCC。
	// 这个字段不会被序列化到磁盘或网络。
	// 主要用于内存中的并发控制和冲突检测。
	// 每次值更新时，版本号会递增。
	Version uint64
}

// ---------------------------------------------工具方法------------------------------------------
// sizeVarint 计算给定无符号64位整数x的变长编码所需的字节数。
// 变长编码是一种用于存储整数的编码方式，其中每个整数都由一个或多个字节组成，
// 每个字节的最高位（第8位）表示是否还有更多的字节跟随。
// 该函数通过连续右移x来计算需要多少个这样的字节来编码x。
// 参数:
//
//	x - 需要计算变长编码大小的无符号64位整数。
//
// 返回值:
//
//	n - 编码给定整数所需的字节数。

func sizeVarInt(v uint64) int {
	// This computes 1 + (bits.Len64(v)-1)/7.
	// 9/64 is a good enough approximation of 1/7
	return int(9*uint32(bits.Len64(v))+64) / 64
}

//---------------------------------------------工具方法 end------------------------------------------

//---------------------------------------------Entry 相关方法---------------------------------------------

/*
	NewEntry 创建一个新的Entry实例。
	该函数接收两个参数：key和value，这两个参数都是字节切片类型。
	返回值是一个指向Entry结构体的指针，该结构体包含了传入的key和value。

*/

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// Entry 方法返回当前 Entry 实例。
// 该方法的存在是为了提供一个统一的接口，使得外部可以一致地获取 Entry 实例。
// 参数: 无
// 返回值: 当前的 Entry 实例
func (e *Entry) Entry() *Entry {
	return e
}

// IsDeletedOrExpired 检查条目是否已被删除或过期。
// 如果条目的值为nil，表示条目已被删除或从未被设置，函数返回true。
// 如果条目设置了过期时间且当前时间已超过过期时间，则认为条目已过期，函数返回true。
// 如果条目没有过期时间（ExpiryAt为0），则认为条目既未被删除也未过期，函数返回false。
func (e *Entry) IsDeletedOrExpired() bool {
	// 检查条目的值是否为空，为空则认为条目已被删除
	if e.Value == nil {
		return true
	}

	// 检查条目是否设置了过期时间，没有设置则认为条目未过期
	if e.ExpiresAt == 0 {
		return false
	}

	// 比较当前时间与条目的过期时间，判断条目是否已过期
	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// WithTTL _
// WithTTL 方法用于为一个 Entry 实例设置过期时间。
// 该方法通过指定一个时间间隔 dur 来设置 Entry 的过期时间，
// 过期时间从当前时间开始计算。
// 参数:
//
//	dur - time.Duration 类型，表示要设置的时间间隔。
//
// 返回值:
//
//	返回更新了过期时间的 Entry 实例。
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	// 将当前时间加上指定的时间间隔 dur，然后转换为 Unix 时间戳，
	// 最后将结果赋值给 Entry 实例的 ExpiresAt 字段。
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	// 返回更新后的 Entry 实例。
	return e
}

// EncodedSize 计算Entry编码后的大小。
// 该函数通过计算Value字段的长度和Meta、ExpiresAt字段的变长编码长度之和来实现。
// 返回值为编码后的总大小（以uint32类型表示）。
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarInt(uint64(e.Meta))
	enc += sizeVarInt(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize 估算条目大小。
// 如果值的长度小于给定阈值，则假设值是内联的，大小计算包括键、值和一个字节的元数据。
// 否则，假设值是指向外部的，大小计算包括键、一个12字节的值指针和两个字节的元数据。
// 参数:
//
//	threshold - 用于决定值是否内联的阈值。
//
// 返回值:
//
//	条目的估计大小。
func (e *Entry) EstimateSize(threshold int) int {
	// 检查值的长度是否小于阈值，用于决定是否内联值。
	if len(e.Value) < threshold {
		// 当值内联时，大小包括键长、值长和一个字节的元数据。
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	// 当值不内联时，大小包括键长、一个12字节的值指针和两个字节的元数据。
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 2 for meta.
}

//---------------------------------------------Entry 相关方法 end---------------------------------------------

//---------------------------------------------Header 相关方法---------------------------------------------

// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
// Encode 将 Header 结构体编码为字节数组。
// 参数 out 是一个字节数组，用于存储编码后的结果。
// 返回值是编码到 out 中的字节数。

// Encode 将 Header 结构体编码为字节数组。
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode 从字节数组中解码出Header对象。
// buf: 输入的字节数组，包含Header的所有信息。
// 返回值: 解码后消耗的字节数。
func (h *Header) Decode(buf []byte) int {
	if len(buf) == 0 {
		return 0
	}
	h.Meta = buf[0]
	index := 1
	if index >= len(buf) {
		return index
	}
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	if index >= len(buf) {
		return index
	}
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	if index >= len(buf) {
		return index
	}
	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}

// DecodeFrom reads the header from the hashReader.
// Returns the number of bytes read.
func (h *Header) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

//---------------------------------------------Header 相关方法 end---------------------------------------------

//---------------------------------------------ValueStruct 相关方法---------------------------------------------

// value只持久化具体的value值和过期时间
// EncodedSize 计算ValueStruct序列化后的大小。
// 它通过计算值字段的长度加上元数据字节的大小，
// 再加上过期时间字段的变长编码大小，来得出总的编码大小。
func (vs *ValueStruct) EncodedSize() uint32 {
	// 计算值字段的长度加上一个字节的元数据大小
	sz := len(vs.Value) + 1 // meta
	// 获取过期时间字段的变长编码大小
	enc := sizeVarInt(vs.ExpiresAt)
	// 返回总的编码大小
	return uint32(sz + enc)
}

// DecodeValue
// DecodeValue 解码字节切片中的数据到ValueStruct中。
// buf: 输入的字节切片，包含编码后的值。
func (vs *ValueStruct) DecodeValue(buf []byte) {
	// 解码元数据到Meta字段。
	vs.Meta = buf[0]

	// 定义变量sz用于存储解析出的长度。
	var sz int
	// 从buf的第二个字节开始解析出过期时间。
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	// 根据解析出的长度，提取出Value字段对应的字节切片。
	vs.Value = buf[1+sz:]
}

// 对value进行编码，并将编码后的字节写入byte
// 这里将过期时间和value的值一起编码
// EncodeValue 将ValueStruct中的值编码到给定的字节切片中。
// 它返回编码后使用的字节数。
// 参数b是用于存储编码值的字节切片。
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	// 将元数据编码到字节切片的第一个位置。
	b[0] = vs.Meta

	// 使用变长编码方式将过期时间编码到字节切片中。
	// binary.PutUvarint函数用于将无符号整数以变长编码形式写入到切片中。
	// 返回值sz表示已编码的过期时间所占用的字节数。

	/*函数调用: binary.PutUvarint(b[1:], vs.ExpiresAt)
	binary.PutUvarint 是一个用于编码无符号整数到字节切片的函数。
	第一个参数是一个字节切片的引用：b[1:]。这表示从b切片的第二个元素开始存放编码结果。
	第二个参数是需要编码的无符号整数值：vs.ExpiresAt。

	*/
	//sz表示已编码的过期时间所占用的字节数。
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)

	// 将值复制到字节切片的剩余部分。
	// copy函数用于安全地复制内存，n表示复制的字节数。
	n := copy(b[1+sz:], vs.Value)

	// 返回编码后使用的总字节数，包括元数据、过期时间和值所占用的字节数。
	return uint32(1 + sz + n)
}
