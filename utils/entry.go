package utils

import (
	"encoding/binary"
	"math/bits"
	"time"
)

// Entry 结构体定义了一个条目的各种属性，用于存储和管理具体的数据项。
type Entry struct {
	// ExpiresAt 表示该条目何时过期，以时间戳形式表示。
	ExpiresAt uint64
	// Version 表示条目的版本号，用于版本控制和并发管理。
	Version uint64
	// Hlen 表示条目中哈希头部的长度，以字节为单位。
	// 此值应始终大于0。在实现时，应添加检查以防止创建具有负长度的条目。
	Hlen int
	// ValThreshold 表示值的阈值，用于某些特定的逻辑判断或操作。
	// 阈值机制可以用于快速回收被删除条目占用的空间，提高存储效率。
	// 注意：在实现时，应定义该阈值的具体用途和范围，防止误用。
	ValThreshold int64
	// Key 是条目的键，唯一标识一个条目。
	Key []byte
	// Value 是条目的值，存储具体的数据内容。
	Value []byte
	// Meta 存储关于条目的元数据，如状态、标记等。
	Meta byte
	// Offset 表示条目在存储介质中的偏移量，用于快速定位条目。
	Offset uint32
}

// Header 结构体用于定义数据块的头部信息。
// 它包含了关于数据块内容的长度、过期时间和元数据信息。
type Header struct {
	// KLen 表示键的长度，以字节为单位。
	KLen uint32

	// VLen 表示值的长度，以字节为单位。
	VLen uint32

	// ExpiresAt 表示数据块的过期时间，使用UNIX时间戳格式。
	// 过期时间用于确定数据块何时被视为过期并可能被清除。
	ExpiresAt uint64

	// Meta 包含关于数据块的附加元数据。
	// 元数据可能包括关于存储、安全或其他属性的标记。
	Meta byte
}

// ValueStruct 是一个用于存储值的结构体。
// 它包含了元数据信息、实际的值数据、以及该值的过期时间。
// 此结构体还包含了一个内部使用的版本字段，该字段不会被序列化。
type ValueStruct struct {
	Meta      byte   // 元数据字段，用途和具体含义根据实际业务逻辑定义。
	Value     []byte // 值数据字段，存储实际的值信息，类型为字节切片。
	ExpiresAt uint64 // 过期时间字段，使用Unix时间戳表示，用于确定值的过期时间。

	Version uint64 // 版本字段，用于内部跟踪值的版本变化，不会被序列化。
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
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
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
