// Copyright 2021 rookieLiuyutao Project Authors
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
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"unsafe"

	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
)

//-----------------------------------------------------SST在内存中的结构,需要做持久化操作-----------------------------------------------------------------------------------

// tableBuilder 是一个用于构建表的结构体。
// 它包含了与构建过程相关的各种参数和状态，例如表大小、块信息、配置选项等。
type tableBuilder struct {
	// sstSize 表示要构建的 SSTable 的预估大小。
	sstSize int64

	// curBlock 指向当前正在构建的块。
	curBlock *block

	// opt 是构建表的配置选项。
	opt *Options

	// blockList 存储已构建的块列表。
	blockList []*block

	// keyCount 记录已添加的键的数量。
	keyCount uint32

	// keyHashes 存储键的哈希值，以便快速查找。
	keyHashes []uint32

	// maxVersion 记录已添加键的最大版本号。
	maxVersion uint64

	// baseKey 是在当前表中进行查找的基本键。
	baseKey []byte

	// staleDataSize 是无效数据的大小。
	staleDataSize int

	// estimateSz 是用于计算表大小的预估值。
	estimateSz int64
}

// buildData 定义了构建数据的结构，用于存储和操作数据块。
// 它包含数据块列表、索引、校验和以及数据大小。
type buildData struct {
	// blockList 存储了一系列的数据块指针。
	blockList []*block
	// index 保存了数据的索引信息。
	index []byte
	// checksum 保存了数据的校验和，用于验证数据的完整性。
	checksum []byte
	// size 表示数据的大小。
	size int
}

// block 结构体用于表示一个数据块，它包含了数据的元信息和实际内容。
// 该结构体主要用于存储和操作基于给定偏移量的数据块，提供了必要的信息
// 来验证、索引和访问数据块内的数据条目。
type block struct {
	offset            int      // 当前block的offset 首地址
	checksum          []byte   // 数据块的校验和，用于验证数据的完整性
	entriesIndexStart int      // 数据块中条目索引的起始位置
	chkLen            int      // 校验和的长度
	data              []byte   // 数据块中的原始数据内容
	baseKey           []byte   // 数据块的基础键，用于某些加密或哈希计算
	entryOffsets      []uint32 // 数据块中各个数据条目的偏移量数组
	end               int      // 数据块的结束位置
	estimateSz        int64    // 数据块的预估大小
}

// header 结构体用于描述差异存储中差异块的元数据。
// 其中包括了差异块与基础键的重叠部分以及差异的长度。
type header struct {
	overlap uint16 // 与基础键的重叠字节数
	diff    uint16 // 差异的长度
}

const headerSize = uint16(unsafe.Sizeof(header{}))

//----------------------------------------------------------SST在内存中的结构end-------------------------------------------------------------------------------

//================================================================SST接口部分begin===========================================================================================

// seek方法用于在blockIterator中查找给定key的位置。
// 它通过二分查找来定位key所在的条目。
// 参数key是要查找的键。
func (itr *blockIterator) seek(key []byte) {
	// 初始化错误状态为nil，表示当前操作没有错误。
	itr.err = nil

	// startIndex表示应该从哪个索引开始二分查找。
	// 这对于优化重复的seek操作很有用，可以从上一次操作结束的地方开始查找。
	startIndex := 0

	// 使用二分查找确定key应该位于entryOffsets中的哪个位置。
	// sort.Search函数会返回一个索引，该索引处的条目大于或等于给定的key。
	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// 如果当前索引小于起始索引，则直接返回false。
		// 这是为了确保查找操作从正确的索引开始。
		if idx < startIndex {
			return false
		}
		// 设置当前索引为idx，以便后续比较操作可以正确进行。
		itr.setIdx(idx)
		// 比较当前条目的key与给定的key，如果大于或等于，则说明找到了合适的位置。
		return utils.CompareKeys(itr.key, key) >= 0
	})

	// 设置找到的条目索引为当前索引，完成查找操作。
	itr.setIdx(foundEntryIdx)
}

// add函数向tableBuilder中添加一个条目。
// 该函数主要负责将条目(key-value对)添加到当前正在构建的block中，
// 并在需要时结束当前block并创建一个新的block。
// 参数:
//   - e: 一个条目，包含键值对和元数据等信息。
//   - isStale: 表示条目是否过时。
func (tb *tableBuilder) add(e *utils.Entry, isStale bool) error {
	key := e.Key
	val := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}

	// 检查是否需要分配一个新的block
	if tb.tryFinishBlock(e) {
		if isStale {
			// 如果条目是过时的，增加过时数据的大小
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()

		// 创建一个新的block并开始写入
		//TODO: 加密block后块的大小会增加，需要预留一些填充位置,这里先预留20%的空间
		blockSizeWithPadding := int(float64(tb.opt.BlockSize) * 1.2)
		tb.curBlock = &block{
			data: make([]byte, 0, blockSizeWithPadding),
		}
	}

	// 添加键的哈希值到列表中
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))

	// 更新最大版本号
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	// 如果当前block没有基础键，设置基础键为当前键
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		// 计算当前键与基础键的差异
		diffKey = tb.keyDiff(key)
		if len(diffKey) > len(key) {
			diffKey = key // 修正diffKey
		}
	}

	// 确保键的差异符合预期
	if !(len(key)-len(diffKey) <= math.MaxUint16) {
		return fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16")
	}
	if !(len(diffKey) <= math.MaxUint16) {
		return fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16")
	}

	// 创建一个头信息，记录键的重叠部分和差异部分的长度
	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// 将当前block的末尾位置添加到条目偏移量列表中
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// 将头信息编码后追加到当前block的数据中
	tb.append(h.encode())
	// 将键的差异部分追加到当前block的数据中
	tb.append(diffKey)

	// 分配足够的空间来存储当前值，并将值编码后写入
	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)

	return nil
}

//================================================================SST接口部分end===========================================================================================

// Decode 用于解码头部信息。
// 函数通过不安全指针操作将 buf 中的数据复制到 header 结构体中。
// 参数:
//   buf []byte: 包含编码后的头部数据的字节切片。

func (h *header) decode(buf []byte) {
	// 使用 unsafe 包将 buf 中的数据深层复制到 header 结构体中。
	// 解释：通过 unsafe.Pointer 将 header 结构体转换为字节数组，
	// 进行字节级别的复制，确保头部信息正确解码。
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

// encode 将header实例编码为字节切片。
// 这个方法利用了unsafe包进行类型转换，以便将header结构直接转换为字节切片。 这在需要将协议头高效序列化为字节数据时非常有用，例如在网络通信中。
// 返回值是包含header数据的字节切片。
func (h header) encode() []byte {
	// 创建一个长度为4的字节数组，足以容纳header结构。
	var b [4]byte

	// 通过unsafe.Pointer将数组的第一个元素地址转换为header类型的指针，
	// 然后将当前header实例的值赋给这个字节数组。
	// 这里利用了Go语言的unsafe特性，实现了结构到字节的直接转换，
	// 这种方式非常高效，但使用时需要开发者非常小心以避免安全问题。
	*(*header)(unsafe.Pointer(&b[0])) = h

	// 返回字节数组切片，包含了转换后的header数据。
	return b[:]
}

func newTableBuilderWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}
func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

// Empty returns whether it's empty.
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (tb *tableBuilder) finish() []byte {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}

// tryFinishBlock 判断当前块是否需要结束，并基于新条目的加入进行判断。
// 此函数主要检查整数溢出以及估计的块大小是否超过块大小限制。

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	// 检查当前块是否为 nil，如果是，则不需要结束，直接返回 true。
	if tb.curBlock == nil {
		return true
	}

	// 如果当前块没有条目，则块还不能结束，返回 false。
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}

	// 使用 CondPanic 检查计算块大小时的整数溢出。
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("整数溢出"))

	// 计算条目偏移量和其他相关大小。
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // 列表大小
		8 + // checksum proto 中的 Sum64
		4) // 校验和长度

	// 计算当前块的估计大小。
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*条目头部大小*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// 使用 CondPanic 检查计算表大小时的整数溢出。
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("整数溢出"))

	// 判断当前块的估计大小是否超过块大小限制。
	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, true)
}

// AddKey _
func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}

// Close closes the TableBuilder.
func (tb *tableBuilder) Close() {
	// 结合内存分配器
}

// finishBlock 完成当前块的构建并将其追加到块列表中。
// 该方法首先检查当前块是否为空或不包含任何条目偏移量，如果是，则直接返回，不进行任何操作。
// 这是为了避免在空块上执行不必要的操作，提高效率。
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	// 将当前块的条目偏移量列表及其长度追加到构建器中。
	// 这是为了在块内部维护一个关于条目位置的索引，以便后续快速查找。
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	// Calculate and append the checksum for the current block's data.
	// 计算当前块数据的校验和，并将其追加到构建器中。
	// 校验和用于确保块数据的完整性和正确性，防止数据损坏或错误。
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	// 将计算得到的校验和及其长度追加到构建器中。
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

	// 更新估计大小和块列表。
	// 估算是为了后续可能的磁盘写入操作做准备，确保能够合理分配空间和资源。
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: 预估整理builder写入磁盘后，sst文件的大小
	// 更新当前块包含的键数量，以便后续统计或优化使用。
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))

	// 当前块已被序列化到内存，将其设置为nil，表示当前无活动块。
	// 这是为了准备构建下一个块，确保状态清晰。
	tb.curBlock = nil
	return
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// keyDiff 计算新键与当前块基础键的差异部分。
// 该函数用于确定新键与当前块基础键的最长公共前缀的长度， 并返回新键中超出这个公共前缀的部分。
func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	// 逐字节比较新键和当前块的基础键，直到找到第一个不匹配的字节或达到任一键的末尾。
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	// 返回新键中从第一个不匹配的字节开始到末尾的部分。
	return newKey[i:]
}

// TODO: 这里存在多次的用户空间拷贝过程，需要优化
// flush 方法用于将表构建器中的数据刷新到磁盘上的 SSTable 文件中。
// 它接受一个 levelManager 实例和表名作为参数，
// 并返回一个表实例和可能的错误。
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	// 调用 done 方法准备表构建器关闭，返回表的状态信息。
	bd := tb.done()

	// 初始化一个 table 实例，设置 levelManager 和文件标识符。
	t = &table{lm: lm, fid: utils.FID(tableName)}

	// 如果没有builder 则打开一个已经存在的sst文件
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size),
	})

	// 创建一个字节缓冲区，大小与表构建器完成时的大小相同。
	buf := make([]byte, bd.size)

	// 将表构建器中的数据复制到缓冲区中。
	written := bd.Copy(buf)

	// 确保所有数据都被正确复制到缓冲区中。
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))

	// 从 SSTable 文件中获取字节切片，用于写入数据。
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}

	// 将缓冲区中的数据复制到 SSTable 文件的字节切片中。
	copy(dst, buf)

	// 返回表实例和 nil 错误，表示操作成功。
	return t, nil
}

func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// done 方法用于完成表格构建的过程。
// 它会返回一个包含构建数据的 buildData 结构体，包括块列表、索引、校验和和总大小。
func (tb *tableBuilder) done() buildData {
	// 调用 finishBlock 方法来结束最后一个块的构建。
	tb.finishBlock()

	// 如果块列表为空，则返回一个空的 buildData 结构体。
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	// 创建一个 buildData 结构体，包含当前的块列表。
	bd := buildData{
		blockList: tb.blockList,
	}

	// 初始化一个过滤器实例。
	var f utils.Filter

	// 如果设置了布隆过滤器的误报率，则根据当前的键哈希列表创建一个布隆过滤器。
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	// TODO 构建 sst 的索引。
	// 根据过滤器、键哈希列表和选项构建表格的索引，并计算数据部分的大小。
	index, dataSize := tb.buildIndex(f)

	// 计算表格的校验和。
	checksum := tb.calculateChecksum(index)

	// 将索引、校验和和总大小添加到 buildData 结构体中。
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4

	// 返回包含构建数据的 buildData 结构体。
	return bd
}

// buildIndex 构建表索引信息。
// 此函数用于在表构建过程中生成表索引，该索引包含了有关表的元数据信息，如布隆过滤器、键数量和最大版本号。
// 参数 bloom: 布隆过滤器数据，用于快速判断一个元素是否在集合中。
// 返回值:
// - []byte: 序列化后的表索引数据。
// - uint32: 所有数据块的总大小。
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	// 初始化一个表索引对象。
	tableIndex := &pb.TableIndex{}

	// 如果布隆过滤器数据非空，则将其设置到表索引中。
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}

	// 设置表索引的键数量和最大版本号。
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion

	// 生成并设置表索引的偏移量列表，这些偏移量指向表中的各个数据块。
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)

	// 初始化数据大小累加器。
	var dataSize uint32

	// 遍历数据块列表，累加计算所有数据块的总大小。
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}

	// 序列化表索引对象为字节流。
	data, err := tableIndex.Marshal()
	// 确保序列化不发生错误，否则抛出异常。
	utils.Panic(err)

	// 返回序列化后的表索引数据和数据块的总大小。
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

// TODO: 如何能更好的预估builder的长度呢？
func (b *tableBuilder) ReachedCapacity() bool {
	return b.estimateSz > b.sstSize
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

// setBlock 设置迭代器的当前块。这涉及到重置迭代器的状态，
// 包括清除之前块的残留数据，准备好解析新块的内容。
func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

// seekToFirst brings us to the first element.
// 没有参数，因为这个方法使用块迭代器的内部状态来定位到块的第一个元素。
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

// seekToLast brings us to the last element.
// 同样没有参数，理由与seekToFirst相同。
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}

func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key.
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	e := &utils.Entry{Key: itr.key}
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	itr.val = val.Value
	e.Value = val.Value
	e.ExpiresAt = val.ExpiresAt
	e.Meta = val.Meta
	itr.it = &Item{e: e}
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.err != io.EOF // TODO 这里用err比较好
}
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}
func (itr *blockIterator) Item() utils.Item {
	return itr.it
}
func (itr *blockIterator) Close() error {
	return nil
}
