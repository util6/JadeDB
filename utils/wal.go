// Copyright 2021 logicrec Project Authors
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

package utils

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
)

// LogEntry
// LogEntry 定义了日志条目的操作函数类型。
// 该函数用于处理特定的日志条目，包含其操作逻辑。
// 参数 e 是指向 Entry 结构体的指针，包含了日志条目的具体信息。
// 参数 vp 是指向 ValuePtr 结构体的指针，代表了日志条目关联的值的指针。
// 返回值为 error 类型，表示在处理日志条目时可能遇到的错误。
type LogEntry func(e *Entry, vp *ValuePtr) error

// WalHeader 结构体定义了 Write-Ahead Log (WAL) 的头部信息。
// 它包含了记录此日志条目所需的关键信息，如键值对的长度、元数据和过期时间。
// KeyLen 表示键的长度。
// ValueLen 表示值的长度。
// Meta 表示与日志条目相关的元数据。
// ExpiresAt 表示日志条目过期的时间戳。
type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

const maxHeaderSize int = 21

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode 从给定的 HashReader 中解码 WAL（Write-Ahead Log）头信息。
// 它读取并解析键的长度、值的长度、元数据和过期时间。
// 参数:
//
//	reader - 用于从底层字节流中读取数据的 HashReader。
//
// 返回值:
//
//	读取的字节数，可能的错误。
func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	// 读取键长度
	var err error
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	// 读取值长度
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	// 读取元数据
	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)

	// 读取过期时间
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}

	// 返回已读取的字节数和可能的错误
	return reader.BytesRead, nil
}

// WalCodec 写入wal文件的编码
// | header | key | valueIndex | crc32 |
// WalCodec 编码给定的 Entry 对象到 Wal 格式。
// 它首先重置提供的缓冲区，然后将 Entry 的 Key、Value 以及过期时间编码到缓冲区中，
// 并在末尾附加一个 CRC32 校验和以确保数据的完整性。
// 返回值是编码后数据的总长度。
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	// 重置缓冲区，以便从头开始写入。
	buf.Reset()

	// 初始化 WalHeader 结构体，用于存储 Key 和 Value 的长度以及过期时间。
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}

	// 创建 CRC32 校验和的哈希对象，用于计算数据的校验和。
	hash := crc32.New(CastagnoliCrcTable)
	// 创建一个多重写入器，同时写入缓冲区和哈希对象，以便在写入数据时同时计算校验和。
	writer := io.MultiWriter(buf, hash)

	// 编码头部信息。
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	// 写入头部信息。
	Panic2(writer.Write(headerEnc[:sz]))
	// 写入 Key。
	Panic2(writer.Write(e.Key))
	// 写入 Value。
	Panic2(writer.Write(e.Value))
	// 写入 CRC32 校验和。
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	Panic2(buf.Write(crcBuf[:]))
	// 返回编码后的总长度。
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

// EstimateWalCodecSize 预估当前kv 写入wal文件占用的空间大小
func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + maxHeaderSize
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int // Number of bytes read.
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}

// IsZero _
func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

// LogHeaderLen _
func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

// LogOffset _
func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
