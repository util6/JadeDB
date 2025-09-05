package percolator

import (
	"time"

	"github.com/util6/JadeDB/common"
)

// PercolatorLockRecord Percolator锁记录
// 对应需求文档中的Lock列族
type PercolatorLockRecord struct {
	// 锁的基本信息
	TxnID      string          `json:"txn_id"`      // 事务ID
	StartTS    uint64          `json:"start_ts"`    // 开始时间戳
	PrimaryKey []byte          `json:"primary_key"` // 主键
	LockType   common.LockType `json:"lock_type"`   // 锁类型（Put/Delete)
	TTL        uint64          `json:"ttl"`         // 生存时间(秒)
	CreatedAt  time.Time       `json:"created_at"`  // 创建时间

	// 扩展信息
	ValueSize int `json:"value_size"` // 值大小（用于优化）
}

// PercolatorWriteRecord Percolator写入记录
// 对应需求文档中的Write列族
type PercolatorWriteRecord struct {
	StartTS   uint64           `json:"start_ts"`   // 开始时间戳
	CommitTS  uint64           `json:"commit_ts"`  // 提交时间戳
	WriteType common.WriteType `json:"write_type"` // 写入类型
	TxnID     string           `json:"txn_id"`     // 事务ID
	CreatedAt time.Time        `json:"created_at"` // 创建时间
}

// PercolatorDataRecord Percolator数据记录
// 对应需求文档中的Data列族
type PercolatorDataRecord struct {
	Value     []byte    `json:"value"`      // 实际数据值
	StartTS   uint64    `json:"start_ts"`   // 开始时间戳
	TxnID     string    `json:"txn_id"`     // 事务ID
	CreatedAt time.Time `json:"created_at"` // 创建时间
}

// 注意：WriteType已移至common包，避免重复定义

// 注意：PercolatorTxnState已移至common包，避免重复定义

// 注意：PercolatorTxnInfo已移至common包，避免重复定义
