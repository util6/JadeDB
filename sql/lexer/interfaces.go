package lexer

import (
	"fmt"
	"time"
)

// Lexer 词法分析器接口
type Lexer interface {
	Tokenize(input string) ([]Token, error)
	NextToken() Token
	Reset(input string)
	GetPosition() Position
}

// Token 词法单元 - 参考TiDB设计
type Token struct {
	Type     TokenType     // Token类型
	Value    string        // Token值（标准化后）
	Raw      string        // 原始文本（保留格式）
	Position Position      // 开始位置
	EndPos   Position      // 结束位置
	Line     int           // 行号（兼容性）
	Column   int           // 列号（兼容性）
}



// TokenType Token类型枚举 - 参考TiDB设计
type TokenType int

const (
	// 特殊Token
	INVALID TokenType = iota
	EOF
	
	// 字面量类型
	IDENTIFIER
	INTEGER_LIT
	FLOAT_LIT
	STRING_LIT
	HEX_LIT
	BIT_LIT
	NULL_LIT
	TRUE_LIT
	FALSE_LIT
	
	// 注释和空白
	COMMENT
	WHITESPACE
	
	// 操作符 - 按优先级分组
	// 算术操作符
	PLUS     // +
	MINUS    // -
	MULTIPLY // *
	DIVIDE   // /
	MODULO   // %
	
	// 比较操作符
	EQUAL         // =
	NOT_EQUAL     // != 或 <>
	LESS          // <
	LESS_EQUAL    // <=
	GREATER       // >
	GREATER_EQUAL // >=
	NULL_SAFE_EQUAL // <=>
	
	// 逻辑操作符
	AND_OP // &&
	OR_OP  // ||
	NOT_OP // !
	
	// 位操作符
	BIT_AND    // &
	BIT_OR     // |
	BIT_XOR    // ^
	BIT_NOT    // ~
	LEFT_SHIFT // <<
	RIGHT_SHIFT // >>
	
	// 赋值操作符
	ASSIGN // :=
	
	// 分隔符和标点
	COMMA       // ,
	SEMICOLON   // ;
	LEFT_PAREN  // (
	RIGHT_PAREN // )
	LEFT_BRACE  // {
	RIGHT_BRACE // }
	LEFT_BRACKET // [
	RIGHT_BRACKET // ]
	DOT         // .
	QUESTION    // ?
	COLON       // :
	DOUBLE_COLON // ::
	
	// SQL关键字 - 按字母顺序
	ADD
	ALL
	ALTER
	AND
	AS
	ASC
	BETWEEN
	BY
	CASE
	COLUMN
	CONSTRAINT
	CREATE
	CROSS
	DATABASE
	DEFAULT
	DELETE
	DESC
	DISTINCT
	DROP
	ELSE
	END
	EXISTS
	FALSE
	FOREIGN
	FROM
	FULL
	GROUP
	HAVING
	IF
	IN
	INDEX
	INNER
	INSERT
	INTO
	IS
	JOIN
	KEY
	LEFT
	LIKE
	LIMIT
	NOT
	NULL_KW
	OFFSET
	ON
	OR
	ORDER
	OUTER
	PRIMARY
	REFERENCES
	RIGHT
	SELECT
	SET
	TABLE
	THEN
	TRUE
	UNION
	UNIQUE
	UPDATE
	VALUES
	WHEN
	WHERE
	
	// 数据类型关键字
	BIGINT
	BINARY
	BIT
	BLOB
	BOOLEAN
	CHAR
	DATE
	DATETIME
	DECIMAL
	DOUBLE
	ENUM
	FLOAT
	GEOMETRY
	INT
	INTEGER
	JSON
	LONGBLOB
	LONGTEXT
	MEDIUMBLOB
	MEDIUMINT
	MEDIUMTEXT
	NUMERIC
	REAL
	SET_TYPE
	SMALLINT
	TEXT
	TIME
	TIMESTAMP
	TINYBLOB
	TINYINT
	TINYTEXT
	VARBINARY
	VARCHAR
	YEAR
	
	// 函数名关键字
	AVG
	COUNT
	MAX
	MIN
	SUM
	
	// 最大Token值，用于范围检查
	MAX_TOKEN
)

// LexError 词法错误
type LexError struct {
	Position Position
	Message  string
}

// LexErrorList 词法错误列表
type LexErrorList struct {
	Errors []LexError
}

// Error 实现error接口
func (e *LexErrorList) Error() string {
	var result string
	result += "词法分析错误:\n"
	for _, err := range e.Errors {
		result += fmt.Sprintf("  第%d行，第%d列: %s\n", err.Position.Line, err.Position.Column, err.Message)
	}
	return result
}

// CacheManager 缓存管理器接口
type CacheManager interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{}, ttl time.Duration)
	Delete(key string)
	Clear()
	GetStats() *CacheStats
}

// CacheStats 缓存统计信息
type CacheStats struct {
	Size     int
	Capacity int
	Hits     int64
	Misses   int64
	HitRate  float64
}