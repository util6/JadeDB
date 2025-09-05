package parser

import (
	"time"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// SQLParser 是SQL解析器的主接口
type SQLParser interface {
	Parse(sql string) (*ast.AST, error)
	ParseWithOptions(sql string, opts *ParseOptions) (*ast.AST, error)
	GetStatistics() *ParseStatistics
	ClearCache()
}

// ParseOptions 解析选项
type ParseOptions struct {
	EnableCache bool
	StrictMode  bool
	MaxDepth    int
	Timeout     time.Duration
	Dialect     SQLDialect
}

// ParseStatistics 解析统计信息
type ParseStatistics struct {
	TotalParses  int64
	CacheHits    int64
	CacheMisses  int64
	AvgParseTime time.Duration
	ErrorCount   int64
}

// SQLDialect SQL方言
type SQLDialect int

const (
	StandardSQL SQLDialect = iota
	MySQL
	PostgreSQL
	SQLite
)

// Parser 语法分析器接口
type Parser interface {
	Parse(tokens []lexer.Token) (*ast.AST, error)
	ParseStatement() (ast.Statement, error)
	ParseExpression() (ast.Expression, error)
	GetErrors() []ParseError
}

// ParseError 解析错误
type ParseError struct {
	Position lexer.Position
	Message  string
	Expected []lexer.TokenType
	Actual   lexer.TokenType
	Context  string // 错误上下文信息
}

// ParserFactory SQL解析器工厂
type ParserFactory interface {
	CreateParser(dialect SQLDialect) SQLParser
	GetSupportedDialects() []SQLDialect
}

// SQLNormalizer SQL标准化器
type SQLNormalizer interface {
	Normalize(sql string) string
	RemoveComments(sql string) string
	FormatSQL(sql string) string
}

// ASTValidator AST验证器
type ASTValidator interface {
	Validate(ast *ast.AST) []ValidationError
	ValidateStatement(stmt ast.Statement) []ValidationError
	ValidateExpression(expr ast.Expression) []ValidationError
}

// ValidationError 验证错误
type ValidationError struct {
	Position lexer.Position
	Message  string
	Severity Severity
}

// Severity 严重程度
type Severity int

const (
	Warning Severity = iota
	Error
	Fatal
)

// PerformanceMonitor 性能监控器
type PerformanceMonitor interface {
	RecordParseTime(duration time.Duration)
	RecordCacheHit()
	RecordCacheMiss()
	GetMetrics() *PerformanceMetrics
}

// PerformanceMetrics 性能指标
type PerformanceMetrics struct {
	TotalParses  int64
	AvgParseTime time.Duration
	MaxParseTime time.Duration
	MinParseTime time.Duration
	CacheHitRate float64
	MemoryUsage  int64
}
