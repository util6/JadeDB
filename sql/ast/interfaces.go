package ast

import (
	"time"

	"github.com/util6/JadeDB/sql/lexer"
)

// ASTNode AST节点基接口
type ASTNode interface {
	String() string
	Accept(visitor Visitor) interface{}
	GetPosition() lexer.Position
	GetChildren() []ASTNode
}

// Statement 语句接口
type Statement interface {
	ASTNode
	statementNode()
}

// Expression 表达式接口
type Expression interface {
	ASTNode
	expressionNode()
}

// AST 抽象语法树根节点
type AST struct {
	Statements []Statement
	Comments   []Comment
	Metadata   *ASTMetadata
}

// ASTMetadata AST元数据
type ASTMetadata struct {
	ParseTime     time.Time
	ParseDuration time.Duration
	TokenCount    int
	NodeCount     int
}

// Comment 注释节点
type Comment struct {
	Position lexer.Position
	Text     string
	Type     CommentType
}

// CommentType 注释类型
type CommentType int

const (
	SingleLineComment CommentType = iota
	MultiLineComment
)

// Visitor 访问者接口
type Visitor interface {
	VisitSelectStatement(stmt *SelectStatement) interface{}
	VisitInsertStatement(stmt *InsertStatement) interface{}
	VisitUpdateStatement(stmt *UpdateStatement) interface{}
	VisitDeleteStatement(stmt *DeleteStatement) interface{}
	VisitCreateTableStatement(stmt *CreateTableStatement) interface{}
	VisitDropTableStatement(stmt *DropTableStatement) interface{}

	VisitColumnReference(expr *ColumnReference) interface{}
	VisitLiteral(expr *Literal) interface{}
	VisitBinaryExpression(expr *BinaryExpression) interface{}
	VisitUnaryExpression(expr *UnaryExpression) interface{}
	VisitFunctionCall(expr *FunctionCall) interface{}
	VisitCaseExpression(expr *CaseExpression) interface{}
	VisitWildcard(expr *Wildcard) interface{}
}

// BaseVisitor 基础访问者实现
type BaseVisitor struct{}

// JoinType JOIN类型
type JoinType int

const (
	INNER_JOIN JoinType = iota
	LEFT_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)
