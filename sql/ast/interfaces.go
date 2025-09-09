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
	VisitCreateIndexStatement(stmt *CreateIndexStatement) interface{}
	VisitDropIndexStatement(stmt *DropIndexStatement) interface{}
	VisitBeginTransactionStatement(stmt *BeginTransactionStatement) interface{}
	VisitCommitTransactionStatement(stmt *CommitTransactionStatement) interface{}
	VisitRollbackTransactionStatement(stmt *RollbackTransactionStatement) interface{}
	VisitUnionStatement(stmt *UnionStatement) interface{}

	VisitColumnReference(expr *ColumnReference) interface{}
	VisitLiteral(expr *Literal) interface{}
	VisitBinaryExpression(expr *BinaryExpression) interface{}
	VisitUnaryExpression(expr *UnaryExpression) interface{}
	VisitFunctionCall(expr *FunctionCall) interface{}
	VisitCaseExpression(expr *CaseExpression) interface{}
	VisitWildcard(expr *Wildcard) interface{}
	VisitSubqueryExpression(expr *SubqueryExpression) interface{}
	VisitBetweenExpression(expr *BetweenExpression) interface{}
	VisitValueListExpression(expr *ValueListExpression) interface{}
}

// BaseVisitor 基础访问者实现
type BaseVisitor struct{}

func (v *BaseVisitor) VisitSelectStatement(stmt *SelectStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitInsertStatement(stmt *InsertStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitUpdateStatement(stmt *UpdateStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitDeleteStatement(stmt *DeleteStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitCreateTableStatement(stmt *CreateTableStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitDropTableStatement(stmt *DropTableStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitCreateIndexStatement(stmt *CreateIndexStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitDropIndexStatement(stmt *DropIndexStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitBeginTransactionStatement(stmt *BeginTransactionStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitCommitTransactionStatement(stmt *CommitTransactionStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitRollbackTransactionStatement(stmt *RollbackTransactionStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitUnionStatement(stmt *UnionStatement) interface{} {
	for _, child := range stmt.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitColumnReference(expr *ColumnReference) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitLiteral(expr *Literal) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitBinaryExpression(expr *BinaryExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitUnaryExpression(expr *UnaryExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitFunctionCall(expr *FunctionCall) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitCaseExpression(expr *CaseExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitWildcard(expr *Wildcard) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitSubqueryExpression(expr *SubqueryExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitBetweenExpression(expr *BetweenExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

func (v *BaseVisitor) VisitValueListExpression(expr *ValueListExpression) interface{} {
	for _, child := range expr.GetChildren() {
		child.Accept(v)
	}
	return nil
}

// JoinType JOIN类型
type JoinType int

const (
	INNER_JOIN JoinType = iota
	LEFT_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

// UnionType 集合操作类型
type UnionType int

const (
	UNION_TYPE UnionType = iota
	INTERSECT_TYPE
	EXCEPT_TYPE
)
