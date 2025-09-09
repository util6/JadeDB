package ast

import "github.com/util6/JadeDB/sql/lexer"

// 语句类型实现

// SelectStatement SELECT语句
type SelectStatement struct {
	Position      lexer.Position
	SelectList    []SelectItem
	FromClause    *FromClause
	WhereClause   Expression
	GroupByClause *GroupByClause
	HavingClause  Expression
	OrderByClause *OrderByClause
	LimitClause   *LimitClause
}

func (s *SelectStatement) String() string                     { return "SELECT" }
func (s *SelectStatement) Accept(visitor Visitor) interface{} { return visitor.VisitSelectStatement(s) }
func (s *SelectStatement) GetPosition() lexer.Position        { return s.Position }
func (s *SelectStatement) GetChildren() []ASTNode             { return nil }
func (s *SelectStatement) statementNode()                     {}

// InsertStatement INSERT语句
type InsertStatement struct {
	Position lexer.Position
	Table    string
	Columns  []string
	Values   [][]Expression
}

func (i *InsertStatement) String() string                     { return "INSERT" }
func (i *InsertStatement) Accept(visitor Visitor) interface{} { return visitor.VisitInsertStatement(i) }
func (i *InsertStatement) GetPosition() lexer.Position        { return i.Position }
func (i *InsertStatement) GetChildren() []ASTNode             { return nil }
func (i *InsertStatement) statementNode()                     {}

// UpdateStatement UPDATE语句
type UpdateStatement struct {
	Position    lexer.Position
	Table       string
	SetClauses  []SetClause
	WhereClause Expression
}

func (u *UpdateStatement) String() string                     { return "UPDATE" }
func (u *UpdateStatement) Accept(visitor Visitor) interface{} { return visitor.VisitUpdateStatement(u) }
func (u *UpdateStatement) GetPosition() lexer.Position        { return u.Position }
func (u *UpdateStatement) GetChildren() []ASTNode             { return nil }
func (u *UpdateStatement) statementNode()                     {}

// DeleteStatement DELETE语句
type DeleteStatement struct {
	Position    lexer.Position
	Table       string
	WhereClause Expression
}

func (d *DeleteStatement) String() string                     { return "DELETE" }
func (d *DeleteStatement) Accept(visitor Visitor) interface{} { return visitor.VisitDeleteStatement(d) }
func (d *DeleteStatement) GetPosition() lexer.Position        { return d.Position }
func (d *DeleteStatement) GetChildren() []ASTNode             { return nil }
func (d *DeleteStatement) statementNode()                     {}

// CreateTableStatement CREATE TABLE语句
type CreateTableStatement struct {
	Position    lexer.Position
	TableName   string
	Columns     []ColumnDefinition
	Constraints []Constraint
}

func (c *CreateTableStatement) String() string { return "CREATE TABLE" }
func (c *CreateTableStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitCreateTableStatement(c)
}
func (c *CreateTableStatement) GetPosition() lexer.Position { return c.Position }
func (c *CreateTableStatement) GetChildren() []ASTNode      { return nil }
func (c *CreateTableStatement) statementNode()              {}

// DropTableStatement DROP TABLE语句
type DropTableStatement struct {
	Position  lexer.Position
	TableName string
	IfExists  bool
}

func (d *DropTableStatement) String() string { return "DROP TABLE" }
func (d *DropTableStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitDropTableStatement(d)
}
func (d *DropTableStatement) GetPosition() lexer.Position { return d.Position }
func (d *DropTableStatement) GetChildren() []ASTNode      { return nil }
func (d *DropTableStatement) statementNode()              {}

// CreateIndexStatement CREATE INDEX语句
type CreateIndexStatement struct {
	Position  lexer.Position
	IndexName string
	TableName string
	Columns   []string
	Unique    bool
}

func (c *CreateIndexStatement) String() string { return "CREATE INDEX" }
func (c *CreateIndexStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitCreateIndexStatement(c)
}
func (c *CreateIndexStatement) GetPosition() lexer.Position { return c.Position }
func (c *CreateIndexStatement) GetChildren() []ASTNode      { return nil }
func (c *CreateIndexStatement) statementNode()              {}

// DropIndexStatement DROP INDEX语句
type DropIndexStatement struct {
	Position  lexer.Position
	IndexName string
}

func (d *DropIndexStatement) String() string { return "DROP INDEX" }
func (d *DropIndexStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitDropIndexStatement(d)
}
func (d *DropIndexStatement) GetPosition() lexer.Position { return d.Position }
func (d *DropIndexStatement) GetChildren() []ASTNode      { return nil }
func (d *DropIndexStatement) statementNode()              {}

// BeginTransactionStatement BEGIN TRANSACTION语句
type BeginTransactionStatement struct {
	Position lexer.Position
}

func (b *BeginTransactionStatement) String() string { return "BEGIN TRANSACTION" }
func (b *BeginTransactionStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitBeginTransactionStatement(b)
}
func (b *BeginTransactionStatement) GetPosition() lexer.Position { return b.Position }
func (b *BeginTransactionStatement) GetChildren() []ASTNode      { return nil }
func (b *BeginTransactionStatement) statementNode()              {}

// CommitTransactionStatement COMMIT TRANSACTION语句
type CommitTransactionStatement struct {
	Position lexer.Position
}

func (c *CommitTransactionStatement) String() string { return "COMMIT TRANSACTION" }
func (c *CommitTransactionStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitCommitTransactionStatement(c)
}
func (c *CommitTransactionStatement) GetPosition() lexer.Position { return c.Position }
func (c *CommitTransactionStatement) GetChildren() []ASTNode      { return nil }
func (c *CommitTransactionStatement) statementNode()              {}

// RollbackTransactionStatement ROLLBACK TRANSACTION语句
type RollbackTransactionStatement struct {
	Position lexer.Position
}

func (r *RollbackTransactionStatement) String() string { return "ROLLBACK TRANSACTION" }
func (r *RollbackTransactionStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitRollbackTransactionStatement(r)
}
func (r *RollbackTransactionStatement) GetPosition() lexer.Position { return r.Position }
func (r *RollbackTransactionStatement) GetChildren() []ASTNode      { return nil }
func (r *RollbackTransactionStatement) statementNode()              {}

// UnionStatement UNION语句（集合操作）
type UnionStatement struct {
	Position  lexer.Position
	Left      *SelectStatement
	Right     *SelectStatement
	UnionType UnionType
	All       bool // 是否为UNION ALL
}

func (u *UnionStatement) String() string { return "UNION" }
func (u *UnionStatement) Accept(visitor Visitor) interface{} {
	return visitor.VisitUnionStatement(u)
}
func (u *UnionStatement) GetPosition() lexer.Position { return u.Position }
func (u *UnionStatement) GetChildren() []ASTNode {
	return []ASTNode{u.Left, u.Right}
}
func (u *UnionStatement) statementNode() {}

// 表达式类型实现

// ColumnReference 列引用
// 支持以下格式：
// - column_name（简单列引用）
// - table_name.column_name（表限定列引用）
// - schema_name.table_name.column_name（完全限定列引用）
type ColumnReference struct {
	Position lexer.Position
	Schema   string // 模式名（可选）
	Table    string // 表名（可选）
	Column   string // 列名（必需）
}

func (c *ColumnReference) String() string                     { return c.Column }
func (c *ColumnReference) Accept(visitor Visitor) interface{} { return visitor.VisitColumnReference(c) }
func (c *ColumnReference) GetPosition() lexer.Position        { return c.Position }
func (c *ColumnReference) GetChildren() []ASTNode             { return nil }
func (c *ColumnReference) expressionNode()                    {}

// Literal 字面量
type Literal struct {
	Position lexer.Position
	Type     lexer.TokenType
	Value    interface{}
}

func (l *Literal) String() string                     { return "LITERAL" }
func (l *Literal) Accept(visitor Visitor) interface{} { return visitor.VisitLiteral(l) }
func (l *Literal) GetPosition() lexer.Position        { return l.Position }
func (l *Literal) GetChildren() []ASTNode             { return nil }
func (l *Literal) expressionNode()                    {}

// BinaryExpression 二元表达式
type BinaryExpression struct {
	Position lexer.Position
	Left     Expression
	Operator lexer.TokenType
	Right    Expression
}

func (b *BinaryExpression) String() string { return "BINARY_EXPR" }
func (b *BinaryExpression) Accept(visitor Visitor) interface{} {
	return visitor.VisitBinaryExpression(b)
}
func (b *BinaryExpression) GetPosition() lexer.Position { return b.Position }
func (b *BinaryExpression) GetChildren() []ASTNode      { return nil }
func (b *BinaryExpression) expressionNode()             {}

// UnaryExpression 一元表达式
type UnaryExpression struct {
	Position lexer.Position
	Operator lexer.TokenType
	Operand  Expression
}

func (u *UnaryExpression) String() string                     { return "UNARY_EXPR" }
func (u *UnaryExpression) Accept(visitor Visitor) interface{} { return visitor.VisitUnaryExpression(u) }
func (u *UnaryExpression) GetPosition() lexer.Position        { return u.Position }
func (u *UnaryExpression) GetChildren() []ASTNode             { return nil }
func (u *UnaryExpression) expressionNode()                    {}

// FunctionCall 函数调用
type FunctionCall struct {
	Position lexer.Position
	Name     string
	Args     []Expression
	Distinct bool
}

func (f *FunctionCall) String() string                     { return f.Name }
func (f *FunctionCall) Accept(visitor Visitor) interface{} { return visitor.VisitFunctionCall(f) }
func (f *FunctionCall) GetPosition() lexer.Position        { return f.Position }
func (f *FunctionCall) GetChildren() []ASTNode             { return nil }
func (f *FunctionCall) expressionNode()                    {}

// CaseExpression CASE表达式
type CaseExpression struct {
	Position    lexer.Position
	Expr        Expression // CASE后的表达式，可选
	WhenClauses []*WhenClause
	ElseClause  Expression
}

func (c *CaseExpression) String() string                     { return "CASE" }
func (c *CaseExpression) Accept(visitor Visitor) interface{} { return visitor.VisitCaseExpression(c) }
func (c *CaseExpression) GetPosition() lexer.Position        { return c.Position }
func (c *CaseExpression) GetChildren() []ASTNode             { return nil }
func (c *CaseExpression) expressionNode()                    {}

// SubqueryExpression 子查询表达式
type SubqueryExpression struct {
	Position lexer.Position
	Query    *SelectStatement
}

func (s *SubqueryExpression) String() string { return "SUBQUERY" }
func (s *SubqueryExpression) Accept(visitor Visitor) interface{} {
	return visitor.VisitSubqueryExpression(s)
}
func (s *SubqueryExpression) GetPosition() lexer.Position { return s.Position }
func (s *SubqueryExpression) GetChildren() []ASTNode      { return []ASTNode{s.Query} }
func (s *SubqueryExpression) expressionNode()             {}

// 辅助结构

// SelectItem SELECT项
type SelectItem struct {
	Expression Expression
	Alias      string
	Position   lexer.Position
}

// FromClause FROM子句
type FromClause struct {
	Position lexer.Position
	Tables   []TableReference
}

// TableReference 表引用
type TableReference struct {
	Position lexer.Position
	Table    Expression
	Alias    string
	Joins    []*JoinClause
}

// JoinClause JOIN子句
type JoinClause struct {
	Position  lexer.Position
	Type      JoinType
	Table     Expression
	Condition Expression
}

// GroupByClause GROUP BY子句
type GroupByClause struct {
	Position lexer.Position
	Columns  []Expression
}

// OrderByClause ORDER BY子句
type OrderByClause struct {
	Position lexer.Position
	Items    []OrderByItem
}

// OrderByItem ORDER BY项
type OrderByItem struct {
	Expression Expression
	Ascending  bool
}

// LimitClause LIMIT子句
type LimitClause struct {
	Position lexer.Position
	Count    Expression
	Offset   Expression
}

// SetClause SET子句
type SetClause struct {
	Column string
	Value  Expression
}

// WhenClause WHEN子句
type WhenClause struct {
	Position  lexer.Position
	Condition Expression
	Result    Expression
}

// ColumnDefinition 列定义
type ColumnDefinition struct {
	Name         string
	DataType     DataType
	NotNull      bool
	DefaultValue Expression
}

// DataType 数据类型
type DataType struct {
	Name      string
	Length    int
	Precision int
	Scale     int
}

// Constraint 约束
type Constraint struct {
	Type    ConstraintType
	Name    string
	Columns []string
}

// ConstraintType 约束类型
type ConstraintType int

const (
	PrimaryKey ConstraintType = iota
	ForeignKey
	Unique
	Check
)

// Wildcard 通配符表达式（用于SELECT *和COUNT(*)等）
type Wildcard struct {
	Position lexer.Position
}

func (w *Wildcard) String() string                     { return "*" }
func (w *Wildcard) Accept(visitor Visitor) interface{} { return visitor.VisitWildcard(w) }
func (w *Wildcard) GetPosition() lexer.Position        { return w.Position }
func (w *Wildcard) GetChildren() []ASTNode             { return nil }
func (w *Wildcard) expressionNode()                    {}

// BetweenExpression BETWEEN表达式
type BetweenExpression struct {
	Position lexer.Position
	Expr     Expression
	Start    Expression
	End      Expression
}

func (b *BetweenExpression) String() string { return "BETWEEN" }
func (b *BetweenExpression) Accept(visitor Visitor) interface{} {
	return visitor.VisitBetweenExpression(b)
}
func (b *BetweenExpression) GetPosition() lexer.Position { return b.Position }
func (b *BetweenExpression) GetChildren() []ASTNode      { return nil }
func (b *BetweenExpression) expressionNode()             {}

// ValueListExpression 值列表表达式（用于IN操作符）
type ValueListExpression struct {
	Position lexer.Position
	Values   []Expression
}

func (v *ValueListExpression) String() string { return "VALUE_LIST" }
func (v *ValueListExpression) Accept(visitor Visitor) interface{} {
	return visitor.VisitValueListExpression(v)
}
func (v *ValueListExpression) GetPosition() lexer.Position { return v.Position }
func (v *ValueListExpression) GetChildren() []ASTNode      { return nil }
func (v *ValueListExpression) expressionNode()             {}
