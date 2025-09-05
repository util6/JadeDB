package ast

import (
	"fmt"
	"strings"

	"github.com/util6/JadeDB/sql/lexer"
)

// BaseASTNode AST节点基础实现 - 提供所有AST节点的通用功能
// 参考TiDB的AST设计，提供位置信息、子节点管理、访问者模式支持
type BaseASTNode struct {
	Position lexer.Position // 节点在源代码中的位置信息
	Parent   ASTNode        // 父节点引用，用于向上遍历
	children []ASTNode      // 子节点列表，用于向下遍历
}

// NewBaseASTNode 创建新的基础AST节点
// 参数:
//
//	position: 节点在源代码中的位置
//
// 返回:
//
//	*BaseASTNode: 基础AST节点实例
func NewBaseASTNode(position lexer.Position) *BaseASTNode {
	return &BaseASTNode{
		Position: position,
		children: make([]ASTNode, 0),
	}
}

// GetPosition 获取节点位置信息
// 返回:
//
//	lexer.Position: 节点在源代码中的位置
func (b *BaseASTNode) GetPosition() lexer.Position {
	return b.Position
}

// GetChildren 获取子节点列表
// 返回:
//
//	[]ASTNode: 子节点列表
func (b *BaseASTNode) GetChildren() []ASTNode {
	return b.children
}

// String 返回节点的字符串表示
func (b *BaseASTNode) String() string {
	return "BaseASTNode"
}

// Accept 接受访问者（基础实现，具体节点类型应该重写）
func (b *BaseASTNode) Accept(visitor Visitor) interface{} {
	// 基础实现，返回nil
	return nil
}

// AddChild 添加子节点
// 参数:
//
//	child: 要添加的子节点
func (b *BaseASTNode) AddChild(child ASTNode) {
	if child != nil {
		b.children = append(b.children, child)
		// 设置父节点引用（如果子节点支持）
		if baseChild, ok := child.(*BaseASTNode); ok {
			baseChild.Parent = b
		}
	}
}

// RemoveChild 移除子节点
// 参数:
//
//	child: 要移除的子节点
//
// 返回:
//
//	bool: 是否成功移除
func (b *BaseASTNode) RemoveChild(child ASTNode) bool {
	for i, c := range b.children {
		if c == child {
			// 移除子节点
			b.children = append(b.children[:i], b.children[i+1:]...)
			// 清除父节点引用
			if baseChild, ok := child.(*BaseASTNode); ok {
				baseChild.Parent = nil
			}
			return true
		}
	}
	return false
}

// GetParent 获取父节点
// 返回:
//
//	ASTNode: 父节点，如果没有则返回nil
func (b *BaseASTNode) GetParent() ASTNode {
	return b.Parent
}

// SetParent 设置父节点
// 参数:
//
//	parent: 父节点
func (b *BaseASTNode) SetParent(parent ASTNode) {
	b.Parent = parent
}

// IsRoot 检查是否为根节点
// 返回:
//
//	bool: 是否为根节点
func (b *BaseASTNode) IsRoot() bool {
	return b.Parent == nil
}

// IsLeaf 检查是否为叶子节点
// 返回:
//
//	bool: 是否为叶子节点
func (b *BaseASTNode) IsLeaf() bool {
	return len(b.children) == 0
}

// GetDepth 获取节点深度（从根节点开始计算）
// 返回:
//
//	int: 节点深度
func (b *BaseASTNode) GetDepth() int {
	depth := 0
	current := b.Parent
	for current != nil {
		depth++
		if baseParent, ok := current.(*BaseASTNode); ok {
			current = baseParent.Parent
		} else {
			break
		}
	}
	return depth
}

// GetRoot 获取根节点
// 返回:
//
//	ASTNode: 根节点
func (b *BaseASTNode) GetRoot() ASTNode {
	current := ASTNode(b)
	for {
		if baseNode, ok := current.(*BaseASTNode); ok && baseNode.Parent != nil {
			current = baseNode.Parent
		} else {
			break
		}
	}
	return current
}

// BaseStatement 语句基础实现 - 所有SQL语句的基类
// 提供语句通用功能，如执行上下文、错误处理等
type BaseStatement struct {
	*BaseASTNode
	StatementType StatementType // 语句类型
	Errors        []ParseError  // 解析错误列表
}

// NewBaseStatement 创建新的基础语句节点
// 参数:
//
//	position: 节点位置
//	stmtType: 语句类型
//
// 返回:
//
//	*BaseStatement: 基础语句节点实例
func NewBaseStatement(position lexer.Position, stmtType StatementType) *BaseStatement {
	return &BaseStatement{
		BaseASTNode:   NewBaseASTNode(position),
		StatementType: stmtType,
		Errors:        make([]ParseError, 0),
	}
}

// statementNode 标记为语句节点
func (b *BaseStatement) statementNode() {}

// GetStatementType 获取语句类型
// 返回:
//
//	StatementType: 语句类型
func (b *BaseStatement) GetStatementType() StatementType {
	return b.StatementType
}

// AddError 添加解析错误
// 参数:
//
//	err: 解析错误
func (b *BaseStatement) AddError(err ParseError) {
	b.Errors = append(b.Errors, err)
}

// HasErrors 检查是否有解析错误
// 返回:
//
//	bool: 是否有错误
func (b *BaseStatement) HasErrors() bool {
	return len(b.Errors) > 0
}

// GetErrors 获取所有解析错误
// 返回:
//
//	[]ParseError: 错误列表
func (b *BaseStatement) GetErrors() []ParseError {
	return b.Errors
}

// BaseExpression 表达式基础实现 - 所有SQL表达式的基类
// 提供表达式通用功能，如类型推断、值计算等
type BaseExpression struct {
	*BaseASTNode
	ExpressionType ExpressionType // 表达式类型
	DataType       *DataType      // 推断的数据类型
	IsConstant     bool           // 是否为常量表达式
}

// NewBaseExpression 创建新的基础表达式节点
// 参数:
//
//	position: 节点位置
//	exprType: 表达式类型
//
// 返回:
//
//	*BaseExpression: 基础表达式节点实例
func NewBaseExpression(position lexer.Position, exprType ExpressionType) *BaseExpression {
	return &BaseExpression{
		BaseASTNode:    NewBaseASTNode(position),
		ExpressionType: exprType,
		IsConstant:     false,
	}
}

// expressionNode 标记为表达式节点
func (b *BaseExpression) expressionNode() {}

// GetExpressionType 获取表达式类型
// 返回:
//
//	ExpressionType: 表达式类型
func (b *BaseExpression) GetExpressionType() ExpressionType {
	return b.ExpressionType
}

// SetDataType 设置数据类型
// 参数:
//
//	dataType: 数据类型
func (b *BaseExpression) SetDataType(dataType *DataType) {
	b.DataType = dataType
}

// GetDataType 获取数据类型
// 返回:
//
//	*DataType: 数据类型，如果未推断则返回nil
func (b *BaseExpression) GetDataType() *DataType {
	return b.DataType
}

// SetConstant 设置是否为常量表达式
// 参数:
//
//	isConstant: 是否为常量
func (b *BaseExpression) SetConstant(isConstant bool) {
	b.IsConstant = isConstant
}

// GetIsConstant 获取是否为常量表达式
// 返回:
//
//	bool: 是否为常量
func (b *BaseExpression) GetIsConstant() bool {
	return b.IsConstant
}

// StatementType 语句类型枚举
type StatementType int

const (
	SELECT_STMT StatementType = iota
	INSERT_STMT
	UPDATE_STMT
	DELETE_STMT
	CREATE_TABLE_STMT
	DROP_TABLE_STMT
	CREATE_INDEX_STMT
	DROP_INDEX_STMT
	ALTER_TABLE_STMT
	TRUNCATE_STMT
	EXPLAIN_STMT
	DESCRIBE_STMT
)

// String 返回语句类型的字符串表示
func (st StatementType) String() string {
	switch st {
	case SELECT_STMT:
		return "SELECT"
	case INSERT_STMT:
		return "INSERT"
	case UPDATE_STMT:
		return "UPDATE"
	case DELETE_STMT:
		return "DELETE"
	case CREATE_TABLE_STMT:
		return "CREATE TABLE"
	case DROP_TABLE_STMT:
		return "DROP TABLE"
	case CREATE_INDEX_STMT:
		return "CREATE INDEX"
	case DROP_INDEX_STMT:
		return "DROP INDEX"
	case ALTER_TABLE_STMT:
		return "ALTER TABLE"
	case TRUNCATE_STMT:
		return "TRUNCATE"
	case EXPLAIN_STMT:
		return "EXPLAIN"
	case DESCRIBE_STMT:
		return "DESCRIBE"
	default:
		return "UNKNOWN"
	}
}

// ExpressionType 表达式类型枚举
type ExpressionType int

const (
	LITERAL_EXPR ExpressionType = iota
	COLUMN_REF_EXPR
	BINARY_EXPR
	UNARY_EXPR
	FUNCTION_CALL_EXPR
	CASE_EXPR
	SUBQUERY_EXPR
	LIST_EXPR
	BETWEEN_EXPR
	IN_EXPR
	EXISTS_EXPR
	LIKE_EXPR
	IS_NULL_EXPR
)

// String 返回表达式类型的字符串表示
func (et ExpressionType) String() string {
	switch et {
	case LITERAL_EXPR:
		return "LITERAL"
	case COLUMN_REF_EXPR:
		return "COLUMN_REF"
	case BINARY_EXPR:
		return "BINARY"
	case UNARY_EXPR:
		return "UNARY"
	case FUNCTION_CALL_EXPR:
		return "FUNCTION_CALL"
	case CASE_EXPR:
		return "CASE"
	case SUBQUERY_EXPR:
		return "SUBQUERY"
	case LIST_EXPR:
		return "LIST"
	case BETWEEN_EXPR:
		return "BETWEEN"
	case IN_EXPR:
		return "IN"
	case EXISTS_EXPR:
		return "EXISTS"
	case LIKE_EXPR:
		return "LIKE"
	case IS_NULL_EXPR:
		return "IS_NULL"
	default:
		return "UNKNOWN"
	}
}

// ParseError 解析错误结构
type ParseError struct {
	Position lexer.Position // 错误位置
	Message  string         // 错误消息
	Code     ErrorCode      // 错误代码
	Context  string         // 错误上下文
}

// Error 实现error接口
func (pe ParseError) Error() string {
	return fmt.Sprintf("解析错误 [%s] 在 %s: %s", pe.Code, pe.Position.String(), pe.Message)
}

// ErrorCode 错误代码枚举
type ErrorCode string

const (
	SYNTAX_ERROR         ErrorCode = "SYNTAX_ERROR"
	UNEXPECTED_TOKEN     ErrorCode = "UNEXPECTED_TOKEN"
	MISSING_TOKEN        ErrorCode = "MISSING_TOKEN"
	INVALID_EXPRESSION   ErrorCode = "INVALID_EXPRESSION"
	INVALID_STATEMENT    ErrorCode = "INVALID_STATEMENT"
	TYPE_MISMATCH        ErrorCode = "TYPE_MISMATCH"
	UNDEFINED_REFERENCE  ErrorCode = "UNDEFINED_REFERENCE"
	DUPLICATE_DEFINITION ErrorCode = "DUPLICATE_DEFINITION"
)

// ASTNodeType AST节点类型枚举 - 用于运行时类型检查
type ASTNodeType int

const (
	STATEMENT_NODE ASTNodeType = iota
	EXPRESSION_NODE
	CLAUSE_NODE
	DEFINITION_NODE
	REFERENCE_NODE
)

// NodeInfo 节点信息结构 - 提供节点的元数据
type NodeInfo struct {
	NodeType    ASTNodeType            // 节点类型
	Name        string                 // 节点名称
	Description string                 // 节点描述
	Properties  map[string]interface{} // 节点属性
}

// GetNodeInfo 获取节点信息（需要具体节点类型实现）
func GetNodeInfo(node ASTNode) *NodeInfo {
	// 基础实现，具体节点类型可以重写此方法
	return &NodeInfo{
		NodeType:    STATEMENT_NODE, // 默认为语句节点
		Name:        node.String(),
		Description: fmt.Sprintf("AST节点: %s", node.String()),
		Properties:  make(map[string]interface{}),
	}
}

// ASTWalker AST遍历器 - 提供深度优先和广度优先遍历
type ASTWalker struct {
	visitedNodes map[ASTNode]bool // 已访问节点集合，防止循环引用
}

// NewASTWalker 创建新的AST遍历器
// 返回:
//
//	*ASTWalker: AST遍历器实例
func NewASTWalker() *ASTWalker {
	return &ASTWalker{
		visitedNodes: make(map[ASTNode]bool),
	}
}

// WalkDepthFirst 深度优先遍历AST
// 参数:
//
//	root: 根节点
//	visitor: 访问函数
func (w *ASTWalker) WalkDepthFirst(root ASTNode, visitor func(ASTNode) bool) {
	if root == nil || w.visitedNodes[root] {
		return
	}

	w.visitedNodes[root] = true

	// 访问当前节点，如果返回false则停止遍历
	if !visitor(root) {
		return
	}

	// 递归访问子节点
	for _, child := range root.GetChildren() {
		w.WalkDepthFirst(child, visitor)
	}
}

// WalkBreadthFirst 广度优先遍历AST
// 参数:
//
//	root: 根节点
//	visitor: 访问函数
func (w *ASTWalker) WalkBreadthFirst(root ASTNode, visitor func(ASTNode) bool) {
	if root == nil {
		return
	}

	queue := []ASTNode{root}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if w.visitedNodes[current] {
			continue
		}

		w.visitedNodes[current] = true

		// 访问当前节点，如果返回false则停止遍历
		if !visitor(current) {
			return
		}

		// 将子节点加入队列
		for _, child := range current.GetChildren() {
			if !w.visitedNodes[child] {
				queue = append(queue, child)
			}
		}
	}
}

// Reset 重置遍历器状态
func (w *ASTWalker) Reset() {
	w.visitedNodes = make(map[ASTNode]bool)
}

// ASTFormatter AST格式化器 - 将AST转换为可读的字符串表示
type ASTFormatter struct {
	indentLevel int    // 当前缩进级别
	indentStr   string // 缩进字符串
}

// NewASTFormatter 创建新的AST格式化器
// 参数:
//
//	indentStr: 缩进字符串（如"  "或"\t"）
//
// 返回:
//
//	*ASTFormatter: AST格式化器实例
func NewASTFormatter(indentStr string) *ASTFormatter {
	return &ASTFormatter{
		indentLevel: 0,
		indentStr:   indentStr,
	}
}

// FormatAST 格式化AST为字符串
// 参数:
//
//	root: 根节点
//
// 返回:
//
//	string: 格式化后的字符串
func (f *ASTFormatter) FormatAST(root ASTNode) string {
	if root == nil {
		return "<nil>"
	}

	var result strings.Builder
	f.formatNode(root, &result)
	return result.String()
}

// formatNode 格式化单个节点
func (f *ASTFormatter) formatNode(node ASTNode, result *strings.Builder) {
	// 添加缩进
	for i := 0; i < f.indentLevel; i++ {
		result.WriteString(f.indentStr)
	}

	// 添加节点信息
	result.WriteString(fmt.Sprintf("%s [%s]\n", node.String(), node.GetPosition().String()))

	// 递归格式化子节点
	f.indentLevel++
	for _, child := range node.GetChildren() {
		f.formatNode(child, result)
	}
	f.indentLevel--
}

// ASTValidator AST验证器 - 验证AST的语义正确性
type ASTValidator struct {
	errors []ParseError // 验证错误列表
}

// NewASTValidator 创建新的AST验证器
// 返回:
//
//	*ASTValidator: AST验证器实例
func NewASTValidator() *ASTValidator {
	return &ASTValidator{
		errors: make([]ParseError, 0),
	}
}

// ValidateAST 验证AST
// 参数:
//
//	root: 根节点
//
// 返回:
//
//	[]ParseError: 验证错误列表
func (v *ASTValidator) ValidateAST(root ASTNode) []ParseError {
	v.errors = make([]ParseError, 0)

	if root == nil {
		v.addError(lexer.Position{}, "AST根节点不能为空", INVALID_STATEMENT)
		return v.errors
	}

	// 使用遍历器验证所有节点
	walker := NewASTWalker()
	walker.WalkDepthFirst(root, func(node ASTNode) bool {
		v.validateNode(node)
		return true
	})

	return v.errors
}

// validateNode 验证单个节点
func (v *ASTValidator) validateNode(node ASTNode) {
	if node == nil {
		return
	}

	// 基础验证：检查位置信息
	pos := node.GetPosition()
	if pos.Line <= 0 || pos.Column <= 0 {
		v.addError(pos, "节点位置信息无效", SYNTAX_ERROR)
	}

	// 检查子节点
	children := node.GetChildren()
	for _, child := range children {
		if child == nil {
			v.addError(pos, "子节点不能为空", INVALID_STATEMENT)
		}
	}
}

// addError 添加验证错误
func (v *ASTValidator) addError(pos lexer.Position, message string, code ErrorCode) {
	v.errors = append(v.errors, ParseError{
		Position: pos,
		Message:  message,
		Code:     code,
		Context:  "",
	})
}

// GetErrors 获取所有验证错误
// 返回:
//
//	[]ParseError: 错误列表
func (v *ASTValidator) GetErrors() []ParseError {
	return v.errors
}

// HasErrors 检查是否有验证错误
// 返回:
//
//	bool: 是否有错误
func (v *ASTValidator) HasErrors() bool {
	return len(v.errors) > 0
}
