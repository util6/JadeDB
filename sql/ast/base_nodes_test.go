package ast

import (
	"strings"
	"testing"

	"github.com/util6/JadeDB/sql/lexer"
)

// TestBaseASTNode 测试基础AST节点功能
func TestBaseASTNode(t *testing.T) {
	pos := lexer.NewPosition(1, 1, 0)

	// 创建基础节点
	node := NewBaseASTNode(pos)

	// 测试位置信息
	if node.GetPosition() != pos {
		t.Errorf("位置信息不匹配: 期望 %v, 得到 %v", pos, node.GetPosition())
	}

	// 测试初始状态
	if !node.IsRoot() {
		t.Error("新创建的节点应该是根节点")
	}

	if !node.IsLeaf() {
		t.Error("新创建的节点应该是叶子节点")
	}

	if len(node.GetChildren()) != 0 {
		t.Error("新创建的节点应该没有子节点")
	}

	if node.GetDepth() != 0 {
		t.Error("根节点的深度应该为0")
	}
}

// TestASTNodeHierarchy 测试AST节点层次结构
func TestASTNodeHierarchy(t *testing.T) {
	// 创建父节点和子节点
	parentPos := lexer.NewPosition(1, 1, 0)
	childPos := lexer.NewPosition(2, 1, 0)

	parent := NewBaseASTNode(parentPos)
	child := NewBaseASTNode(childPos)

	// 添加子节点
	parent.AddChild(child)

	// 验证父子关系
	if len(parent.GetChildren()) != 1 {
		t.Error("父节点应该有1个子节点")
	}

	if parent.GetChildren()[0] != child {
		t.Error("子节点引用不正确")
	}

	if child.GetParent() != parent {
		t.Error("父节点引用不正确")
	}

	if parent.IsLeaf() {
		t.Error("有子节点的节点不应该是叶子节点")
	}

	if !child.IsLeaf() {
		t.Error("没有子节点的节点应该是叶子节点")
	}

	if child.GetDepth() != 1 {
		t.Error("子节点的深度应该为1")
	}

	if child.GetRoot() != parent {
		t.Error("子节点的根节点应该是父节点")
	}
}

// TestASTNodeRemoval 测试AST节点移除
func TestASTNodeRemoval(t *testing.T) {
	parent := NewBaseASTNode(lexer.NewPosition(1, 1, 0))
	child1 := NewBaseASTNode(lexer.NewPosition(2, 1, 0))
	child2 := NewBaseASTNode(lexer.NewPosition(3, 1, 0))

	// 添加子节点
	parent.AddChild(child1)
	parent.AddChild(child2)

	if len(parent.GetChildren()) != 2 {
		t.Error("父节点应该有2个子节点")
	}

	// 移除第一个子节点
	if !parent.RemoveChild(child1) {
		t.Error("移除子节点应该成功")
	}

	if len(parent.GetChildren()) != 1 {
		t.Error("移除后父节点应该有1个子节点")
	}

	if child1.GetParent() != nil {
		t.Error("移除后子节点的父引用应该被清除")
	}

	// 尝试移除不存在的子节点
	if parent.RemoveChild(child1) {
		t.Error("移除不存在的子节点应该失败")
	}
}

// TestBaseStatement 测试基础语句节点
func TestBaseStatement(t *testing.T) {
	pos := lexer.NewPosition(1, 1, 0)
	stmt := NewBaseStatement(pos, SELECT_STMT)

	// 测试语句类型
	if stmt.GetStatementType() != SELECT_STMT {
		t.Errorf("语句类型不匹配: 期望 %v, 得到 %v", SELECT_STMT, stmt.GetStatementType())
	}

	// 测试错误处理
	if stmt.HasErrors() {
		t.Error("新创建的语句不应该有错误")
	}

	// 添加错误
	err := ParseError{
		Position: pos,
		Message:  "测试错误",
		Code:     SYNTAX_ERROR,
	}
	stmt.AddError(err)

	if !stmt.HasErrors() {
		t.Error("添加错误后应该有错误")
	}

	errors := stmt.GetErrors()
	if len(errors) != 1 {
		t.Errorf("错误数量不匹配: 期望 1, 得到 %d", len(errors))
	}

	if errors[0].Message != "测试错误" {
		t.Errorf("错误消息不匹配: 期望 '测试错误', 得到 '%s'", errors[0].Message)
	}
}

// TestBaseExpression 测试基础表达式节点
func TestBaseExpression(t *testing.T) {
	pos := lexer.NewPosition(1, 1, 0)
	expr := NewBaseExpression(pos, LITERAL_EXPR)

	// 测试表达式类型
	if expr.GetExpressionType() != LITERAL_EXPR {
		t.Errorf("表达式类型不匹配: 期望 %v, 得到 %v", LITERAL_EXPR, expr.GetExpressionType())
	}

	// 测试常量标记
	if expr.GetIsConstant() {
		t.Error("新创建的表达式默认不应该是常量")
	}

	expr.SetConstant(true)
	if !expr.GetIsConstant() {
		t.Error("设置为常量后应该返回true")
	}

	// 测试数据类型
	if expr.GetDataType() != nil {
		t.Error("新创建的表达式不应该有数据类型")
	}

	dataType := &DataType{Name: "INTEGER"}
	expr.SetDataType(dataType)

	if expr.GetDataType() != dataType {
		t.Error("数据类型设置不正确")
	}
}

// TestStatementTypeString 测试语句类型字符串表示
func TestStatementTypeString(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected string
	}{
		{SELECT_STMT, "SELECT"},
		{INSERT_STMT, "INSERT"},
		{UPDATE_STMT, "UPDATE"},
		{DELETE_STMT, "DELETE"},
		{CREATE_TABLE_STMT, "CREATE TABLE"},
		{DROP_TABLE_STMT, "DROP TABLE"},
	}

	for _, test := range tests {
		if test.stmtType.String() != test.expected {
			t.Errorf("语句类型字符串不匹配: 期望 '%s', 得到 '%s'",
				test.expected, test.stmtType.String())
		}
	}
}

// TestExpressionTypeString 测试表达式类型字符串表示
func TestExpressionTypeString(t *testing.T) {
	tests := []struct {
		exprType ExpressionType
		expected string
	}{
		{LITERAL_EXPR, "LITERAL"},
		{COLUMN_REF_EXPR, "COLUMN_REF"},
		{BINARY_EXPR, "BINARY"},
		{UNARY_EXPR, "UNARY"},
		{FUNCTION_CALL_EXPR, "FUNCTION_CALL"},
		{CASE_EXPR, "CASE"},
	}

	for _, test := range tests {
		if test.exprType.String() != test.expected {
			t.Errorf("表达式类型字符串不匹配: 期望 '%s', 得到 '%s'",
				test.expected, test.exprType.String())
		}
	}
}

// TestParseError 测试解析错误
func TestParseError(t *testing.T) {
	pos := lexer.NewPosition(1, 5, 4)
	err := ParseError{
		Position: pos,
		Message:  "意外的标识符",
		Code:     UNEXPECTED_TOKEN,
		Context:  "SELECT * FROM",
	}

	errorStr := err.Error()
	if !strings.Contains(errorStr, "UNEXPECTED_TOKEN") {
		t.Error("错误字符串应该包含错误代码")
	}

	if !strings.Contains(errorStr, "意外的标识符") {
		t.Error("错误字符串应该包含错误消息")
	}

	if !strings.Contains(errorStr, pos.String()) {
		t.Error("错误字符串应该包含位置信息")
	}
}

// TestASTWalker 测试AST遍历器
func TestASTWalker(t *testing.T) {
	// 创建简单的AST结构
	//     root
	//    /    \
	//  child1  child2
	//   |
	// grandchild

	root := NewBaseASTNode(lexer.NewPosition(1, 1, 0))
	child1 := NewBaseASTNode(lexer.NewPosition(2, 1, 0))
	child2 := NewBaseASTNode(lexer.NewPosition(3, 1, 0))
	grandchild := NewBaseASTNode(lexer.NewPosition(4, 1, 0))

	root.AddChild(child1)
	root.AddChild(child2)
	child1.AddChild(grandchild)

	walker := NewASTWalker()

	// 测试深度优先遍历
	var visitOrder []ASTNode
	walker.WalkDepthFirst(root, func(node ASTNode) bool {
		visitOrder = append(visitOrder, node)
		return true
	})

	expectedOrder := []ASTNode{root, child1, grandchild, child2}
	if len(visitOrder) != len(expectedOrder) {
		t.Errorf("深度优先遍历节点数量不匹配: 期望 %d, 得到 %d",
			len(expectedOrder), len(visitOrder))
	}

	for i, expected := range expectedOrder {
		if i < len(visitOrder) && visitOrder[i] != expected {
			t.Errorf("深度优先遍历顺序不正确: 位置 %d, 期望 %v, 得到 %v",
				i, expected, visitOrder[i])
		}
	}

	// 重置并测试广度优先遍历
	walker.Reset()
	visitOrder = nil

	walker.WalkBreadthFirst(root, func(node ASTNode) bool {
		visitOrder = append(visitOrder, node)
		return true
	})

	expectedBreadthOrder := []ASTNode{root, child1, child2, grandchild}
	if len(visitOrder) != len(expectedBreadthOrder) {
		t.Errorf("广度优先遍历节点数量不匹配: 期望 %d, 得到 %d",
			len(expectedBreadthOrder), len(visitOrder))
	}
}

// TestASTFormatter 测试AST格式化器
func TestASTFormatter(t *testing.T) {
	// 创建简单的AST结构
	root := NewBaseASTNode(lexer.NewPosition(1, 1, 0))
	child := NewBaseASTNode(lexer.NewPosition(2, 1, 0))
	root.AddChild(child)

	formatter := NewASTFormatter("  ")
	result := formatter.FormatAST(root)

	if result == "" {
		t.Error("格式化结果不应该为空")
	}

	// 检查是否包含缩进
	if !strings.Contains(result, "  ") {
		t.Error("格式化结果应该包含缩进")
	}

	// 测试nil节点
	nilResult := formatter.FormatAST(nil)
	if nilResult != "<nil>" {
		t.Errorf("nil节点格式化结果不正确: 期望 '<nil>', 得到 '%s'", nilResult)
	}
}

// TestASTValidator 测试AST验证器
func TestASTValidator(t *testing.T) {
	validator := NewASTValidator()

	// 测试nil根节点
	errors := validator.ValidateAST(nil)
	if len(errors) == 0 {
		t.Error("nil根节点应该产生验证错误")
	}

	// 测试有效节点
	validNode := NewBaseASTNode(lexer.NewPosition(1, 1, 0))
	errors = validator.ValidateAST(validNode)
	if len(errors) != 0 {
		t.Errorf("有效节点不应该产生验证错误: %v", errors)
	}

	// 测试无效位置的节点
	invalidNode := NewBaseASTNode(lexer.NewPosition(0, 0, 0))
	errors = validator.ValidateAST(invalidNode)
	if len(errors) == 0 {
		t.Error("无效位置的节点应该产生验证错误")
	}
}
