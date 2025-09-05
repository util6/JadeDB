package ast

import (
	"fmt"
	"testing"

	"github.com/util6/JadeDB/sql/lexer"
)

// TestASTBasicUsage 展示AST基础使用方法
func TestASTBasicUsage(t *testing.T) {
	fmt.Println("=== AST基础使用示例 ===")

	// 创建一个简单的SELECT语句AST
	// SELECT id, name FROM users WHERE age > 18

	// 1. 创建SELECT语句节点
	selectPos := lexer.NewPosition(1, 1, 0)
	selectStmt := NewBaseStatement(selectPos, SELECT_STMT)
	selectStmt.SetParent(nil) // 根节点

	fmt.Printf("创建SELECT语句: %s 在位置 %s\n", selectStmt.String(), selectStmt.GetPosition().String())

	// 2. 创建列引用表达式 (id, name)
	idPos := lexer.NewPosition(1, 8, 7)
	idExpr := NewBaseExpression(idPos, COLUMN_REF_EXPR)
	selectStmt.AddChild(idExpr)

	namePos := lexer.NewPosition(1, 12, 11)
	nameExpr := NewBaseExpression(namePos, COLUMN_REF_EXPR)
	selectStmt.AddChild(nameExpr)

	// 3. 创建WHERE条件表达式 (age > 18)
	wherePos := lexer.NewPosition(1, 30, 29)
	whereExpr := NewBaseExpression(wherePos, BINARY_EXPR)
	selectStmt.AddChild(whereExpr)

	// 4. 为WHERE表达式添加左右操作数
	agePos := lexer.NewPosition(1, 36, 35)
	ageExpr := NewBaseExpression(agePos, COLUMN_REF_EXPR)
	whereExpr.AddChild(ageExpr)

	literalPos := lexer.NewPosition(1, 42, 41)
	literalExpr := NewBaseExpression(literalPos, LITERAL_EXPR)
	literalExpr.SetConstant(true) // 字面量是常量
	whereExpr.AddChild(literalExpr)

	fmt.Printf("AST结构创建完成，根节点有 %d 个子节点\n", len(selectStmt.GetChildren()))

	// 5. 遍历AST
	fmt.Println("\n=== 深度优先遍历AST ===")
	walker := NewASTWalker()
	walker.WalkDepthFirst(selectStmt, func(node ASTNode) bool {
		depth := 0
		if baseNode, ok := node.(*BaseASTNode); ok {
			depth = baseNode.GetDepth()
		} else if baseStmt, ok := node.(*BaseStatement); ok {
			depth = baseStmt.GetDepth()
		} else if baseExpr, ok := node.(*BaseExpression); ok {
			depth = baseExpr.GetDepth()
		}

		indent := ""
		for i := 0; i < depth; i++ {
			indent += "  "
		}
		fmt.Printf("%s- %s [%s]\n", indent, node.String(), node.GetPosition().String())
		return true
	})

	// 6. 格式化AST
	fmt.Println("\n=== AST格式化输出 ===")
	formatter := NewASTFormatter("  ")
	formatted := formatter.FormatAST(selectStmt)
	fmt.Print(formatted)

	// 7. 验证AST
	fmt.Println("=== AST验证 ===")
	validator := NewASTValidator()
	errors := validator.ValidateAST(selectStmt)
	if len(errors) == 0 {
		fmt.Println("AST验证通过，没有发现错误")
	} else {
		fmt.Printf("AST验证发现 %d 个错误:\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  - %s\n", err.Error())
		}
	}
}

// TestASTErrorHandling 展示AST错误处理
func TestASTErrorHandling(t *testing.T) {
	fmt.Println("\n=== AST错误处理示例 ===")

	// 创建一个有错误的语句
	pos := lexer.NewPosition(1, 1, 0)
	stmt := NewBaseStatement(pos, SELECT_STMT)

	// 添加一些解析错误
	stmt.AddError(ParseError{
		Position: lexer.NewPosition(1, 15, 14),
		Message:  "意外的标识符 'FORM'，期望 'FROM'",
		Code:     UNEXPECTED_TOKEN,
		Context:  "SELECT * FORM users",
	})

	stmt.AddError(ParseError{
		Position: lexer.NewPosition(1, 25, 24),
		Message:  "缺少分号",
		Code:     MISSING_TOKEN,
		Context:  "SELECT * FROM users",
	})

	fmt.Printf("语句类型: %s\n", stmt.GetStatementType().String())
	fmt.Printf("是否有错误: %v\n", stmt.HasErrors())
	fmt.Printf("错误数量: %d\n", len(stmt.GetErrors()))

	fmt.Println("\n错误详情:")
	for i, err := range stmt.GetErrors() {
		fmt.Printf("  %d. %s\n", i+1, err.Error())
	}
}

// TestASTTypeSystem 展示AST类型系统
func TestASTTypeSystem(t *testing.T) {
	fmt.Println("\n=== AST类型系统示例 ===")

	// 创建不同类型的表达式
	pos := lexer.NewPosition(1, 1, 0)

	// 1. 字面量表达式
	literalExpr := NewBaseExpression(pos, LITERAL_EXPR)
	literalExpr.SetConstant(true)
	literalExpr.SetDataType(&DataType{Name: "INTEGER"})

	fmt.Printf("字面量表达式:\n")
	fmt.Printf("  类型: %s\n", literalExpr.GetExpressionType().String())
	fmt.Printf("  是否常量: %v\n", literalExpr.GetIsConstant())
	fmt.Printf("  数据类型: %s\n", literalExpr.GetDataType().Name)

	// 2. 列引用表达式
	columnExpr := NewBaseExpression(pos, COLUMN_REF_EXPR)
	columnExpr.SetConstant(false)
	columnExpr.SetDataType(&DataType{Name: "VARCHAR", Length: 255})

	fmt.Printf("\n列引用表达式:\n")
	fmt.Printf("  类型: %s\n", columnExpr.GetExpressionType().String())
	fmt.Printf("  是否常量: %v\n", columnExpr.GetIsConstant())
	fmt.Printf("  数据类型: %s(%d)\n", columnExpr.GetDataType().Name, columnExpr.GetDataType().Length)

	// 3. 二元表达式
	binaryExpr := NewBaseExpression(pos, BINARY_EXPR)
	binaryExpr.AddChild(literalExpr)
	binaryExpr.AddChild(columnExpr)

	fmt.Printf("\n二元表达式:\n")
	fmt.Printf("  类型: %s\n", binaryExpr.GetExpressionType().String())
	fmt.Printf("  子节点数量: %d\n", len(binaryExpr.GetChildren()))
	fmt.Printf("  是否叶子节点: %v\n", binaryExpr.IsLeaf())
}

// TestASTNodeNavigation 展示AST节点导航
func TestASTNodeNavigation(t *testing.T) {
	fmt.Println("\n=== AST节点导航示例 ===")

	// 创建多层AST结构
	//     root (SELECT)
	//    /     |      \
	//  col1   col2   WHERE
	//               /     \
	//            col3    literal

	root := NewBaseStatement(lexer.NewPosition(1, 1, 0), SELECT_STMT)

	col1 := NewBaseExpression(lexer.NewPosition(1, 8, 7), COLUMN_REF_EXPR)
	col2 := NewBaseExpression(lexer.NewPosition(1, 13, 12), COLUMN_REF_EXPR)
	whereExpr := NewBaseExpression(lexer.NewPosition(1, 25, 24), BINARY_EXPR)

	root.AddChild(col1)
	root.AddChild(col2)
	root.AddChild(whereExpr)

	col3 := NewBaseExpression(lexer.NewPosition(1, 31, 30), COLUMN_REF_EXPR)
	literal := NewBaseExpression(lexer.NewPosition(1, 37, 36), LITERAL_EXPR)

	whereExpr.AddChild(col3)
	whereExpr.AddChild(literal)

	fmt.Printf("AST结构信息:\n")
	fmt.Printf("根节点: %s, 深度: %d, 是否根: %v\n",
		root.String(), root.GetDepth(), root.IsRoot())

	fmt.Printf("WHERE表达式: 深度: %d, 是否叶子: %v\n",
		whereExpr.GetDepth(), whereExpr.IsLeaf())

	fmt.Printf("字面量节点: 深度: %d, 是否叶子: %v\n",
		literal.GetDepth(), literal.IsLeaf())

	// 测试父子关系
	fmt.Printf("\n父子关系验证:\n")
	fmt.Printf("col3的父节点是WHERE表达式: %v\n", col3.GetParent() == whereExpr)
	fmt.Printf("literal的根节点是SELECT语句: %v\n", literal.GetRoot() == root)

	// 测试节点移除
	fmt.Printf("\n节点操作:\n")
	fmt.Printf("移除前WHERE表达式子节点数: %d\n", len(whereExpr.GetChildren()))
	whereExpr.RemoveChild(literal)
	fmt.Printf("移除后WHERE表达式子节点数: %d\n", len(whereExpr.GetChildren()))
	fmt.Printf("literal节点的父引用已清除: %v\n", literal.GetParent() == nil)
}

// TestASTValidationScenarios 展示AST验证场景
func TestASTValidationScenarios(t *testing.T) {
	fmt.Println("\n=== AST验证场景示例 ===")

	validator := NewASTValidator()

	// 场景1: 验证nil根节点
	fmt.Println("场景1: 验证nil根节点")
	errors := validator.ValidateAST(nil)
	fmt.Printf("  错误数量: %d\n", len(errors))
	if len(errors) > 0 {
		fmt.Printf("  错误: %s\n", errors[0].Error())
	}

	// 场景2: 验证有效AST
	fmt.Println("\n场景2: 验证有效AST")
	validAST := NewBaseStatement(lexer.NewPosition(1, 1, 0), SELECT_STMT)
	validChild := NewBaseExpression(lexer.NewPosition(1, 8, 7), COLUMN_REF_EXPR)
	validAST.AddChild(validChild)

	errors = validator.ValidateAST(validAST)
	fmt.Printf("  错误数量: %d\n", len(errors))
	if len(errors) == 0 {
		fmt.Println("  验证通过")
	}

	// 场景3: 验证无效位置的AST
	fmt.Println("\n场景3: 验证无效位置的AST")
	invalidAST := NewBaseStatement(lexer.NewPosition(0, 0, 0), SELECT_STMT)

	errors = validator.ValidateAST(invalidAST)
	fmt.Printf("  错误数量: %d\n", len(errors))
	for _, err := range errors {
		fmt.Printf("  错误: %s\n", err.Error())
	}
}
