package parser

import (
	"testing"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

func TestBasicSelectParsing(t *testing.T) {
	// 创建一个简单的SELECT语句的token流
	tokens := []lexer.Token{
		lexer.NewToken(lexer.SELECT, "SELECT", "SELECT", lexer.NewPosition(1, 1, 0)),
		lexer.NewToken(lexer.IDENTIFIER, "name", "name", lexer.NewPosition(1, 8, 7)),
		lexer.NewToken(lexer.FROM, "FROM", "FROM", lexer.NewPosition(1, 13, 12)),
		lexer.NewToken(lexer.IDENTIFIER, "users", "users", lexer.NewPosition(1, 18, 17)),
		lexer.NewToken(lexer.EOF, "", "", lexer.NewPosition(1, 23, 22)),
	}

	// 创建解析器
	parser := NewParser(tokens)

	// 解析
	astResult, err := parser.Parse(tokens)

	// 验证结果
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}

	if astResult == nil {
		t.Fatal("AST不应该为nil")
	}

	if len(astResult.Statements) != 1 {
		t.Fatalf("期望1个语句，得到%d个", len(astResult.Statements))
	}

	// 检查是否为SELECT语句
	selectStmt, ok := astResult.Statements[0].(*ast.SelectStatement)
	if !ok {
		t.Fatal("期望SELECT语句")
	}

	if len(selectStmt.SelectList) != 1 {
		t.Fatalf("期望1个SELECT项，得到%d个", len(selectStmt.SelectList))
	}

	t.Log("基本SELECT语句解析成功")
}

func TestParserErrors(t *testing.T) {
	// 测试错误的token流
	tokens := []lexer.Token{
		lexer.NewToken(lexer.SELECT, "SELECT", "SELECT", lexer.NewPosition(1, 1, 0)),
		lexer.NewToken(lexer.FROM, "FROM", "FROM", lexer.NewPosition(1, 8, 7)), // 缺少SELECT列表
		lexer.NewToken(lexer.EOF, "", "", lexer.NewPosition(1, 13, 12)),
	}

	parser := NewParser(tokens)
	_, err := parser.Parse(tokens)

	// 应该有错误
	if err == nil {
		t.Fatal("期望解析错误，但没有错误")
	}

	t.Logf("正确捕获了解析错误: %v", err)
}
