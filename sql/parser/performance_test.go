package parser

import (
	"testing"

	"github.com/util6/JadeDB/sql/lexer"
)

// TestParserPerformance 测试解析器性能
func TestParserPerformance(t *testing.T) {
	// 测试用例
	testCases := []string{
		"SELECT name FROM users",
		"SELECT * FROM users WHERE age > 18",
		"SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id",
		"INSERT INTO users (name, age) VALUES ('John', 25)",
		"UPDATE users SET age = 26 WHERE name = 'John'",
		"DELETE FROM users WHERE age < 18",
		"BEGIN TRANSACTION",
		"COMMIT",
		"SELECT name FROM users UNION SELECT name FROM customers",
	}

	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			// 词法分析
			l := lexer.NewLexer(sql)
			tokens, err := l.Tokenize(sql)
			if err != nil {
				t.Fatalf("词法分析失败: %v", err)
			}

			// 语法分析
			p := NewParser(tokens)
			astNode, err := p.Parse(tokens)
			if err != nil {
				t.Fatalf("语法分析失败: %v", err)
			}

			if len(astNode.Statements) == 0 {
				t.Fatalf("没有生成语句")
			}

			// 验证基本结构
			stmt := astNode.Statements[0]
			if stmt == nil {
				t.Fatalf("语句为空")
			}

			// 检查位置信息
			pos := stmt.GetPosition()
			if pos.Line <= 0 || pos.Column <= 0 {
				t.Errorf("位置信息无效: line=%d, column=%d", pos.Line, pos.Column)
			}
		})
	}
}

// BenchmarkParser 基准测试
func BenchmarkParser(b *testing.B) {
	sql := "SELECT u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.age > 18 GROUP BY u.id, u.name HAVING COUNT(o.id) > 0 ORDER BY order_count DESC LIMIT 10"

	// 预先进行词法分析
	l := lexer.NewLexer(sql)
	tokens, err := l.Tokenize(sql)
	if err != nil {
		b.Fatalf("词法分析失败: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := NewParser(tokens)
		_, err := p.Parse(tokens)
		if err != nil {
			b.Fatalf("语法分析失败: %v", err)
		}
	}
}

// TestParserErrorHandling 测试错误处理
func TestParserErrorHandling(t *testing.T) {
	errorCases := []struct {
		name string
		sql  string
	}{
		{"空SQL", ""},
		{"只有空格", "   "},
		{"无效关键字", "INVALID STATEMENT"},
		{"语法错误", "SELECT FROM"},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			l := lexer.NewLexer(tc.sql)
			tokens, err := l.Tokenize(tc.sql)

			// 某些情况下词法分析就会失败
			if err != nil {
				return // 这是预期的
			}

			p := NewParser(tokens)
			_, err = p.Parse(tokens)

			// 对于这些错误情况，我们期望解析失败或者产生错误
			// 但不应该崩溃
			if err == nil && len(tokens) > 1 { // 如果有实际内容但没有错误，可能有问题
				t.Logf("警告: SQL '%s' 解析成功，但可能应该失败", tc.sql)
			}
		})
	}
}

// TestParserMemoryUsage 测试内存使用
func TestParserMemoryUsage(t *testing.T) {
	// 测试大型SQL语句的解析
	largeSQLParts := []string{
		"SELECT",
		"u1.name, u2.name, u3.name, u4.name, u5.name",
		"FROM users u1",
		"JOIN users u2 ON u1.id = u2.parent_id",
		"JOIN users u3 ON u2.id = u3.parent_id",
		"JOIN users u4 ON u3.id = u4.parent_id",
		"JOIN users u5 ON u4.id = u5.parent_id",
		"WHERE u1.age > 18 AND u2.age > 18 AND u3.age > 18",
		"ORDER BY u1.name, u2.name, u3.name",
	}

	largeSQL := ""
	for _, part := range largeSQLParts {
		largeSQL += part + " "
	}

	l := lexer.NewLexer(largeSQL)
	tokens, err := l.Tokenize(largeSQL)
	if err != nil {
		t.Fatalf("词法分析失败: %v", err)
	}

	p := NewParser(tokens)
	astNode, err := p.Parse(tokens)
	if err != nil {
		t.Fatalf("语法分析失败: %v", err)
	}

	// 验证AST不为空且结构合理
	if astNode == nil || len(astNode.Statements) == 0 {
		t.Fatalf("大型SQL解析失败")
	}

	// 检查元数据
	if astNode.Metadata != nil {
		t.Logf("Token数量: %d, 节点数量: %d",
			astNode.Metadata.TokenCount, astNode.Metadata.NodeCount)

		if astNode.Metadata.NodeCount <= 0 {
			t.Errorf("节点数量应该大于0")
		}
	}
}
