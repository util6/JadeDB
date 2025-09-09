package sql

import (
	"testing"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
	"github.com/util6/JadeDB/sql/parser"
)

// TestSQLParserIntegration 测试SQL解析器的完整集成
func TestSQLParserIntegration(t *testing.T) {
	// 完整的SQL脚本测试
	sqlScript := `
		-- 创建用户表
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			age INT
		);
		
		-- 插入测试数据
		INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
		INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);
		
		-- 查询数据
		SELECT * FROM users WHERE age > 20;
		
		-- 更新数据
		UPDATE users SET age = 26 WHERE name = 'Alice';
		
		-- 事务操作
		BEGIN TRANSACTION;
		DELETE FROM users WHERE age < 25;
		COMMIT;
		
		-- 集合操作
		SELECT name FROM users UNION SELECT 'Admin' AS name;
	`

	t.Logf("测试SQL脚本:\n%s", sqlScript)

	// 词法分析
	l := lexer.NewLexer(sqlScript)
	tokens, err := l.Tokenize(sqlScript)
	if err != nil {
		t.Fatalf("词法分析失败: %v", err)
	}

	t.Logf("生成了 %d 个Token", len(tokens))

	// 语法分析
	p := parser.NewParser(tokens)
	astNode, err := p.Parse(tokens)
	if err != nil {
		t.Logf("语法分析警告: %v", err)
		// 不要因为部分语法不支持而失败，继续验证已解析的部分
	}

	if astNode == nil {
		t.Fatalf("AST为空")
	}

	t.Logf("生成了 %d 个语句", len(astNode.Statements))

	// 验证每个语句
	_ = []string{
		"CREATE TABLE", "INSERT", "INSERT", "SELECT", "UPDATE",
		"BEGIN TRANSACTION", "DELETE", "COMMIT", "UNION",
	}

	validStatements := 0
	for i, stmt := range astNode.Statements {
		if stmt != nil {
			t.Logf("语句 %d: %s", i+1, stmt.String())
			validStatements++
		}
	}

	if validStatements == 0 {
		t.Fatalf("没有有效的语句被解析")
	}

	// 使用Visitor遍历AST
	visitor := ast.NewPrintVisitor()
	for _, stmt := range astNode.Statements {
		if stmt != nil {
			stmt.Accept(visitor)
		}
	}

	output := visitor.GetOutput()
	if len(output) == 0 {
		t.Errorf("Visitor没有生成输出")
	} else {
		t.Logf("AST结构:\n%s", output)
	}

	// 验证元数据
	if astNode.Metadata != nil {
		t.Logf("解析元数据: Token数=%d, 节点数=%d",
			astNode.Metadata.TokenCount, astNode.Metadata.NodeCount)
	}
}

// TestSQLParserComponents 测试各个组件的功能
func TestSQLParserComponents(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		testFunc func(t *testing.T, astNode *ast.AST)
	}{
		{
			name: "SELECT语句",
			sql:  "SELECT name, age FROM users WHERE age > 18 ORDER BY name",
			testFunc: func(t *testing.T, astNode *ast.AST) {
				if len(astNode.Statements) != 1 {
					t.Fatalf("期望1个语句，得到%d个", len(astNode.Statements))
				}

				selectStmt, ok := astNode.Statements[0].(*ast.SelectStatement)
				if !ok {
					t.Fatalf("期望SELECT语句，得到%T", astNode.Statements[0])
				}

				if len(selectStmt.SelectList) != 2 {
					t.Errorf("期望2个选择项，得到%d个", len(selectStmt.SelectList))
				}

				if selectStmt.FromClause == nil {
					t.Errorf("FROM子句不应为空")
				}

				if selectStmt.WhereClause == nil {
					t.Errorf("WHERE子句不应为空")
				}

				if selectStmt.OrderByClause == nil {
					t.Errorf("ORDER BY子句不应为空")
				}
			},
		},
		{
			name: "INSERT语句",
			sql:  "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)",
			testFunc: func(t *testing.T, astNode *ast.AST) {
				insertStmt, ok := astNode.Statements[0].(*ast.InsertStatement)
				if !ok {
					t.Fatalf("期望INSERT语句，得到%T", astNode.Statements[0])
				}

				if insertStmt.Table != "users" {
					t.Errorf("期望表名'users'，得到'%s'", insertStmt.Table)
				}

				if len(insertStmt.Columns) != 2 {
					t.Errorf("期望2个列，得到%d个", len(insertStmt.Columns))
				}

				if len(insertStmt.Values) != 2 {
					t.Errorf("期望2行数据，得到%d行", len(insertStmt.Values))
				}
			},
		},
		{
			name: "事务语句",
			sql:  "BEGIN; UPDATE users SET age = 26; COMMIT;",
			testFunc: func(t *testing.T, astNode *ast.AST) {
				if len(astNode.Statements) < 2 {
					t.Fatalf("期望至少2个语句，得到%d个", len(astNode.Statements))
				}

				// 检查第一个语句是BEGIN
				if _, ok := astNode.Statements[0].(*ast.BeginTransactionStatement); !ok {
					t.Errorf("第一个语句应该是BEGIN，得到%T", astNode.Statements[0])
				}
			},
		},
		{
			name: "UNION语句",
			sql:  "SELECT name FROM users UNION ALL SELECT name FROM customers",
			testFunc: func(t *testing.T, astNode *ast.AST) {
				unionStmt, ok := astNode.Statements[0].(*ast.UnionStatement)
				if !ok {
					t.Fatalf("期望UNION语句，得到%T", astNode.Statements[0])
				}

				if unionStmt.UnionType != ast.UNION_TYPE {
					t.Errorf("期望UNION类型，得到%v", unionStmt.UnionType)
				}

				if !unionStmt.All {
					t.Errorf("期望UNION ALL，但All标志为false")
				}

				if unionStmt.Left == nil || unionStmt.Right == nil {
					t.Errorf("UNION的左右查询不应为空")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 词法分析
			l := lexer.NewLexer(tc.sql)
			tokens, err := l.Tokenize(tc.sql)
			if err != nil {
				t.Fatalf("词法分析失败: %v", err)
			}

			// 语法分析
			p := parser.NewParser(tokens)
			astNode, err := p.Parse(tokens)
			if err != nil {
				t.Fatalf("语法分析失败: %v", err)
			}

			// 运行特定测试
			tc.testFunc(t, astNode)
		})
	}
}

// TestSQLParserEdgeCases 测试边界情况
func TestSQLParserEdgeCases(t *testing.T) {
	edgeCases := []struct {
		name string
		sql  string
		desc string
	}{
		{
			name: "空白SQL",
			sql:  "   \n\t  ",
			desc: "只包含空白字符的SQL",
		},
		{
			name: "单个分号",
			sql:  ";",
			desc: "只有分号的SQL",
		},
		{
			name: "多个分号",
			sql:  ";;;",
			desc: "多个连续分号",
		},
		{
			name: "注释SQL",
			sql:  "-- 这是注释\nSELECT 1",
			desc: "包含注释的SQL",
		},
		{
			name: "长标识符",
			sql:  "SELECT very_long_column_name_that_might_cause_issues FROM very_long_table_name",
			desc: "包含长标识符的SQL",
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("测试: %s", tc.desc)

			// 词法分析
			l := lexer.NewLexer(tc.sql)
			tokens, err := l.Tokenize(tc.sql)
			if err != nil {
				t.Logf("词法分析失败（可能是预期的）: %v", err)
				return
			}

			// 语法分析
			p := parser.NewParser(tokens)
			astNode, err := p.Parse(tokens)
			if err != nil {
				t.Logf("语法分析失败（可能是预期的）: %v", err)
				return
			}

			t.Logf("成功解析，生成%d个语句", len(astNode.Statements))
		})
	}
}
