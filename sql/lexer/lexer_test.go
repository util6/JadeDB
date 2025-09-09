package lexer

import (
	"testing"
)

// TestBasicTokenization 测试基础分词功能
func TestBasicTokenization(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	lexer := NewLexer(input)

	expectedTokens := []TokenType{
		SELECT, MULTIPLY, FROM, IDENTIFIER, WHERE, IDENTIFIER, EQUAL, INTEGER_LIT, EOF,
	}

	for i, expected := range expectedTokens {
		token := lexer.NextToken()
		if token.Type != expected {
			t.Errorf("Token %d: expected %v, got %v", i, expected, token.Type)
		}
	}
}

// TestTokenizeMethod 测试Tokenize方法
func TestTokenizeMethod(t *testing.T) {
	input := "SELECT name, age FROM users"
	lexer := NewLexer(input)

	tokens, err := lexer.Tokenize(input)
	if err != nil {
		t.Errorf("Tokenize failed: %v", err)
	}

	expectedTypes := []TokenType{
		SELECT, IDENTIFIER, COMMA, IDENTIFIER, FROM, IDENTIFIER, EOF,
	}

	if len(tokens) != len(expectedTypes) {
		t.Errorf("Expected %d tokens, got %d", len(expectedTypes), len(tokens))
	}

	for i, expected := range expectedTypes {
		if i < len(tokens) && tokens[i].Type != expected {
			t.Errorf("Token %d: expected %v, got %v", i, expected, tokens[i].Type)
		}
	}
}

// TestStringLiterals 测试字符串字面量
func TestStringLiterals(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'hello'", "hello"},
		{"\"world\"", "world"},
		{"'hello\\nworld'", "hello\nworld"},
		{"'it\\'s'", "it's"},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		token := lexer.NextToken()

		if token.Type != STRING_LIT {
			t.Errorf("Expected STRING_LIT, got %v", token.Type)
		}

		if token.Value != test.expected {
			t.Errorf("Expected value '%s', got '%s'", test.expected, token.Value)
		}
	}
}

// TestNumbers 测试数字字面量
func TestNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"123", INTEGER_LIT},
		{"123.45", FLOAT_LIT},
		{"1.23e10", FLOAT_LIT},
		{"0x1A2B", HEX_LIT},
		{"0b1010", BIT_LIT},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		token := lexer.NextToken()

		if token.Type != test.expected {
			t.Errorf("Input '%s': expected %v, got %v", test.input, test.expected, token.Type)
		}
	}
}

// TestOperators 测试操作符
func TestOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"+", PLUS},
		{"-", MINUS},
		{"*", MULTIPLY},
		{"/", DIVIDE},
		{"=", EQUAL},
		{"!=", NOT_EQUAL},
		{"<>", NOT_EQUAL},
		{"<", LESS},
		{"<=", LESS_EQUAL},
		{">", GREATER},
		{">=", GREATER_EQUAL},
		{"<=>", NULL_SAFE_EQUAL},
		{"&&", AND_OP},
		{"||", OR_OP},
		{"<<", LEFT_SHIFT},
		{">>", RIGHT_SHIFT},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		token := lexer.NextToken()

		if token.Type != test.expected {
			t.Errorf("Input '%s': expected %v, got %v", test.input, test.expected, token.Type)
		}
	}
}

// TestComments 测试注释
func TestComments(t *testing.T) {
	tests := []struct {
		input       string
		description string
	}{
		{"-- 这是单行注释", "单行注释"},
		{"/* 这是块注释 */", "块注释"},
		{"/* 外层 /* 内层 */ 注释 */", "嵌套块注释"},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		token := lexer.NextToken()

		if token.Type != COMMENT {
			t.Errorf("%s: expected COMMENT, got %v", test.description, token.Type)
		}
	}
}

// TestKeywords 测试关键字识别
func TestKeywords(t *testing.T) {
	keywords := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", SELECT},
		{"select", SELECT}, // 大小写不敏感
		{"FROM", FROM},
		{"WHERE", WHERE},
		{"INSERT", INSERT},
		{"UPDATE", UPDATE},
		{"DELETE", DELETE},
		{"CREATE", CREATE},
		{"TABLE", TABLE},
		{"INDEX", INDEX},
	}

	for _, keyword := range keywords {
		lexer := NewLexer(keyword.input)
		token := lexer.NextToken()

		if token.Type != keyword.expected {
			t.Errorf("Keyword '%s': expected %v, got %v", keyword.input, keyword.expected, token.Type)
		}
	}
}

// TestComplexSQL 测试复杂SQL语句
func TestComplexSQL(t *testing.T) {
	input := `
		SELECT u.name, u.age, COUNT(*) as cnt
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.age >= 18 AND u.status = 'active'
		GROUP BY u.name, u.age
		HAVING COUNT(*) > 5
		ORDER BY cnt DESC
		LIMIT 10;
	`

	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize(input)

	if err != nil {
		t.Errorf("Failed to tokenize complex SQL: %v", err)
	}

	// 验证包含了预期的关键字
	expectedKeywords := []TokenType{
		SELECT, FROM, LEFT, JOIN, ON, WHERE, AND, GROUP, BY, HAVING, ORDER, BY, LIMIT,
	}

	foundKeywords := make(map[TokenType]bool)
	for _, token := range tokens {
		if token.IsKeyword() {
			foundKeywords[token.Type] = true
		}
	}

	for _, expected := range expectedKeywords {
		if !foundKeywords[expected] {
			t.Errorf("Expected keyword %v not found in tokens", expected)
		}
	}
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		input       string
		description string
	}{
		{"'未闭合字符串", "未闭合字符串"},
		{"/* 未闭合注释", "未闭合块注释"},
		{"0x", "无效十六进制数"},
		{"0b", "无效二进制数"},
	}

	for _, test := range tests {
		lexer := NewLexer(test.input)
		_, err := lexer.Tokenize(test.input)

		if err == nil {
			t.Errorf("%s: expected error but got none", test.description)
		}
	}
}

// TestPositionTracking 测试位置跟踪
func TestPositionTracking(t *testing.T) {
	input := "SELECT\n  *\nFROM users"
	lexer := NewLexer(input)

	// SELECT 应该在第1行第1列
	token1 := lexer.NextToken()
	if token1.Position.Line != 1 || token1.Position.Column != 1 {
		t.Errorf("SELECT position: expected (1,1), got (%d,%d)",
			token1.Position.Line, token1.Position.Column)
	}

	// * 应该在第2行第3列
	token2 := lexer.NextToken()
	if token2.Position.Line != 2 || token2.Position.Column != 3 {
		t.Errorf("* position: expected (2,3), got (%d,%d)",
			token2.Position.Line, token2.Position.Column)
	}

	// FROM 应该在第3行第1列
	token3 := lexer.NextToken()
	if token3.Position.Line != 3 || token3.Position.Column != 1 {
		t.Errorf("FROM position: expected (3,1), got (%d,%d)",
			token3.Position.Line, token3.Position.Column)
	}
}
