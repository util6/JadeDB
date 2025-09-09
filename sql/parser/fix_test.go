package parser

import (
	"testing"

	"github.com/util6/JadeDB/sql/lexer"
)

// TestParseExpressionMethod 测试ParseExpression方法是否正常工作
func TestParseExpressionMethod(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "简单算术表达式",
			input:    "1 + 2",
			expected: "BINARY_EXPR",
		},
		{
			name:     "列引用",
			input:    "name",
			expected: "name",
		},
		{
			name:     "字符串字面量",
			input:    "'hello'",
			expected: "LITERAL",
		},
		{
			name:     "函数调用",
			input:    "COUNT(*)",
			expected: "COUNT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建词法分析器
			l := lexer.NewLexer(tt.input)
			tokens, err := l.Tokenize(tt.input)
			if err != nil {
				t.Fatalf("词法分析失败: %v", err)
			}

			// 创建解析器
			p := NewParser(tokens)

			// 解析表达式
			expr := p.ParseExpression()
			if expr == nil {
				t.Fatalf("解析表达式失败，期望得到表达式，但得到nil")
			}

			// 检查结果
			if expr.String() != tt.expected {
				t.Errorf("表达式类型不匹配，期望 %s，得到 %s", tt.expected, expr.String())
			}

			// 检查是否有错误
			if p.HasErrors() {
				t.Errorf("解析过程中出现错误: %v", p.GetErrors())
			}
		})
	}
}

// TestCanBeUsedAsIdentifier 测试canBeUsedAsIdentifier方法
func TestCanBeUsedAsIdentifier(t *testing.T) {
	tests := []struct {
		tokenType lexer.TokenType
		expected  bool
	}{
		{lexer.INT, true},
		{lexer.VARCHAR, true},
		{lexer.COUNT, true},
		{lexer.SELECT, false},
		{lexer.FROM, false},
		{lexer.IDENTIFIER, false}, // 标识符本身不需要这个检查
	}

	p := NewParser([]lexer.Token{})

	for _, tt := range tests {
		result := p.canBeUsedAsIdentifier(tt.tokenType)
		if result != tt.expected {
			t.Errorf("canBeUsedAsIdentifier(%v) = %v, 期望 %v",
				tt.tokenType, result, tt.expected)
		}
	}
}

// TestPrattParserPrecedence 测试Pratt解析器的优先级处理
func TestPrattParserPrecedence(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "算术优先级",
			input: "1 + 2 * 3",
		},
		{
			name:  "逻辑优先级",
			input: "a AND b OR c",
		},
		{
			name:  "括号优先级",
			input: "(1 + 2) * 3",
		},
		{
			name:  "比较操作符",
			input: "a = b AND c > d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建词法分析器
			l := lexer.NewLexer(tt.input)
			tokens, err := l.Tokenize(tt.input)
			if err != nil {
				t.Fatalf("词法分析失败: %v", err)
			}

			// 创建解析器
			p := NewParser(tokens)

			// 解析表达式
			expr := p.ParseExpression()
			if expr == nil {
				t.Fatalf("解析表达式失败")
			}

			// 检查是否有错误
			if p.HasErrors() {
				t.Errorf("解析过程中出现错误: %v", p.GetErrors())
			}
		})
	}
}
