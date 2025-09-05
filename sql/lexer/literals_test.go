package lexer

import (
	"testing"
)

// TestParseIntegerLiteral 测试整数字面量解析
func TestParseIntegerLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		input     string
		tokenType TokenType
		expected  int64
		shouldErr bool
	}{
		{"123", INTEGER_LIT, 123, false},
		{"0", INTEGER_LIT, 0, false},
		{"9223372036854775807", INTEGER_LIT, 9223372036854775807, false}, // int64最大值
		{"0x1A", HEX_LIT, 26, false},
		{"0xFF", HEX_LIT, 255, false},
		{"0b1010", BIT_LIT, 10, false},
		{"0b1111", BIT_LIT, 15, false},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, test.input, test.input, NewPosition(1, 1, 0))
		result, err := lp.ParseIntegerLiteral(token)

		if test.shouldErr {
			if err == nil {
				t.Errorf("输入 '%s': 期望错误但没有发生", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("输入 '%s': 意外错误 %v", test.input, err)
			}
			if result != test.expected {
				t.Errorf("输入 '%s': 期望 %d, 得到 %d", test.input, test.expected, result)
			}
		}
	}
}

// TestParseFloatLiteral 测试浮点数字面量解析
func TestParseFloatLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		input     string
		expected  float64
		shouldErr bool
	}{
		{"123.45", 123.45, false},
		{"0.0", 0.0, false},
		{"3.14159", 3.14159, false},
		{"1.23e10", 1.23e10, false},
		{"1.23E-5", 1.23e-5, false},
		{"0.123456789", 0.123456789, false},
	}

	for _, test := range tests {
		token := NewToken(FLOAT_LIT, test.input, test.input, NewPosition(1, 1, 0))
		result, err := lp.ParseFloatLiteral(token)

		if test.shouldErr {
			if err == nil {
				t.Errorf("输入 '%s': 期望错误但没有发生", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("输入 '%s': 意外错误 %v", test.input, err)
			}
			if result != test.expected {
				t.Errorf("输入 '%s': 期望 %g, 得到 %g", test.input, test.expected, result)
			}
		}
	}
}

// TestParseStringLiteral 测试字符串字面量解析
func TestParseStringLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		value    string // Token的Value（已处理转义）
		expected string
	}{
		{"hello", "hello"},
		{"world", "world"},
		{"hello\\nworld", "hello\\nworld"}, // 已处理的换行符
		{"it's", "it's"},                   // 已处理的单引号
		{"", ""},                           // 空字符串
		{"测试中文", "测试中文"},                   // 中文字符
	}

	for _, test := range tests {
		token := NewToken(STRING_LIT, test.value, "'"+test.value+"'", NewPosition(1, 1, 0))
		result, err := lp.ParseStringLiteral(token)

		if err != nil {
			t.Errorf("字符串 '%s': 意外错误 %v", test.value, err)
		}
		if result != test.expected {
			t.Errorf("字符串 '%s': 期望 '%s', 得到 '%s'", test.value, test.expected, result)
		}
	}
}

// TestParseBooleanLiteral 测试布尔字面量解析
func TestParseBooleanLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		tokenType TokenType
		expected  bool
	}{
		{TRUE_LIT, true},
		{FALSE_LIT, false},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, "", "", NewPosition(1, 1, 0))
		result, err := lp.ParseBooleanLiteral(token)

		if err != nil {
			t.Errorf("布尔类型 %v: 意外错误 %v", test.tokenType, err)
		}
		if result != test.expected {
			t.Errorf("布尔类型 %v: 期望 %v, 得到 %v", test.tokenType, test.expected, result)
		}
	}
}

// TestIsNullLiteral 测试NULL字面量检查
func TestIsNullLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		tokenType TokenType
		expected  bool
	}{
		{NULL_LIT, true},
		{INTEGER_LIT, false},
		{STRING_LIT, false},
		{TRUE_LIT, false},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, "", "", NewPosition(1, 1, 0))
		result := lp.IsNullLiteral(token)

		if result != test.expected {
			t.Errorf("Token类型 %v: 期望 %v, 得到 %v", test.tokenType, test.expected, result)
		}
	}
}

// TestGetLiteralType 测试字面量类型推断
func TestGetLiteralType(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		tokenType TokenType
		expected  string
	}{
		{INTEGER_LIT, "INTEGER"},
		{HEX_LIT, "INTEGER"},
		{BIT_LIT, "INTEGER"},
		{FLOAT_LIT, "FLOAT"},
		{STRING_LIT, "VARCHAR"},
		{TRUE_LIT, "BOOLEAN"},
		{FALSE_LIT, "BOOLEAN"},
		{NULL_LIT, "NULL"},
		{IDENTIFIER, "UNKNOWN"},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, "", "", NewPosition(1, 1, 0))
		result := lp.GetLiteralType(token)

		if result != test.expected {
			t.Errorf("Token类型 %v: 期望 '%s', 得到 '%s'", test.tokenType, test.expected, result)
		}
	}
}

// TestConvertLiteralValue 测试字面量值转换
func TestConvertLiteralValue(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		tokenType TokenType
		value     string
		expected  interface{}
		shouldErr bool
	}{
		{INTEGER_LIT, "123", int64(123), false},
		{FLOAT_LIT, "123.45", 123.45, false},
		{STRING_LIT, "hello", "hello", false},
		{TRUE_LIT, "TRUE", true, false},
		{FALSE_LIT, "FALSE", false, false},
		{NULL_LIT, "NULL", nil, false},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, test.value, test.value, NewPosition(1, 1, 0))
		result, err := lp.ConvertLiteralValue(token)

		if test.shouldErr {
			if err == nil {
				t.Errorf("Token %v '%s': 期望错误但没有发生", test.tokenType, test.value)
			}
		} else {
			if err != nil {
				t.Errorf("Token %v '%s': 意外错误 %v", test.tokenType, test.value, err)
			}
			if result != test.expected {
				t.Errorf("Token %v '%s': 期望 %v, 得到 %v", test.tokenType, test.value, test.expected, result)
			}
		}
	}
}

// TestFormatLiteralForSQL 测试SQL格式化
func TestFormatLiteralForSQL(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		value    interface{}
		dataType string
		expected string
	}{
		{nil, "NULL", "NULL"},
		{123, "INTEGER", "123"},
		{123.45, "FLOAT", "123.45"},
		{"hello", "VARCHAR", "'hello'"},
		{"it's", "VARCHAR", "'it''s'"}, // 转义单引号
		{true, "BOOLEAN", "TRUE"},
		{false, "BOOLEAN", "FALSE"},
	}

	for _, test := range tests {
		result := lp.FormatLiteralForSQL(test.value, test.dataType)

		if result != test.expected {
			t.Errorf("值 %v (%s): 期望 '%s', 得到 '%s'", test.value, test.dataType, test.expected, result)
		}
	}
}

// TestLiteralTypeCheckers 测试字面量类型检查器
func TestLiteralTypeCheckers(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)

	tests := []struct {
		tokenType TokenType
		isNumeric bool
		isString  bool
		isBoolean bool
	}{
		{INTEGER_LIT, true, false, false},
		{FLOAT_LIT, true, false, false},
		{HEX_LIT, true, false, false},
		{BIT_LIT, true, false, false},
		{STRING_LIT, false, true, false},
		{TRUE_LIT, false, false, true},
		{FALSE_LIT, false, false, true},
		{NULL_LIT, false, false, false},
		{IDENTIFIER, false, false, false},
	}

	for _, test := range tests {
		token := NewToken(test.tokenType, "", "", NewPosition(1, 1, 0))

		if lp.IsNumericLiteral(token) != test.isNumeric {
			t.Errorf("Token %v: IsNumericLiteral期望 %v", test.tokenType, test.isNumeric)
		}

		if lp.IsStringLiteral(token) != test.isString {
			t.Errorf("Token %v: IsStringLiteral期望 %v", test.tokenType, test.isString)
		}

		if lp.IsBooleanLiteral(token) != test.isBoolean {
			t.Errorf("Token %v: IsBooleanLiteral期望 %v", test.tokenType, test.isBoolean)
		}
	}
}
