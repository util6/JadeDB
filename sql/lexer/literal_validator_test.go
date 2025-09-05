package lexer

import (
	"testing"
)

// TestValidateIntegerLiteral 测试整数字面量验证
func TestValidateIntegerLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)
	lv := NewLiteralValidator(lp)

	tests := []struct {
		value       string
		targetType  string
		shouldPass  bool
		description string
	}{
		{"100", "TINYINT", true, "TINYINT范围内的值"},
		{"200", "TINYINT", false, "TINYINT范围外的值"},
		{"200", "TINYINT UNSIGNED", true, "TINYINT UNSIGNED范围内的值"},
		{"300", "TINYINT UNSIGNED", false, "TINYINT UNSIGNED范围外的值"},
		{"30000", "SMALLINT", true, "SMALLINT范围内的值"},
		{"40000", "SMALLINT", false, "SMALLINT范围外的值"},
		{"2000000000", "INT", true, "INT范围内的值"},
		{"3000000000", "INT", false, "INT范围外的值"},
		{"9223372036854775807", "BIGINT", true, "BIGINT最大值"},
	}

	for _, test := range tests {
		token := NewToken(INTEGER_LIT, test.value, test.value, NewPosition(1, 1, 0))
		result := lv.ValidateIntegerLiteral(token, test.targetType)

		if test.shouldPass {
			if result.HasErrors() {
				t.Errorf("%s: 期望通过但有错误: %v", test.description, result.Errors)
			}
		} else {
			if !result.HasErrors() {
				t.Errorf("%s: 期望失败但没有错误", test.description)
			}
		}
	}
}

// TestValidateFloatLiteral 测试浮点数字面量验证
func TestValidateFloatLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)
	lv := NewLiteralValidator(lp)

	tests := []struct {
		value       string
		targetType  string
		precision   int
		scale       int
		shouldPass  bool
		description string
	}{
		{"123.45", "FLOAT", 0, 0, true, "普通FLOAT值"},
		{"123.456789", "DOUBLE", 0, 0, true, "普通DOUBLE值"},
		{"123.45", "DECIMAL", 5, 2, true, "DECIMAL范围内的值"},
		{"123.456", "DECIMAL", 5, 2, false, "DECIMAL小数位数超出"},
	}

	for _, test := range tests {
		token := NewToken(FLOAT_LIT, test.value, test.value, NewPosition(1, 1, 0))
		result := lv.ValidateFloatLiteral(token, test.targetType, test.precision, test.scale)

		if test.shouldPass {
			if result.HasErrors() {
				t.Errorf("%s: 期望通过但有错误: %v", test.description, result.Errors)
			}
		} else {
			if !result.HasErrors() {
				t.Errorf("%s: 期望失败但没有错误", test.description)
			}
		}
	}
}

// TestValidateStringLiteral 测试字符串字面量验证
func TestValidateStringLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)
	lv := NewLiteralValidator(lp)

	tests := []struct {
		value       string
		targetType  string
		maxLength   int
		charset     string
		shouldPass  bool
		description string
	}{
		{"hello", "VARCHAR", 10, "utf8", true, "VARCHAR范围内的值"},
		{"hello world", "VARCHAR", 5, "utf8", false, "VARCHAR长度超出"},
		{"test", "CHAR", 4, "utf8", true, "CHAR固定长度"},
		{"测试", "VARCHAR", 10, "utf8mb4", true, "中文字符"},
		{"hello", "TEXT", 0, "utf8", true, "TEXT类型"},
	}

	for _, test := range tests {
		token := NewToken(STRING_LIT, test.value, "'"+test.value+"'", NewPosition(1, 1, 0))
		result := lv.ValidateStringLiteral(token, test.targetType, test.maxLength, test.charset)

		if test.shouldPass {
			if result.HasErrors() {
				t.Errorf("%s: 期望通过但有错误: %v", test.description, result.Errors)
			}
		} else {
			if !result.HasErrors() {
				t.Errorf("%s: 期望失败但没有错误", test.description)
			}
		}
	}
}

// TestValidateDateTimeLiteral 测试日期时间字面量验证
func TestValidateDateTimeLiteral(t *testing.T) {
	lexer := NewLexer("")
	lp := NewLiteralParser(lexer)
	lv := NewLiteralValidator(lp)

	tests := []struct {
		value       string
		targetType  string
		shouldPass  bool
		description string
	}{
		{"2023-12-25", "DATE", true, "有效的日期格式"},
		{"2023/12/25", "DATE", false, "无效的日期格式"},
		{"14:30:00", "TIME", true, "有效的时间格式"},
		{"14:30", "TIME", false, "无效的时间格式"},
		{"2023-12-25 14:30:00", "DATETIME", true, "有效的日期时间格式"},
		{"2023-12-25T14:30:00", "DATETIME", false, "无效的日期时间格式"},
		{"1703520600", "TIMESTAMP", true, "Unix时间戳"},
		{"2023-12-25 14:30:00", "TIMESTAMP", true, "日期时间格式的时间戳"},
	}

	for _, test := range tests {
		token := NewToken(STRING_LIT, test.value, "'"+test.value+"'", NewPosition(1, 1, 0))
		result := lv.ValidateDateTimeLiteral(token, test.targetType)

		if test.shouldPass {
			if result.HasErrors() {
				t.Errorf("%s: 期望通过但有错误: %v", test.description, result.Errors)
			}
		} else {
			if !result.HasErrors() {
				t.Errorf("%s: 期望失败但没有错误", test.description)
			}
		}
	}
}

// TestValidationResult 测试验证结果结构
func TestValidationResult(t *testing.T) {
	result := &ValidationResult{IsValid: true}

	// 测试添加错误
	result.AddError("测试错误")
	if result.IsValid {
		t.Error("添加错误后IsValid应该为false")
	}
	if !result.HasErrors() {
		t.Error("应该有错误")
	}
	if len(result.Errors) != 1 {
		t.Errorf("期望1个错误，得到%d个", len(result.Errors))
	}

	// 测试添加警告
	result.AddWarning("测试警告")
	if !result.HasWarnings() {
		t.Error("应该有警告")
	}
	if len(result.Warnings) != 1 {
		t.Errorf("期望1个警告，得到%d个", len(result.Warnings))
	}
}
