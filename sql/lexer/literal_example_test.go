package lexer

import (
	"fmt"
	"testing"
)

// TestLiteralProcessingExample 展示字面量处理的完整流程
func TestLiteralProcessingExample(t *testing.T) {
	// 创建词法分析器和字面量处理器
	lexer := NewLexer("SELECT 123, 45.67, 'hello world', TRUE, NULL, 0xFF FROM users")
	parser := NewLiteralParser(lexer)
	validator := NewLiteralValidator(parser)

	fmt.Println("=== SQL字面量处理示例 ===")
	fmt.Println("SQL: SELECT 123, 45.67, 'hello world', TRUE, NULL, 0xFF FROM users")
	fmt.Println()

	// 逐个处理Token
	for {
		token := lexer.NextToken()
		if token.Type == EOF {
			break
		}

		// 只处理字面量Token
		if parser.IsNumericLiteral(token) || parser.IsStringLiteral(token) ||
			parser.IsBooleanLiteral(token) || parser.IsNullLiteral(token) {

			fmt.Printf("发现字面量: %s (类型: %v)\n", token.Value, token.Type)

			// 获取SQL数据类型
			sqlType := parser.GetLiteralType(token)
			fmt.Printf("  SQL类型: %s\n", sqlType)

			// 转换为Go值
			value, err := parser.ConvertLiteralValue(token)
			if err != nil {
				fmt.Printf("  转换错误: %v\n", err)
			} else {
				fmt.Printf("  Go值: %v (类型: %T)\n", value, value)
			}

			// 格式化为SQL
			sqlFormatted := parser.FormatLiteralForSQL(value, sqlType)
			fmt.Printf("  SQL格式: %s\n", sqlFormatted)

			// 根据类型进行验证
			switch token.Type {
			case INTEGER_LIT, HEX_LIT, BIT_LIT:
				result := validator.ValidateIntegerLiteral(token, "INT")
				if result.HasErrors() {
					fmt.Printf("  验证错误: %v\n", result.Errors)
				} else {
					fmt.Printf("  验证: 通过 (INT类型)\n")
				}

			case FLOAT_LIT:
				result := validator.ValidateFloatLiteral(token, "DOUBLE", 0, 0)
				if result.HasErrors() {
					fmt.Printf("  验证错误: %v\n", result.Errors)
				} else {
					fmt.Printf("  验证: 通过 (DOUBLE类型)\n")
				}

			case STRING_LIT:
				result := validator.ValidateStringLiteral(token, "VARCHAR", 255, "utf8")
				if result.HasErrors() {
					fmt.Printf("  验证错误: %v\n", result.Errors)
				} else {
					fmt.Printf("  验证: 通过 (VARCHAR(255)类型)\n")
				}
				if result.HasWarnings() {
					fmt.Printf("  验证警告: %v\n", result.Warnings)
				}
			}

			fmt.Println()
		}
	}
}

// TestLiteralValidationScenarios 测试各种验证场景
func TestLiteralValidationScenarios(t *testing.T) {
	lexer := NewLexer("")
	parser := NewLiteralParser(lexer)
	validator := NewLiteralValidator(parser)

	fmt.Println("=== 字面量验证场景测试 ===")

	// 测试场景1: 整数范围验证
	fmt.Println("场景1: 整数范围验证")
	intTests := []struct {
		value       string
		targetType  string
		description string
	}{
		{"100", "TINYINT", "TINYINT范围内"},
		{"300", "TINYINT", "TINYINT范围外"},
		{"65000", "SMALLINT", "SMALLINT范围外"},
		{"30000", "SMALLINT", "SMALLINT范围内"},
	}

	for _, test := range intTests {
		token := NewToken(INTEGER_LIT, test.value, test.value, NewPosition(1, 1, 0))
		result := validator.ValidateIntegerLiteral(token, test.targetType)

		status := "✓ 通过"
		if result.HasErrors() {
			status = "✗ 失败: " + result.Errors[0]
		}
		fmt.Printf("  %s -> %s: %s\n", test.value, test.targetType, status)
	}
	fmt.Println()

	// 测试场景2: 字符串长度验证
	fmt.Println("场景2: 字符串长度验证")
	stringTests := []struct {
		value       string
		targetType  string
		maxLength   int
		description string
	}{
		{"hello", "VARCHAR", 10, "VARCHAR长度内"},
		{"hello world test", "VARCHAR", 5, "VARCHAR长度超出"},
		{"测试", "VARCHAR", 10, "中文字符"},
	}

	for _, test := range stringTests {
		token := NewToken(STRING_LIT, test.value, "'"+test.value+"'", NewPosition(1, 1, 0))
		result := validator.ValidateStringLiteral(token, test.targetType, test.maxLength, "utf8")

		status := "✓ 通过"
		if result.HasErrors() {
			status = "✗ 失败: " + result.Errors[0]
		}
		fmt.Printf("  '%s' -> %s(%d): %s\n", test.value, test.targetType, test.maxLength, status)
	}
	fmt.Println()

	// 测试场景3: 日期时间格式验证
	fmt.Println("场景3: 日期时间格式验证")
	dateTests := []struct {
		value       string
		targetType  string
		description string
	}{
		{"2023-12-25", "DATE", "标准日期格式"},
		{"2023/12/25", "DATE", "非标准日期格式"},
		{"14:30:00", "TIME", "标准时间格式"},
		{"2023-12-25 14:30:00", "DATETIME", "标准日期时间格式"},
	}

	for _, test := range dateTests {
		token := NewToken(STRING_LIT, test.value, "'"+test.value+"'", NewPosition(1, 1, 0))
		result := validator.ValidateDateTimeLiteral(token, test.targetType)

		status := "✓ 通过"
		if result.HasErrors() {
			status = "✗ 失败: " + result.Errors[0]
		}
		fmt.Printf("  '%s' -> %s: %s\n", test.value, test.targetType, status)
	}
}

// TestLiteralConversionAndFormatting 测试字面量转换和格式化
func TestLiteralConversionAndFormatting(t *testing.T) {
	lexer := NewLexer("")
	parser := NewLiteralParser(lexer)

	fmt.Println("=== 字面量转换和格式化测试 ===")

	testCases := []struct {
		tokenType   TokenType
		value       string
		description string
	}{
		{INTEGER_LIT, "42", "整数"},
		{FLOAT_LIT, "3.14159", "浮点数"},
		{STRING_LIT, "hello world", "字符串"},
		{STRING_LIT, "it's a test", "包含单引号的字符串"},
		{TRUE_LIT, "TRUE", "布尔值true"},
		{FALSE_LIT, "FALSE", "布尔值false"},
		{NULL_LIT, "NULL", "NULL值"},
		{HEX_LIT, "0xFF", "十六进制"},
		{BIT_LIT, "0b1010", "二进制"},
	}

	for _, test := range testCases {
		fmt.Printf("%s (%s):\n", test.description, test.value)

		token := NewToken(test.tokenType, test.value, test.value, NewPosition(1, 1, 0))

		// 获取SQL类型
		sqlType := parser.GetLiteralType(token)
		fmt.Printf("  SQL类型: %s\n", sqlType)

		// 转换为Go值
		goValue, err := parser.ConvertLiteralValue(token)
		if err != nil {
			fmt.Printf("  转换错误: %v\n", err)
		} else {
			fmt.Printf("  Go值: %v (类型: %T)\n", goValue, goValue)

			// 格式化回SQL
			sqlFormatted := parser.FormatLiteralForSQL(goValue, sqlType)
			fmt.Printf("  SQL格式: %s\n", sqlFormatted)
		}

		fmt.Println()
	}
}
