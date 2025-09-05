package lexer

import (
	"fmt"
	"strconv"
	"strings"
)

// LiteralParser 字面量解析器 - 提供更高级的字面量解析功能
// 参考TiDB设计，支持各种SQL字面量类型的解析和验证
type LiteralParser struct {
	lexer *LexerImpl // 关联的词法分析器
}

// NewLiteralParser 创建新的字面量解析器
// 参数:
//
//	lexer: 词法分析器实例
//
// 返回:
//
//	*LiteralParser: 字面量解析器实例
func NewLiteralParser(lexer *LexerImpl) *LiteralParser {
	return &LiteralParser{
		lexer: lexer,
	}
}

// ParseIntegerLiteral 解析整数字面量并返回其数值
// 支持十进制、十六进制、八进制、二进制格式
// 参数:
//
//	token: 整数字面量Token
//
// 返回:
//
//	int64: 解析后的整数值
//	error: 解析错误
func (lp *LiteralParser) ParseIntegerLiteral(token Token) (int64, error) {
	if token.Type != INTEGER_LIT && token.Type != HEX_LIT && token.Type != BIT_LIT {
		return 0, fmt.Errorf("不是有效的整数字面量类型: %v", token.Type)
	}

	value := strings.TrimSpace(token.Value)
	if value == "" {
		return 0, fmt.Errorf("空的整数字面量")
	}

	switch token.Type {
	case INTEGER_LIT:
		// 十进制整数
		return strconv.ParseInt(value, 10, 64)

	case HEX_LIT:
		// 十六进制整数 (0x前缀)
		if len(value) < 3 || !strings.HasPrefix(strings.ToLower(value), "0x") {
			return 0, fmt.Errorf("无效的十六进制格式: %s", value)
		}
		return strconv.ParseInt(value[2:], 16, 64)

	case BIT_LIT:
		// 二进制整数 (0b前缀)
		if len(value) < 3 || !strings.HasPrefix(strings.ToLower(value), "0b") {
			return 0, fmt.Errorf("无效的二进制格式: %s", value)
		}
		return strconv.ParseInt(value[2:], 2, 64)

	default:
		return 0, fmt.Errorf("不支持的整数字面量类型: %v", token.Type)
	}
}

// ParseFloatLiteral 解析浮点数字面量并返回其数值
// 支持标准浮点数和科学计数法格式
// 参数:
//
//	token: 浮点数字面量Token
//
// 返回:
//
//	float64: 解析后的浮点数值
//	error: 解析错误
func (lp *LiteralParser) ParseFloatLiteral(token Token) (float64, error) {
	if token.Type != FLOAT_LIT {
		return 0, fmt.Errorf("不是有效的浮点数字面量类型: %v", token.Type)
	}

	value := strings.TrimSpace(token.Value)
	if value == "" {
		return 0, fmt.Errorf("空的浮点数字面量")
	}

	// 使用Go标准库解析浮点数
	return strconv.ParseFloat(value, 64)
}

// ParseStringLiteral 解析字符串字面量并返回其内容
// 处理各种转义序列和引号类型
// 参数:
//
//	token: 字符串字面量Token
//
// 返回:
//
//	string: 解析后的字符串内容
//	error: 解析错误
func (lp *LiteralParser) ParseStringLiteral(token Token) (string, error) {
	if token.Type != STRING_LIT {
		return "", fmt.Errorf("不是有效的字符串字面量类型: %v", token.Type)
	}

	// Token.Value已经包含了处理转义后的字符串内容
	return token.Value, nil
}

// ParseBooleanLiteral 解析布尔字面量并返回其值
// 支持TRUE/FALSE关键字
// 参数:
//
//	token: 布尔字面量Token
//
// 返回:
//
//	bool: 解析后的布尔值
//	error: 解析错误
func (lp *LiteralParser) ParseBooleanLiteral(token Token) (bool, error) {
	switch token.Type {
	case TRUE_LIT:
		return true, nil
	case FALSE_LIT:
		return false, nil
	default:
		return false, fmt.Errorf("不是有效的布尔字面量类型: %v", token.Type)
	}
}

// IsNullLiteral 检查Token是否为NULL字面量
// 参数:
//
//	token: 要检查的Token
//
// 返回:
//
//	bool: 是否为NULL字面量
func (lp *LiteralParser) IsNullLiteral(token Token) bool {
	return token.Type == NULL_LIT
}

// GetLiteralType 获取字面量的SQL数据类型
// 根据Token类型推断对应的SQL数据类型
// 参数:
//
//	token: 字面量Token
//
// 返回:
//
//	string: SQL数据类型名称
func (lp *LiteralParser) GetLiteralType(token Token) string {
	switch token.Type {
	case INTEGER_LIT, HEX_LIT, BIT_LIT:
		return "INTEGER"
	case FLOAT_LIT:
		return "FLOAT"
	case STRING_LIT:
		return "VARCHAR"
	case TRUE_LIT, FALSE_LIT:
		return "BOOLEAN"
	case NULL_LIT:
		return "NULL"
	default:
		return "UNKNOWN"
	}
}

// IsNumericLiteral 检查Token是否为数值字面量
// 参数:
//
//	token: 要检查的Token
//
// 返回:
//
//	bool: 是否为数值字面量
func (lp *LiteralParser) IsNumericLiteral(token Token) bool {
	switch token.Type {
	case INTEGER_LIT, FLOAT_LIT, HEX_LIT, BIT_LIT:
		return true
	default:
		return false
	}
}

// IsStringLiteral 检查Token是否为字符串字面量
// 参数:
//
//	token: 要检查的Token
//
// 返回:
//
//	bool: 是否为字符串字面量
func (lp *LiteralParser) IsStringLiteral(token Token) bool {
	return token.Type == STRING_LIT
}

// IsBooleanLiteral 检查Token是否为布尔字面量
// 参数:
//
//	token: 要检查的Token
//
// 返回:
//
//	bool: 是否为布尔字面量
func (lp *LiteralParser) IsBooleanLiteral(token Token) bool {
	return token.Type == TRUE_LIT || token.Type == FALSE_LIT
}

// ConvertLiteralValue 将字面量Token转换为Go语言的对应类型值
// 这是一个通用的转换方法，自动识别类型并进行转换
// 参数:
//
//	token: 字面量Token
//
// 返回:
//
//	interface{}: 转换后的值
//	error: 转换错误
func (lp *LiteralParser) ConvertLiteralValue(token Token) (interface{}, error) {
	switch token.Type {
	case INTEGER_LIT, HEX_LIT, BIT_LIT:
		return lp.ParseIntegerLiteral(token)

	case FLOAT_LIT:
		return lp.ParseFloatLiteral(token)

	case STRING_LIT:
		return lp.ParseStringLiteral(token)

	case TRUE_LIT, FALSE_LIT:
		return lp.ParseBooleanLiteral(token)

	case NULL_LIT:
		return nil, nil // NULL值用nil表示

	default:
		return nil, fmt.Errorf("不支持的字面量类型: %v", token.Type)
	}
}

// FormatLiteralForSQL 将字面量值格式化为SQL字符串
// 用于生成SQL语句时的字面量格式化
// 参数:
//
//	value: 字面量值
//	dataType: 数据类型
//
// 返回:
//
//	string: 格式化后的SQL字符串
func (lp *LiteralParser) FormatLiteralForSQL(value interface{}, dataType string) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)

	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)

	case float32, float64:
		return fmt.Sprintf("%g", v)

	case string:
		// 转义单引号并用单引号包围
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)

	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"

	default:
		return fmt.Sprintf("'%v'", v)
	}
}
