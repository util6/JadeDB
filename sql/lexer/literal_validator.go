package lexer

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"
)

// LiteralValidator 字面量验证器 - 提供SQL字面量的语义验证功能
// 参考TiDB的验证规则，确保字面量符合SQL标准和数据库约束
type LiteralValidator struct {
	parser *LiteralParser // 关联的字面量解析器
}

// NewLiteralValidator 创建新的字面量验证器
// 参数:
//
//	parser: 字面量解析器实例
//
// 返回:
//
//	*LiteralValidator: 字面量验证器实例
func NewLiteralValidator(parser *LiteralParser) *LiteralValidator {
	return &LiteralValidator{
		parser: parser,
	}
}

// ValidationResult 验证结果
type ValidationResult struct {
	IsValid  bool     // 是否有效
	Errors   []string // 错误列表
	Warnings []string // 警告列表
}

// AddError 添加错误
func (vr *ValidationResult) AddError(message string) {
	vr.IsValid = false
	vr.Errors = append(vr.Errors, message)
}

// AddWarning 添加警告
func (vr *ValidationResult) AddWarning(message string) {
	vr.Warnings = append(vr.Warnings, message)
}

// HasErrors 检查是否有错误
func (vr *ValidationResult) HasErrors() bool {
	return len(vr.Errors) > 0
}

// HasWarnings 检查是否有警告
func (vr *ValidationResult) HasWarnings() bool {
	return len(vr.Warnings) > 0
}

// ValidateIntegerLiteral 验证整数字面量
// 检查整数是否在指定数据类型的范围内
// 参数:
//
//	token: 整数字面量Token
//	targetType: 目标数据类型（如TINYINT, SMALLINT, INT, BIGINT）
//
// 返回:
//
//	*ValidationResult: 验证结果
func (lv *LiteralValidator) ValidateIntegerLiteral(token Token, targetType string) *ValidationResult {
	result := &ValidationResult{IsValid: true}

	// 解析整数值
	value, err := lv.parser.ParseIntegerLiteral(token)
	if err != nil {
		result.AddError(fmt.Sprintf("无法解析整数字面量: %v", err))
		return result
	}

	// 根据目标类型检查范围
	switch strings.ToUpper(targetType) {
	case "TINYINT":
		if value < -128 || value > 127 {
			result.AddError(fmt.Sprintf("TINYINT值%d超出范围[-128, 127]", value))
		}

	case "TINYINT UNSIGNED":
		if value < 0 || value > 255 {
			result.AddError(fmt.Sprintf("TINYINT UNSIGNED值%d超出范围[0, 255]", value))
		}

	case "SMALLINT":
		if value < -32768 || value > 32767 {
			result.AddError(fmt.Sprintf("SMALLINT值%d超出范围[-32768, 32767]", value))
		}

	case "SMALLINT UNSIGNED":
		if value < 0 || value > 65535 {
			result.AddError(fmt.Sprintf("SMALLINT UNSIGNED值%d超出范围[0, 65535]", value))
		}

	case "INT", "INTEGER":
		if value < -2147483648 || value > 2147483647 {
			result.AddError(fmt.Sprintf("INT值%d超出范围[-2147483648, 2147483647]", value))
		}

	case "INT UNSIGNED", "INTEGER UNSIGNED":
		if value < 0 || value > 4294967295 {
			result.AddError(fmt.Sprintf("INT UNSIGNED值%d超出范围[0, 4294967295]", value))
		}

	case "BIGINT":
		// BIGINT使用int64，已经是最大范围
		if value == math.MinInt64 {
			result.AddWarning("BIGINT值接近最小值边界")
		} else if value == math.MaxInt64 {
			result.AddWarning("BIGINT值接近最大值边界")
		}

	default:
		result.AddWarning(fmt.Sprintf("未知的整数类型: %s", targetType))
	}

	return result
}

// ValidateFloatLiteral 验证浮点数字面量
// 检查浮点数是否在指定数据类型的范围和精度内
// 参数:
//
//	token: 浮点数字面量Token
//	targetType: 目标数据类型（如FLOAT, DOUBLE, DECIMAL）
//	precision: 精度（总位数）
//	scale: 标度（小数位数）
//
// 返回:
//
//	*ValidationResult: 验证结果
func (lv *LiteralValidator) ValidateFloatLiteral(token Token, targetType string, precision, scale int) *ValidationResult {
	result := &ValidationResult{IsValid: true}

	// 解析浮点数值
	value, err := lv.parser.ParseFloatLiteral(token)
	if err != nil {
		result.AddError(fmt.Sprintf("无法解析浮点数字面量: %v", err))
		return result
	}

	// 检查特殊值
	if math.IsInf(value, 0) {
		result.AddError("浮点数值为无穷大")
		return result
	}
	if math.IsNaN(value) {
		result.AddError("浮点数值为NaN")
		return result
	}

	// 根据目标类型检查范围
	switch strings.ToUpper(targetType) {
	case "FLOAT":
		if math.Abs(value) > math.MaxFloat32 {
			result.AddError(fmt.Sprintf("FLOAT值%g超出范围", value))
		}

	case "DOUBLE":
		// DOUBLE使用float64，已经是最大范围
		if math.Abs(value) == math.MaxFloat64 {
			result.AddWarning("DOUBLE值接近边界")
		}

	case "DECIMAL", "NUMERIC":
		// 检查DECIMAL的精度和标度
		if precision > 0 {
			valueStr := fmt.Sprintf("%g", value)
			if err := lv.validateDecimalPrecision(valueStr, precision, scale); err != nil {
				result.AddError(err.Error())
			}
		}

	default:
		result.AddWarning(fmt.Sprintf("未知的浮点数类型: %s", targetType))
	}

	return result
}

// ValidateStringLiteral 验证字符串字面量
// 检查字符串长度、字符集、编码等
// 参数:
//
//	token: 字符串字面量Token
//	targetType: 目标数据类型（如VARCHAR, CHAR, TEXT）
//	maxLength: 最大长度
//	charset: 字符集
//
// 返回:
//
//	*ValidationResult: 验证结果
func (lv *LiteralValidator) ValidateStringLiteral(token Token, targetType string, maxLength int, charset string) *ValidationResult {
	result := &ValidationResult{IsValid: true}

	// 解析字符串值
	value, err := lv.parser.ParseStringLiteral(token)
	if err != nil {
		result.AddError(fmt.Sprintf("无法解析字符串字面量: %v", err))
		return result
	}

	// 检查UTF-8编码有效性
	if !utf8.ValidString(value) {
		result.AddError("字符串包含无效的UTF-8字符")
	}

	// 检查长度
	if maxLength > 0 {
		actualLength := utf8.RuneCountInString(value) // 使用字符数而不是字节数
		if actualLength > maxLength {
			result.AddError(fmt.Sprintf("字符串长度%d超出最大限制%d", actualLength, maxLength))
		}
	}

	// 根据目标类型进行特定检查
	switch strings.ToUpper(targetType) {
	case "CHAR":
		// CHAR类型的长度检查
		if maxLength > 0 && utf8.RuneCountInString(value) != maxLength {
			result.AddWarning(fmt.Sprintf("CHAR(%d)类型建议使用固定长度字符串", maxLength))
		}

	case "VARCHAR":
		// VARCHAR类型的长度检查已在上面完成

	case "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
		// TEXT类型通常没有严格的长度限制，但可以给出建议
		if len(value) > 65535 && strings.ToUpper(targetType) == "TEXT" {
			result.AddWarning("TEXT类型建议长度不超过65535字节")
		}
	}

	// 检查字符集兼容性（简化实现）
	if charset != "" && strings.ToUpper(charset) != "UTF8" && strings.ToUpper(charset) != "UTF8MB4" {
		if lv.containsNonASCII(value) {
			result.AddWarning(fmt.Sprintf("字符串包含非ASCII字符，但目标字符集为%s", charset))
		}
	}

	return result
}

// ValidateDateTimeLiteral 验证日期时间字面量
// 检查日期时间格式和有效性
// 参数:
//
//	token: 字符串Token（包含日期时间）
//	targetType: 目标数据类型（如DATE, TIME, DATETIME, TIMESTAMP）
//
// 返回:
//
//	*ValidationResult: 验证结果
func (lv *LiteralValidator) ValidateDateTimeLiteral(token Token, targetType string) *ValidationResult {
	result := &ValidationResult{IsValid: true}

	// 解析字符串值
	value, err := lv.parser.ParseStringLiteral(token)
	if err != nil {
		result.AddError(fmt.Sprintf("无法解析日期时间字面量: %v", err))
		return result
	}

	value = strings.TrimSpace(value)
	if value == "" {
		result.AddError("空的日期时间字面量")
		return result
	}

	// 根据目标类型验证格式
	switch strings.ToUpper(targetType) {
	case "DATE":
		if err := lv.validateDateFormat(value); err != nil {
			result.AddError(err.Error())
		}

	case "TIME":
		if err := lv.validateTimeFormat(value); err != nil {
			result.AddError(err.Error())
		}

	case "DATETIME":
		if err := lv.validateDateTimeFormat(value); err != nil {
			result.AddError(err.Error())
		}

	case "TIMESTAMP":
		if err := lv.validateTimestampFormat(value); err != nil {
			result.AddError(err.Error())
		}

	default:
		result.AddWarning(fmt.Sprintf("未知的日期时间类型: %s", targetType))
	}

	return result
}

// 辅助方法

// validateDecimalPrecision 验证DECIMAL类型的精度和标度
func (lv *LiteralValidator) validateDecimalPrecision(valueStr string, precision, scale int) error {
	// 移除符号
	valueStr = strings.TrimPrefix(valueStr, "-")
	valueStr = strings.TrimPrefix(valueStr, "+")

	// 分割整数和小数部分
	parts := strings.Split(valueStr, ".")
	integerPart := parts[0]
	decimalPart := ""
	if len(parts) > 1 {
		decimalPart = parts[1]
	}

	// 检查总精度
	totalDigits := len(strings.ReplaceAll(integerPart, "0", "")) + len(decimalPart)
	if totalDigits > precision {
		return fmt.Errorf("DECIMAL值的总位数%d超出精度限制%d", totalDigits, precision)
	}

	// 检查小数位数
	if len(decimalPart) > scale {
		return fmt.Errorf("DECIMAL值的小数位数%d超出标度限制%d", len(decimalPart), scale)
	}

	return nil
}

// containsNonASCII 检查字符串是否包含非ASCII字符
func (lv *LiteralValidator) containsNonASCII(s string) bool {
	for _, r := range s {
		if r > 127 {
			return true
		}
	}
	return false
}

// validateDateFormat 验证日期格式 (YYYY-MM-DD)
func (lv *LiteralValidator) validateDateFormat(value string) error {
	datePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	if !datePattern.MatchString(value) {
		return fmt.Errorf("无效的日期格式，期望YYYY-MM-DD: %s", value)
	}
	return nil
}

// validateTimeFormat 验证时间格式 (HH:MM:SS)
func (lv *LiteralValidator) validateTimeFormat(value string) error {
	timePattern := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	if !timePattern.MatchString(value) {
		return fmt.Errorf("无效的时间格式，期望HH:MM:SS: %s", value)
	}
	return nil
}

// validateDateTimeFormat 验证日期时间格式 (YYYY-MM-DD HH:MM:SS)
func (lv *LiteralValidator) validateDateTimeFormat(value string) error {
	datetimePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	if !datetimePattern.MatchString(value) {
		return fmt.Errorf("无效的日期时间格式，期望YYYY-MM-DD HH:MM:SS: %s", value)
	}
	return nil
}

// validateTimestampFormat 验证时间戳格式
func (lv *LiteralValidator) validateTimestampFormat(value string) error {
	// TIMESTAMP可以是日期时间格式或Unix时间戳
	if err := lv.validateDateTimeFormat(value); err != nil {
		// 尝试作为Unix时间戳验证
		timestampPattern := regexp.MustCompile(`^\d+$`)
		if !timestampPattern.MatchString(value) {
			return fmt.Errorf("无效的时间戳格式: %s", value)
		}
	}
	return nil
}
