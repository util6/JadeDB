package lexer

import (
	"unicode"
)

// readIdentifier 读取标识符或关键字
// 标识符可以包含字母、数字、下划线，但必须以字母或下划线开头
// 参数:
//
//	startPos: 标识符开始位置
//
// 返回:
//
//	Token: 标识符或关键字Token
func (l *LexerImpl) readIdentifier(startPos Position) Token {
	startOffset := l.position

	// 读取标识符字符（字母、数字、下划线）
	// 注意：当前字符已经是标识符的一部分，所以先读取再检查
	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}

	// 提取标识符文本
	literal := l.input[startOffset:l.position]

	// 查找是否为关键字
	tokenType := LookupKeyword(literal)

	return NewToken(tokenType, literal, literal, startPos)
}

// readNumber 读取数字字面量（整数或浮点数）
// 支持十进制、十六进制（0x前缀）、二进制（0b前缀）
// 参数:
//
//	startPos: 数字开始位置
//
// 返回:
//
//	Token: 数字字面量Token
func (l *LexerImpl) readNumber(startPos Position) Token {
	startOffset := l.position
	tokenType := INTEGER_LIT

	// 检查特殊前缀
	if l.ch == '0' {
		next := l.peekChar()
		switch next {
		case 'x', 'X':
			// 十六进制数 0x...
			return l.readHexNumber(startPos)
		case 'b', 'B':
			// 二进制数 0b...
			return l.readBinaryNumber(startPos)
		}
	}

	// 读取整数部分
	for unicode.IsDigit(l.ch) {
		l.readChar()
	}

	// 检查是否有小数点
	if l.ch == '.' && unicode.IsDigit(l.peekChar()) {
		tokenType = FLOAT_LIT
		l.readChar() // 跳过小数点

		// 读取小数部分
		for unicode.IsDigit(l.ch) {
			l.readChar()
		}
	}

	// 检查科学计数法 (e/E)
	if l.ch == 'e' || l.ch == 'E' {
		tokenType = FLOAT_LIT
		l.readChar() // 跳过 e/E

		// 检查正负号
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}

		// 读取指数部分
		if !unicode.IsDigit(l.ch) {
			l.addError("科学计数法中缺少指数数字")
			return NewToken(INVALID, "", "", startPos)
		}

		for unicode.IsDigit(l.ch) {
			l.readChar()
		}
	}

	literal := l.input[startOffset:l.position]
	return NewToken(tokenType, literal, literal, startPos)
}

// readHexNumber 读取十六进制数字
// 格式: 0x[0-9a-fA-F]+
// 参数:
//
//	startPos: 数字开始位置
//
// 返回:
//
//	Token: 十六进制字面量Token
func (l *LexerImpl) readHexNumber(startPos Position) Token {
	startOffset := l.position

	l.readChar() // 跳过 '0'
	l.readChar() // 跳过 'x' 或 'X'

	// 必须至少有一个十六进制数字
	if !isHexDigit(l.ch) {
		l.addError("十六进制数字中缺少有效数字")
		return NewToken(INVALID, "", "", startPos)
	}

	// 读取十六进制数字
	for isHexDigit(l.ch) {
		l.readChar()
	}

	literal := l.input[startOffset:l.position]
	return NewToken(HEX_LIT, literal, literal, startPos)
}

// readBinaryNumber 读取二进制数字
// 格式: 0b[01]+
// 参数:
//
//	startPos: 数字开始位置
//
// 返回:
//
//	Token: 二进制字面量Token
func (l *LexerImpl) readBinaryNumber(startPos Position) Token {
	startOffset := l.position

	l.readChar() // 跳过 '0'
	l.readChar() // 跳过 'b' 或 'B'

	// 必须至少有一个二进制数字
	if l.ch != '0' && l.ch != '1' {
		l.addError("二进制数字中缺少有效数字")
		return NewToken(INVALID, "", "", startPos)
	}

	// 读取二进制数字
	for l.ch == '0' || l.ch == '1' {
		l.readChar()
	}

	literal := l.input[startOffset:l.position]
	return NewToken(BIT_LIT, literal, literal, startPos)
}

// readString 读取字符串字面量
// 支持单引号和双引号字符串，处理转义字符
// 参数:
//
//	startPos: 字符串开始位置
//
// 返回:
//
//	Token: 字符串字面量Token
func (l *LexerImpl) readString(startPos Position) Token {
	startOffset := l.position
	quote := l.ch // 记录引号类型（单引号或双引号）

	l.readChar() // 跳过开始引号

	var value []rune // 存储解析后的字符串值（处理转义）

	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' {
			// 处理转义字符
			l.readChar() // 跳过反斜杠

			switch l.ch {
			case 'n':
				value = append(value, '\n')
			case 't':
				value = append(value, '\t')
			case 'r':
				value = append(value, '\r')
			case '\\':
				value = append(value, '\\')
			case '\'':
				value = append(value, '\'')
			case '"':
				value = append(value, '"')
			case '0':
				value = append(value, '\000')
			default:
				// 无效的转义序列，保持原样
				value = append(value, '\\', l.ch)
				l.addErrorf("无效的转义序列: \\%c", l.ch)
			}
		} else {
			value = append(value, l.ch)
		}
		l.readChar()
	}

	// 检查字符串是否正确闭合
	if l.ch != quote {
		l.addError("字符串未正确闭合")
		return NewToken(INVALID, "", "", startPos)
	}

	l.readChar() // 跳过结束引号

	literal := l.input[startOffset:l.position]
	return NewToken(STRING_LIT, string(value), literal, startPos)
}

// readLineComment 读取单行注释 (-- 注释内容)
// 参数:
//
//	startPos: 注释开始位置
//
// 返回:
//
//	Token: 注释Token
func (l *LexerImpl) readLineComment(startPos Position) Token {
	startOffset := l.position

	l.readChar() // 跳过第一个 -
	l.readChar() // 跳过第二个 -

	// 读取到行尾
	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}

	literal := l.input[startOffset:l.position]
	return NewToken(COMMENT, literal, literal, startPos)
}

// readBlockComment 读取块注释 (/* 注释内容 */)
// 支持嵌套注释
// 参数:
//
//	startPos: 注释开始位置
//
// 返回:
//
//	Token: 注释Token
func (l *LexerImpl) readBlockComment(startPos Position) Token {
	startOffset := l.position

	l.readChar() // 跳过 /
	l.readChar() // 跳过 *

	nestLevel := 1 // 嵌套层级

	for nestLevel > 0 && l.ch != 0 {
		if l.ch == '/' && l.peekChar() == '*' {
			// 嵌套注释开始
			nestLevel++
			l.readChar() // 跳过 /
			l.readChar() // 跳过 *
		} else if l.ch == '*' && l.peekChar() == '/' {
			// 注释结束
			nestLevel--
			l.readChar() // 跳过 *
			l.readChar() // 跳过 /
		} else {
			l.readChar()
		}
	}

	// 检查注释是否正确闭合
	if nestLevel > 0 {
		l.addError("块注释未正确闭合")
		return NewToken(INVALID, "", "", startPos)
	}

	literal := l.input[startOffset:l.position]
	return NewToken(COMMENT, literal, literal, startPos)
}

// isHexDigit 检查字符是否为十六进制数字
// 参数:
//
//	ch: 要检查的字符
//
// 返回:
//
//	bool: 是否为十六进制数字
func isHexDigit(ch rune) bool {
	return unicode.IsDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}
