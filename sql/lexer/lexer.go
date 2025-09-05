package lexer

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// LexerImpl 词法分析器实现 - 参考TiDB设计
// 采用状态机模式进行词法分析，支持UTF-8编码
type LexerImpl struct {
	// 输入相关字段
	input        string // 输入的SQL文本
	inputLength  int    // 输入文本长度（字节数）
	
	// 位置跟踪字段
	position     int    // 当前字符在输入中的位置（字节偏移）
	readPosition int    // 下一个要读取的字符位置
	line         int    // 当前行号（从1开始）
	column       int    // 当前列号（从1开始）
	
	// 字符相关字段
	ch           rune   // 当前正在检查的字符（Unicode码点）
	
	// 错误收集
	errors       []LexError // 词法分析过程中遇到的错误
	
	// 状态标志
	atEOF        bool   // 是否已到达文件末尾
}

// NewLexer 创建新的词法分析器实例
// 参数:
//   input: 要分析的SQL文本
// 返回:
//   *LexerImpl: 词法分析器实例
func NewLexer(input string) *LexerImpl {
	l := &LexerImpl{
		input:       input,
		inputLength: len(input),
		line:        1,    // 行号从1开始
		column:      0,    // 列号从0开始，readChar()会递增到1
		errors:      make([]LexError, 0),
		atEOF:       false,
	}
	
	// 读取第一个字符，初始化词法分析器状态
	l.readChar()
	return l
}

// Reset 重置词法分析器状态，用于分析新的输入
// 参数:
//   input: 新的SQL文本
func (l *LexerImpl) Reset(input string) {
	l.input = input
	l.inputLength = len(input)
	l.position = 0
	l.readPosition = 0
	l.line = 1
	l.column = 0
	l.ch = 0
	l.errors = l.errors[:0] // 清空错误列表但保留容量
	l.atEOF = false
	
	// 读取第一个字符
	l.readChar()
}

// GetPosition 获取当前位置信息
// 返回:
//   Position: 当前位置
func (l *LexerImpl) GetPosition() Position {
	return NewPosition(l.line, l.column, l.position)
}

// readChar 读取下一个字符并更新位置信息
// 这是词法分析器的核心方法，负责字符流的推进和位置跟踪
func (l *LexerImpl) readChar() {
	// 检查是否已到达输入末尾
	if l.readPosition >= l.inputLength {
		l.position = l.inputLength // 设置position到输入末尾
		l.ch = 0 // 使用0表示EOF
		l.atEOF = true
		return
	}
	
	// 记录当前字符位置（用于换行检测）
	prevCh := l.ch
	
	// 更新位置指针
	l.position = l.readPosition
	
	// 读取UTF-8字符
	r, size := utf8.DecodeRuneInString(l.input[l.readPosition:])
	if r == utf8.RuneError {
		// 处理无效的UTF-8序列
		l.addError("无效的UTF-8字符序列")
		l.ch = '?' // 使用替换字符
		l.readPosition++
	} else {
		l.ch = r
		l.readPosition += size
	}
	
	// 更新行列号
	if prevCh == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
}

// peekChar 查看下一个字符但不移动位置指针
// 返回:
//   rune: 下一个字符，如果到达末尾则返回0
func (l *LexerImpl) peekChar() rune {
	if l.readPosition >= l.inputLength {
		return 0
	}
	
	r, _ := utf8.DecodeRuneInString(l.input[l.readPosition:])
	return r
}

// peekCharN 查看第n个字符但不移动位置指针
// 参数:
//   n: 要查看的字符偏移量（1表示下一个字符）
// 返回:
//   rune: 第n个字符，如果超出范围则返回0
func (l *LexerImpl) peekCharN(n int) rune {
	pos := l.readPosition
	for i := 0; i < n && pos < l.inputLength; i++ {
		_, size := utf8.DecodeRuneInString(l.input[pos:])
		pos += size
	}
	
	if pos >= l.inputLength {
		return 0
	}
	
	r, _ := utf8.DecodeRuneInString(l.input[pos:])
	return r
}

// skipWhitespace 跳过空白字符（空格、制表符、换行符等）
// 但保留换行符的位置信息用于错误报告
func (l *LexerImpl) skipWhitespace() {
	for unicode.IsSpace(l.ch) && l.ch != 0 {
		l.readChar()
	}
}

// addError 添加词法错误到错误列表
// 参数:
//   message: 错误消息
func (l *LexerImpl) addError(message string) {
	l.errors = append(l.errors, LexError{
		Position: l.GetPosition(),
		Message:  message,
	})
}

// addErrorf 添加格式化的词法错误
// 参数:
//   format: 格式字符串
//   args: 格式参数
func (l *LexerImpl) addErrorf(format string, args ...interface{}) {
	l.addError(fmt.Sprintf(format, args...))
}

// Tokenize 将输入文本完全分词为Token列表
// 参数:
//   input: 要分析的SQL文本
// 返回:
//   []Token: Token列表
//   error: 如果有词法错误则返回错误
func (l *LexerImpl) Tokenize(input string) ([]Token, error) {
	l.Reset(input)
	tokens := make([]Token, 0, 64) // 预分配容量以提高性能
	
	for {
		token := l.NextToken()
		
		// 跳过空白字符和注释（可选）
		if token.Type == WHITESPACE || token.Type == COMMENT {
			continue
		}
		
		tokens = append(tokens, token)
		
		// 遇到EOF或无效Token时停止
		if token.Type == EOF || token.Type == INVALID {
			break
		}
	}
	
	// 如果有错误，返回错误信息
	if len(l.errors) > 0 {
		return tokens, &LexErrorList{Errors: l.errors}
	}
	
	return tokens, nil
}

// NextToken 获取下一个Token
// 这是词法分析器的主要方法，实现状态机逻辑
// 返回:
//   Token: 下一个词法单元
func (l *LexerImpl) NextToken() Token {
	var token Token
	
	// 跳过空白字符
	l.skipWhitespace()
	
	// 记录Token开始位置
	startPos := l.GetPosition()
	
	switch l.ch {
	case 0:
		// 文件结束
		token = NewToken(EOF, "", "", startPos)
		
	case '+':
		token = NewToken(PLUS, "+", "+", startPos)
		l.readChar()
		
	case '-':
		// 处理减号和注释 --
		if l.peekChar() == '-' {
			token = l.readLineComment(startPos)
		} else {
			token = NewToken(MINUS, "-", "-", startPos)
			l.readChar()
		}
		
	case '*':
		token = NewToken(MULTIPLY, "*", "*", startPos)
		l.readChar()
		
	case '/':
		// 处理除号和注释 /* */
		if l.peekChar() == '*' {
			token = l.readBlockComment(startPos)
		} else {
			token = NewToken(DIVIDE, "/", "/", startPos)
			l.readChar()
		}
		
	case '%':
		token = NewToken(MODULO, "%", "%", startPos)
		l.readChar()
		
	case '=':
		token = NewToken(EQUAL, "=", "=", startPos)
		l.readChar()
		
	case '!':
		// 处理 != 和 !
		if l.peekChar() == '=' {
			l.readChar() // 跳过 !
			l.readChar() // 跳过 =
			token = NewToken(NOT_EQUAL, "!=", "!=", startPos)
		} else {
			token = NewToken(NOT_OP, "!", "!", startPos)
			l.readChar()
		}
		
	case '<':
		// 处理 <, <=, <>, <<, <=>
		next := l.peekChar()
		switch next {
		case '=':
			// 检查是否是 <=>
			if l.peekCharN(1) == '>' {
				l.readChar() // 跳过 <
				l.readChar() // 跳过 =
				l.readChar() // 跳过 >
				token = NewToken(NULL_SAFE_EQUAL, "<=>", "<=>", startPos)
			} else {
				l.readChar() // 跳过 <
				l.readChar() // 跳过 =
				token = NewToken(LESS_EQUAL, "<=", "<=", startPos)
			}
		case '>':
			l.readChar() // 跳过 <
			l.readChar() // 跳过 >
			token = NewToken(NOT_EQUAL, "<>", "<>", startPos)
		case '<':
			l.readChar() // 跳过第一个 <
			l.readChar() // 跳过第二个 <
			token = NewToken(LEFT_SHIFT, "<<", "<<", startPos)
		default:
			token = NewToken(LESS, "<", "<", startPos)
			l.readChar()
		}
		
	case '>':
		// 处理 >, >=, >>
		next := l.peekChar()
		switch next {
		case '=':
			l.readChar() // 跳过 >
			l.readChar() // 跳过 =
			token = NewToken(GREATER_EQUAL, ">=", ">=", startPos)
		case '>':
			l.readChar() // 跳过第一个 >
			l.readChar() // 跳过第二个 >
			token = NewToken(RIGHT_SHIFT, ">>", ">>", startPos)
		default:
			token = NewToken(GREATER, ">", ">", startPos)
			l.readChar()
		}
		
	case '&':
		// 处理 & 和 &&
		if l.peekChar() == '&' {
			l.readChar() // 跳过第一个 &
			l.readChar() // 跳过第二个 &
			token = NewToken(AND_OP, "&&", "&&", startPos)
		} else {
			token = NewToken(BIT_AND, "&", "&", startPos)
			l.readChar()
		}
		
	case '|':
		// 处理 | 和 ||
		if l.peekChar() == '|' {
			l.readChar() // 跳过第一个 |
			l.readChar() // 跳过第二个 |
			token = NewToken(OR_OP, "||", "||", startPos)
		} else {
			token = NewToken(BIT_OR, "|", "|", startPos)
			l.readChar()
		}
		
	case '^':
		token = NewToken(BIT_XOR, "^", "^", startPos)
		l.readChar()
		
	case '~':
		token = NewToken(BIT_NOT, "~", "~", startPos)
		l.readChar()
		
	case ':':
		// 处理 :, :=, ::
		next := l.peekChar()
		switch next {
		case '=':
			l.readChar() // 跳过 :
			l.readChar() // 跳过 =
			token = NewToken(ASSIGN, ":=", ":=", startPos)
		case ':':
			l.readChar() // 跳过第一个 :
			l.readChar() // 跳过第二个 :
			token = NewToken(DOUBLE_COLON, "::", "::", startPos)
		default:
			token = NewToken(COLON, ":", ":", startPos)
			l.readChar()
		}
		
	case ',':
		token = NewToken(COMMA, ",", ",", startPos)
		l.readChar()
		
	case ';':
		token = NewToken(SEMICOLON, ";", ";", startPos)
		l.readChar()
		
	case '(':
		token = NewToken(LEFT_PAREN, "(", "(", startPos)
		l.readChar()
		
	case ')':
		token = NewToken(RIGHT_PAREN, ")", ")", startPos)
		l.readChar()
		
	case '{':
		token = NewToken(LEFT_BRACE, "{", "{", startPos)
		l.readChar()
		
	case '}':
		token = NewToken(RIGHT_BRACE, "}", "}", startPos)
		l.readChar()
		
	case '[':
		token = NewToken(LEFT_BRACKET, "[", "[", startPos)
		l.readChar()
		
	case ']':
		token = NewToken(RIGHT_BRACKET, "]", "]", startPos)
		l.readChar()
		
	case '.':
		token = NewToken(DOT, ".", ".", startPos)
		l.readChar()
		
	case '?':
		token = NewToken(QUESTION, "?", "?", startPos)
		l.readChar()
		
	case '\'', '"':
		// 字符串字面量
		token = l.readString(startPos)
		
	default:
		if unicode.IsLetter(l.ch) || l.ch == '_' {
			// 标识符或关键字
			token = l.readIdentifier(startPos)
		} else if unicode.IsDigit(l.ch) {
			// 数字字面量
			token = l.readNumber(startPos)
		} else {
			// 无效字符
			l.addErrorf("无效字符: %c", l.ch)
			token = NewToken(INVALID, string(l.ch), string(l.ch), startPos)
			l.readChar()
		}
	}
	
	return token
}