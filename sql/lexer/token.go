package lexer

import "fmt"

// NewToken 创建新的Token - 参考TiDB设计
func NewToken(tokenType TokenType, value, raw string, pos Position) Token {
	return Token{
		Type:     tokenType,
		Value:    value,
		Raw:      raw,
		Position: pos,
		EndPos:   Position{Line: pos.Line, Column: pos.Column + len(raw), Offset: pos.Offset + len(raw)},
		Line:     pos.Line,   // 兼容性字段
		Column:   pos.Column, // 兼容性字段
	}
}

// String 返回Token的字符串表示
func (t Token) String() string {
	if t.Value != "" {
		return fmt.Sprintf("%s(%s)", TokenName(t.Type), t.Value)
	}
	return TokenName(t.Type)
}

// IsLiteral 检查是否为字面量Token
func (t Token) IsLiteral() bool {
	switch t.Type {
	case INTEGER_LIT, FLOAT_LIT, STRING_LIT, HEX_LIT, BIT_LIT, NULL_LIT, TRUE_LIT, FALSE_LIT:
		return true
	default:
		return false
	}
}

// IsKeyword 检查是否为关键字Token
func (t Token) IsKeyword() bool {
	return IsKeyword(t.Type)
}

// IsOperator 检查是否为操作符Token
func (t Token) IsOperator() bool {
	switch t.Type {
	case PLUS, MINUS, MULTIPLY, DIVIDE, MODULO:
		return true
	case EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, NULL_SAFE_EQUAL:
		return true
	case AND_OP, OR_OP, NOT_OP:
		return true
	case BIT_AND, BIT_OR, BIT_XOR, BIT_NOT, LEFT_SHIFT, RIGHT_SHIFT:
		return true
	case ASSIGN:
		return true
	default:
		return false
	}
}

// IsDelimiter 检查是否为分隔符Token
func (t Token) IsDelimiter() bool {
	switch t.Type {
	case COMMA, SEMICOLON, LEFT_PAREN, RIGHT_PAREN, LEFT_BRACE, RIGHT_BRACE:
		return true
	case LEFT_BRACKET, RIGHT_BRACKET, DOT, QUESTION, COLON, DOUBLE_COLON:
		return true
	default:
		return false
	}
}

// IsEOF 检查是否为EOF Token
func (t Token) IsEOF() bool {
	return t.Type == EOF
}

// IsInvalid 检查是否为无效Token
func (t Token) IsInvalid() bool {
	return t.Type == INVALID
}

// GetRange 获取Token的位置范围
func (t Token) GetRange() PositionRange {
	return NewPositionRange(t.Position, t.EndPos)
}

// Length 返回Token的长度
func (t Token) Length() int {
	return len(t.Raw)
}

// Equal 检查两个Token是否相等
func (t Token) Equal(other Token) bool {
	return t.Type == other.Type && t.Value == other.Value && t.Position.Equal(other.Position)
}

// TokenList Token列表类型
type TokenList []Token

// String 返回Token列表的字符串表示
func (tl TokenList) String() string {
	result := "["
	for i, token := range tl {
		if i > 0 {
			result += ", "
		}
		result += token.String()
	}
	result += "]"
	return result
}

// Length 返回Token列表的长度
func (tl TokenList) Length() int {
	return len(tl)
}

// IsEmpty 检查Token列表是否为空
func (tl TokenList) IsEmpty() bool {
	return len(tl) == 0
}

// First 返回第一个Token
func (tl TokenList) First() *Token {
	if len(tl) == 0 {
		return nil
	}
	return &tl[0]
}

// Last 返回最后一个Token
func (tl TokenList) Last() *Token {
	if len(tl) == 0 {
		return nil
	}
	return &tl[len(tl)-1]
}

// At 返回指定索引的Token
func (tl TokenList) At(index int) *Token {
	if index < 0 || index >= len(tl) {
		return nil
	}
	return &tl[index]
}
