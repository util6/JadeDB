package errors

import (
	"fmt"
	"strings"

	"github.com/util6/JadeDB/sql/lexer"
)

// ParseErrors 解析错误集合
type ParseErrors struct {
	Errors []ParseError
}

func (e *ParseErrors) Error() string {
	var builder strings.Builder
	builder.WriteString("Parse errors:\n")

	for _, err := range e.Errors {
		builder.WriteString(fmt.Sprintf("  Line %d, Column %d: %s\n",
			err.Position.Line, err.Position.Column, err.Message))
	}

	return builder.String()
}

// ParseError 解析错误
type ParseError struct {
	Position lexer.Position
	Message  string
	Expected []lexer.TokenType
	Actual   lexer.TokenType
}

// LexErrors 词法错误集合
type LexErrors struct {
	Errors []lexer.LexError
}

func (e *LexErrors) Error() string {
	var builder strings.Builder
	builder.WriteString("Lexical errors:\n")

	for _, err := range e.Errors {
		builder.WriteString(fmt.Sprintf("  Line %d, Column %d: %s\n",
			err.Position.Line, err.Position.Column, err.Message))
	}

	return builder.String()
}

// ErrorRecovery 错误恢复策略
type ErrorRecovery struct {
	syncTokens []lexer.TokenType
}

// NewErrorRecovery 创建错误恢复实例
func NewErrorRecovery() *ErrorRecovery {
	return &ErrorRecovery{
		syncTokens: []lexer.TokenType{
			lexer.SEMICOLON, lexer.SELECT, lexer.INSERT, lexer.UPDATE,
			lexer.DELETE, lexer.CREATE, lexer.DROP,
		},
	}
}

// IsSyncToken 检查是否为同步Token
func (er *ErrorRecovery) IsSyncToken(tokenType lexer.TokenType) bool {
	for _, syncToken := range er.syncTokens {
		if tokenType == syncToken {
			return true
		}
	}
	return false
}