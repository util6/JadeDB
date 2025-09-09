package parser

import (
	"fmt"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// ParserImpl 递归下降解析器实现 - 参考TiDB设计
// 采用递归下降解析算法，支持LL(k)语法分析
type ParserImpl struct {
	// Token流相关字段
	tokens       []lexer.Token // 输入的Token流
	tokenCount   int           // Token总数
	position     int           // 当前Token位置
	currentToken lexer.Token   // 当前正在处理的Token
	peekToken    lexer.Token   // 下一个Token（用于前瞻）

	// 错误处理相关字段
	errors    []ParseError // 解析过程中遇到的错误
	maxErrors int          // 最大错误数量（防止错误过多）

	// 解析控制相关字段
	maxDepth     int  // 最大递归深度（防止栈溢出）
	currentDepth int  // 当前递归深度
	strictMode   bool // 是否启用严格模式

	// 性能统计相关字段
	nodeCount int // 已创建的AST节点数量
}

// 使用interfaces.go中定义的ParseError

// NewParser 创建新的解析器实例
// 参数:
//
//	tokens: 词法分析器生成的Token流
//
// 返回:
//
//	*ParserImpl: 解析器实例
func NewParser(tokens []lexer.Token) *ParserImpl {
	p := &ParserImpl{
		tokens:       tokens,
		tokenCount:   len(tokens),
		position:     0,
		errors:       make([]ParseError, 0),
		maxErrors:    10,  // 最多收集10个错误
		maxDepth:     100, // 最大递归深度100层
		currentDepth: 0,
		strictMode:   false,
		nodeCount:    0,
	}

	// 初始化当前Token和前瞻Token
	p.initializeTokens()
	return p
}

// NewParserWithOptions 创建带选项的解析器实例
// 参数:
//
//	tokens: Token流
//	options: 解析选项
//
// 返回:
//
//	*ParserImpl: 解析器实例
func NewParserWithOptions(tokens []lexer.Token, options *ParseOptions) *ParserImpl {
	p := NewParser(tokens)

	if options != nil {
		p.maxDepth = options.MaxDepth
		p.strictMode = options.StrictMode
		if options.MaxDepth <= 0 {
			p.maxDepth = 100 // 默认值
		}
	}

	return p
}

// initializeTokens 初始化Token流状态
// 设置currentToken和peekToken的初始值
func (p *ParserImpl) initializeTokens() {
	if p.tokenCount > 0 {
		p.currentToken = p.tokens[0]
	} else {
		// 如果没有Token，创建一个EOF Token
		p.currentToken = lexer.NewToken(lexer.EOF, "", "", lexer.NewPosition(1, 1, 0))
	}

	if p.tokenCount > 1 {
		p.peekToken = p.tokens[1]
	} else {
		// 如果只有一个Token或没有Token，peekToken也是EOF
		p.peekToken = lexer.NewToken(lexer.EOF, "", "", lexer.NewPosition(1, 1, 0))
	}
}

// nextToken 移动到下一个Token
// 这是解析器的核心导航方法，负责Token流的推进
func (p *ParserImpl) nextToken() {
	// 检查是否已到达Token流末尾
	if p.position >= p.tokenCount-1 {
		// 已经是最后一个Token或超出范围
		p.currentToken = lexer.NewToken(lexer.EOF, "", "", p.currentToken.Position)
		p.peekToken = p.currentToken
		return
	}

	// 移动到下一个Token
	p.position++
	p.currentToken = p.tokens[p.position]

	// 更新peekToken
	if p.position+1 < p.tokenCount {
		p.peekToken = p.tokens[p.position+1]
	} else {
		p.peekToken = lexer.NewToken(lexer.EOF, "", "", p.currentToken.Position)
	}
}

// peekTokenN 查看第n个Token（不移动位置）
// 参数:
//
//	n: 要查看的Token偏移量（1表示下一个Token）
//
// 返回:
//
//	lexer.Token: 第n个Token，如果超出范围则返回EOF Token
func (p *ParserImpl) peekTokenN(n int) lexer.Token {
	targetPos := p.position + n
	if targetPos >= p.tokenCount {
		return lexer.NewToken(lexer.EOF, "", "", p.currentToken.Position)
	}
	return p.tokens[targetPos]
}

// expectToken 期望特定类型的Token
// 如果当前Token类型匹配，则移动到下一个Token；否则记录错误
// 参数:
//
//	tokenType: 期望的Token类型
//
// 返回:
//
//	bool: 是否匹配成功
func (p *ParserImpl) expectToken(tokenType lexer.TokenType) bool {
	if p.currentToken.Type == tokenType {
		p.nextToken()
		return true
	}

	// 记录期望Token错误
	p.addError(fmt.Sprintf("期望 %s，但遇到 %s",
		lexer.TokenName(tokenType),
		lexer.TokenName(p.currentToken.Type)),
		[]lexer.TokenType{tokenType},
		p.currentToken.Type)

	return false
}

// expectTokens 期望多个可能的Token类型之一
// 参数:
//
//	tokenTypes: 期望的Token类型列表
//
// 返回:
//
//	bool: 是否匹配成功
//	lexer.TokenType: 匹配的Token类型
func (p *ParserImpl) expectTokens(tokenTypes ...lexer.TokenType) (bool, lexer.TokenType) {
	for _, tokenType := range tokenTypes {
		if p.currentToken.Type == tokenType {
			matchedType := p.currentToken.Type
			p.nextToken()
			return true, matchedType
		}
	}

	// 记录期望Token错误
	p.addError(fmt.Sprintf("期望 %v 中的一个，但遇到 %s",
		p.tokenTypesToNames(tokenTypes),
		lexer.TokenName(p.currentToken.Type)),
		tokenTypes,
		p.currentToken.Type)

	return false, lexer.INVALID
}

// matchToken 检查当前Token是否匹配指定类型（不移动位置）
// 参数:
//
//	tokenType: 要检查的Token类型
//
// 返回:
//
//	bool: 是否匹配
func (p *ParserImpl) matchToken(tokenType lexer.TokenType) bool {
	return p.currentToken.Type == tokenType
}

// matchTokens 检查当前Token是否匹配多个类型之一（不移动位置）
// 参数:
//
//	tokenTypes: 要检查的Token类型列表
//
// 返回:
//
//	bool: 是否匹配任意一个类型
func (p *ParserImpl) matchTokens(tokenTypes ...lexer.TokenType) bool {
	for _, tokenType := range tokenTypes {
		if p.currentToken.Type == tokenType {
			return true
		}
	}
	return false
}

// skipToken 跳过当前Token（用于错误恢复）
func (p *ParserImpl) skipToken() {
	p.nextToken()
}

// skipToToken 跳过Token直到遇到指定类型（用于错误恢复）
// 参数:
//
//	tokenType: 目标Token类型
//
// 返回:
//
//	bool: 是否找到目标Token
func (p *ParserImpl) skipToToken(tokenType lexer.TokenType) bool {
	for !p.isAtEnd() && p.currentToken.Type != tokenType {
		p.nextToken()
	}
	return p.currentToken.Type == tokenType
}

// skipToTokens 跳过Token直到遇到指定类型之一（用于错误恢复）
// 参数:
//
//	tokenTypes: 目标Token类型列表
//
// 返回:
//
//	bool: 是否找到任意目标Token
//	lexer.TokenType: 找到的Token类型
func (p *ParserImpl) skipToTokens(tokenTypes ...lexer.TokenType) (bool, lexer.TokenType) {
	for !p.isAtEnd() {
		for _, tokenType := range tokenTypes {
			if p.currentToken.Type == tokenType {
				return true, tokenType
			}
		}
		p.nextToken()
	}
	return false, lexer.EOF
}

// isAtEnd 检查是否已到达Token流末尾
// 返回:
//
//	bool: 是否到达末尾
func (p *ParserImpl) isAtEnd() bool {
	return p.currentToken.Type == lexer.EOF || p.position >= p.tokenCount
}

// getCurrentPosition 获取当前解析位置
// 返回:
//
//	lexer.Position: 当前位置信息
func (p *ParserImpl) getCurrentPosition() lexer.Position {
	return p.currentToken.Position
}

// addError 添加解析错误
// 参数:
//
//	message: 错误消息
//	expected: 期望的Token类型列表
//	actual: 实际遇到的Token类型
func (p *ParserImpl) addError(message string, expected []lexer.TokenType, actual lexer.TokenType) {
	// 检查是否已达到最大错误数量
	if len(p.errors) >= p.maxErrors {
		return
	}

	error := ParseError{
		Position: p.getCurrentPosition(),
		Message:  message,
		Expected: expected,
		Actual:   actual,
		Context:  p.getErrorContext(),
	}

	p.errors = append(p.errors, error)
}

// addErrorf 添加格式化的解析错误
// 参数:
//
//	format: 格式字符串
//	args: 格式参数
func (p *ParserImpl) addErrorf(format string, args ...interface{}) {
	p.addError(fmt.Sprintf(format, args...), nil, p.currentToken.Type)
}

// getErrorContext 获取错误上下文信息
// 返回当前解析位置附近的Token信息，用于更好的错误报告
// 返回:
//
//	string: 上下文信息
func (p *ParserImpl) getErrorContext() string {
	const contextSize = 3 // 前后各3个Token

	start := p.position - contextSize
	if start < 0 {
		start = 0
	}

	end := p.position + contextSize + 1
	if end > p.tokenCount {
		end = p.tokenCount
	}

	context := "上下文: "
	for i := start; i < end; i++ {
		if i == p.position {
			context += fmt.Sprintf("[%s] ", p.tokens[i].String())
		} else {
			context += fmt.Sprintf("%s ", p.tokens[i].String())
		}
	}

	return context
}

// tokenTypesToNames 将Token类型列表转换为名称列表
// 参数:
//
//	tokenTypes: Token类型列表
//
// 返回:
//
//	[]string: Token名称列表
func (p *ParserImpl) tokenTypesToNames(tokenTypes []lexer.TokenType) []string {
	names := make([]string, len(tokenTypes))
	for i, tokenType := range tokenTypes {
		names[i] = lexer.TokenName(tokenType)
	}
	return names
}

// GetErrors 获取所有解析错误
// 返回:
//
//	[]ParseError: 错误列表
func (p *ParserImpl) GetErrors() []ParseError {
	return p.errors
}

// HasErrors 检查是否有解析错误
// 返回:
//
//	bool: 是否有错误
func (p *ParserImpl) HasErrors() bool {
	return len(p.errors) > 0
}

// ClearErrors 清空错误列表
func (p *ParserImpl) ClearErrors() {
	p.errors = p.errors[:0]
}

// GetStatistics 获取解析统计信息
// 返回:
//
//	*ParseStatistics: 统计信息
func (p *ParserImpl) GetStatistics() *ParseStatistics {
	return &ParseStatistics{
		TotalParses:  1, // 当前解析次数
		CacheHits:    0, // 暂未实现缓存
		CacheMisses:  0,
		AvgParseTime: 0, // 需要在调用方计算
		ErrorCount:   int64(len(p.errors)),
	}
}

// incrementDepth 增加递归深度并检查是否超限
// 返回:
//
//	bool: 是否可以继续递归
func (p *ParserImpl) incrementDepth() bool {
	p.currentDepth++
	if p.currentDepth > p.maxDepth {
		p.addErrorf("递归深度超过限制 (%d)，可能存在无限递归", p.maxDepth)
		return false
	}
	return true
}

// decrementDepth 减少递归深度
func (p *ParserImpl) decrementDepth() {
	if p.currentDepth > 0 {
		p.currentDepth--
	}
}

// incrementNodeCount 增加节点计数
func (p *ParserImpl) incrementNodeCount() {
	p.nodeCount++
}

// canBeUsedAsIdentifier 检查Token是否可以用作标识符
// 某些关键字在特定上下文中可以用作标识符（如别名）
func (p *ParserImpl) canBeUsedAsIdentifier(tokenType lexer.TokenType) bool {
	switch tokenType {
	// 数据类型关键字可以用作别名
	case lexer.INT, lexer.INTEGER, lexer.BIGINT, lexer.SMALLINT, lexer.TINYINT:
		return true
	case lexer.DECIMAL, lexer.NUMERIC, lexer.FLOAT, lexer.DOUBLE, lexer.REAL:
		return true
	case lexer.VARCHAR, lexer.CHAR, lexer.TEXT, lexer.BLOB:
		return true
	case lexer.DATE, lexer.TIME, lexer.TIMESTAMP, lexer.DATETIME:
		return true
	case lexer.BOOLEAN, lexer.BINARY, lexer.VARBINARY:
		return true

	// 某些SQL关键字在特定上下文中可以用作标识符
	case lexer.KEY, lexer.INDEX, lexer.COLUMN:
		return true
	case lexer.ASC, lexer.DESC:
		return true

	// 函数名关键字可以用作别名
	case lexer.COUNT, lexer.SUM, lexer.AVG, lexer.MAX, lexer.MIN:
		return true

	default:
		return false
	}
}

// Parse 实现Parser接口的Parse方法
// 解析Token流并返回AST
func (p *ParserImpl) Parse(tokens []lexer.Token) (*ast.AST, error) {
	p.tokens = tokens
	p.tokenCount = len(tokens)
	p.position = 0
	p.errors = make([]ParseError, 0)
	p.nodeCount = 0

	// 重新初始化Token状态
	p.initializeTokens()

	// 解析所有语句
	statements := make([]ast.Statement, 0)

	for !p.isAtEnd() {
		// 跳过分号
		if p.matchToken(lexer.SEMICOLON) {
			p.nextToken()
			continue
		}

		stmt := p.ParseStatement()
		if stmt != nil {
			statements = append(statements, stmt)
		}

		// 如果遇到错误且无法恢复，跳出循环
		if p.HasErrors() && len(p.errors) >= p.maxErrors {
			break
		}
	}

	// 创建AST
	astNode := &ast.AST{
		Statements: statements,
		Metadata: &ast.ASTMetadata{
			TokenCount: p.tokenCount,
			NodeCount:  p.nodeCount,
		},
	}

	// 如果有错误，返回错误
	if p.HasErrors() {
		return astNode, &ParseErrors{Errors: p.errors}
	}

	return astNode, nil
}

// ParseStatement 实现Parser接口的ParseStatement方法
func (p *ParserImpl) ParseStatement() ast.Statement {
	switch p.currentToken.Type {
	case lexer.SELECT:
		return p.parseSelectStatementWithUnion()
	case lexer.INSERT:
		return p.parseInsertStatement()
	case lexer.UPDATE:
		return p.parseUpdateStatement()
	case lexer.DELETE:
		return p.parseDeleteStatement()
	case lexer.CREATE:
		return p.parseCreateStatement()
	case lexer.DROP:
		return p.parseDropStatement()
	case lexer.BEGIN:
		return p.parseBeginTransactionStatement()
	case lexer.COMMIT:
		return p.parseCommitTransactionStatement()
	case lexer.ROLLBACK:
		return p.parseRollbackTransactionStatement()
	default:
		p.addErrorf("意外的语句开始Token: %s", lexer.TokenName(p.currentToken.Type))
		p.skipToken() // 跳过无效Token
		return nil
	}
}

// ParseErrors 解析错误集合
type ParseErrors struct {
	Errors []ParseError
}

func (e *ParseErrors) Error() string {
	if len(e.Errors) == 0 {
		return "解析错误"
	}

	result := "解析错误:\n"
	for _, err := range e.Errors {
		result += fmt.Sprintf("  第%d行，第%d列: %s\n",
			err.Position.Line, err.Position.Column, err.Message)
	}
	return result
}
