package parser

import (
	"fmt"
	"time"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// Parse 解析Token流为AST
// 这是解析器的主入口方法，实现Parser接口
// 参数:
//
//	tokens: 词法分析器生成的Token流
//
// 返回:
//
//	*ast.AST: 抽象语法树
//	error: 解析错误（如果有）
func (p *ParserImpl) Parse(tokens []lexer.Token) (*ast.AST, error) {
	// 重新初始化解析器状态
	p.tokens = tokens
	p.tokenCount = len(tokens)
	p.position = 0
	p.errors = p.errors[:0] // 清空错误列表但保留容量
	p.currentDepth = 0
	p.nodeCount = 0

	// 初始化Token状态
	p.initializeTokens()

	// 记录解析开始时间
	startTime := time.Now()

	// 解析所有语句
	statements := make([]ast.Statement, 0)

	for !p.isAtEnd() {
		// 跳过分号（语句分隔符）
		if p.matchToken(lexer.SEMICOLON) {
			p.nextToken()
			continue
		}

		// 解析单个语句
		if stmt := p.ParseStatement(); stmt != nil {
			statements = append(statements, stmt)
		}

		// 如果遇到错误且无法恢复，跳到下一个同步点
		if p.HasErrors() && !p.recoverFromError() {
			break
		}
	}

	// 创建AST根节点
	ast := &ast.AST{
		Statements: statements,
		Comments:   make([]ast.Comment, 0), // 暂时不处理注释
		Metadata: &ast.ASTMetadata{
			ParseTime:     startTime,
			ParseDuration: time.Since(startTime),
			TokenCount:    p.tokenCount,
			NodeCount:     p.nodeCount,
		},
	}

	// 如果有错误，返回错误信息
	if p.HasErrors() {
		return ast, &ParseErrors{Errors: p.errors}
	}

	return ast, nil
}

// ParseStatement 解析单个SQL语句
// 根据当前Token类型判断语句类型并调用相应的解析方法
// 返回:
//
//	ast.Statement: 解析得到的语句AST节点
func (p *ParserImpl) ParseStatement() ast.Statement {
	// 检查递归深度
	if !p.incrementDepth() {
		return nil
	}
	defer p.decrementDepth()

	// 跳过空白和注释（如果词法分析器没有过滤）
	p.skipWhitespaceAndComments()

	// 根据当前Token类型选择解析方法
	switch p.currentToken.Type {
	case lexer.SELECT:
		return p.parseSelectStatement()
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
	case lexer.ALTER:
		return p.parseAlterStatement()
	case lexer.EOF:
		// 到达文件末尾，正常结束
		return nil
	default:
		// 未识别的语句类型
		p.addErrorf("未识别的语句类型: %s", lexer.TokenName(p.currentToken.Type))
		p.skipToNextStatement()
		return nil
	}
}

// ParseExpression 解析表达式
// 这是表达式解析的入口方法，使用优先级爬升算法
// 返回:
//
//	ast.Expression: 解析得到的表达式AST节点
func (p *ParserImpl) ParseExpression() ast.Expression {
	return p.parseExpressionWithPrecedence(0)
}

// parseExpressionWithPrecedence 使用优先级爬升算法解析表达式
// 参数:
//
//	minPrecedence: 最小优先级
//
// 返回:
//
//	ast.Expression: 表达式AST节点
func (p *ParserImpl) parseExpressionWithPrecedence(minPrecedence int) ast.Expression {
	// 检查递归深度
	if !p.incrementDepth() {
		return nil
	}
	defer p.decrementDepth()

	// 解析左操作数（前缀表达式）
	left := p.parsePrimaryExpression()
	if left == nil {
		return nil
	}

	// 处理中缀操作符
	for {
		// 检查当前Token是否为操作符
		if !p.isInfixOperator(p.currentToken.Type) {
			break
		}

		// 获取操作符优先级
		precedence := p.getOperatorPrecedence(p.currentToken.Type)
		if precedence < minPrecedence {
			break
		}

		// 保存操作符信息
		operator := p.currentToken.Type
		operatorPos := p.getCurrentPosition()
		p.nextToken() // 跳过操作符

		// 解析右操作数
		var right ast.Expression
		if p.isRightAssociative(operator) {
			// 右结合操作符
			right = p.parseExpressionWithPrecedence(precedence)
		} else {
			// 左结合操作符
			right = p.parseExpressionWithPrecedence(precedence + 1)
		}

		if right == nil {
			p.addError("操作符后缺少右操作数", nil, p.currentToken.Type)
			return left
		}

		// 创建二元表达式节点
		left = &ast.BinaryExpression{
			Position: operatorPos,
			Left:     left,
			Operator: operator,
			Right:    right,
		}
		p.incrementNodeCount()
	}

	return left
}

// parsePrimaryExpression 解析基础表达式（字面量、标识符、括号表达式等）
// 返回:
//
//	ast.Expression: 基础表达式AST节点
func (p *ParserImpl) parsePrimaryExpression() ast.Expression {
	switch p.currentToken.Type {
	case lexer.INTEGER_LIT, lexer.FLOAT_LIT, lexer.STRING_LIT, lexer.HEX_LIT, lexer.BIT_LIT:
		return p.parseLiteralExpression()
	case lexer.NULL_LIT:
		return p.parseNullLiteral()
	case lexer.TRUE_LIT, lexer.FALSE_LIT:
		return p.parseBooleanLiteral()
	case lexer.IDENTIFIER:
		return p.parseIdentifierExpression()
	case lexer.LEFT_PAREN:
		return p.parseParenthesizedExpression()
	case lexer.NOT, lexer.NOT_OP, lexer.MINUS, lexer.PLUS:
		return p.parseUnaryExpression()
	case lexer.CASE:
		return p.parseCaseExpression()
	case lexer.MULTIPLY:
		// 处理通配符 * (在SELECT列表中)
		return p.parseWildcardExpression()
	// 处理聚合函数关键字（它们可以作为函数名使用）
	case lexer.COUNT, lexer.SUM, lexer.AVG, lexer.MAX, lexer.MIN:
		// 检查是否为函数调用（后面跟着左括号）
		if p.peekToken.Type == lexer.LEFT_PAREN {
			return p.parseAggregateFunctionExpression()
		} else {
			// 作为标识符处理（列名）
			return p.parseKeywordAsIdentifier()
		}
	default:
		// 检查是否为其他可以作为标识符使用的关键字
		if p.canBeUsedAsIdentifier(p.currentToken.Type) {
			return p.parseKeywordAsIdentifier()
		}
		p.addErrorf("意外的Token: %s，期望表达式", lexer.TokenName(p.currentToken.Type))
		return nil
	}
}

// skipWhitespaceAndComments 跳过空白字符和注释
// 注意：通常词法分析器已经过滤了这些，但为了健壮性保留此方法
func (p *ParserImpl) skipWhitespaceAndComments() {
	for p.matchTokens(lexer.WHITESPACE, lexer.COMMENT) {
		p.nextToken()
	}
}

// skipToNextStatement 跳到下一个语句开始位置（用于错误恢复）
func (p *ParserImpl) skipToNextStatement() {
	// 跳过Token直到遇到语句开始关键字或分号
	syncTokens := []lexer.TokenType{
		lexer.SELECT, lexer.INSERT, lexer.UPDATE, lexer.DELETE,
		lexer.CREATE, lexer.DROP, lexer.ALTER, lexer.SEMICOLON, lexer.EOF,
	}

	found, _ := p.skipToTokens(syncTokens...)
	if found && p.matchToken(lexer.SEMICOLON) {
		p.nextToken() // 跳过分号
	}
}

// recoverFromError 从解析错误中恢复
// 返回:
//
//	bool: 是否成功恢复
func (p *ParserImpl) recoverFromError() bool {
	// 简单的错误恢复策略：跳到下一个语句
	p.skipToNextStatement()
	return !p.isAtEnd()
}

// isInfixOperator 检查Token是否为中缀操作符
// 参数:
//
//	tokenType: Token类型
//
// 返回:
//
//	bool: 是否为中缀操作符
func (p *ParserImpl) isInfixOperator(tokenType lexer.TokenType) bool {
	switch tokenType {
	case lexer.PLUS, lexer.MINUS, lexer.MULTIPLY, lexer.DIVIDE, lexer.MODULO:
		return true
	case lexer.EQUAL, lexer.NOT_EQUAL, lexer.LESS, lexer.LESS_EQUAL, lexer.GREATER, lexer.GREATER_EQUAL:
		return true
	case lexer.AND, lexer.OR:
		return true
	case lexer.LIKE, lexer.IN, lexer.BETWEEN:
		return true
	default:
		return false
	}
}

// getOperatorPrecedence 获取操作符优先级
// 参考SQL标准和TiDB的优先级定义
// 参数:
//
//	tokenType: 操作符Token类型
//
// 返回:
//
//	int: 优先级（数值越大优先级越高）
func (p *ParserImpl) getOperatorPrecedence(tokenType lexer.TokenType) int {
	switch tokenType {
	case lexer.OR:
		return 1
	case lexer.AND:
		return 2
	case lexer.NOT:
		return 3
	case lexer.BETWEEN, lexer.IN, lexer.LIKE:
		return 4
	case lexer.EQUAL, lexer.NOT_EQUAL, lexer.LESS, lexer.LESS_EQUAL, lexer.GREATER, lexer.GREATER_EQUAL:
		return 5
	case lexer.PLUS, lexer.MINUS:
		return 6
	case lexer.MULTIPLY, lexer.DIVIDE, lexer.MODULO:
		return 7
	default:
		return 0
	}
}

// isRightAssociative 检查操作符是否为右结合
// 参数:
//
//	tokenType: 操作符Token类型
//
// 返回:
//
//	bool: 是否为右结合
func (p *ParserImpl) isRightAssociative(tokenType lexer.TokenType) bool {
	// 大多数SQL操作符都是左结合的
	// 这里可以根据需要添加右结合操作符
	return false
}

// canBeUsedAsIdentifier 检查关键字是否可以作为标识符使用
// 参数:
//
//	tokenType: Token类型
//
// 返回:
//
//	bool: 是否可以作为标识符使用
func (p *ParserImpl) canBeUsedAsIdentifier(tokenType lexer.TokenType) bool {
	// 这些关键字在某些上下文中可以作为标识符使用
	switch tokenType {
	// 聚合函数
	case lexer.COUNT, lexer.SUM, lexer.AVG, lexer.MAX, lexer.MIN:
		return true
	// 日期时间相关
	case lexer.DATE, lexer.TIME, lexer.TIMESTAMP, lexer.YEAR:
		return true
	// 数据类型关键字
	case lexer.TEXT, lexer.JSON:
		return true
	// JOIN相关关键字（可以作为列名）
	case lexer.LEFT, lexer.RIGHT, lexer.FULL, lexer.INNER, lexer.OUTER:
		return true
	// 其他可能作为函数名或列名的关键字
	case lexer.IF, lexer.CASE, lexer.WHEN, lexer.THEN, lexer.ELSE, lexer.END:
		return true
	default:
		return false
	}
}

// parseKeywordAsIdentifier 将关键字解析为标识符表达式
// 返回:
//
//	ast.Expression: 标识符表达式AST节点
func (p *ParserImpl) parseKeywordAsIdentifier() ast.Expression {
	pos := p.getCurrentPosition()
	identifier := p.currentToken.Value
	p.nextToken()

	// 检查是否为函数调用（关键字后跟左括号）
	if p.matchToken(lexer.LEFT_PAREN) {
		return p.parseFunctionCall(pos, identifier)
	}

	// 解析可能的限定名称（表名.列名 或 schema.table.column）
	var schemaName, tableName, columnName string

	if p.matchToken(lexer.DOT) {
		// 第一个点：可能是 table.column 或 schema.table
		p.nextToken() // 跳过点号

		if !p.matchToken(lexer.IDENTIFIER) && !p.canBeUsedAsIdentifier(p.currentToken.Type) {
			p.addError("期望标识符", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
			return nil
		}

		secondIdentifier := p.currentToken.Value
		p.nextToken()

		// 检查是否还有第二个点（schema.table.column格式）
		if p.matchToken(lexer.DOT) {
			p.nextToken() // 跳过第二个点号

			if !p.matchToken(lexer.IDENTIFIER) && !p.canBeUsedAsIdentifier(p.currentToken.Type) {
				p.addError("期望列名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
				return nil
			}

			// 三段式：schema.table.column
			schemaName = identifier
			tableName = secondIdentifier
			columnName = p.currentToken.Value
			p.nextToken()
		} else {
			// 两段式：table.column
			tableName = identifier
			columnName = secondIdentifier
		}
	} else {
		// 单段式：column
		columnName = identifier
	}

	// 创建列引用节点
	columnRef := &ast.ColumnReference{
		Position: pos,
		Schema:   schemaName,
		Table:    tableName,
		Column:   columnName,
	}
	p.incrementNodeCount()

	return columnRef
}

// parseWildcardExpression 解析通配符表达式（*）
// 返回:
//
//	ast.Expression: 通配符表达式AST节点
func (p *ParserImpl) parseWildcardExpression() ast.Expression {
	pos := p.getCurrentPosition()

	// 期望 * 符号
	if !p.expectToken(lexer.MULTIPLY) {
		return nil
	}

	// 创建通配符节点
	wildcard := &ast.Wildcard{
		Position: pos,
	}
	p.incrementNodeCount()

	return wildcard
}

// ParseErrors 解析错误集合，实现error接口
type ParseErrors struct {
	Errors []ParseError
}

// Error 实现error接口
func (e *ParseErrors) Error() string {
	if len(e.Errors) == 0 {
		return "解析错误"
	}

	result := fmt.Sprintf("发现 %d 个解析错误:\n", len(e.Errors))
	for i, err := range e.Errors {
		result += fmt.Sprintf("  %d. 第%d行，第%d列: %s\n",
			i+1, err.Position.Line, err.Position.Column, err.Message)
		if err.Context != "" {
			result += fmt.Sprintf("     %s\n", err.Context)
		}
	}

	return result
}
