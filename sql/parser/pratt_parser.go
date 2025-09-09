package parser

import (
	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// Precedence 操作符优先级
type Precedence int

const (
	LOWEST      Precedence = iota // 最低优先级
	LOGICAL_OR                    // OR
	LOGICAL_AND                   // AND
	EQUALITY                      // =, !=, <>, <=>
	COMPARISON                    // <, <=, >, >=
	BITWISE_OR                    // |
	BITWISE_XOR                   // ^
	BITWISE_AND                   // &
	SHIFT                         // <<, >>
	TERM                          // +, -
	FACTOR                        // *, /, %
	UNARY                         // +x, -x, !x, ~x
	CALL                          // function()
	INDEX                         // array[index]
)

// precedences 操作符优先级映射表
var precedences = map[lexer.TokenType]Precedence{
	// 逻辑操作符
	lexer.OR:     LOGICAL_OR,
	lexer.OR_OP:  LOGICAL_OR,
	lexer.AND:    LOGICAL_AND,
	lexer.AND_OP: LOGICAL_AND,

	// 相等性操作符
	lexer.EQUAL:           EQUALITY,
	lexer.NOT_EQUAL:       EQUALITY,
	lexer.NULL_SAFE_EQUAL: EQUALITY,

	// 比较操作符
	lexer.LESS:          COMPARISON,
	lexer.LESS_EQUAL:    COMPARISON,
	lexer.GREATER:       COMPARISON,
	lexer.GREATER_EQUAL: COMPARISON,
	lexer.LIKE:          COMPARISON,
	lexer.IN:            COMPARISON,
	lexer.IS:            COMPARISON,
	lexer.BETWEEN:       COMPARISON,

	// 位操作符
	lexer.BIT_OR:      BITWISE_OR,
	lexer.BIT_XOR:     BITWISE_XOR,
	lexer.BIT_AND:     BITWISE_AND,
	lexer.LEFT_SHIFT:  SHIFT,
	lexer.RIGHT_SHIFT: SHIFT,

	// 算术操作符
	lexer.PLUS:     TERM,
	lexer.MINUS:    TERM,
	lexer.MULTIPLY: FACTOR,
	lexer.DIVIDE:   FACTOR,
	lexer.MODULO:   FACTOR,

	// 函数调用和索引
	lexer.LEFT_PAREN:   CALL,
	lexer.LEFT_BRACKET: INDEX,
}

// prefixParseFn 前缀解析函数类型
type prefixParseFn func() ast.Expression

// infixParseFn 中缀解析函数类型
type infixParseFn func(ast.Expression) ast.Expression

// ParseExpression 使用Pratt解析器解析表达式
// 这是表达式解析的主入口点，实现了Pratt解析算法
func (p *ParserImpl) ParseExpression() ast.Expression {
	return p.parseExpression(LOWEST)
}

// parseExpression 按优先级解析表达式
func (p *ParserImpl) parseExpression(precedence Precedence) ast.Expression {
	// 获取前缀解析函数
	prefix := p.getPrefixParseFn(p.currentToken.Type)
	if prefix == nil {
		p.addErrorf("没有找到前缀解析函数: %s", lexer.TokenName(p.currentToken.Type))
		return nil
	}

	// 解析左表达式
	leftExpr := prefix()

	// 处理中缀操作符
	for !p.isAtEnd() && precedence < p.peekPrecedence() {
		infix := p.getInfixParseFn(p.currentToken.Type)
		if infix == nil {
			return leftExpr
		}

		leftExpr = infix(leftExpr)
	}

	return leftExpr
}

// getPrefixParseFn 获取前缀解析函数
func (p *ParserImpl) getPrefixParseFn(tokenType lexer.TokenType) prefixParseFn {
	switch tokenType {
	case lexer.IDENTIFIER:
		return p.parseIdentifierExpression
	case lexer.INTEGER_LIT, lexer.FLOAT_LIT, lexer.STRING_LIT, lexer.HEX_LIT, lexer.BIT_LIT:
		return p.parseLiteralExpression
	case lexer.NULL_LIT:
		return p.parseNullLiteral
	case lexer.TRUE_LIT, lexer.FALSE_LIT:
		return p.parseBooleanLiteral
	case lexer.LEFT_PAREN:
		return p.parseGroupedExpression
	case lexer.CASE:
		return p.parseCaseExpression
	case lexer.PLUS, lexer.MINUS, lexer.NOT, lexer.NOT_OP, lexer.BIT_NOT:
		return p.parseUnaryExpression
	case lexer.COUNT, lexer.SUM, lexer.AVG, lexer.MAX, lexer.MIN:
		return p.parseAggregateFunctionExpression
	case lexer.MULTIPLY: // 支持 * 通配符
		return p.parseWildcardExpression
	default:
		return nil
	}
}

// getInfixParseFn 获取中缀解析函数
func (p *ParserImpl) getInfixParseFn(tokenType lexer.TokenType) infixParseFn {
	switch tokenType {
	case lexer.PLUS, lexer.MINUS, lexer.MULTIPLY, lexer.DIVIDE, lexer.MODULO:
		return p.parseInfixExpression
	case lexer.EQUAL, lexer.NOT_EQUAL, lexer.LESS, lexer.LESS_EQUAL, lexer.GREATER, lexer.GREATER_EQUAL, lexer.NULL_SAFE_EQUAL:
		return p.parseInfixExpression
	case lexer.AND, lexer.AND_OP, lexer.OR, lexer.OR_OP:
		return p.parseInfixExpression
	case lexer.BIT_AND, lexer.BIT_OR, lexer.BIT_XOR, lexer.LEFT_SHIFT, lexer.RIGHT_SHIFT:
		return p.parseInfixExpression
	case lexer.LIKE:
		return p.parseLikeExpression
	case lexer.IN:
		return p.parseInExpression
	case lexer.IS:
		return p.parseIsExpression
	case lexer.BETWEEN:
		return p.parseBetweenExpression
	default:
		return nil
	}
}

// peekPrecedence 获取下一个Token的优先级
func (p *ParserImpl) peekPrecedence() Precedence {
	if prec, ok := precedences[p.currentToken.Type]; ok {
		return prec
	}
	return LOWEST
}

// parseGroupedExpression 解析分组表达式（括号表达式或子查询）
func (p *ParserImpl) parseGroupedExpression() ast.Expression {
	// 检查是否为子查询
	if p.peekToken.Type == lexer.SELECT {
		return p.parseSubqueryExpression()
	}

	// 普通括号表达式
	return p.parseParenthesizedExpression()
}

// parseInfixExpression 解析中缀表达式
func (p *ParserImpl) parseInfixExpression(left ast.Expression) ast.Expression {
	pos := p.getCurrentPosition()
	operator := p.currentToken.Type
	precedence := p.peekPrecedence()

	p.nextToken() // 跳过操作符

	right := p.parseExpression(precedence)
	if right == nil {
		return left
	}

	binaryExpr := &ast.BinaryExpression{
		Position: pos,
		Left:     left,
		Operator: operator,
		Right:    right,
	}
	p.incrementNodeCount()

	return binaryExpr
}

// parseLikeExpression 解析LIKE表达式
func (p *ParserImpl) parseLikeExpression(left ast.Expression) ast.Expression {
	pos := p.getCurrentPosition()
	p.nextToken() // 跳过LIKE

	right := p.parseExpression(COMPARISON)
	if right == nil {
		return left
	}

	binaryExpr := &ast.BinaryExpression{
		Position: pos,
		Left:     left,
		Operator: lexer.LIKE,
		Right:    right,
	}
	p.incrementNodeCount()

	return binaryExpr
}

// parseInExpression 解析IN表达式
func (p *ParserImpl) parseInExpression(left ast.Expression) ast.Expression {
	pos := p.getCurrentPosition()
	p.nextToken() // 跳过IN

	// IN可以跟子查询或值列表
	var right ast.Expression

	if p.matchToken(lexer.LEFT_PAREN) {
		// 检查是否为子查询
		if p.peekTokenN(1).Type == lexer.SELECT {
			right = p.parseSubqueryExpression()
		} else {
			// 值列表
			right = p.parseValueList()
		}
	} else {
		right = p.parseExpression(COMPARISON)
	}

	if right == nil {
		return left
	}

	binaryExpr := &ast.BinaryExpression{
		Position: pos,
		Left:     left,
		Operator: lexer.IN,
		Right:    right,
	}
	p.incrementNodeCount()

	return binaryExpr
}

// parseIsExpression 解析IS表达式
func (p *ParserImpl) parseIsExpression(left ast.Expression) ast.Expression {
	pos := p.getCurrentPosition()
	p.nextToken() // 跳过IS

	// 检查NOT
	operator := lexer.IS
	if p.matchToken(lexer.NOT) {
		operator = lexer.NOT // 这里应该是IS NOT的组合，但简化处理
		p.nextToken()
	}

	right := p.parseExpression(COMPARISON)
	if right == nil {
		return left
	}

	binaryExpr := &ast.BinaryExpression{
		Position: pos,
		Left:     left,
		Operator: operator,
		Right:    right,
	}
	p.incrementNodeCount()

	return binaryExpr
}

// parseBetweenExpression 解析BETWEEN表达式
func (p *ParserImpl) parseBetweenExpression(left ast.Expression) ast.Expression {
	pos := p.getCurrentPosition()
	p.nextToken() // 跳过BETWEEN

	// 解析第一个值
	start := p.parseExpression(COMPARISON)
	if start == nil {
		return left
	}

	// 期望AND
	if !p.expectToken(lexer.AND) {
		return left
	}

	// 解析第二个值
	end := p.parseExpression(COMPARISON)
	if end == nil {
		return left
	}

	// 创建BETWEEN表达式（可以用特殊的AST节点或者用BinaryExpression表示）
	betweenExpr := &ast.BetweenExpression{
		Position: pos,
		Expr:     left,
		Start:    start,
		End:      end,
	}
	p.incrementNodeCount()

	return betweenExpr
}

// parseValueList 解析值列表（用于IN表达式）
func (p *ParserImpl) parseValueList() ast.Expression {
	pos := p.getCurrentPosition()

	if !p.expectToken(lexer.LEFT_PAREN) {
		return nil
	}

	values := make([]ast.Expression, 0)

	if !p.matchToken(lexer.RIGHT_PAREN) {
		// 解析第一个值
		expr := p.ParseExpression()
		if expr != nil {
			values = append(values, expr)
		}

		// 解析其余值
		for p.matchToken(lexer.COMMA) {
			p.nextToken() // 跳过逗号
			expr := p.ParseExpression()
			if expr != nil {
				values = append(values, expr)
			} else {
				break
			}
		}
	}

	if !p.expectToken(lexer.RIGHT_PAREN) {
		return nil
	}

	// 创建值列表表达式
	valueList := &ast.ValueListExpression{
		Position: pos,
		Values:   values,
	}
	p.incrementNodeCount()

	return valueList
}

// parseWildcardExpression 解析通配符表达式 (*)
func (p *ParserImpl) parseWildcardExpression() ast.Expression {
	pos := p.getCurrentPosition()

	// 创建通配符节点
	wildcard := &ast.Wildcard{
		Position: pos,
	}
	p.incrementNodeCount()

	// 移动到下一个Token
	p.nextToken()

	return wildcard
}
