package parser

import (
	"strconv"
	"strings"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// parseLiteralExpression 解析字面量表达式
// 根据Token类型将字符串值转换为相应的Go类型
// 返回:
//
//	ast.Expression: 字面量表达式AST节点
func (p *ParserImpl) parseLiteralExpression() ast.Expression {
	pos := p.getCurrentPosition()
	tokenType := p.currentToken.Type
	rawValue := p.currentToken.Value

	// 根据Token类型转换值
	var convertedValue interface{}
	switch tokenType {
	case lexer.INTEGER_LIT:
		// 整数字面量：将字符串转换为int64
		if val, err := parseIntegerLiteral(rawValue); err == nil {
			convertedValue = val
		} else {
			p.addErrorf("无效的整数字面量: %s", rawValue)
			convertedValue = rawValue // 保留原始值
		}

	case lexer.FLOAT_LIT:
		// 浮点数字面量：将字符串转换为float64
		if val, err := parseFloatLiteral(rawValue); err == nil {
			convertedValue = val
		} else {
			p.addErrorf("无效的浮点数字面量: %s", rawValue)
			convertedValue = rawValue // 保留原始值
		}

	case lexer.HEX_LIT:
		// 十六进制字面量：将字符串转换为int64
		if val, err := parseHexLiteral(rawValue); err == nil {
			convertedValue = val
		} else {
			p.addErrorf("无效的十六进制字面量: %s", rawValue)
			convertedValue = rawValue // 保留原始值
		}

	case lexer.BIT_LIT:
		// 二进制字面量：将字符串转换为int64
		if val, err := parseBinaryLiteral(rawValue); err == nil {
			convertedValue = val
		} else {
			p.addErrorf("无效的二进制字面量: %s", rawValue)
			convertedValue = rawValue // 保留原始值
		}

	case lexer.STRING_LIT:
		// 字符串字面量：保持为字符串类型
		convertedValue = rawValue

	default:
		// 其他类型保持原始值
		convertedValue = rawValue
	}

	// 创建字面量节点
	literal := &ast.Literal{
		Position: pos,
		Type:     tokenType,
		Value:    convertedValue,
	}
	p.incrementNodeCount()

	// 移动到下一个Token
	p.nextToken()

	return literal
}

// parseNullLiteral 解析NULL字面量
// 返回:
//
//	ast.Expression: NULL字面量表达式AST节点
func (p *ParserImpl) parseNullLiteral() ast.Expression {
	pos := p.getCurrentPosition()

	// 创建NULL字面量节点
	literal := &ast.Literal{
		Position: pos,
		Type:     lexer.NULL_LIT,
		Value:    nil,
	}
	p.incrementNodeCount()

	// 移动到下一个Token
	p.nextToken()

	return literal
}

// parseBooleanLiteral 解析布尔字面量
// 返回:
//
//	ast.Expression: 布尔字面量表达式AST节点
func (p *ParserImpl) parseBooleanLiteral() ast.Expression {
	pos := p.getCurrentPosition()
	tokenType := p.currentToken.Type

	var value bool
	if tokenType == lexer.TRUE_LIT {
		value = true
	} else {
		value = false
	}

	// 创建布尔字面量节点
	literal := &ast.Literal{
		Position: pos,
		Type:     tokenType,
		Value:    value,
	}
	p.incrementNodeCount()

	// 移动到下一个Token
	p.nextToken()

	return literal
}

// parseIdentifierExpression 解析标识符表达式（列引用或函数调用）
//
// 此方法处理以标识符开头的表达式，包括：
// 1. 简单列引用：column_name
// 2. 限定列引用：table_name.column_name 或 schema.table.column
// 3. 函数调用：function_name(args...)
// 4. 别名引用：alias.column_name
//
// 返回:
//
//	ast.Expression: 标识符表达式AST节点（ColumnReference或FunctionCall）
func (p *ParserImpl) parseIdentifierExpression() ast.Expression {
	pos := p.getCurrentPosition()
	identifier := p.currentToken.Value
	p.nextToken()

	// 检查是否为函数调用（标识符后跟左括号）
	if p.matchToken(lexer.LEFT_PAREN) {
		return p.parseFunctionCall(pos, identifier)
	}

	// 解析可能的限定名称（表名.列名 或 schema.table.column）
	var schemaName, tableName, columnName string

	if p.matchToken(lexer.DOT) {
		// 第一个点：可能是 table.column 或 schema.table
		p.nextToken() // 跳过点号

		if !p.matchToken(lexer.IDENTIFIER) {
			p.addError("期望标识符", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
			return nil
		}

		secondIdentifier := p.currentToken.Value
		p.nextToken()

		// 检查是否还有第二个点（schema.table.column格式）
		if p.matchToken(lexer.DOT) {
			p.nextToken() // 跳过第二个点号

			if !p.matchToken(lexer.IDENTIFIER) {
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
		Schema:   schemaName, // 添加schema支持
		Table:    tableName,
		Column:   columnName,
	}
	p.incrementNodeCount()

	return columnRef
}

// parseFunctionCall 解析函数调用表达式
// 参数:
//
//	pos: 函数名位置
//	functionName: 函数名
//
// 返回:
//
//	ast.Expression: 函数调用表达式AST节点
func (p *ParserImpl) parseFunctionCall(pos lexer.Position, functionName string) ast.Expression {
	// 期望左括号（已经检查过）
	if !p.expectToken(lexer.LEFT_PAREN) {
		return nil
	}

	// 创建函数调用节点
	funcCall := &ast.FunctionCall{
		Position: pos,
		Name:     functionName,
		Args:     make([]ast.Expression, 0),
		Distinct: false,
	}
	p.incrementNodeCount()

	// 检查DISTINCT关键字（用于聚合函数）
	if p.matchToken(lexer.DISTINCT) {
		funcCall.Distinct = true
		p.nextToken()
	}

	// 解析参数列表
	if !p.matchToken(lexer.RIGHT_PAREN) {
		// 解析第一个参数
		arg := p.ParseExpression()
		if arg != nil {
			funcCall.Args = append(funcCall.Args, arg)
		}

		// 解析其余参数
		for p.matchToken(lexer.COMMA) {
			p.nextToken() // 跳过逗号

			arg := p.ParseExpression()
			if arg != nil {
				funcCall.Args = append(funcCall.Args, arg)
			} else {
				break
			}
		}
	}

	// 期望右括号
	if !p.expectToken(lexer.RIGHT_PAREN) {
		return funcCall
	}

	return funcCall
}

// parseParenthesizedExpression 解析括号表达式
// 返回:
//
//	ast.Expression: 括号内的表达式AST节点
func (p *ParserImpl) parseParenthesizedExpression() ast.Expression {
	// 期望左括号
	if !p.expectToken(lexer.LEFT_PAREN) {
		return nil
	}

	// 解析括号内的表达式
	expr := p.ParseExpression()
	if expr == nil {
		return nil
	}

	// 期望右括号
	if !p.expectToken(lexer.RIGHT_PAREN) {
		return expr
	}

	return expr
}

// parseUnaryExpression 解析一元表达式
// 返回:
//
//	ast.Expression: 一元表达式AST节点
func (p *ParserImpl) parseUnaryExpression() ast.Expression {
	pos := p.getCurrentPosition()
	operator := p.currentToken.Type
	p.nextToken()

	// 解析操作数
	operand := p.parsePrimaryExpression()
	if operand == nil {
		p.addError("一元操作符后缺少操作数", nil, p.currentToken.Type)
		return nil
	}

	// 创建一元表达式节点
	unaryExpr := &ast.UnaryExpression{
		Position: pos,
		Operator: operator,
		Operand:  operand,
	}
	p.incrementNodeCount()

	return unaryExpr
}

// parseCaseExpression 解析CASE表达式
// 返回:
//
//	ast.Expression: CASE表达式AST节点
func (p *ParserImpl) parseCaseExpression() ast.Expression {
	pos := p.getCurrentPosition()

	// 期望CASE关键字
	if !p.expectToken(lexer.CASE) {
		return nil
	}

	// 创建CASE表达式节点
	caseExpr := &ast.CaseExpression{
		Position:    pos,
		WhenClauses: make([]*ast.WhenClause, 0),
	}
	p.incrementNodeCount()

	// 检查是否为简单CASE表达式（CASE expr WHEN ...）
	if !p.matchToken(lexer.WHEN) {
		// 解析CASE后的表达式
		expr := p.ParseExpression()
		if expr != nil {
			caseExpr.Expr = expr
		}
	}

	// 解析WHEN子句
	for p.matchToken(lexer.WHEN) {
		whenClause := p.parseWhenClause()
		if whenClause != nil {
			caseExpr.WhenClauses = append(caseExpr.WhenClauses, whenClause)
		} else {
			break
		}
	}

	// 解析ELSE子句（可选）
	if p.matchToken(lexer.ELSE) {
		p.nextToken() // 跳过ELSE关键字
		elseExpr := p.ParseExpression()
		if elseExpr != nil {
			caseExpr.ElseClause = elseExpr
		}
	}

	// 期望END关键字
	if !p.expectToken(lexer.END) {
		return caseExpr
	}

	return caseExpr
}

// parseWhenClause 解析WHEN子句
// 返回:
//
//	*ast.WhenClause: WHEN子句AST节点
func (p *ParserImpl) parseWhenClause() *ast.WhenClause {
	pos := p.getCurrentPosition()

	// 期望WHEN关键字
	if !p.expectToken(lexer.WHEN) {
		return nil
	}

	// 解析条件表达式
	condition := p.ParseExpression()
	if condition == nil {
		p.addError("WHEN子句缺少条件表达式", nil, p.currentToken.Type)
		return nil
	}

	// 期望THEN关键字
	if !p.expectToken(lexer.THEN) {
		return nil
	}

	// 解析结果表达式
	result := p.ParseExpression()
	if result == nil {
		p.addError("THEN子句缺少结果表达式", nil, p.currentToken.Type)
		return nil
	}

	// 创建WHEN子句节点
	whenClause := &ast.WhenClause{
		Position:  pos,
		Condition: condition,
		Result:    result,
	}

	return whenClause
}

// 字面量值转换辅助函数

// parseIntegerLiteral 解析整数字面量
// 参数:
//
//	value: 整数字符串
//
// 返回:
//
//	int64: 转换后的整数值
//	error: 转换错误
func parseIntegerLiteral(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

// parseFloatLiteral 解析浮点数字面量
// 参数:
//
//	value: 浮点数字符串
//
// 返回:
//
//	float64: 转换后的浮点数值
//	error: 转换错误
func parseFloatLiteral(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

// parseHexLiteral 解析十六进制字面量
// 参数:
//
//	value: 十六进制字符串（如"0xFF"）
//
// 返回:
//
//	int64: 转换后的整数值
//	error: 转换错误
func parseHexLiteral(value string) (int64, error) {
	// 移除"0x"或"0X"前缀
	hexStr := strings.TrimPrefix(strings.TrimPrefix(value, "0x"), "0X")
	return strconv.ParseInt(hexStr, 16, 64)
}

// parseBinaryLiteral 解析二进制字面量
// 参数:
//
//	value: 二进制字符串（如"0b1010"）
//
// 返回:
//
//	int64: 转换后的整数值
//	error: 转换错误
func parseBinaryLiteral(value string) (int64, error) {
	// 移除"0b"或"0B"前缀
	binStr := strings.TrimPrefix(strings.TrimPrefix(value, "0b"), "0B")
	return strconv.ParseInt(binStr, 2, 64)
}

// parseAggregateFunctionExpression 解析聚合函数表达式
// 处理COUNT、SUM、AVG、MAX、MIN等聚合函数
// 返回:
//
//	ast.Expression: 聚合函数表达式AST节点
func (p *ParserImpl) parseAggregateFunctionExpression() ast.Expression {
	pos := p.getCurrentPosition()
	functionName := p.currentToken.Value

	// 将关键字Token转换为函数名字符串
	switch p.currentToken.Type {
	case lexer.COUNT:
		functionName = "COUNT"
	case lexer.SUM:
		functionName = "SUM"
	case lexer.AVG:
		functionName = "AVG"
	case lexer.MAX:
		functionName = "MAX"
	case lexer.MIN:
		functionName = "MIN"
	}

	p.nextToken() // 跳过函数名

	// 期望左括号
	if !p.expectToken(lexer.LEFT_PAREN) {
		return nil
	}

	// 创建函数调用节点
	funcCall := &ast.FunctionCall{
		Position: pos,
		Name:     functionName,
		Args:     make([]ast.Expression, 0),
		Distinct: false,
	}
	p.incrementNodeCount()

	// 检查DISTINCT关键字（用于聚合函数）
	if p.matchToken(lexer.DISTINCT) {
		funcCall.Distinct = true
		p.nextToken()
	}

	// 解析参数列表
	if !p.matchToken(lexer.RIGHT_PAREN) {
		// 特殊处理COUNT(*)
		if p.matchToken(lexer.MULTIPLY) && functionName == "COUNT" {
			// 创建通配符表达式
			wildcardExpr := &ast.Wildcard{
				Position: p.getCurrentPosition(),
			}
			p.incrementNodeCount()
			funcCall.Args = append(funcCall.Args, wildcardExpr)
			p.nextToken() // 跳过*
		} else {
			// 解析第一个参数
			arg := p.ParseExpression()
			if arg != nil {
				funcCall.Args = append(funcCall.Args, arg)
			}

			// 解析其余参数
			for p.matchToken(lexer.COMMA) {
				p.nextToken() // 跳过逗号

				arg := p.ParseExpression()
				if arg != nil {
					funcCall.Args = append(funcCall.Args, arg)
				} else {
					break
				}
			}
		}
	}

	// 期望右括号
	if !p.expectToken(lexer.RIGHT_PAREN) {
		return funcCall
	}

	return funcCall
}
