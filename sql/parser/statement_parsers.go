package parser

import (
	"fmt"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// parseSelectStatement 解析SELECT语句
// 实现SELECT语句的完整解析逻辑
// 返回:
//
//	ast.Statement: SELECT语句AST节点
func (p *ParserImpl) parseSelectStatement() ast.Statement {
	// 记录开始位置
	startPos := p.getCurrentPosition()

	// 期望SELECT关键字
	if !p.expectToken(lexer.SELECT) {
		return nil
	}

	// 创建SELECT语句节点
	selectStmt := &ast.SelectStatement{
		Position: startPos,
	}
	p.incrementNodeCount()

	// 解析SELECT列表
	selectList, err := p.parseSelectList()
	if err != nil {
		p.addErrorf("解析SELECT列表失败: %v", err)
		return selectStmt
	}
	selectStmt.SelectList = selectList

	// 解析FROM子句（可选）
	if p.matchToken(lexer.FROM) {
		p.nextToken() // 跳过FROM关键字
		fromClause, err := p.parseFromClause()
		if err != nil {
			p.addErrorf("解析FROM子句失败: %v", err)
		} else {
			selectStmt.FromClause = fromClause
		}
	}

	// 解析WHERE子句（可选）
	if p.matchToken(lexer.WHERE) {
		p.nextToken() // 跳过WHERE关键字
		whereExpr := p.ParseExpression()
		if whereExpr != nil {
			selectStmt.WhereClause = whereExpr
		}
	}

	// 解析GROUP BY子句（可选）
	if p.matchToken(lexer.GROUP) {
		p.nextToken() // 跳过GROUP关键字
		if !p.expectToken(lexer.BY) {
			p.addError("GROUP后期望BY关键字", []lexer.TokenType{lexer.BY}, p.currentToken.Type)
		} else {
			groupByClause, err := p.parseGroupByClause()
			if err != nil {
				p.addErrorf("解析GROUP BY子句失败: %v", err)
			} else {
				selectStmt.GroupByClause = groupByClause
			}
		}
	}

	// 解析HAVING子句（可选）
	if p.matchToken(lexer.HAVING) {
		p.nextToken() // 跳过HAVING关键字
		havingExpr := p.ParseExpression()
		if havingExpr != nil {
			selectStmt.HavingClause = havingExpr
		}
	}

	// 解析ORDER BY子句（可选）
	if p.matchToken(lexer.ORDER) {
		p.nextToken() // 跳过ORDER关键字
		if !p.expectToken(lexer.BY) {
			p.addError("ORDER后期望BY关键字", []lexer.TokenType{lexer.BY}, p.currentToken.Type)
		} else {
			orderByClause, err := p.parseOrderByClause()
			if err != nil {
				p.addErrorf("解析ORDER BY子句失败: %v", err)
			} else {
				selectStmt.OrderByClause = orderByClause
			}
		}
	}

	// 解析LIMIT子句（可选）
	if p.matchToken(lexer.LIMIT) {
		p.nextToken() // 跳过LIMIT关键字
		limitClause, err := p.parseLimitClause()
		if err != nil {
			p.addErrorf("解析LIMIT子句失败: %v", err)
		} else {
			selectStmt.LimitClause = limitClause
		}
	}

	return selectStmt
}

// parseInsertStatement 解析INSERT语句
// 返回:
//
//	ast.Statement: INSERT语句AST节点
func (p *ParserImpl) parseInsertStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望INSERT关键字
	if !p.expectToken(lexer.INSERT) {
		return nil
	}

	// 期望INTO关键字
	if !p.expectToken(lexer.INTO) {
		return nil
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建INSERT语句节点
	insertStmt := &ast.InsertStatement{
		Position: startPos,
		Table:    tableName,
	}
	p.incrementNodeCount()

	// 解析列名列表（可选）
	if p.matchToken(lexer.LEFT_PAREN) {
		columns, err := p.parseColumnList()
		if err != nil {
			p.addErrorf("解析列名列表失败: %v", err)
		} else {
			insertStmt.Columns = columns
		}
	}

	// 期望VALUES关键字
	if !p.expectToken(lexer.VALUES) {
		return insertStmt
	}

	// 解析VALUES列表
	values, err := p.parseValuesClause()
	if err != nil {
		p.addErrorf("解析VALUES列表失败: %v", err)
	} else {
		insertStmt.Values = values
	}

	return insertStmt
}

// parseUpdateStatement 解析UPDATE语句
// 返回:
//
//	ast.Statement: UPDATE语句AST节点
func (p *ParserImpl) parseUpdateStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望UPDATE关键字
	if !p.expectToken(lexer.UPDATE) {
		return nil
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建UPDATE语句节点
	updateStmt := &ast.UpdateStatement{
		Position: startPos,
		Table:    tableName,
	}
	p.incrementNodeCount()

	// 期望SET关键字
	if !p.expectToken(lexer.SET) {
		return updateStmt
	}

	// 解析SET子句列表
	setClauses, err := p.parseSetClause()
	if err != nil {
		p.addErrorf("解析SET子句失败: %v", err)
	} else {
		updateStmt.SetClauses = setClauses
	}

	// 解析WHERE子句（可选）
	if p.matchToken(lexer.WHERE) {
		p.nextToken() // 跳过WHERE关键字
		whereExpr := p.ParseExpression()
		if whereExpr != nil {
			updateStmt.WhereClause = whereExpr
		}
	}

	return updateStmt
}

// parseDeleteStatement 解析DELETE语句
// 返回:
//
//	ast.Statement: DELETE语句AST节点
func (p *ParserImpl) parseDeleteStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望DELETE关键字
	if !p.expectToken(lexer.DELETE) {
		return nil
	}

	// 期望FROM关键字
	if !p.expectToken(lexer.FROM) {
		return nil
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建DELETE语句节点
	deleteStmt := &ast.DeleteStatement{
		Position: startPos,
		Table:    tableName,
	}
	p.incrementNodeCount()

	// 解析WHERE子句（可选）
	if p.matchToken(lexer.WHERE) {
		p.nextToken() // 跳过WHERE关键字
		whereExpr := p.ParseExpression()
		if whereExpr != nil {
			deleteStmt.WhereClause = whereExpr
		}
	}

	return deleteStmt
}

// parseCreateStatement 解析CREATE语句
// 返回:
//
//	ast.Statement: CREATE语句AST节点
func (p *ParserImpl) parseCreateStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望CREATE关键字
	if !p.expectToken(lexer.CREATE) {
		return nil
	}

	// 检查CREATE的类型
	if p.matchToken(lexer.TABLE) {
		return p.parseCreateTableStatement(startPos)
	} else if p.matchToken(lexer.INDEX) {
		return p.parseCreateIndexStatement(startPos)
	} else {
		p.addError("期望TABLE或INDEX", []lexer.TokenType{lexer.TABLE, lexer.INDEX}, p.currentToken.Type)
		return nil
	}
}

// parseDropStatement 解析DROP语句
// 返回:
//
//	ast.Statement: DROP语句AST节点
func (p *ParserImpl) parseDropStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望DROP关键字
	if !p.expectToken(lexer.DROP) {
		return nil
	}

	// 检查DROP的类型
	if p.matchToken(lexer.TABLE) {
		return p.parseDropTableStatement(startPos)
	} else if p.matchToken(lexer.INDEX) {
		return p.parseDropIndexStatement(startPos)
	} else {
		p.addError("期望TABLE或INDEX", []lexer.TokenType{lexer.TABLE, lexer.INDEX}, p.currentToken.Type)
		return nil
	}
}

// parseAlterStatement 解析ALTER语句
// 返回:
//
//	ast.Statement: ALTER语句AST节点
func (p *ParserImpl) parseAlterStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望ALTER关键字
	if !p.expectToken(lexer.ALTER) {
		return nil
	}

	// 目前只支持ALTER TABLE
	if !p.expectToken(lexer.TABLE) {
		return nil
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建基础的ALTER TABLE语句节点（具体实现可以后续扩展）
	// 这里返回一个简单的实现
	_ = startPos
	_ = tableName

	// 暂时返回nil，表示未完全实现
	p.addError("ALTER语句暂未完全实现", nil, p.currentToken.Type)
	return nil
}

// parseCreateTableStatement 解析CREATE TABLE语句
// 参数:
//
//	startPos: 语句开始位置
//
// 返回:
//
//	ast.Statement: CREATE TABLE语句AST节点
func (p *ParserImpl) parseCreateTableStatement(startPos lexer.Position) ast.Statement {
	// 期望TABLE关键字
	if !p.expectToken(lexer.TABLE) {
		return nil
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建CREATE TABLE语句节点
	createStmt := &ast.CreateTableStatement{
		Position:  startPos,
		TableName: tableName,
	}
	p.incrementNodeCount()

	// 期望左括号
	if !p.expectToken(lexer.LEFT_PAREN) {
		return createStmt
	}

	// 解析列定义列表
	columns, constraints, err := p.parseTableDefinition()
	if err != nil {
		p.addErrorf("解析表定义失败: %v", err)
	} else {
		createStmt.Columns = columns
		createStmt.Constraints = constraints
	}

	// 期望右括号
	if !p.expectToken(lexer.RIGHT_PAREN) {
		return createStmt
	}

	return createStmt
}

// parseDropTableStatement 解析DROP TABLE语句
// 参数:
//
//	startPos: 语句开始位置
//
// 返回:
//
//	ast.Statement: DROP TABLE语句AST节点
func (p *ParserImpl) parseDropTableStatement(startPos lexer.Position) ast.Statement {
	// 期望TABLE关键字
	if !p.expectToken(lexer.TABLE) {
		return nil
	}

	// 检查IF EXISTS（可选）
	ifExists := false
	if p.matchToken(lexer.IF) {
		p.nextToken()
		if p.expectToken(lexer.EXISTS) {
			ifExists = true
		}
	}

	// 期望表名
	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("期望表名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}

	tableName := p.currentToken.Value
	p.nextToken()

	// 创建DROP TABLE语句节点
	dropStmt := &ast.DropTableStatement{
		Position:  startPos,
		TableName: tableName,
		IfExists:  ifExists,
	}
	p.incrementNodeCount()

	return dropStmt
}

// parseColumnList 解析列名列表
// 期望格式: (col1, col2, col3)
func (p *ParserImpl) parseColumnList() ([]string, error) {
	if !p.expectToken(lexer.LEFT_PAREN) {
		return nil, fmt.Errorf("expected '(' for column list")
	}

	var columns []string

	// Parse first column
	if !p.matchToken(lexer.IDENTIFIER) {
		return nil, fmt.Errorf("expected column name")
	}
	columns = append(columns, p.currentToken.Value)
	p.nextToken()

	// Parse additional columns separated by commas
	for p.matchToken(lexer.COMMA) {
		p.nextToken() // consume comma
		if !p.matchToken(lexer.IDENTIFIER) {
			return nil, fmt.Errorf("expected column name after comma")
		}
		columns = append(columns, p.currentToken.Value)
		p.nextToken()
	}

	if !p.expectToken(lexer.RIGHT_PAREN) {
		return nil, fmt.Errorf("expected ')' after column list")
	}

	return columns, nil
}

// parseTableDefinition 解析表定义
// 解析CREATE TABLE语句中的列定义和约束
func (p *ParserImpl) parseTableDefinition() ([]ast.ColumnDefinition, []ast.Constraint, error) {
	var columns []ast.ColumnDefinition
	var constraints []ast.Constraint

	for !p.matchToken(lexer.RIGHT_PAREN) && !p.isAtEnd() {
		// 简化实现：只解析基本的列定义
		if !p.matchToken(lexer.IDENTIFIER) {
			return nil, nil, fmt.Errorf("expected column name or constraint")
		}

		colName := p.currentToken.Value
		p.nextToken()

		// 期望数据类型
		if !p.matchToken(lexer.IDENTIFIER) {
			return nil, nil, fmt.Errorf("expected data type for column %s", colName)
		}

		dataType := p.currentToken.Value
		p.nextToken()

		// 创建列定义
		column := ast.ColumnDefinition{
			Name: colName,
			DataType: ast.DataType{
				Name: dataType,
			},
		}
		columns = append(columns, column)

		// 跳过可选的列约束（简化实现）
		for p.matchTokens(lexer.NOT, lexer.NULL_LIT, lexer.PRIMARY, lexer.KEY, lexer.UNIQUE, lexer.DEFAULT) {
			p.nextToken()
		}

		// 如果有逗号，继续解析下一列
		if p.matchToken(lexer.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return columns, constraints, nil
}

// parseCreateIndexStatement 解析CREATE INDEX语句（基础实现）
// 参数:
//
//	startPos: 语句开始位置
//
// 返回:
//
//	ast.Statement: CREATE INDEX语句AST节点
func (p *ParserImpl) parseCreateIndexStatement(startPos lexer.Position) ast.Statement {
	// 暂时返回nil，表示未实现
	p.addError("CREATE INDEX语句暂未实现", nil, p.currentToken.Type)
	return nil
}

// parseDropIndexStatement 解析DROP INDEX语句（基础实现）
// 参数:
//
//	startPos: 语句开始位置
//
// 返回:
//
//	ast.Statement: DROP INDEX语句AST节点
func (p *ParserImpl) parseDropIndexStatement(startPos lexer.Position) ast.Statement {
	// 暂时返回nil，表示未实现
	p.addError("DROP INDEX语句暂未实现", nil, p.currentToken.Type)
	return nil
}
