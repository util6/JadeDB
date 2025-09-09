package parser

import (
	"fmt"
	"strings"

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
	} else if p.matchToken(lexer.INDEX) || p.matchToken(lexer.UNIQUE) {
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
// This function now parses column definitions with constraints like NOT NULL and DEFAULT.
func (p *ParserImpl) parseTableDefinition() ([]ast.ColumnDefinition, []ast.Constraint, error) {
	var columns []ast.ColumnDefinition
	var constraints []ast.Constraint

	for !p.matchToken(lexer.RIGHT_PAREN) && !p.isAtEnd() {
		// Check for table-level constraints
		if p.matchTokens(lexer.CONSTRAINT, lexer.PRIMARY, lexer.UNIQUE, lexer.FOREIGN, lexer.CHECK) {
			constraint, err := p.parseTableConstraint()
			if err != nil {
				p.addErrorf("failed to parse table constraint: %v", err)
			} else if constraint != nil {
				constraints = append(constraints, *constraint)
			}
		} else if p.matchToken(lexer.IDENTIFIER) {
			// Parse column definition
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

			// 解析列约束
			for {
				if p.matchToken(lexer.NOT) {
					p.nextToken() // consume NOT
					if p.expectToken(lexer.NULL_LIT) {
						column.NotNull = true
					}
				} else if p.matchToken(lexer.DEFAULT) {
					p.nextToken() // consume DEFAULT
					defaultValue := p.ParseExpression()
					if defaultValue != nil {
						column.DefaultValue = defaultValue
					} else {
						p.addError("DEFAULT后缺少表达式", nil, p.currentToken.Type)
					}
				} else {
					break
				}
			}

			columns = append(columns, column)
		} else {
			return nil, nil, fmt.Errorf("expected column name or constraint")
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

func (p *ParserImpl) parseTableConstraint() (*ast.Constraint, error) {
	constraint := &ast.Constraint{}

	if p.matchToken(lexer.CONSTRAINT) {
		p.nextToken() // consume CONSTRAINT
		if p.matchToken(lexer.IDENTIFIER) {
			constraint.Name = p.currentToken.Value
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected constraint name after CONSTRAINT")
		}
	}

	if p.matchToken(lexer.PRIMARY) {
		p.nextToken() // consume PRIMARY
		if p.expectToken(lexer.KEY) {
			constraint.Type = ast.PrimaryKey
			columns, err := p.parseColumnList()
			if err != nil {
				return nil, fmt.Errorf("failed to parse column list for PRIMARY KEY: %v", err)
			}
			constraint.Columns = columns
		} else {
			return nil, fmt.Errorf("expected KEY after PRIMARY")
		}
	} else if p.matchToken(lexer.UNIQUE) {
		p.nextToken() // consume UNIQUE
		constraint.Type = ast.Unique
		columns, err := p.parseColumnList()
		if err != nil {
			return nil, fmt.Errorf("failed to parse column list for UNIQUE constraint: %v", err)
		}
		constraint.Columns = columns
	} else {
		// Add other constraints like FOREIGN KEY, CHECK here
		return nil, fmt.Errorf("unsupported constraint type")
	}

	return constraint, nil
}

// parseCreateIndexStatement 解析CREATE INDEX语句
func (p *ParserImpl) parseCreateIndexStatement(startPos lexer.Position) ast.Statement {
	unique := false
	if p.matchToken(lexer.UNIQUE) {
		p.nextToken() // consume UNIQUE
		unique = true
	}

	if !p.expectToken(lexer.INDEX) {
		return nil
	}

	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("expected index name", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}
	indexName := p.currentToken.Value
	p.nextToken()

	if !p.expectToken(lexer.ON) {
		return nil
	}

	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("expected table name", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}
	tableName := p.currentToken.Value
	p.nextToken()

	columns, err := p.parseColumnList()
	if err != nil {
		p.addErrorf("failed to parse column list for index: %v", err)
		return nil
	}

	return &ast.CreateIndexStatement{
		Position:  startPos,
		IndexName: indexName,
		TableName: tableName,
		Columns:   columns,
		Unique:    unique,
	}
}

// parseDropIndexStatement 解析DROP INDEX语句
func (p *ParserImpl) parseDropIndexStatement(startPos lexer.Position) ast.Statement {
	if !p.expectToken(lexer.INDEX) {
		return nil
	}

	if !p.matchToken(lexer.IDENTIFIER) {
		p.addError("expected index name", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
		return nil
	}
	indexName := p.currentToken.Value
	p.nextToken()

	return &ast.DropIndexStatement{
		Position:  startPos,
		IndexName: indexName,
	}
}

// parseBeginTransactionStatement 解析BEGIN TRANSACTION语句
// 支持以下格式：
// - BEGIN
// - BEGIN TRANSACTION
// - BEGIN WORK
func (p *ParserImpl) parseBeginTransactionStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望BEGIN关键字
	if !p.expectToken(lexer.BEGIN) {
		return nil
	}

	// 创建BEGIN TRANSACTION语句节点
	beginStmt := &ast.BeginTransactionStatement{
		Position: startPos,
	}
	p.incrementNodeCount()

	// 可选的TRANSACTION或WORK关键字
	if p.matchToken(lexer.IDENTIFIER) {
		tokenValue := strings.ToUpper(p.currentToken.Value)
		if tokenValue == "TRANSACTION" || tokenValue == "WORK" {
			p.nextToken() // 跳过TRANSACTION/WORK关键字
		}
	}

	return beginStmt
}

// parseCommitTransactionStatement 解析COMMIT TRANSACTION语句
// 支持以下格式：
// - COMMIT
// - COMMIT TRANSACTION
// - COMMIT WORK
func (p *ParserImpl) parseCommitTransactionStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望COMMIT关键字
	if !p.expectToken(lexer.COMMIT) {
		return nil
	}

	// 创建COMMIT TRANSACTION语句节点
	commitStmt := &ast.CommitTransactionStatement{
		Position: startPos,
	}
	p.incrementNodeCount()

	// 可选的TRANSACTION或WORK关键字
	if p.matchToken(lexer.IDENTIFIER) {
		tokenValue := strings.ToUpper(p.currentToken.Value)
		if tokenValue == "TRANSACTION" || tokenValue == "WORK" {
			p.nextToken() // 跳过TRANSACTION/WORK关键字
		}
	}

	return commitStmt
}

// parseRollbackTransactionStatement 解析ROLLBACK TRANSACTION语句
// 支持以下格式：
// - ROLLBACK
// - ROLLBACK TRANSACTION
// - ROLLBACK WORK
func (p *ParserImpl) parseRollbackTransactionStatement() ast.Statement {
	startPos := p.getCurrentPosition()

	// 期望ROLLBACK关键字
	if !p.expectToken(lexer.ROLLBACK) {
		return nil
	}

	// 创建ROLLBACK TRANSACTION语句节点
	rollbackStmt := &ast.RollbackTransactionStatement{
		Position: startPos,
	}
	p.incrementNodeCount()

	// 可选的TRANSACTION或WORK关键字
	if p.matchToken(lexer.IDENTIFIER) {
		tokenValue := strings.ToUpper(p.currentToken.Value)
		if tokenValue == "TRANSACTION" || tokenValue == "WORK" {
			p.nextToken() // 跳过TRANSACTION/WORK关键字
		}
	}

	return rollbackStmt
}

// parseUnionStatement 解析集合操作语句 (UNION, INTERSECT, EXCEPT)
// 这个方法处理两个SELECT语句之间的集合操作
func (p *ParserImpl) parseUnionStatement(leftSelect *ast.SelectStatement) ast.Statement {
	startPos := p.getCurrentPosition()

	// 确定集合操作类型
	var unionType ast.UnionType
	var all bool = false

	switch p.currentToken.Type {
	case lexer.UNION:
		unionType = ast.UNION_TYPE
		p.nextToken() // 跳过UNION

		// 检查是否为UNION ALL
		if p.matchToken(lexer.ALL) {
			all = true
			p.nextToken() // 跳过ALL
		}

	case lexer.INTERSECT:
		unionType = ast.INTERSECT_TYPE
		p.nextToken() // 跳过INTERSECT

		// 检查是否为INTERSECT ALL
		if p.matchToken(lexer.ALL) {
			all = true
			p.nextToken() // 跳过ALL
		}

	case lexer.EXCEPT:
		unionType = ast.EXCEPT_TYPE
		p.nextToken() // 跳过EXCEPT

		// 检查是否为EXCEPT ALL
		if p.matchToken(lexer.ALL) {
			all = true
			p.nextToken() // 跳过ALL
		}

	default:
		p.addErrorf("期望集合操作关键字 (UNION, INTERSECT, EXCEPT)，但遇到 %s",
			lexer.TokenName(p.currentToken.Type))
		return nil
	}

	// 解析右侧的SELECT语句
	if !p.matchToken(lexer.SELECT) {
		p.addError("集合操作后期望SELECT语句", []lexer.TokenType{lexer.SELECT}, p.currentToken.Type)
		return nil
	}

	rightSelect := p.parseSelectStatement()
	if rightSelect == nil {
		return nil
	}

	// 创建UNION语句节点
	unionStmt := &ast.UnionStatement{
		Position:  startPos,
		Left:      leftSelect,
		Right:     rightSelect.(*ast.SelectStatement),
		UnionType: unionType,
		All:       all,
	}
	p.incrementNodeCount()

	// 注意：暂时不支持复杂的链式集合操作
	// 如果需要链式操作，可以使用括号来明确优先级

	return unionStmt
}

// parseSelectStatementWithUnion 解析可能包含集合操作的SELECT语句
// 这是对原有parseSelectStatement的扩展，支持集合操作
func (p *ParserImpl) parseSelectStatementWithUnion() ast.Statement {
	// 首先解析基本的SELECT语句
	selectStmt := p.parseSelectStatement()
	if selectStmt == nil {
		return nil
	}

	// 检查是否有集合操作
	if p.matchTokens(lexer.UNION, lexer.INTERSECT, lexer.EXCEPT) {
		return p.parseUnionStatement(selectStmt.(*ast.SelectStatement))
	}

	return selectStmt
}
