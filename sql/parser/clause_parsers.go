package parser

import (
	"fmt"

	"github.com/util6/JadeDB/sql/ast"
	"github.com/util6/JadeDB/sql/lexer"
)

// parseSelectList parses the SELECT list.
// Returns the list of SelectItem with expressions and aliases.
func (p *ParserImpl) parseSelectList() ([]ast.SelectItem, error) {
	var selectItems []ast.SelectItem

	// Parse first select item
	item, err := p.parseSelectItem()
	if err != nil {
		return nil, err
	}
	selectItems = append(selectItems, item)

	// Parse additional select items separated by commas
	for p.matchToken(lexer.COMMA) {
		p.nextToken() // consume comma
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		selectItems = append(selectItems, item)
	}

	return selectItems, nil
}

// parseSelectItem parses a single SELECT item (expression with optional alias)
func (p *ParserImpl) parseSelectItem() (ast.SelectItem, error) {
	// Parse the expression
	expr := p.ParseExpression()
	if expr == nil {
		return ast.SelectItem{}, fmt.Errorf("expected expression in SELECT list")
	}

	item := ast.SelectItem{
		Expression: expr,
		Position:   expr.GetPosition(),
	}

	// Check for alias
	if p.matchToken(lexer.AS) {
		p.nextToken() // consume AS
		if p.matchToken(lexer.IDENTIFIER) {
			item.Alias = p.currentToken.Value
			p.nextToken()
		} else if p.canBeUsedAsIdentifier(p.currentToken.Type) {
			// 允许关键字作为别名
			item.Alias = p.currentToken.Value
			p.nextToken()
		} else {
			return item, fmt.Errorf("expected alias name after AS")
		}
	} else if p.matchToken(lexer.IDENTIFIER) {
		// Alias without AS keyword - 但要确保不是下一个子句的关键字
		if !p.isNextClauseKeyword(p.currentToken.Type) {
			item.Alias = p.currentToken.Value
			p.nextToken()
		}
	} else if p.canBeUsedAsIdentifier(p.currentToken.Type) && !p.isNextClauseKeyword(p.currentToken.Type) {
		// 关键字作为别名（无AS）
		item.Alias = p.currentToken.Value
		p.nextToken()
	}

	return item, nil
}

// isNextClauseKeyword 检查Token是否为下一个子句的关键字
func (p *ParserImpl) isNextClauseKeyword(tokenType lexer.TokenType) bool {
	switch tokenType {
	case lexer.FROM, lexer.WHERE, lexer.GROUP, lexer.HAVING, lexer.ORDER, lexer.LIMIT:
		return true
	case lexer.UNION: // 集合操作
		return true
	default:
		return false
	}
}

// parseFromClause parses the FROM clause.
// Assumes `FROM` keyword has been consumed.
func (p *ParserImpl) parseFromClause() (*ast.FromClause, error) {
	pos := p.getCurrentPosition()
	clause := &ast.FromClause{
		Position: pos,
		Tables:   make([]ast.TableReference, 0),
	}
	p.incrementNodeCount()

	// Parse first table reference
	tableRef, err := p.parseTableReference()
	if err != nil {
		return nil, err
	}
	clause.Tables = append(clause.Tables, *tableRef)

	// Parse additional table references separated by commas
	for p.matchToken(lexer.COMMA) {
		p.nextToken() // consume comma
		tableRef, err := p.parseTableReference()
		if err != nil {
			return nil, err
		}
		clause.Tables = append(clause.Tables, *tableRef)
	}

	return clause, nil
}

// parseTableReference parses a table reference which can be:
// 1. A simple table name
// 2. A subquery with an alias
func (p *ParserImpl) parseTableReference() (*ast.TableReference, error) {
	pos := p.getCurrentPosition()
	tableRef := &ast.TableReference{
		Position: pos,
		Joins:    make([]*ast.JoinClause, 0),
	}
	p.incrementNodeCount()

	// Check if it's a subquery (parenthesized SELECT statement)
	if p.matchToken(lexer.LEFT_PAREN) {
		nextToken := p.peekTokenN(1)
		if nextToken.Type == lexer.SELECT {
			// It's a subquery
			subquery := p.parseSubqueryExpression()
			if subquery == nil {
				return nil, fmt.Errorf("failed to parse subquery in FROM clause")
			}
			tableRef.Table = subquery

			// Expect an alias for subquery
			if !p.matchToken(lexer.IDENTIFIER) {
				p.addError("子查询必须有别名", []lexer.TokenType{lexer.IDENTIFIER}, p.currentToken.Type)
				return tableRef, nil
			}

			tableRef.Alias = p.currentToken.Value
			p.nextToken() // consume alias
		} else {
			// Regular parenthesized expression, fallback to expression parsing
			expr := p.parseParenthesizedExpression()
			if expr == nil {
				return nil, fmt.Errorf("failed to parse parenthesized expression in FROM clause")
			}
			// This is not a valid table reference
			return nil, fmt.Errorf("invalid table reference")
		}
	} else if p.matchToken(lexer.IDENTIFIER) {
		// Simple table name
		tableName := p.currentToken.Value
		p.nextToken()

		// Create a column reference to represent the table name
		tableExpr := &ast.ColumnReference{
			Position: pos,
			Column:   tableName,
		}
		p.incrementNodeCount()

		tableRef.Table = tableExpr

		// Check for optional alias
		if p.matchToken(lexer.IDENTIFIER) {
			tableRef.Alias = p.currentToken.Value
			p.nextToken()
		}
	} else {
		return nil, fmt.Errorf("expected table name or subquery in FROM clause")
	}

	// Parse JOIN clauses if present
	for p.matchTokens(lexer.INNER, lexer.LEFT, lexer.RIGHT, lexer.FULL, lexer.CROSS, lexer.JOIN) {
		joinClause, err := p.parseJoinClause()
		if err != nil {
			return nil, fmt.Errorf("failed to parse JOIN clause: %v", err)
		}
		tableRef.Joins = append(tableRef.Joins, joinClause)
	}

	return tableRef, nil
}

// parseWhereClause parses the WHERE clause.
// Assumes the `WHERE` token has already been consumed.
func (p *ParserImpl) parseWhereClause() (ast.Expression, error) {
	expr := p.ParseExpression()
	if expr == nil {
		return nil, fmt.Errorf("expected expression in WHERE clause")
	}
	return expr, nil
}

// parseGroupByClause parses the GROUP BY clause.
// Assumes `GROUP BY` has been consumed.
func (p *ParserImpl) parseGroupByClause() (*ast.GroupByClause, error) {
	pos := p.getCurrentPosition()
	clause := &ast.GroupByClause{
		Position: pos,
		Columns:  make([]ast.Expression, 0),
	}
	p.incrementNodeCount()

	// Parse first expression
	expr := p.ParseExpression()
	if expr == nil {
		return nil, fmt.Errorf("expected expression in GROUP BY clause")
	}
	clause.Columns = append(clause.Columns, expr)

	// Parse additional expressions separated by commas
	for p.matchToken(lexer.COMMA) {
		p.nextToken() // consume comma
		expr := p.ParseExpression()
		if expr == nil {
			return nil, fmt.Errorf("expected expression after comma in GROUP BY clause")
		}
		clause.Columns = append(clause.Columns, expr)
	}

	return clause, nil
}

// parseOrderByClause parses the ORDER BY clause.
// Assumes `ORDER BY` has been consumed.
func (p *ParserImpl) parseOrderByClause() (*ast.OrderByClause, error) {
	pos := p.getCurrentPosition()
	clause := &ast.OrderByClause{
		Position: pos,
		Items:    make([]ast.OrderByItem, 0),
	}
	p.incrementNodeCount()

	for {
		expr := p.ParseExpression()
		if expr == nil {
			return nil, fmt.Errorf("expected expression in ORDER BY clause")
		}

		item := ast.OrderByItem{Expression: expr}

		// Check for ASC/DESC
		if p.matchToken(lexer.ASC) {
			item.Ascending = true
			p.nextToken()
		} else if p.matchToken(lexer.DESC) {
			item.Ascending = false
			p.nextToken()
		} else {
			item.Ascending = true // default
		}

		clause.Items = append(clause.Items, item)

		// Continue if there's a comma
		if !p.matchToken(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return clause, nil
}

// parseLimitClause parses the LIMIT clause.
// Assumes `LIMIT` has been consumed.
func (p *ParserImpl) parseLimitClause() (*ast.LimitClause, error) {
	pos := p.getCurrentPosition()
	clause := &ast.LimitClause{
		Position: pos,
	}
	p.incrementNodeCount()

	// Parse count
	countExpr := p.ParseExpression()
	if countExpr == nil {
		return nil, fmt.Errorf("expected expression for LIMIT count")
	}
	clause.Count = countExpr

	// Parse offset if present
	if p.matchToken(lexer.COMMA) {
		// Format: LIMIT offset, count
		clause.Offset = clause.Count // What we thought was count is actually offset
		p.nextToken()                // consume comma
		offsetExpr := p.ParseExpression()
		if offsetExpr == nil {
			return nil, fmt.Errorf("expected expression for LIMIT count")
		}
		clause.Count = offsetExpr
	} else if p.matchToken(lexer.OFFSET) {
		// Format: LIMIT count OFFSET offset
		p.nextToken() // consume OFFSET
		offsetExpr := p.ParseExpression()
		if offsetExpr == nil {
			return nil, fmt.Errorf("expected expression for LIMIT offset")
		}
		clause.Offset = offsetExpr
	}

	return clause, nil
}

// parseJoinClause parses a JOIN clause.
// Assumes we're positioned at a JOIN-related keyword.
func (p *ParserImpl) parseJoinClause() (*ast.JoinClause, error) {
	pos := p.getCurrentPosition()

	clause := &ast.JoinClause{
		Position: pos,
		Type:     ast.INNER_JOIN, // default
	}

	// Determine join type
	if p.matchToken(lexer.INNER) {
		clause.Type = ast.INNER_JOIN
		p.nextToken()
		if !p.expectToken(lexer.JOIN) {
			return nil, fmt.Errorf("expected JOIN after INNER")
		}
	} else if p.matchToken(lexer.LEFT) {
		clause.Type = ast.LEFT_JOIN
		p.nextToken()
		// Optional OUTER keyword
		if p.matchToken(lexer.OUTER) {
			p.nextToken()
		}
		if !p.expectToken(lexer.JOIN) {
			return nil, fmt.Errorf("expected JOIN after LEFT")
		}
	} else if p.matchToken(lexer.RIGHT) {
		clause.Type = ast.RIGHT_JOIN
		p.nextToken()
		// Optional OUTER keyword
		if p.matchToken(lexer.OUTER) {
			p.nextToken()
		}
		if !p.expectToken(lexer.JOIN) {
			return nil, fmt.Errorf("expected JOIN after RIGHT")
		}
	} else if p.matchToken(lexer.FULL) {
		clause.Type = ast.FULL_JOIN
		p.nextToken()
		// Optional OUTER keyword
		if p.matchToken(lexer.OUTER) {
			p.nextToken()
		}
		if !p.expectToken(lexer.JOIN) {
			return nil, fmt.Errorf("expected JOIN after FULL")
		}
	} else if p.matchToken(lexer.CROSS) {
		clause.Type = ast.CROSS_JOIN
		p.nextToken()
		if !p.expectToken(lexer.JOIN) {
			return nil, fmt.Errorf("expected JOIN after CROSS")
		}
	} else if p.matchToken(lexer.JOIN) {
		// Simple JOIN (defaults to INNER JOIN)
		clause.Type = ast.INNER_JOIN
		p.nextToken()
	} else {
		return nil, fmt.Errorf("expected JOIN keyword")
	}

	// Parse the right table reference
	rightTableRef, err := p.parseTableReference()
	if err != nil {
		return nil, fmt.Errorf("failed to parse table in JOIN clause: %v", err)
	}
	clause.Table = rightTableRef.Table

	// Parse the join condition (ON clause) - not required for CROSS JOIN
	if clause.Type != ast.CROSS_JOIN {
		if !p.expectToken(lexer.ON) {
			return nil, fmt.Errorf("expected ON clause for JOIN")
		}

		condition := p.ParseExpression()
		if condition == nil {
			return nil, fmt.Errorf("expected expression in JOIN ON clause")
		}
		clause.Condition = condition
	}

	return clause, nil
}

// parseValuesClause parses the VALUES clause for an INSERT statement.
// Assumes `VALUES` has been consumed.
func (p *ParserImpl) parseValuesClause() ([][]ast.Expression, error) {
	var values [][]ast.Expression

	for {
		if !p.matchToken(lexer.LEFT_PAREN) {
			return nil, fmt.Errorf("expected '(' for values clause")
		}
		p.nextToken() // consume '('

		var row []ast.Expression

		// Parse first expression
		expr := p.ParseExpression()
		if expr == nil {
			return nil, fmt.Errorf("expected expression in VALUES clause")
		}
		row = append(row, expr)

		// Parse additional expressions separated by commas
		for p.matchToken(lexer.COMMA) {
			p.nextToken() // consume comma
			expr := p.ParseExpression()
			if expr == nil {
				return nil, fmt.Errorf("expected expression after comma in VALUES clause")
			}
			row = append(row, expr)
		}

		values = append(values, row)

		if !p.matchToken(lexer.RIGHT_PAREN) {
			return nil, fmt.Errorf("expected ')' after values")
		}
		p.nextToken() // consume ')'

		if !p.matchToken(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma for next row
	}

	return values, nil
}

// parseSetClause parses the SET clause for an UPDATE statement.
// Assumes `SET` has been consumed.
func (p *ParserImpl) parseSetClause() ([]ast.SetClause, error) {
	var clauses []ast.SetClause

	for {
		if !p.matchToken(lexer.IDENTIFIER) {
			return nil, fmt.Errorf("expected column name in SET clause")
		}
		col := p.currentToken.Value
		p.nextToken()

		if !p.matchToken(lexer.EQUAL) {
			return nil, fmt.Errorf("expected '=' after column name in SET clause")
		}
		p.nextToken()

		expr := p.ParseExpression()
		if expr == nil {
			return nil, fmt.Errorf("expected expression in SET clause")
		}

		clauses = append(clauses, ast.SetClause{Column: col, Value: expr})

		if !p.matchToken(lexer.COMMA) {
			break
		}
		p.nextToken()
	}
	return clauses, nil
}
