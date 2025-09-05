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
// Assumes the `FROM` token has already been consumed.
func (p *ParserImpl) parseFromClause() (*ast.FromClause, error) {
	// Parse the first table reference
	tableRef, err := p.parseTableReference()
	if err != nil {
		return nil, err
	}

	clause := &ast.FromClause{
		Position: p.getCurrentPosition(),
		Tables:   []ast.TableReference{*tableRef},
	}

	// Parse JOINs or additional table references
	for {
		if p.matchTokens(lexer.JOIN, lexer.INNER, lexer.LEFT, lexer.RIGHT, lexer.FULL, lexer.CROSS) {
			// Parse JOIN clause
			joinClause, err := p.parseJoinClause()
			if err != nil {
				return nil, err
			}
			// Add the joined table to the last table reference
			lastIdx := len(clause.Tables) - 1
			clause.Tables[lastIdx].Joins = append(clause.Tables[lastIdx].Joins, joinClause)
		} else if p.matchToken(lexer.COMMA) {
			// Parse additional table reference (comma-separated)
			p.nextToken() // consume comma
			tableRef, err := p.parseTableReference()
			if err != nil {
				return nil, err
			}
			clause.Tables = append(clause.Tables, *tableRef)
		} else {
			break
		}
	}

	return clause, nil
}

// parseTableReference parses a table reference with optional alias
func (p *ParserImpl) parseTableReference() (*ast.TableReference, error) {
	pos := p.getCurrentPosition()

	// Check for subquery (parenthesized SELECT)
	if p.matchToken(lexer.LEFT_PAREN) {
		// This would be a subquery - for now, return an error
		return nil, fmt.Errorf("subqueries in FROM clause not yet supported")
	}

	// Parse table name (possibly qualified: schema.table or db.schema.table)
	if !p.matchToken(lexer.IDENTIFIER) {
		return nil, fmt.Errorf("expected table name in FROM clause")
	}

	firstPart := p.currentToken.Value
	p.nextToken()

	var schemaName, tableName string

	// Check for qualified table name
	if p.matchToken(lexer.DOT) {
		p.nextToken() // consume dot
		if !p.matchToken(lexer.IDENTIFIER) {
			return nil, fmt.Errorf("expected table name after schema")
		}

		secondPart := p.currentToken.Value
		p.nextToken()

		// Check for three-part name (db.schema.table)
		if p.matchToken(lexer.DOT) {
			p.nextToken() // consume second dot
			if !p.matchToken(lexer.IDENTIFIER) {
				return nil, fmt.Errorf("expected table name after schema")
			}

			// Three parts: db.schema.table (we'll use schema.table for now)
			schemaName = secondPart
			tableName = p.currentToken.Value
			p.nextToken()
		} else {
			// Two parts: schema.table
			schemaName = firstPart
			tableName = secondPart
		}
	} else {
		// Single part: table
		tableName = firstPart
	}

	// Create table expression
	tableExpr := &ast.ColumnReference{
		Position: pos,
		Schema:   schemaName,
		Table:    "",
		Column:   tableName, // In FROM context, Column represents the table name
	}

	tableRef := &ast.TableReference{
		Position: pos,
		Table:    tableExpr,
		Alias:    "",
	}

	// Check for table alias
	if p.matchToken(lexer.AS) {
		p.nextToken() // consume AS
		if p.matchToken(lexer.IDENTIFIER) {
			tableRef.Alias = p.currentToken.Value
			p.nextToken()
		} else if p.canBeUsedAsIdentifier(p.currentToken.Type) {
			tableRef.Alias = p.currentToken.Value
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected alias name after AS")
		}
	} else if p.matchToken(lexer.IDENTIFIER) && !p.isNextClauseKeyword(p.currentToken.Type) {
		// Alias without AS keyword
		tableRef.Alias = p.currentToken.Value
		p.nextToken()
	} else if p.canBeUsedAsIdentifier(p.currentToken.Type) && !p.isNextClauseKeyword(p.currentToken.Type) {
		// Keyword as alias (without AS)
		tableRef.Alias = p.currentToken.Value
		p.nextToken()
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
	clause := &ast.GroupByClause{}

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
	clause := &ast.OrderByClause{}
	var items []ast.OrderByItem

	for {
		expr := p.ParseExpression()
		if expr == nil {
			return nil, fmt.Errorf("expected expression in ORDER BY clause")
		}

		item := &ast.OrderByItem{Expression: expr}

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

		items = append(items, *item)

		if !p.matchToken(lexer.COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	clause.Items = items
	return clause, nil
}

// parseLimitClause parses the LIMIT clause.
// Assumes `LIMIT` has been consumed.
func (p *ParserImpl) parseLimitClause() (*ast.LimitClause, error) {
	clause := &ast.LimitClause{}

	// Parse count
	countExpr := p.ParseExpression()
	if countExpr == nil {
		return nil, fmt.Errorf("expected expression for LIMIT count")
	}
	clause.Count = countExpr

	// Parse offset if present
	if p.matchToken(lexer.COMMA) || p.matchToken(lexer.OFFSET) {
		p.nextToken() // consume comma or OFFSET
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
