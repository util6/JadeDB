package ast

import (
	"fmt"
	"strings"

	"github.com/util6/JadeDB/sql/lexer"
)

// PrintVisitor AST打印访问者
type PrintVisitor struct {
	indentLevel int
	output      strings.Builder
}

// NewPrintVisitor 创建新的AST打印访问者
func NewPrintVisitor() *PrintVisitor {
	return &PrintVisitor{
		indentLevel: 0,
		output:      strings.Builder{},
	}
}

// GetOutput 获取打印输出
func (v *PrintVisitor) GetOutput() string {
	return v.output.String()
}

// indent 获取当前缩进字符串
func (v *PrintVisitor) indent() string {
	return strings.Repeat("  ", v.indentLevel)
}

// VisitSelectStatement 访问SELECT语句节点
func (v *PrintVisitor) VisitSelectStatement(stmt *SelectStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sSELECT STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++

	// 打印SELECT列表
	v.output.WriteString(fmt.Sprintf("%sSELECT LIST:\n", v.indent()))
	v.indentLevel++
	for _, item := range stmt.SelectList {
		v.output.WriteString(fmt.Sprintf("%s- %s (alias: %s)\n",
			v.indent(), item.Expression.String(), item.Alias))
	}
	v.indentLevel--

	// 打印FROM子句
	if stmt.FromClause != nil {
		v.output.WriteString(fmt.Sprintf("%sFROM CLAUSE:\n", v.indent()))
		v.indentLevel++
		for _, table := range stmt.FromClause.Tables {
			v.output.WriteString(fmt.Sprintf("%sTABLE: %s (alias: %s)\n",
				v.indent(), table.Table.String(), table.Alias))
		}
		v.indentLevel--
	}

	// 打印WHERE子句
	if stmt.WhereClause != nil {
		v.output.WriteString(fmt.Sprintf("%sWHERE CLAUSE: %s\n",
			v.indent(), stmt.WhereClause.String()))
	}

	// 打印GROUP BY子句
	if stmt.GroupByClause != nil {
		v.output.WriteString(fmt.Sprintf("%sGROUP BY CLAUSE:\n", v.indent()))
		v.indentLevel++
		for _, col := range stmt.GroupByClause.Columns {
			v.output.WriteString(fmt.Sprintf("%s- %s\n", v.indent(), col.String()))
		}
		v.indentLevel--
	}

	// 打印HAVING子句
	if stmt.HavingClause != nil {
		v.output.WriteString(fmt.Sprintf("%sHAVING CLAUSE: %s\n",
			v.indent(), stmt.HavingClause.String()))
	}

	// 打印ORDER BY子句
	if stmt.OrderByClause != nil {
		v.output.WriteString(fmt.Sprintf("%sORDER BY CLAUSE:\n", v.indent()))
		v.indentLevel++
		for _, item := range stmt.OrderByClause.Items {
			order := "ASC"
			if !item.Ascending {
				order = "DESC"
			}
			v.output.WriteString(fmt.Sprintf("%s- %s %s\n",
				v.indent(), item.Expression.String(), order))
		}
		v.indentLevel--
	}

	// 打印LIMIT子句
	if stmt.LimitClause != nil {
		v.output.WriteString(fmt.Sprintf("%sLIMIT CLAUSE:\n", v.indent()))
		v.indentLevel++
		if stmt.LimitClause.Offset != nil {
			v.output.WriteString(fmt.Sprintf("%sOFFSET: %s, COUNT: %s\n",
				v.indent(), stmt.LimitClause.Offset.String(), stmt.LimitClause.Count.String()))
		} else {
			v.output.WriteString(fmt.Sprintf("%sCOUNT: %s\n",
				v.indent(), stmt.LimitClause.Count.String()))
		}
		v.indentLevel--
	}

	v.indentLevel--
	return nil
}

// VisitInsertStatement 访问INSERT语句节点
func (v *PrintVisitor) VisitInsertStatement(stmt *InsertStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sINSERT STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sTABLE: %s\n", v.indent(), stmt.Table))

	if len(stmt.Columns) > 0 {
		v.output.WriteString(fmt.Sprintf("%sCOLUMNS: %s\n", v.indent(), strings.Join(stmt.Columns, ", ")))
	}

	v.output.WriteString(fmt.Sprintf("%sVALUES: %d rows\n", v.indent(), len(stmt.Values)))
	v.indentLevel++
	for i, row := range stmt.Values {
		v.output.WriteString(fmt.Sprintf("%sROW %d:\n", v.indent(), i+1))
		v.indentLevel++
		for j, value := range row {
			v.output.WriteString(fmt.Sprintf("%sVALUE %d: %s\n", v.indent(), j+1, value.String()))
		}
		v.indentLevel--
	}
	v.indentLevel--

	v.indentLevel--
	return nil
}

// VisitUpdateStatement 访问UPDATE语句节点
func (v *PrintVisitor) VisitUpdateStatement(stmt *UpdateStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sUPDATE STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sTABLE: %s\n", v.indent(), stmt.Table))

	v.output.WriteString(fmt.Sprintf("%sSET CLAUSES:\n", v.indent()))
	v.indentLevel++
	for _, setClause := range stmt.SetClauses {
		v.output.WriteString(fmt.Sprintf("%s- %s = %s\n",
			v.indent(), setClause.Column, setClause.Value.String()))
	}
	v.indentLevel--

	if stmt.WhereClause != nil {
		v.output.WriteString(fmt.Sprintf("%sWHERE CLAUSE: %s\n",
			v.indent(), stmt.WhereClause.String()))
	}

	v.indentLevel--
	return nil
}

// VisitDeleteStatement 访问DELETE语句节点
func (v *PrintVisitor) VisitDeleteStatement(stmt *DeleteStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sDELETE STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sTABLE: %s\n", v.indent(), stmt.Table))

	if stmt.WhereClause != nil {
		v.output.WriteString(fmt.Sprintf("%sWHERE CLAUSE: %s\n",
			v.indent(), stmt.WhereClause.String()))
	}

	v.indentLevel--
	return nil
}

// VisitCreateTableStatement 访问CREATE TABLE语句节点
func (v *PrintVisitor) VisitCreateTableStatement(stmt *CreateTableStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sCREATE TABLE STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sTABLE: %s\n", v.indent(), stmt.TableName))

	v.output.WriteString(fmt.Sprintf("%sCOLUMNS:\n", v.indent()))
	v.indentLevel++
	for _, col := range stmt.Columns {
		v.output.WriteString(fmt.Sprintf("%s- %s %s",
			v.indent(), col.Name, col.DataType.Name))

		if col.DataType.Length > 0 {
			v.output.WriteString(fmt.Sprintf("(%d)", col.DataType.Length))
		} else if col.DataType.Precision > 0 {
			v.output.WriteString(fmt.Sprintf("(%d,%d)", col.DataType.Precision, col.DataType.Scale))
		}

		if col.NotNull {
			v.output.WriteString(" NOT NULL")
		}

		if col.DefaultValue != nil {
			v.output.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue.String()))
		}

		v.output.WriteString("\n")
	}
	v.indentLevel--

	if len(stmt.Constraints) > 0 {
		v.output.WriteString(fmt.Sprintf("%sCONSTRAINTS:\n", v.indent()))
		v.indentLevel++
		for _, constraint := range stmt.Constraints {
			v.output.WriteString(fmt.Sprintf("%s- %v: %s [%s]\n",
				v.indent(), constraint.Type, constraint.Name, strings.Join(constraint.Columns, ", ")))
		}
		v.indentLevel--
	}

	v.indentLevel--
	return nil
}

// VisitDropTableStatement 访问DROP TABLE语句节点
func (v *PrintVisitor) VisitDropTableStatement(stmt *DropTableStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sDROP TABLE STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	if stmt.IfExists {
		v.output.WriteString(fmt.Sprintf("%sTABLE: %s (IF EXISTS)\n", v.indent(), stmt.TableName))
	} else {
		v.output.WriteString(fmt.Sprintf("%sTABLE: %s\n", v.indent(), stmt.TableName))
	}
	v.indentLevel--

	return nil
}

// VisitCreateIndexStatement 访问CREATE INDEX语句节点
func (v *PrintVisitor) VisitCreateIndexStatement(stmt *CreateIndexStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sCREATE INDEX STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	if stmt.Unique {
		v.output.WriteString(fmt.Sprintf("%sUNIQUE INDEX: %s\n", v.indent(), stmt.IndexName))
	} else {
		v.output.WriteString(fmt.Sprintf("%sINDEX: %s\n", v.indent(), stmt.IndexName))
	}

	v.output.WriteString(fmt.Sprintf("%sON TABLE: %s\n", v.indent(), stmt.TableName))
	v.output.WriteString(fmt.Sprintf("%sCOLUMNS: %s\n", v.indent(), strings.Join(stmt.Columns, ", ")))
	v.indentLevel--

	return nil
}

// VisitDropIndexStatement 访问DROP INDEX语句节点
func (v *PrintVisitor) VisitDropIndexStatement(stmt *DropIndexStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sDROP INDEX STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sINDEX: %s\n", v.indent(), stmt.IndexName))
	v.indentLevel--

	return nil
}

// VisitColumnReference 访问列引用表达式节点
func (v *PrintVisitor) VisitColumnReference(expr *ColumnReference) interface{} {
	var fullName string
	if expr.Schema != "" {
		fullName = expr.Schema + "." + expr.Table + "." + expr.Column
	} else if expr.Table != "" {
		fullName = expr.Table + "." + expr.Column
	} else {
		fullName = expr.Column
	}

	v.output.WriteString(fmt.Sprintf("%sCOLUMN REFERENCE: %s (pos: %d:%d)\n",
		v.indent(), fullName, expr.Position.Line, expr.Position.Column))
	return nil
}

// VisitLiteral 访问字面量表达式节点
func (v *PrintVisitor) VisitLiteral(expr *Literal) interface{} {
	v.output.WriteString(fmt.Sprintf("%sLITERAL: %v (type: %v) (pos: %d:%d)\n",
		v.indent(), expr.Value, expr.Type, expr.Position.Line, expr.Position.Column))
	return nil
}

// VisitBinaryExpression 访问二元表达式节点
func (v *PrintVisitor) VisitBinaryExpression(expr *BinaryExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sBINARY EXPRESSION: %s (pos: %d:%d)\n",
		v.indent(), lexer.TokenName(expr.Operator), expr.Position.Line, expr.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sLEFT:\n", v.indent()))
	v.indentLevel++
	expr.Left.Accept(v)
	v.indentLevel--

	v.output.WriteString(fmt.Sprintf("%sRIGHT:\n", v.indent()))
	v.indentLevel++
	expr.Right.Accept(v)
	v.indentLevel--
	v.indentLevel--

	return nil
}

// VisitUnaryExpression 访问一元表达式节点
func (v *PrintVisitor) VisitUnaryExpression(expr *UnaryExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sUNARY EXPRESSION: %s (pos: %d:%d)\n",
		v.indent(), lexer.TokenName(expr.Operator), expr.Position.Line, expr.Position.Column))

	v.indentLevel++
	expr.Operand.Accept(v)
	v.indentLevel--

	return nil
}

// VisitFunctionCall 访问函数调用表达式节点
func (v *PrintVisitor) VisitFunctionCall(expr *FunctionCall) interface{} {
	v.output.WriteString(fmt.Sprintf("%sFUNCTION CALL: %s (pos: %d:%d)\n",
		v.indent(), expr.Name, expr.Position.Line, expr.Position.Column))

	if expr.Distinct {
		v.output.WriteString(fmt.Sprintf("%sDISTINCT: true\n", v.indent()))
	}

	if len(expr.Args) > 0 {
		v.output.WriteString(fmt.Sprintf("%sARGUMENTS:\n", v.indent()))
		v.indentLevel++
		for i, arg := range expr.Args {
			v.output.WriteString(fmt.Sprintf("%sARG %d:\n", v.indent(), i+1))
			v.indentLevel++
			arg.Accept(v)
			v.indentLevel--
		}
		v.indentLevel--
	}

	return nil
}

// VisitCaseExpression 访问CASE表达式节点
func (v *PrintVisitor) VisitCaseExpression(expr *CaseExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sCASE EXPRESSION (pos: %d:%d)\n",
		v.indent(), expr.Position.Line, expr.Position.Column))

	v.indentLevel++

	if expr.Expr != nil {
		v.output.WriteString(fmt.Sprintf("%sCASE EXPRESSION:\n", v.indent()))
		v.indentLevel++
		expr.Expr.Accept(v)
		v.indentLevel--
	}

	v.output.WriteString(fmt.Sprintf("%sWHEN CLAUSES:\n", v.indent()))
	v.indentLevel++
	for i, when := range expr.WhenClauses {
		v.output.WriteString(fmt.Sprintf("%sWHEN %d:\n", v.indent(), i+1))
		v.indentLevel++

		v.output.WriteString(fmt.Sprintf("%sCONDITION:\n", v.indent()))
		v.indentLevel++
		when.Condition.Accept(v)
		v.indentLevel--

		v.output.WriteString(fmt.Sprintf("%sRESULT:\n", v.indent()))
		v.indentLevel++
		when.Result.Accept(v)
		v.indentLevel--

		v.indentLevel--
	}
	v.indentLevel--

	if expr.ElseClause != nil {
		v.output.WriteString(fmt.Sprintf("%sELSE CLAUSE:\n", v.indent()))
		v.indentLevel++
		expr.ElseClause.Accept(v)
		v.indentLevel--
	}

	v.indentLevel--
	return nil
}

// VisitWildcard 访问通配符表达式节点
func (v *PrintVisitor) VisitWildcard(expr *Wildcard) interface{} {
	v.output.WriteString(fmt.Sprintf("%sWILDCARD: * (pos: %d:%d)\n",
		v.indent(), expr.Position.Line, expr.Position.Column))
	return nil
}

// VisitSubqueryExpression 访问子查询表达式节点
func (v *PrintVisitor) VisitSubqueryExpression(expr *SubqueryExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sSUBQUERY EXPRESSION (pos: %d:%d)\n",
		v.indent(), expr.Position.Line, expr.Position.Column))

	v.indentLevel++
	expr.Query.Accept(v)
	v.indentLevel--

	return nil
}

// VisitBetweenExpression 访问BETWEEN表达式节点
func (v *PrintVisitor) VisitBetweenExpression(expr *BetweenExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sBETWEEN EXPRESSION (pos: %d:%d)\n",
		v.indent(), expr.Position.Line, expr.Position.Column))

	v.indentLevel++
	v.output.WriteString(fmt.Sprintf("%sEXPRESSION:\n", v.indent()))
	v.indentLevel++
	expr.Expr.Accept(v)
	v.indentLevel--

	v.output.WriteString(fmt.Sprintf("%sSTART:\n", v.indent()))
	v.indentLevel++
	expr.Start.Accept(v)
	v.indentLevel--

	v.output.WriteString(fmt.Sprintf("%sEND:\n", v.indent()))
	v.indentLevel++
	expr.End.Accept(v)
	v.indentLevel--
	v.indentLevel--

	return nil
}

// VisitValueListExpression 访问值列表表达式节点
func (v *PrintVisitor) VisitValueListExpression(expr *ValueListExpression) interface{} {
	v.output.WriteString(fmt.Sprintf("%sVALUE LIST EXPRESSION (pos: %d:%d)\n",
		v.indent(), expr.Position.Line, expr.Position.Column))

	v.indentLevel++
	for i, value := range expr.Values {
		v.output.WriteString(fmt.Sprintf("%sVALUE %d:\n", v.indent(), i+1))
		v.indentLevel++
		value.Accept(v)
		v.indentLevel--
	}
	v.indentLevel--

	return nil
}

// VisitBeginTransactionStatement 访问BEGIN TRANSACTION语句节点
func (v *PrintVisitor) VisitBeginTransactionStatement(stmt *BeginTransactionStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sBEGIN TRANSACTION STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))
	return nil
}

// VisitCommitTransactionStatement 访问COMMIT TRANSACTION语句节点
func (v *PrintVisitor) VisitCommitTransactionStatement(stmt *CommitTransactionStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sCOMMIT TRANSACTION STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))
	return nil
}

// VisitRollbackTransactionStatement 访问ROLLBACK TRANSACTION语句节点
func (v *PrintVisitor) VisitRollbackTransactionStatement(stmt *RollbackTransactionStatement) interface{} {
	v.output.WriteString(fmt.Sprintf("%sROLLBACK TRANSACTION STATEMENT (pos: %d:%d)\n",
		v.indent(), stmt.Position.Line, stmt.Position.Column))
	return nil
}

// VisitUnionStatement 访问UNION语句节点
func (v *PrintVisitor) VisitUnionStatement(stmt *UnionStatement) interface{} {
	unionTypeName := "UNION"
	switch stmt.UnionType {
	case INTERSECT_TYPE:
		unionTypeName = "INTERSECT"
	case EXCEPT_TYPE:
		unionTypeName = "EXCEPT"
	}

	if stmt.All {
		unionTypeName += " ALL"
	}

	v.output.WriteString(fmt.Sprintf("%s%s STATEMENT (pos: %d:%d)\n",
		v.indent(), unionTypeName, stmt.Position.Line, stmt.Position.Column))

	v.indentLevel++

	v.output.WriteString(fmt.Sprintf("%sLEFT QUERY:\n", v.indent()))
	v.indentLevel++
	stmt.Left.Accept(v)
	v.indentLevel--

	v.output.WriteString(fmt.Sprintf("%sRIGHT QUERY:\n", v.indent()))
	v.indentLevel++
	stmt.Right.Accept(v)
	v.indentLevel--

	v.indentLevel--

	return nil
}
