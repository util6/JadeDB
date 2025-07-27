# JadeDB SQL解析和查询引擎设计

## 文档概述

本文档详细设计了JadeDB分布式SQL系统的SQL解析器和查询引擎，包括词法分析、语法分析、AST构建、查询优化和执行计划生成等核心组件。

## 设计目标

### 核心目标
1. **标准SQL支持**：兼容SQL-92/99/2003标准，支持主流SQL语法
2. **分布式优化**：针对分布式环境的查询优化策略
3. **高性能**：高效的解析和优化算法，支持复杂查询
4. **可扩展性**：模块化设计，易于添加新的SQL特性
5. **错误处理**：友好的错误提示和恢复机制

### 设计原则
- **分层设计**：词法→语法→语义→优化→执行的清晰分层
- **可插拔架构**：支持不同的优化规则和执行策略
- **缓存优化**：解析结果和执行计划缓存
- **并行处理**：支持查询的并行解析和优化

---

## 系统架构

### 整体流程

```
SQL文本
    ↓
┌─────────────┐
│ 词法分析器   │ → Token流
│ (Lexer)     │
└─────────────┘
    ↓
┌─────────────┐
│ 语法分析器   │ → AST
│ (Parser)    │
└─────────────┘
    ↓
┌─────────────┐
│ 语义分析器   │ → 验证后的AST
│ (Analyzer)  │
└─────────────┘
    ↓
┌─────────────┐
│ 查询优化器   │ → 优化后的执行计划
│ (Optimizer) │
└─────────────┘
    ↓
┌─────────────┐
│ 执行引擎     │ → 查询结果
│ (Executor)  │
└─────────────┘
```

### 核心组件

```go
// SQL引擎主接口
type SQLEngine struct {
    lexer      *Lexer
    parser     *Parser
    analyzer   *SemanticAnalyzer
    optimizer  *QueryOptimizer
    executor   *QueryExecutor
    planCache  *PlanCache
    metaStore  *MetadataStore
}

// 查询处理主流程
func (e *SQLEngine) ExecuteQuery(sql string) (*ResultSet, error) {
    // 1. 词法分析
    tokens, err := e.lexer.Tokenize(sql)
    if err != nil {
        return nil, err
    }
    
    // 2. 语法分析
    ast, err := e.parser.Parse(tokens)
    if err != nil {
        return nil, err
    }
    
    // 3. 语义分析
    validatedAST, err := e.analyzer.Analyze(ast)
    if err != nil {
        return nil, err
    }
    
    // 4. 查询优化
    plan, err := e.optimizer.Optimize(validatedAST)
    if err != nil {
        return nil, err
    }
    
    // 5. 执行查询
    return e.executor.Execute(plan)
}
```

---

## 词法分析器设计

### Token定义

```go
// Token类型枚举
type TokenType int

const (
    // 字面量
    IDENTIFIER TokenType = iota
    INTEGER
    FLOAT
    STRING
    BOOLEAN
    NULL
    
    // 关键字
    SELECT
    FROM
    WHERE
    JOIN
    INNER
    LEFT
    RIGHT
    FULL
    ON
    GROUP
    BY
    HAVING
    ORDER
    LIMIT
    OFFSET
    INSERT
    INTO
    VALUES
    UPDATE
    SET
    DELETE
    CREATE
    TABLE
    INDEX
    DROP
    ALTER
    
    // 操作符
    PLUS        // +
    MINUS       // -
    MULTIPLY    // *
    DIVIDE      // /
    MODULO      // %
    EQUAL       // =
    NOT_EQUAL   // !=, <>
    LESS        // <
    LESS_EQUAL  // <=
    GREATER     // >
    GREATER_EQUAL // >=
    AND         // AND
    OR          // OR
    NOT         // NOT
    IN          // IN
    LIKE        // LIKE
    IS          // IS
    
    // 分隔符
    COMMA       // ,
    SEMICOLON   // ;
    LEFT_PAREN  // (
    RIGHT_PAREN // )
    DOT         // .
    
    // 特殊
    EOF
    ILLEGAL
)

// Token结构
type Token struct {
    Type     TokenType
    Value    string
    Position Position
}

type Position struct {
    Line   int
    Column int
    Offset int
}
```

### 词法分析器实现

```go
type Lexer struct {
    input        string
    position     int  // 当前位置
    readPosition int  // 下一个字符位置
    ch           byte // 当前字符
    line         int  // 当前行号
    column       int  // 当前列号
}

func NewLexer(input string) *Lexer {
    l := &Lexer{
        input:  input,
        line:   1,
        column: 0,
    }
    l.readChar()
    return l
}

func (l *Lexer) NextToken() Token {
    var tok Token
    
    l.skipWhitespace()
    
    switch l.ch {
    case '=':
        tok = l.newToken(EQUAL, l.ch)
    case '+':
        tok = l.newToken(PLUS, l.ch)
    case '-':
        tok = l.newToken(MINUS, l.ch)
    case '*':
        tok = l.newToken(MULTIPLY, l.ch)
    case '/':
        tok = l.newToken(DIVIDE, l.ch)
    case '<':
        if l.peekChar() == '=' {
            ch := l.ch
            l.readChar()
            tok = Token{Type: LESS_EQUAL, Value: string(ch) + string(l.ch)}
        } else if l.peekChar() == '>' {
            ch := l.ch
            l.readChar()
            tok = Token{Type: NOT_EQUAL, Value: string(ch) + string(l.ch)}
        } else {
            tok = l.newToken(LESS, l.ch)
        }
    case '>':
        if l.peekChar() == '=' {
            ch := l.ch
            l.readChar()
            tok = Token{Type: GREATER_EQUAL, Value: string(ch) + string(l.ch)}
        } else {
            tok = l.newToken(GREATER, l.ch)
        }
    case '(':
        tok = l.newToken(LEFT_PAREN, l.ch)
    case ')':
        tok = l.newToken(RIGHT_PAREN, l.ch)
    case ',':
        tok = l.newToken(COMMA, l.ch)
    case ';':
        tok = l.newToken(SEMICOLON, l.ch)
    case '.':
        tok = l.newToken(DOT, l.ch)
    case '\'', '"':
        tok.Type = STRING
        tok.Value = l.readString()
    case 0:
        tok.Type = EOF
        tok.Value = ""
    default:
        if isLetter(l.ch) {
            tok.Value = l.readIdentifier()
            tok.Type = lookupIdent(tok.Value)
            return tok
        } else if isDigit(l.ch) {
            tok.Type, tok.Value = l.readNumber()
            return tok
        } else {
            tok = l.newToken(ILLEGAL, l.ch)
        }
    }
    
    l.readChar()
    return tok
}

// 关键字查找表
var keywords = map[string]TokenType{
    "SELECT": SELECT,
    "FROM":   FROM,
    "WHERE":  WHERE,
    "JOIN":   JOIN,
    "INNER":  INNER,
    "LEFT":   LEFT,
    "RIGHT":  RIGHT,
    "FULL":   FULL,
    "ON":     ON,
    "GROUP":  GROUP,
    "BY":     BY,
    "HAVING": HAVING,
    "ORDER":  ORDER,
    "LIMIT":  LIMIT,
    "OFFSET": OFFSET,
    "INSERT": INSERT,
    "INTO":   INTO,
    "VALUES": VALUES,
    "UPDATE": UPDATE,
    "SET":    SET,
    "DELETE": DELETE,
    "CREATE": CREATE,
    "TABLE":  TABLE,
    "INDEX":  INDEX,
    "DROP":   DROP,
    "ALTER":  ALTER,
    "AND":    AND,
    "OR":     OR,
    "NOT":    NOT,
    "IN":     IN,
    "LIKE":   LIKE,
    "IS":     IS,
    "NULL":   NULL,
    "TRUE":   BOOLEAN,
    "FALSE":  BOOLEAN,
}

func lookupIdent(ident string) TokenType {
    if tok, ok := keywords[strings.ToUpper(ident)]; ok {
        return tok
    }
    return IDENTIFIER
}
```

---

## 语法分析器设计

### AST节点定义

```go
// AST节点基接口
type ASTNode interface {
    String() string
    Accept(visitor Visitor) interface{}
}

// 语句节点
type Statement interface {
    ASTNode
    statementNode()
}

// 表达式节点
type Expression interface {
    ASTNode
    expressionNode()
}

// SELECT语句
type SelectStatement struct {
    SelectList []Expression
    FromClause *FromClause
    WhereClause Expression
    GroupByClause []Expression
    HavingClause Expression
    OrderByClause []*OrderByItem
    LimitClause *LimitClause
}

// FROM子句
type FromClause struct {
    Tables []TableReference
}

type TableReference struct {
    Table Expression
    Alias string
    Joins []*JoinClause
}

type JoinClause struct {
    Type      JoinType
    Table     Expression
    Condition Expression
}

type JoinType int

const (
    INNER_JOIN JoinType = iota
    LEFT_JOIN
    RIGHT_JOIN
    FULL_JOIN
    CROSS_JOIN
)

// 表达式类型
type ColumnReference struct {
    Table  string
    Column string
}

type Literal struct {
    Type  TokenType
    Value interface{}
}

type BinaryExpression struct {
    Left     Expression
    Operator TokenType
    Right    Expression
}

type FunctionCall struct {
    Name string
    Args []Expression
}

// INSERT语句
type InsertStatement struct {
    Table   string
    Columns []string
    Values  [][]Expression
}

// UPDATE语句
type UpdateStatement struct {
    Table       string
    SetClauses  []*SetClause
    WhereClause Expression
}

type SetClause struct {
    Column string
    Value  Expression
}

// DELETE语句
type DeleteStatement struct {
    Table       string
    WhereClause Expression
}

// DDL语句
type CreateTableStatement struct {
    Table   string
    Columns []*ColumnDefinition
    Constraints []*Constraint
}

type ColumnDefinition struct {
    Name     string
    DataType DataType
    NotNull  bool
    Default  Expression
}

type DataType struct {
    Type   string
    Length int
    Scale  int
}
```

### 递归下降解析器

```go
type Parser struct {
    lexer        *Lexer
    currentToken Token
    peekToken    Token
    errors       []string
}

func NewParser(lexer *Lexer) *Parser {
    p := &Parser{
        lexer:  lexer,
        errors: []string{},
    }
    
    // 读取两个token，设置currentToken和peekToken
    p.nextToken()
    p.nextToken()
    
    return p
}

func (p *Parser) ParseStatement() Statement {
    switch p.currentToken.Type {
    case SELECT:
        return p.parseSelectStatement()
    case INSERT:
        return p.parseInsertStatement()
    case UPDATE:
        return p.parseUpdateStatement()
    case DELETE:
        return p.parseDeleteStatement()
    case CREATE:
        return p.parseCreateStatement()
    case DROP:
        return p.parseDropStatement()
    default:
        p.addError(fmt.Sprintf("unexpected token: %s", p.currentToken.Value))
        return nil
    }
}

func (p *Parser) parseSelectStatement() *SelectStatement {
    stmt := &SelectStatement{}
    
    if !p.expectToken(SELECT) {
        return nil
    }
    
    // 解析SELECT列表
    stmt.SelectList = p.parseSelectList()
    
    // 解析FROM子句
    if p.currentToken.Type == FROM {
        p.nextToken()
        stmt.FromClause = p.parseFromClause()
    }
    
    // 解析WHERE子句
    if p.currentToken.Type == WHERE {
        p.nextToken()
        stmt.WhereClause = p.parseExpression()
    }
    
    // 解析GROUP BY子句
    if p.currentToken.Type == GROUP {
        p.nextToken()
        if !p.expectToken(BY) {
            return nil
        }
        stmt.GroupByClause = p.parseExpressionList()
    }
    
    // 解析HAVING子句
    if p.currentToken.Type == HAVING {
        p.nextToken()
        stmt.HavingClause = p.parseExpression()
    }
    
    // 解析ORDER BY子句
    if p.currentToken.Type == ORDER {
        p.nextToken()
        if !p.expectToken(BY) {
            return nil
        }
        stmt.OrderByClause = p.parseOrderByList()
    }
    
    // 解析LIMIT子句
    if p.currentToken.Type == LIMIT {
        p.nextToken()
        stmt.LimitClause = p.parseLimitClause()
    }
    
    return stmt
}

func (p *Parser) parseExpression() Expression {
    return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() Expression {
    expr := p.parseAndExpression()
    
    for p.currentToken.Type == OR {
        operator := p.currentToken.Type
        p.nextToken()
        right := p.parseAndExpression()
        expr = &BinaryExpression{
            Left:     expr,
            Operator: operator,
            Right:    right,
        }
    }
    
    return expr
}

func (p *Parser) parseAndExpression() Expression {
    expr := p.parseEqualityExpression()
    
    for p.currentToken.Type == AND {
        operator := p.currentToken.Type
        p.nextToken()
        right := p.parseEqualityExpression()
        expr = &BinaryExpression{
            Left:     expr,
            Operator: operator,
            Right:    right,
        }
    }
    
    return expr
}

func (p *Parser) parseEqualityExpression() Expression {
    expr := p.parseComparisonExpression()
    
    for p.currentToken.Type == EQUAL || p.currentToken.Type == NOT_EQUAL {
        operator := p.currentToken.Type
        p.nextToken()
        right := p.parseComparisonExpression()
        expr = &BinaryExpression{
            Left:     expr,
            Operator: operator,
            Right:    right,
        }
    }
    
    return expr
}

func (p *Parser) parsePrimaryExpression() Expression {
    switch p.currentToken.Type {
    case IDENTIFIER:
        return p.parseColumnReference()
    case INTEGER, FLOAT, STRING, BOOLEAN, NULL:
        return p.parseLiteral()
    case LEFT_PAREN:
        p.nextToken()
        expr := p.parseExpression()
        if !p.expectToken(RIGHT_PAREN) {
            return nil
        }
        return expr
    default:
        p.addError(fmt.Sprintf("unexpected token in expression: %s", p.currentToken.Value))
        return nil
    }
}
```

---

## 语义分析器设计

### 符号表管理

```go
type SemanticAnalyzer struct {
    symbolTable *SymbolTable
    metaStore   *MetadataStore
    errors      []string
}

type SymbolTable struct {
    scopes []*Scope
}

type Scope struct {
    symbols map[string]*Symbol
    parent  *Scope
}

type Symbol struct {
    Name     string
    Type     SymbolType
    DataType DataType
    Table    string
}

type SymbolType int

const (
    COLUMN_SYMBOL SymbolType = iota
    TABLE_SYMBOL
    ALIAS_SYMBOL
    FUNCTION_SYMBOL
)

func (sa *SemanticAnalyzer) Analyze(ast ASTNode) (ASTNode, error) {
    sa.symbolTable = NewSymbolTable()
    sa.errors = []string{}
    
    // 访问AST节点进行语义分析
    result := ast.Accept(sa)
    
    if len(sa.errors) > 0 {
        return nil, fmt.Errorf("semantic errors: %v", sa.errors)
    }
    
    return result.(ASTNode), nil
}

func (sa *SemanticAnalyzer) VisitSelectStatement(stmt *SelectStatement) interface{} {
    // 创建新的作用域
    sa.symbolTable.PushScope()
    defer sa.symbolTable.PopScope()
    
    // 分析FROM子句，建立表符号
    if stmt.FromClause != nil {
        stmt.FromClause.Accept(sa)
    }
    
    // 分析SELECT列表
    for _, expr := range stmt.SelectList {
        expr.Accept(sa)
    }
    
    // 分析WHERE子句
    if stmt.WhereClause != nil {
        stmt.WhereClause.Accept(sa)
    }
    
    // 分析GROUP BY子句
    for _, expr := range stmt.GroupByClause {
        expr.Accept(sa)
    }
    
    // 分析HAVING子句
    if stmt.HavingClause != nil {
        stmt.HavingClause.Accept(sa)
    }
    
    // 分析ORDER BY子句
    for _, item := range stmt.OrderByClause {
        item.Expression.Accept(sa)
    }
    
    return stmt
}

func (sa *SemanticAnalyzer) VisitColumnReference(ref *ColumnReference) interface{} {
    // 查找列符号
    symbol := sa.symbolTable.Lookup(ref.Column)
    if symbol == nil {
        sa.addError(fmt.Sprintf("column '%s' not found", ref.Column))
        return ref
    }
    
    // 验证表名（如果指定）
    if ref.Table != "" && ref.Table != symbol.Table {
        sa.addError(fmt.Sprintf("column '%s' does not belong to table '%s'", ref.Column, ref.Table))
    }
    
    return ref
}
```

---

## 查询优化器设计

### 优化器架构

```go
type QueryOptimizer struct {
    ruleOptimizer *RuleBasedOptimizer
    costOptimizer *CostBasedOptimizer
    statistics    *Statistics
}

type OptimizationRule interface {
    Apply(plan LogicalPlan) LogicalPlan
    Name() string
}

// 基于规则的优化器
type RuleBasedOptimizer struct {
    rules []OptimizationRule
}

func (rbo *RuleBasedOptimizer) Optimize(plan LogicalPlan) LogicalPlan {
    for _, rule := range rbo.rules {
        plan = rule.Apply(plan)
    }
    return plan
}

// 基于成本的优化器
type CostBasedOptimizer struct {
    statistics *Statistics
    costModel  *CostModel
}

func (cbo *CostBasedOptimizer) Optimize(plan LogicalPlan) PhysicalPlan {
    // 生成所有可能的物理计划
    plans := cbo.generatePhysicalPlans(plan)
    
    // 计算每个计划的成本
    bestPlan := plans[0]
    bestCost := cbo.costModel.EstimateCost(bestPlan)
    
    for _, plan := range plans[1:] {
        cost := cbo.costModel.EstimateCost(plan)
        if cost < bestCost {
            bestPlan = plan
            bestCost = cost
        }
    }
    
    return bestPlan
}
```

### 常见优化规则

```go
// 谓词下推优化
type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Apply(plan LogicalPlan) LogicalPlan {
    switch p := plan.(type) {
    case *LogicalJoin:
        // 将WHERE条件下推到JOIN之前
        return r.pushdownJoinPredicates(p)
    case *LogicalFilter:
        // 将过滤条件下推到数据源
        return r.pushdownFilterPredicates(p)
    default:
        return plan
    }
}

// 投影下推优化
type ProjectionPushdownRule struct{}

func (r *ProjectionPushdownRule) Apply(plan LogicalPlan) LogicalPlan {
    // 只选择需要的列，减少数据传输
    return r.eliminateUnusedColumns(plan)
}

// JOIN重排序优化
type JoinReorderRule struct {
    statistics *Statistics
}

func (r *JoinReorderRule) Apply(plan LogicalPlan) LogicalPlan {
    // 基于表大小和选择性重排序JOIN
    return r.reorderJoins(plan)
}

// 常量折叠优化
type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Apply(plan LogicalPlan) LogicalPlan {
    // 计算常量表达式
    return r.foldConstants(plan)
}
```

### 成本模型

```go
type CostModel struct {
    cpuCost    float64
    ioCost     float64
    networkCost float64
}

type Cost struct {
    CPU     float64
    IO      float64
    Network float64
    Total   float64
}

func (cm *CostModel) EstimateCost(plan PhysicalPlan) Cost {
    switch p := plan.(type) {
    case *TableScan:
        return cm.estimateTableScanCost(p)
    case *IndexScan:
        return cm.estimateIndexScanCost(p)
    case *NestedLoopJoin:
        return cm.estimateNestedLoopJoinCost(p)
    case *HashJoin:
        return cm.estimateHashJoinCost(p)
    case *SortMergeJoin:
        return cm.estimateSortMergeJoinCost(p)
    default:
        return Cost{}
    }
}

func (cm *CostModel) estimateTableScanCost(scan *TableScan) Cost {
    stats := scan.Table.Statistics
    
    // IO成本：读取所有页面
    ioCost := float64(stats.PageCount) * cm.ioCost
    
    // CPU成本：处理所有行
    cpuCost := float64(stats.RowCount) * cm.cpuCost
    
    return Cost{
        IO:    ioCost,
        CPU:   cpuCost,
        Total: ioCost + cpuCost,
    }
}
```

这个设计文档为JadeDB的SQL解析和查询引擎提供了完整的架构基础。下一步我将继续设计分布式事务管理系统。
