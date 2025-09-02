# SQL解析器模块设计文档

## 概述

本文档详细设计了JadeDB数据库系统中SQL解析器模块的技术架构、组件结构和实现方案。SQL解析器负责将SQL文本转换为抽象语法树(AST)，是整个SQL处理流水线的第一个关键组件。

## 技术架构

### 整体架构

```
SQL文本输入
    ↓
┌─────────────────┐
│   预处理器       │ → 标准化SQL文本
│ (Preprocessor)  │
└─────────────────┘
    ↓
┌─────────────────┐
│   词法分析器     │ → Token流
│   (Lexer)       │
└─────────────────┘
    ↓
┌─────────────────┐
│   语法分析器     │ → AST
│   (Parser)      │
└─────────────────┘
    ↓
┌─────────────────┐
│   AST优化器     │ → 优化后的AST
│ (ASTOptimizer)  │
└─────────────────┘
    ↓
┌─────────────────┐
│   缓存管理器     │ → 缓存AST
│ (CacheManager)  │
└─────────────────┘
```

### 核心组件设计

#### 1. SQL解析器主接口

```go
// SQL解析器主接口
type SQLParser interface {
    Parse(sql string) (*AST, error)
    ParseWithOptions(sql string, opts *ParseOptions) (*AST, error)
    GetStatistics() *ParseStatistics
    ClearCache()
}

// 解析选项
type ParseOptions struct {
    EnableCache     bool
    StrictMode      bool
    MaxDepth        int
    Timeout         time.Duration
    Dialect         SQLDialect
}

// 解析统计信息
type ParseStatistics struct {
    TotalParses     int64
    CacheHits       int64
    CacheMisses     int64
    AvgParseTime    time.Duration
    ErrorCount      int64
}

// SQL方言
type SQLDialect int

const (
    StandardSQL SQLDialect = iota
    MySQL
    PostgreSQL
    SQLite
)
```

#### 2. 词法分析器设计

```go
// 词法分析器接口
type Lexer interface {
    Tokenize(input string) ([]Token, error)
    NextToken() Token
    Reset(input string)
    GetPosition() Position
}

// Token定义
type Token struct {
    Type     TokenType
    Value    string
    Position Position
    Raw      string // 原始文本，用于保留格式
}

// Token类型枚举
type TokenType int

const (
    // 字面量类型
    IDENTIFIER TokenType = iota
    INTEGER
    FLOAT
    STRING
    BOOLEAN
    NULL
    
    // SQL关键字
    SELECT
    FROM
    WHERE
    JOIN
    INNER
    LEFT
    RIGHT
    FULL
    OUTER
    ON
    GROUP
    BY
    HAVING
    ORDER
    ASC
    DESC
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
    ADD
    COLUMN
    CONSTRAINT
    PRIMARY
    KEY
    FOREIGN
    REFERENCES
    UNIQUE
    NOT
    NULL_KW
    DEFAULT
    
    // 数据类型关键字
    INT
    INTEGER_KW
    BIGINT
    SMALLINT
    TINYINT
    DECIMAL
    NUMERIC
    FLOAT_KW
    DOUBLE
    REAL
    VARCHAR
    CHAR
    TEXT
    BLOB
    DATE
    TIME
    TIMESTAMP
    DATETIME
    
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
    NOT_OP      // NOT
    IN          // IN
    LIKE        // LIKE
    IS          // IS
    BETWEEN     // BETWEEN
    EXISTS      // EXISTS
    
    // 分隔符和标点
    COMMA       // ,
    SEMICOLON   // ;
    LEFT_PAREN  // (
    RIGHT_PAREN // )
    DOT         // .
    ASTERISK    // * (用于SELECT *)
    
    // 特殊Token
    EOF
    ILLEGAL
    COMMENT
    WHITESPACE
)

// 位置信息
type Position struct {
    Line   int
    Column int
    Offset int
}

// 词法分析器实现
type LexerImpl struct {
    input        string
    position     int
    readPosition int
    ch           byte
    line         int
    column       int
    errors       []LexError
}

type LexError struct {
    Position Position
    Message  string
}

func NewLexer(input string) *LexerImpl {
    l := &LexerImpl{
        input:  input,
        line:   1,
        column: 0,
        errors: make([]LexError, 0),
    }
    l.readChar()
    return l
}

func (l *LexerImpl) Tokenize(input string) ([]Token, error) {
    l.Reset(input)
    tokens := make([]Token, 0)
    
    for {
        token := l.NextToken()
        if token.Type == EOF {
            tokens = append(tokens, token)
            break
        }
        if token.Type != WHITESPACE && token.Type != COMMENT {
            tokens = append(tokens, token)
        }
    }
    
    if len(l.errors) > 0 {
        return tokens, &LexErrors{Errors: l.errors}
    }
    
    return tokens, nil
}
```

#### 3. 语法分析器设计

```go
// 语法分析器接口
type Parser interface {
    Parse(tokens []Token) (*AST, error)
    ParseStatement() (Statement, error)
    ParseExpression() (Expression, error)
    GetErrors() []ParseError
}

// AST根节点
type AST struct {
    Statements []Statement
    Comments   []Comment
    Metadata   *ASTMetadata
}

type ASTMetadata struct {
    ParseTime    time.Time
    ParseDuration time.Duration
    TokenCount   int
    NodeCount    int
}

// AST节点基接口
type ASTNode interface {
    String() string
    Accept(visitor Visitor) interface{}
    GetPosition() Position
    GetChildren() []ASTNode
}

// 语句接口
type Statement interface {
    ASTNode
    statementNode()
}

// 表达式接口
type Expression interface {
    ASTNode
    expressionNode()
}

// 具体语句类型
type SelectStatement struct {
    Position      Position
    SelectList    []SelectItem
    FromClause    *FromClause
    WhereClause   Expression
    GroupByClause *GroupByClause
    HavingClause  Expression
    OrderByClause *OrderByClause
    LimitClause   *LimitClause
}

type SelectItem struct {
    Expression Expression
    Alias      string
    Position   Position
}

type FromClause struct {
    Position Position
    Tables   []TableReference
}

type TableReference struct {
    Position Position
    Table    Expression
    Alias    string
    Joins    []*JoinClause
}

type JoinClause struct {
    Position  Position
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
    Position Position
    Table    string
    Column   string
}

type Literal struct {
    Position Position
    Type     TokenType
    Value    interface{}
}

type BinaryExpression struct {
    Position Position
    Left     Expression
    Operator TokenType
    Right    Expression
}

type UnaryExpression struct {
    Position Position
    Operator TokenType
    Operand  Expression
}

type FunctionCall struct {
    Position Position
    Name     string
    Args     []Expression
    Distinct bool
}

type CaseExpression struct {
    Position Position
    Expr     Expression // CASE后的表达式，可选
    WhenClauses []*WhenClause
    ElseClause  Expression
}

type WhenClause struct {
    Position Position
    Condition Expression
    Result    Expression
}

// 递归下降解析器实现
type ParserImpl struct {
    tokens       []Token
    position     int
    currentToken Token
    peekToken    Token
    errors       []ParseError
    maxDepth     int
    currentDepth int
}

type ParseError struct {
    Position Position
    Message  string
    Expected []TokenType
    Actual   TokenType
}

func NewParser(tokens []Token) *ParserImpl {
    p := &ParserImpl{
        tokens:   tokens,
        position: 0,
        errors:   make([]ParseError, 0),
        maxDepth: 100, // 防止栈溢出
    }
    
    if len(tokens) > 0 {
        p.currentToken = tokens[0]
    }
    if len(tokens) > 1 {
        p.peekToken = tokens[1]
    }
    
    return p
}

func (p *ParserImpl) Parse(tokens []Token) (*AST, error) {
    p.tokens = tokens
    p.position = 0
    p.errors = make([]ParseError, 0)
    
    startTime := time.Now()
    statements := make([]Statement, 0)
    
    for p.currentToken.Type != EOF {
        if stmt := p.ParseStatement(); stmt != nil {
            statements = append(statements, stmt)
        }
        
        // 跳过分号
        if p.currentToken.Type == SEMICOLON {
            p.nextToken()
        }
    }
    
    ast := &AST{
        Statements: statements,
        Metadata: &ASTMetadata{
            ParseTime:     startTime,
            ParseDuration: time.Since(startTime),
            TokenCount:    len(tokens),
            NodeCount:     p.countNodes(statements),
        },
    }
    
    if len(p.errors) > 0 {
        return ast, &ParseErrors{Errors: p.errors}
    }
    
    return ast, nil
}
```

#### 4. 访问者模式设计

```go
// 访问者接口
type Visitor interface {
    VisitSelectStatement(stmt *SelectStatement) interface{}
    VisitInsertStatement(stmt *InsertStatement) interface{}
    VisitUpdateStatement(stmt *UpdateStatement) interface{}
    VisitDeleteStatement(stmt *DeleteStatement) interface{}
    VisitCreateTableStatement(stmt *CreateTableStatement) interface{}
    VisitDropTableStatement(stmt *DropTableStatement) interface{}
    
    VisitColumnReference(expr *ColumnReference) interface{}
    VisitLiteral(expr *Literal) interface{}
    VisitBinaryExpression(expr *BinaryExpression) interface{}
    VisitUnaryExpression(expr *UnaryExpression) interface{}
    VisitFunctionCall(expr *FunctionCall) interface{}
    VisitCaseExpression(expr *CaseExpression) interface{}
}

// 基础访问者实现
type BaseVisitor struct{}

func (v *BaseVisitor) VisitSelectStatement(stmt *SelectStatement) interface{} {
    // 访问所有子节点
    for _, item := range stmt.SelectList {
        if item.Expression != nil {
            item.Expression.Accept(v)
        }
    }
    
    if stmt.FromClause != nil {
        stmt.FromClause.Accept(v)
    }
    
    if stmt.WhereClause != nil {
        stmt.WhereClause.Accept(v)
    }
    
    // ... 访问其他子节点
    
    return nil
}

// AST打印访问者
type ASTPrinter struct {
    BaseVisitor
    indent int
    output strings.Builder
}

func (p *ASTPrinter) VisitSelectStatement(stmt *SelectStatement) interface{} {
    p.output.WriteString(p.getIndent() + "SELECT\n")
    p.indent++
    
    for i, item := range stmt.SelectList {
        if i > 0 {
            p.output.WriteString(p.getIndent() + ",\n")
        }
        item.Expression.Accept(p)
    }
    
    p.indent--
    
    if stmt.FromClause != nil {
        p.output.WriteString(p.getIndent() + "FROM\n")
        p.indent++
        stmt.FromClause.Accept(p)
        p.indent--
    }
    
    return nil
}

// 符号收集访问者
type SymbolCollector struct {
    BaseVisitor
    Tables  []string
    Columns []string
    Aliases map[string]string
}

func (c *SymbolCollector) VisitColumnReference(expr *ColumnReference) interface{} {
    if expr.Table != "" {
        c.Tables = append(c.Tables, expr.Table)
    }
    c.Columns = append(c.Columns, expr.Column)
    return nil
}
```

#### 5. 缓存管理器设计

```go
// 缓存管理器接口
type CacheManager interface {
    Get(key string) (*AST, bool)
    Set(key string, ast *AST, ttl time.Duration)
    Delete(key string)
    Clear()
    GetStats() *CacheStats
}

// 缓存统计信息
type CacheStats struct {
    Size     int
    Capacity int
    Hits     int64
    Misses   int64
    HitRate  float64
}

// LRU缓存实现
type LRUCache struct {
    capacity int
    cache    map[string]*CacheNode
    head     *CacheNode
    tail     *CacheNode
    mutex    sync.RWMutex
    hits     int64
    misses   int64
}

type CacheNode struct {
    key      string
    ast      *AST
    expireAt time.Time
    prev     *CacheNode
    next     *CacheNode
}

func NewLRUCache(capacity int) *LRUCache {
    cache := &LRUCache{
        capacity: capacity,
        cache:    make(map[string]*CacheNode),
    }
    
    // 创建哨兵节点
    cache.head = &CacheNode{}
    cache.tail = &CacheNode{}
    cache.head.next = cache.tail
    cache.tail.prev = cache.head
    
    return cache
}

func (c *LRUCache) Get(key string) (*AST, bool) {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    
    if node, exists := c.cache[key]; exists {
        // 检查是否过期
        if time.Now().After(node.expireAt) {
            delete(c.cache, key)
            c.removeNode(node)
            atomic.AddInt64(&c.misses, 1)
            return nil, false
        }
        
        // 移动到头部
        c.moveToHead(node)
        atomic.AddInt64(&c.hits, 1)
        return node.ast, true
    }
    
    atomic.AddInt64(&c.misses, 1)
    return nil, false
}

func (c *LRUCache) Set(key string, ast *AST, ttl time.Duration) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    if node, exists := c.cache[key]; exists {
        // 更新现有节点
        node.ast = ast
        node.expireAt = time.Now().Add(ttl)
        c.moveToHead(node)
    } else {
        // 创建新节点
        newNode := &CacheNode{
            key:      key,
            ast:      ast,
            expireAt: time.Now().Add(ttl),
        }
        
        c.cache[key] = newNode
        c.addToHead(newNode)
        
        // 检查容量
        if len(c.cache) > c.capacity {
            tail := c.removeTail()
            delete(c.cache, tail.key)
        }
    }
}
```

#### 6. 错误处理设计

```go
// 错误类型定义
type ParseErrors struct {
    Errors []ParseError
}

func (e *ParseErrors) Error() string {
    var builder strings.Builder
    builder.WriteString("Parse errors:\n")
    
    for _, err := range e.Errors {
        builder.WriteString(fmt.Sprintf("  Line %d, Column %d: %s\n", 
            err.Position.Line, err.Position.Column, err.Message))
    }
    
    return builder.String()
}

type LexErrors struct {
    Errors []LexError
}

func (e *LexErrors) Error() string {
    var builder strings.Builder
    builder.WriteString("Lexical errors:\n")
    
    for _, err := range e.Errors {
        builder.WriteString(fmt.Sprintf("  Line %d, Column %d: %s\n", 
            err.Position.Line, err.Position.Column, err.Message))
    }
    
    return builder.String()
}

// 错误恢复策略
type ErrorRecovery struct {
    syncTokens []TokenType
}

func NewErrorRecovery() *ErrorRecovery {
    return &ErrorRecovery{
        syncTokens: []TokenType{
            SEMICOLON, SELECT, INSERT, UPDATE, DELETE, CREATE, DROP,
        },
    }
}

func (er *ErrorRecovery) Recover(parser *ParserImpl) {
    // 跳过Token直到找到同步点
    for parser.currentToken.Type != EOF {
        for _, syncToken := range er.syncTokens {
            if parser.currentToken.Type == syncToken {
                return
            }
        }
        parser.nextToken()
    }
}
```

## 数据模型

### AST节点层次结构

```
ASTNode (接口)
├── Statement (接口)
│   ├── SelectStatement
│   ├── InsertStatement
│   ├── UpdateStatement
│   ├── DeleteStatement
│   ├── CreateTableStatement
│   ├── DropTableStatement
│   ├── CreateIndexStatement
│   └── DropIndexStatement
└── Expression (接口)
    ├── ColumnReference
    ├── Literal
    ├── BinaryExpression
    ├── UnaryExpression
    ├── FunctionCall
    ├── CaseExpression
    ├── SubqueryExpression
    └── ParameterExpression
```

### Token分类体系

```
Token
├── 关键字类 (Keywords)
│   ├── SQL语句关键字 (SELECT, FROM, WHERE...)
│   ├── 数据类型关键字 (INT, VARCHAR, DATE...)
│   └── 操作符关键字 (AND, OR, NOT...)
├── 字面量类 (Literals)
│   ├── 数值字面量 (INTEGER, FLOAT)
│   ├── 字符串字面量 (STRING)
│   ├── 布尔字面量 (BOOLEAN)
│   └── 空值字面量 (NULL)
├── 标识符类 (Identifiers)
│   ├── 普通标识符 (表名、列名)
│   └── 引用标识符 (带引号的标识符)
├── 操作符类 (Operators)
│   ├── 算术操作符 (+, -, *, /, %)
│   ├── 比较操作符 (=, !=, <, <=, >, >=)
│   └── 逻辑操作符 (AND, OR, NOT)
└── 分隔符类 (Delimiters)
    ├── 标点符号 (,, ;, .)
    └── 括号 ((, ))
```

## 组件接口

### 主要接口定义

```go
// SQL解析器工厂
type ParserFactory interface {
    CreateParser(dialect SQLDialect) SQLParser
    GetSupportedDialects() []SQLDialect
}

// SQL标准化器
type SQLNormalizer interface {
    Normalize(sql string) string
    RemoveComments(sql string) string
    FormatSQL(sql string) string
}

// AST验证器
type ASTValidator interface {
    Validate(ast *AST) []ValidationError
    ValidateStatement(stmt Statement) []ValidationError
    ValidateExpression(expr Expression) []ValidationError
}

type ValidationError struct {
    Position Position
    Message  string
    Severity Severity
}

type Severity int

const (
    Warning Severity = iota
    Error
    Fatal
)

// 性能监控器
type PerformanceMonitor interface {
    RecordParseTime(duration time.Duration)
    RecordCacheHit()
    RecordCacheMiss()
    GetMetrics() *PerformanceMetrics
}

type PerformanceMetrics struct {
    TotalParses      int64
    AvgParseTime     time.Duration
    MaxParseTime     time.Duration
    MinParseTime     time.Duration
    CacheHitRate     float64
    MemoryUsage      int64
}
```

## 实现计划

### 第一阶段：基础词法分析器
- [ ] 实现Token定义和基础词法分析
- [ ] 支持SQL关键字识别
- [ ] 实现字面量解析（数字、字符串、布尔值）
- [ ] 添加基础错误处理

### 第二阶段：核心语法分析器
- [ ] 实现递归下降解析器框架
- [ ] 支持SELECT语句解析
- [ ] 实现表达式解析（算术、逻辑、比较）
- [ ] 添加AST节点定义

### 第三阶段：完整SQL支持
- [ ] 支持INSERT、UPDATE、DELETE语句
- [ ] 实现DDL语句解析（CREATE、DROP、ALTER）
- [ ] 支持JOIN操作和子查询
- [ ] 添加函数调用和CASE表达式

### 第四阶段：高级特性
- [ ] 实现访问者模式
- [ ] 添加AST缓存机制
- [ ] 实现错误恢复策略
- [ ] 支持多种SQL方言

### 第五阶段：性能优化
- [ ] 优化解析性能
- [ ] 实现并发安全
- [ ] 添加性能监控
- [ ] 内存使用优化

## 测试策略

### 单元测试
- 词法分析器测试：覆盖所有Token类型
- 语法分析器测试：覆盖所有SQL语句类型
- AST节点测试：验证节点结构和访问者模式
- 缓存测试：验证缓存命中率和过期机制

### 集成测试
- 端到端解析测试：完整SQL语句解析
- 错误处理测试：各种语法错误场景
- 性能测试：大型SQL语句和高并发场景
- 兼容性测试：不同SQL方言支持

### 压力测试
- 大量并发解析请求
- 复杂嵌套查询解析
- 内存使用压力测试
- 长时间运行稳定性测试

这个设计为SQL解析器提供了完整的技术架构和实现路径，确保能够满足所有需求并具有良好的可扩展性和性能。