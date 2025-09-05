# JadeDB SQL解析器模块

这是JadeDB数据库系统的SQL解析器模块，负责将SQL文本转换为抽象语法树(AST)。

## 模块结构

```
sql/
├── README.md           # 模块说明文档
├── parser/             # 语法分析器
│   └── interfaces.go   # 解析器接口定义
├── lexer/              # 词法分析器
│   └── interfaces.go   # 词法分析器接口定义
├── ast/                # 抽象语法树
│   ├── interfaces.go   # AST接口定义
│   └── nodes.go        # AST节点实现
└── errors/             # 错误处理
    └── errors.go       # 错误类型定义
```

## 核心组件

### 1. 词法分析器 (Lexer)
- 将SQL文本分解为Token流
- 支持所有SQL关键字、操作符、字面量识别
- 提供详细的错误位置信息

### 2. 语法分析器 (Parser)
- 将Token流解析为AST
- 支持完整的SQL语法
- 实现错误恢复机制

### 3. 抽象语法树 (AST)
- 表示SQL语句的树形结构
- 支持访问者模式遍历
- 包含完整的位置信息

### 4. 错误处理 (Errors)
- 提供详细的错误信息
- 支持多错误收集
- 实现错误恢复策略

## 设计特性

- **高性能**: 基于递归下降解析器，解析速度快
- **可扩展**: 模块化设计，易于添加新的SQL特性
- **错误友好**: 提供详细的错误信息和位置
- **线程安全**: 支持并发解析
- **缓存优化**: 支持AST缓存，提高重复查询性能

## 使用示例

```go
// 创建解析器
parser := parser.NewSQLParser()

// 解析SQL语句
ast, err := parser.Parse("SELECT * FROM users WHERE id = 1")
if err != nil {
    log.Fatal(err)
}

// 使用访问者模式遍历AST
visitor := ast.NewPrintVisitor()
ast.Accept(visitor)
```

## 开发状态

当前模块处于开发阶段，已完成：
- [x] 核心接口定义
- [x] 基础AST节点结构
- [x] 错误处理框架
- [ ] 词法分析器实现
- [ ] 语法分析器实现
- [ ] 访问者模式实现
- [ ] 缓存机制实现