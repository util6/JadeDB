# SQL解析器任务

本文档概述了为JadeDB构建SQL解析器的任务。

## 阶段1：核心解析器基础架构

- [x] 1.1：定义令牌类型和关键字（`lexer/token.go`）
- [x] 1.2：实现词法分析器（`lexer/lexer.go`）
- [x] 1.3：向词法分析器添加位置跟踪（行/列）

## 阶段2：基本SELECT语句解析

- [x] 2.1：实现 `SELECT col1, col2 FROM table`
- [x] 2.2：支持 `*` （`SELECT * FROM table`）
- [x] 2.3：支持别名（`SELECT col1 AS c1 FROM table`）

## 阶段3：表达式解析

- [x] 3.1：实现Pratt解析器用于表达式
- [x] 3.2：支持数字和字符串字面量
- [x] 3.3：支持算术运算符（+、-、*、/）
- [x] 3.4：支持比较运算符（=、!=、<、>）
- [x] 3.5：支持逻辑运算符（AND、OR、NOT）
- [x] 3.6：支持运算符优先级和括号

## 阶段4：解析器和AST基础

- [x] 4.1：实现递归下降解析器基本结构（`parser/parser.go`，`ParserImpl`）
- [x] 4.2：实现AST节点基础定义（`ast/nodes.go`，`ast/interfaces.go`）

## 阶段5：WHERE子句

- [x] 5.1：实现 `WHERE` 子句解析
- [x] 5.2：将表达式解析集成到 `WHERE` 子句中

## 阶段6：高级SELECT功能

- [x] 6.1：支持 `DISTINCT`
- [x] 6.2：支持 `CASE...WHEN...THEN...ELSE...END`
- [x] 6.3：支持函数调用（例如，`COUNT(*)`，`SUM(col)`）

## 阶段7：聚合和排序子句

- [x] 7.1：实现 `GROUP BY` 子句
- [x] 7.2：支持 `GROUP BY` 中的多个列
- [x] 7.3：实现 `HAVING` 子句
- [x] 7.4：实现带 `ASC`/`DESC` 的 `ORDER BY` 子句
- [x] 7.5：实现 `LIMIT` 和 `OFFSET` 子句

## 阶段8：子查询支持

- [x] 8.1：实现子查询表达式（例如，`(SELECT ...)`）
- [x] 8.2：支持在 `FROM`、`WHERE` 和 `SELECT` 列表中的子查询

## 阶段9：DML语句

- [x] 9.1：实现 `INSERT INTO` 语句
- [x] 9.2：实现 `UPDATE` 语句
- [x] 9.3：实现 `DELETE FROM` 语句

## 阶段10：DDL语句

- [x] 10.1：实现 `CREATE TABLE` 语句（带有列类型和约束）
- [x] 10.2：实现表和列约束（`PRIMARY KEY`、`NOT NULL` 等）
- [x] 10.3：实现 `DROP TABLE` 语句
- [x] 10.4：实现索引相关语句（`CREATE INDEX`、`DROP INDEX`）

## 阶段11：访问者模式

- [x] 11.1：实现用于AST遍历的 `BaseVisitor`
- [x] 11.2：实现用于调试的 `ASTPrinter` 访问者
- [x] 11.3：实现 `SymbolCollector` 访问者
- [x] 11.4：实现用于基本语义检查的 `ASTValidator` 访问者

## 阶段12：错误处理和恢复

- [x] 12.1：实现详细错误报告（位置、上下文）
- [x] 12.2：实现错误恢复策略（恐慌模式、同步）
- [x] 12.3：实现友好的错误消息

## 阶段13：高级功能

- [x] 13.1：添加对注释的支持（单行和多行）
- [x] 13.2：实现 `JOIN` 子句（`INNER`、`LEFT`、`RIGHT`、`FULL`）
- [x] 13.3：支持事务（`BEGIN`、`COMMIT`、`ROLLBACK`）
- [x] 13.4：支持 `UNION`、`INTERSECT`、`EXCEPT`

## 阶段14：最终化

- [x] 14.1：为所有支持的SQL语法编写综合测试
- [x] 14.2：重构和清理解析器和词法分析器代码
- [x] 14.3：记录解析器的功能和限制