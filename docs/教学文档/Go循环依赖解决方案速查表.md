# Go循环依赖解决方案速查表

## 🚨 循环依赖错误识别

### 典型错误信息
```bash
package myproject/a
    imports myproject/b
    imports myproject/a: import cycle not allowed
```

### 快速检查命令
```bash
# 检查是否有循环依赖
go build ./...

# 查看完整依赖图
go mod graph

# 查看特定包的依赖
go list -deps ./mypackage
```

## 🛠️ 解决方案速查

### 方案1：提取公共包 ⭐⭐⭐⭐⭐
**适用场景：** 两个包共享相同的数据结构或常量

**操作步骤：**
1. 创建 `common` 或 `types` 包
2. 将共享类型移动到公共包
3. 两个包都依赖公共包

```go
// 之前：A ↔ B (循环依赖)
// 之后：A → Common ← B (无循环依赖)

// common/types.go
package common

type User struct {
    ID   string
    Name string
}

// packageA/service.go
package packageA
import "myproject/common"

func ProcessUser(user common.User) { }

// packageB/handler.go  
package packageB
import "myproject/common"

func HandleUser(user common.User) { }
```

### 方案2：依赖倒置 ⭐⭐⭐⭐⭐
**适用场景：** 高层模块依赖低层模块的具体实现

**操作步骤：**
1. 在高层模块定义接口
2. 低层模块实现接口
3. 通过依赖注入使用

```go
// 高层模块定义接口
package service

type UserRepository interface {
    Save(user User) error
    FindByID(id string) (*User, error)
}

type UserService struct {
    repo UserRepository  // 依赖接口
}

// 低层模块实现接口
package repository
import "myproject/service"

type MySQLUserRepo struct{}

func (r *MySQLUserRepo) Save(user service.User) error { }
func (r *MySQLUserRepo) FindByID(id string) (*service.User, error) { }
```

### 方案3：分层架构 ⭐⭐⭐⭐
**适用场景：** 包结构混乱，职责不清

**操作步骤：**
1. 重新组织包结构
2. 建立清晰的分层
3. 确保依赖单向流动

```go
// 分层结构
domain/     # 领域模型（最底层）
repository/ # 数据访问层
service/    # 业务逻辑层  
handler/    # 表示层（最顶层）

// 依赖方向：handler → service → repository → domain
```

### 方案4：事件驱动 ⭐⭐⭐
**适用场景：** 模块间需要通信但不应直接依赖

**操作步骤：**
1. 创建事件总线
2. 发布者发布事件
3. 订阅者处理事件

```go
// events/bus.go
package events

type EventBus interface {
    Publish(event Event)
    Subscribe(eventType string, handler func(Event))
}

// userservice/service.go
package userservice
import "myproject/events"

func CreateUser(user User) {
    // 创建用户逻辑
    events.Publish(UserCreatedEvent{User: user})
}

// orderservice/service.go  
package orderservice
import "myproject/events"

func init() {
    events.Subscribe("UserCreated", handleUserCreated)
}
```

### 方案5：中介者模式 ⭐⭐⭐
**适用场景：** 多个模块需要相互通信

**操作步骤：**
1. 创建中介者接口
2. 实现具体中介者
3. 模块通过中介者通信

```go
// mediator/interface.go
package mediator

type Mediator interface {
    SendUserEvent(event UserEvent)
    SendOrderEvent(event OrderEvent)
}

// userservice/service.go
package userservice
import "myproject/mediator"

type UserService struct {
    mediator mediator.Mediator
}

func (s *UserService) CreateUser(user User) {
    s.mediator.SendUserEvent(UserCreatedEvent{User: user})
}
```

## 📋 解决方案选择指南

| 场景 | 推荐方案 | 难度 | 效果 |
|------|----------|------|------|
| 共享数据结构 | 提取公共包 | ⭐ | ⭐⭐⭐⭐⭐ |
| 服务间调用 | 依赖倒置 | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| 架构混乱 | 分层架构 | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| 松耦合通信 | 事件驱动 | ⭐⭐⭐ | ⭐⭐⭐ |
| 复杂交互 | 中介者模式 | ⭐⭐⭐⭐ | ⭐⭐⭐ |

## 🔧 重构步骤模板

### 步骤1：分析现状
```bash
# 1. 检查循环依赖
go build ./...

# 2. 分析依赖关系
go mod graph | grep "myproject"

# 3. 找出问题包
# 记录哪些包相互依赖，依赖的具体类型/函数
```

### 步骤2：选择方案
```go
// 问题分析清单：
// □ 是否有共享的数据结构？ → 提取公共包
// □ 是否有服务间调用？ → 依赖倒置  
// □ 是否架构层次不清？ → 分层架构
// □ 是否需要解耦通信？ → 事件驱动
```

### 步骤3：实施重构
```bash
# 1. 创建新的包结构
mkdir -p common types interfaces

# 2. 移动共享类型
mv shared_types.go common/

# 3. 定义接口
# 在高层模块定义接口

# 4. 重新组织导入
# 更新所有import语句

# 5. 验证修复
go build ./...
```

## 🚀 最佳实践清单

### ✅ 设计阶段
- [ ] 绘制包依赖图
- [ ] 确保依赖关系是DAG（有向无环图）
- [ ] 遵循单一职责原则
- [ ] 优先定义接口

### ✅ 编码阶段  
- [ ] 小而专一的包
- [ ] 使用internal包限制可见性
- [ ] 接口定义在使用方
- [ ] 避免god package（万能包）

### ✅ 重构阶段
- [ ] 渐进式重构，不要一次性大改
- [ ] 先解决最严重的循环依赖
- [ ] 保持向后兼容性
- [ ] 充分测试

### ✅ 维护阶段
- [ ] 定期检查依赖关系
- [ ] 在CI中添加循环依赖检查
- [ ] 代码审查时关注依赖设计
- [ ] 文档化架构决策

## 🛡️ 预防措施

### 项目结构模板
```
myproject/
├── cmd/                    # 命令行工具
├── internal/               # 内部包
│   ├── domain/            # 领域模型
│   ├── repository/        # 数据访问
│   ├── service/           # 业务逻辑
│   └── common/            # 公共类型
├── pkg/                   # 对外API
└── api/                   # HTTP/gRPC接口
```

### CI检查脚本
```yaml
# .github/workflows/deps-check.yml
name: Dependency Check
on: [push, pull_request]
jobs:
  check-deps:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - uses: actions/setup-go@v2
    - name: Check circular dependencies
      run: |
        if ! go build ./...; then
          echo "Build failed - possible circular dependency"
          exit 1
        fi
```

## 🔍 调试技巧

### 可视化依赖关系
```bash
# 安装工具
go install github.com/kisielk/godepgraph@latest

# 生成依赖图
godepgraph -s github.com/yourproject | dot -Tpng -o deps.png

# 查看特定包的依赖
godepgraph -s -p mypackage github.com/yourproject
```

### 分析工具
```bash
# 查看包的导入
go list -f '{{.ImportPath}}: {{.Imports}}' ./...

# 查看包的依赖深度
go list -deps ./... | wc -l

# 查找最复杂的包
go list -f '{{.ImportPath}} {{len .Imports}}' ./... | sort -k2 -nr
```

## 💡 常见陷阱

### ❌ 避免这些做法
- 为了避免循环依赖而过度抽象
- 创建过多的小接口（接口爆炸）
- 通过全局变量隐式创建依赖
- 在init函数中创建隐式依赖

### ✅ 推荐做法
- 保持接口简单和专一
- 优先组合而不是继承
- 明确的依赖注入
- 清晰的包职责划分

---

**记住：循环依赖的限制是Go帮助我们写出更好代码的特性！** 🎯
