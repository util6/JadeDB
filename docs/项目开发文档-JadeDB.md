---

此文档在项目开发中是有最高优先级的文档，是使用AI辅助开发要首先查看并遵循的文档，此文档负责记录大概的产品需求，具体的功能需求，开发时遇到的具体功能需求；开发可能用到的技术选型；
开发过程中遇到的每一个bug记录，以及解决bug用到的方法，错误的，正确的解决方法。以及过程中的知识经验总结
⚠️此文件只记录以上所说内容的关键（最精简，精炼的）信息，对于每一部分的具体详细说明在更具体的独立文档中，统一放在docs文件夹下，项目根目录下只有此文档和README.md文档

❗️此文档具有最高优先级，每次改动后都要提交git，如果项目有多个分支，也要保证每个分支的该文档保持最新一致的状态
---





# JadeDB项目开发文档



## Prompt for AI assistent（AI辅助开发提示词）





### 项目目标描述

此项目是我之前实现的一个不太完善的kv。
现在的目的是参考github上的优秀go语言数据库来完善本项目，同时达到我学习并模仿MySQL的目的
1.github.com/etcd-io/bbolt  go db中最接近 MySQL B+ 树的实现， 针对磁盘 I/O 优化，页面式存储（类似 MySQL）
2.github.com/lotusdblabs/lotusdb  同时支持 LSM 树和 B+ 树
3.CockroachDB、TiDB 实现一个完整的数据库系统，不仅只是存储引擎层



### 开发环境描述







### 功能需求描述

目前已经实现了LSM树
1.参考 github.com/lotusdblabs/lotusdb  完善LSM树
2.参考 github.com/etcd-io/bbolt  实现B+树 同时参考github.com/lotusdblabs/lotusdb  实现B+树与LSM树兼容
3.参考CockroachDB 逐步完善除了存储引擎之外的数据库高级特性





### 项目进度描述







## 项目架构



### 技术选型





### 核心设计（重点）





### 项目目录结构





## bug记录







## bug解决方案记录







