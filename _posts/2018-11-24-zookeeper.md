---
title: "ZooKeeper读书笔记"
excerpt: "ZooKeeper分布式过程协同技术读书笔记"
categories:
  - 大数据
tags:
  - 分布式
---

# 目录
{% include toc %}

## 使命
在分布式系统中是多个独立的程序协同工作，让应用只关心自身的业务逻辑；可体现为:

* 协同
* 管理竞争 

如在Master/Slave模式中，从节点处于空闲，主节点给从节点分配任务，这直接的协同zookeeper可完成；主节点跪了，从节点人人都想成为主节点，需要实现排他锁。