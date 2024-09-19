---
title: "大数据平台搭建"
excerpt: "cdh hdp闭源后的大数据平台搭建及运维"
categories:
  - hadoop
tags:
  - hadoop
---

# 目录
{% include toc %}

目前cdh和hdp都闭源，cdh社区版也只提供到6.0.1，并且不提供后续支持；HDP 也退休了，虽然ambari现在恢复更新，但是HDP stack 并不开源，只提供包下载。
## 组件说明
* bigtop：bigtop 可以打包 hadoop 生态组件为 RPM/DEB 包， linux 系统下的安装包。现在支持25+的组件。
* ambari：为了让 Hadoop 以及相关的大数据软件更容易使用的一个工具， 提供了创建、管理、监视 Hadoop 的集群的功
* HDP：Hortonworks数据平台是一款基于Apache Hadoop的是开源数据平台

## 平台构建的基础思路
编译打包 + 部署（ambari）

* 方案一：其中bigtop+ambari（可以参考：https://mp.weixin.qq.com/s/DHk-sq51aHwLE61aRP9p5w）
* 方案二：基于hdp+ambari

### bigtop+ambari
#### 编译参考（我还没成功过）：
* 环境准备：https://blog.csdn.net/m0_48319997/article/details/128037302?spm=1001.2014.3001.5502
* 大数据组件编译：https://blog.csdn.net/m0_48319997/article/details/128101667?spm=1001.2014.3001.5502
* ambari编译：https://blog.csdn.net/mojiezhongbb/article/details/133153641?spm=1001.2014.3001.5502

#### 编译好的组件直接安装
##### 基于bigtop编译好的包进行安装
1. ambari-server安装：https://blog.csdn.net/m0_48319997/article/details/130069050
2. ambari-server配置：https://blog.csdn.net/m0_48319997/article/details/130233526?spm=1001.2014.3001.5502

上面的第一个教程里需要把bigtop的结果都要构建本地yum 源
##### 基于hdp的包进行安装
包来源HiDataplus:http://www.hdp.link/
安装参考：https://mp.weixin.qq.com/s/KrhgKnbca1IuSR65UcbCzQ

参考上面的安装就能完成