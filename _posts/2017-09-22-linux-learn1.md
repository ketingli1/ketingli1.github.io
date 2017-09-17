---
layout: post
title: LINUX 学习笔记一
author: Kalle
---

## 特殊的文件
* ~/.bash_profile
  用户设置用户环境，这个文件在用户login的时候执行；

* /etc/profile 

  全局所有用户环境设置文件

* ~/.bashrc

  交互式非登录shell 启动将会执行这个文件

* 登录shell，bash shell的启动顺序

  * /etc/profile
  * /etc/profile.d
  * ~/.bash_profile
  * ~/.bash_login
  * ~/.profile

## 别名

* 使用用法

  alias name = command

  * alias : 列出当前别名列表

  * alias name : 赋予name含义

  * unalias name : 取消别名

    可以在~/.bash_profile中定义别名并使用别名 

## 选项

* 使用用法

  set -o optionname 用于打开某个环境变量的设置(on)

  set +o optionname 用户取消某个环境变量的设置(off)

  set -o 列出环境变量使用情况

## 参数

* 定义和使用

  varname = value

  echo $varname

* linux包含了一下内置的参数，我们可以通过修改这些参数来改变配置。在我们自定义的参数名中要避免与之重名，如history相关参数：HISTCMD、HISTSIZE...

* 提示符

  * 普通用户$，root用户#

  * 缺省提示符：PS1/PS2/PS3/PS4

    例如我们需要改为用户名$的方式，可以重新设置PS1，即PS1="/u$"，如果我们还希望加上时间，设为PS1="/u-/A$ ";

    PS2是命令尚未输完，第二上的换行继续的提示符，通常为>

    PS3和PS4用于编程和debug

  ​