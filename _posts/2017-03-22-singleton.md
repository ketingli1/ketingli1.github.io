---
layout: post
title: 设计模式-单例模式
author: Kalle
---

单例模式学习笔记

# 单例模式（整理）

## 概念

保证一个类只有一个实例，并提供一个访问它的全局访问点

## 用途

* 保证在内存中只有该类的一个实例，减少了内存的开销，尤其是频繁的创建的销毁实例
* 避免对资源的多重占用

## 实现方式

对象是通过类的构造函数产生的，如果一个类对外提供了public的构造方法，那么外界就可以任意创建该类的对象。所以，如果想限制对象的产生，一个办法就是构造函数变为private的，但同时要提供类的可用性，所以必须提供一个自己的对象以及访问这个对象的静态方法。

![Snip20161117_2](/images/post/singleton.png)

* **饿汉式**

  所谓饿汉，这是一个形象的比喻。对于一个饿汉来说，他希望他想要用到这个实力的时候就能够立刻拿到，而不需要等待时间。所以，通过static的静态初始化方式，在改类第一次被加载的时候，就有一个实例被创建出来了。这样就保证在第一次想要使用该对象的时候，它已经被初始化好了。

  ~~~java
  //code 1
  public class Singleton {
      //在类内部实例化一个实例
      private static Singleton instance = new Singleton();
      //私有的构造函数,外部无法访问
      private Singleton() {
      }
      //对外提供获取实例的静态方法
      public static Singleton getInstance() {
          return instance;
      }
  }
  ~~~

  同时，由于该实例在类被加载的时候就创建出来了，所以也避免了线程安全问题。

  这种方式的单例，有可能该实例在创建之后有可能根本就使用不到。

* **懒汉式**

  与饿汉对应，懒汉就是不会提前把实例创建出来，将类对自己的实例化延迟到第一次被引用的时候，代码如下：

  ~~~java
  //code 2
  public class Singleton {
      //定义实例
      private static Singleton instance;
      //私有构造方法
      private Singleton(){}
      //对外提供获取实例的静态方法
      public static Singleton getInstance() {
          //在对象被使用的时候才实例化
          if (instance == null) {
              instance = new Singleton();
          }
          return instance;
      }
  }
  ~~~

  懒汉式单例存在线程安全的问题，在多线程环境下，有可能两个线程同时进入if语句中，这样，在两个线程都从if中退出的时候就创建了两个不一样的对象。

  针对线程不安全的懒汉式单例，其实解决方式很简单，就是给创建对象的步骤加锁：

  ~~~java
  //code 3
  public class SynchronizedSingleton {
      //定义实例
      private static SynchronizedSingleton instance;
      //私有构造方法
      private SynchronizedSingleton(){}
      //对外提供获取实例的静态方法,对该方法加锁
      public static synchronized SynchronizedSingleton getInstance() {
          //在对象被使用的时候才实例化
          if (instance == null) {
              instance = new SynchronizedSingleton();
          }
          return instance;
      }
  }
  ~~~

  这种写法能够在多线程中很好的公众，而且看起来也具备很好的延迟加载，但是，效率很低，因为99%的情况不需要通过操作，因为synchronized在第一次创建对象的时候才真正起到作用，第n次取数据的时候根本就是多余的。

  针对以上问题，缩小一下加锁范围，将同步方法改为同步代码块：

  ~~~JAVA
  //code 4
  public class Singleton {

      private static Singleton singleton;

      private Singleton() {
      }

      public static Singleton getSingleton() {
          if (singleton == null) {
              synchronized (Singleton.class) {
                  if (singleton == null) {
                      singleton = new Singleton();
                  }
              }
          }
          return singleton;
      }
  }
  ~~~

  但是，事情真的有这么容易吗？上面的代码看上去好像没有任何问题，实现了惰性初始化，解决了同步问题，还减小了锁的范围，提高了效率。但是，该代码还存在隐患。隐患的原因主要和Java内车模型有关。考虑下面的时间序列：

  > 线程A发现变量没有被初始化, 然后它获取锁并开始变量的初始化。
  >
  > 由于某些编程语言的语义，编译器生成的代码允许在线程A执行完变量的初始化之前，更新变量并将其指向部分初始化的对象。
  >
  > 线程B发现共享变量已经被初始化，并返回变量。由于线程B确信变量已被初始化，它没有获取锁。如果在A完成初始化之前共享变量对B可见（这是由于A没有完成初始化或者因为一些初始化的值还没有穿过B使用的内存(缓存一致性)），程序很可能会崩溃。

  针对以上问题，**volatile**关键字保证多个线程可以正确处理单件实例：

  ~~~java
  //code 5
  public class VolatileSingleton {
      private static volatile VolatileSingleton singleton;

      private VolatileSingleton() {
      }

      public static VolatileSingleton getSingleton() {
          if (singleton == null) {
              synchronized (VolatileSingleton.class) {
                  if (singleton == null) {
                      singleton = new VolatileSingleton();
                  }
              }
          }
          return singleton;
      }
  }
  ~~~

  上面这种双重校验锁的方式用的比较广泛，他解决了前面提到的所有问题。但是，即使是这种看上去完美无缺的方式也可能存在问题，那就是遇到序列化的时候。详细内容后文介绍。

* **静态内部类式**

  ~~~java
  //code 6
  public class StaticInnerClassSingleton {
      //在静态内部类中初始化实例对象
      private static class SingletonHolder {
          private static final StaticInnerClassSingleton INSTANCE = new StaticInnerClassSingleton();
      }
      //私有的构造方法
      private StaticInnerClassSingleton() {
      }
      //对外提供获取实例的静态方法
      public static final StaticInnerClassSingleton getInstance() {
          return SingletonHolder.INSTANCE;
      }
  }
  ~~~

  这种方式同样利用了classloder的机制来保证初始化instance时只有一个线程，它跟饿汉式不同的是（很细微的差别）：饿汉式是只要`Singleton`类被装载了，那么instance就会被实例化（没有达到lazy loading效果），而这种方式是`Singleton`类被装载了，instance不一定被初始化。因为`SingletonHolder`类没有被主动使用，只有显示通过调用getInstance方法时，才会显示装载SingletonHolder类，从而实例化instance。想象一下，如果实例化instance很消耗资源，我想让他延迟加载，另外一方面，我不希望在Singleton类加载时就实例化，因为我不能确保Singleton类还可能在其他的地方被主动使用从而被加载，那么这个时候实例化instance显然是不合适的。这个时候，这种方式相比饿汉式更加合理。


## 总结

一个简单的单例，涉及这么多知识 ​:happy:​​:happy:​ ​:happy:​​:happy:​​:happy:​