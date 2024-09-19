---
title: "flink-cdc采集demo"
excerpt: "基于mysql的flink-cdc的采集demo"
categories:
  - flink
tags:
  - 大数据
---

# 目录
{% include toc %}

基于flink-cdc的采集方案需考虑如下场景：
1. 异常重启，初始化消费，幂等性
    a. 存储binlog不是全量的，如何恢复全量数据需验证
2. 断点续传：
    a. 基于savepoint机制，保证失败能恢复
    b. 考虑binlog的生命周期，恢复机制不能超过生命周期需验证
3. 多数据源
4. 支持范围和版本
    a. 阿里云rds、华为云rds、aws（Amazon Aurora）、自建mysql：华为云试过没问题，其他没环境，但是官方都号称支持采集
    b. 8.0
5. 数据结构的变化
    a. 表变化：新增配置，需重启采集服务
    b. 字段变化：无需变更，动态适配

## 采集工程实现
1. proto定义，来源于Prometheus源码
pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>flink-cdc-test</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <java.version>1.8</java.version>
        <scala.binary.version>2.12</scala.binary.version>
        <maven.compiler.source>${java.version}</maven.compiler.source>
        <maven.compiler.target>${java.version}</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <!-- Enforce single fork execution due to heavy mini cluster use in the tests -->
        <flink.forkCount>1</flink.forkCount>
        <flink.reuseForks>true</flink.reuseForks>

        <!-- dependencies versions -->
        <flink.version>1.18.0</flink.version>
        <slf4j.version>1.7.15</slf4j.version>
        <log4j.version>2.17.1</log4j.version>
        <debezium.version>1.9.7.Final</debezium.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-core</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
        </dependency>
        <!-- Checked the dependencies of the Flink project and below is a feasible reference. -->
        <!--  Use flink shaded guava  18.0-13.0 for flink 1.13   -->
        <!--  Use flink shaded guava  30.1.1-jre-14.0 for flink-1.14  -->
        <!--  Use flink shaded guava  30.1.1-jre-15.0 for flink-1.15  -->
        <!--  Use flink shaded guava  30.1.1-jre-15.0 for flink-1.16  -->
        <!--  Use flink shaded guava  30.1.1-jre-16.1 for flink-1.17  -->
        <!--  Use flink shaded guava  31.1-jre-17.0   for flink-1.18  -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-shaded-guava</artifactId>
            <version>31.1-jre-17.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-mysql-cdc</artifactId>
<!--            <version>2.4.2</version>-->
            <version>3.1.0</version>
        </dependency>

        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-connector-mysql</artifactId>
            <version>${debezium.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-base</artifactId>
            <version>${flink.version}</version>

        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
            <version>1.13.0</version>
        </dependency>
<!--        <dependency>-->
<!--            <groupId>mysql</groupId>-->
<!--            <artifactId>mysql-connector-java</artifactId>-->
<!--            <version>8.0.11</version>-->
<!--        </dependency>-->

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.83</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <id>shade-flink</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <!-- Shading test jar have bug in some previous version, so close this configuration here,
                            see https://issues.apache.org/jira/browse/MSHADE-284 -->
                            <shadeTestJar>false</shadeTestJar>
                            <shadedArtifactAttached>false</shadedArtifactAttached>
                            <createDependencyReducedPom>true</createDependencyReducedPom>
                            <dependencyReducedPomLocation>
                                ${project.basedir}/target/dependency-reduced-pom.xml
                            </dependencyReducedPomLocation>
                            <filters combine.children="append">
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>module-info.class</exclude>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <artifactSet>
                                <includes>
                                    <!-- include nothing -->
                                    <include>io.debezium:debezium-api</include>
                                    <include>io.debezium:debezium-embedded</include>
                                    <include>io.debezium:debezium-core</include>
                                    <include>io.debezium:debezium-ddl-parser</include>
                                    <include>io.debezium:debezium-connector-mysql</include>
                                    <include>org.apache.flink:flink-connector-debezium</include>
                                    <include>org.apache.flink:flink-connector-mysql-cdc</include>
                                    <include>org.antlr:antlr4-runtime</include>
                                    <include>org.apache.kafka:*</include>
                                    <include>mysql:mysql-connector-java</include>
                                    <include>com.zendesk:mysql-binlog-connector-java</include>
                                    <include>com.fasterxml.*:*</include>
                                    <include>com.google.guava:*</include>
                                    <include>com.esri.geometry:esri-geometry-api</include>
                                    <include>com.zaxxer:HikariCP</include>
                                    <!--  Include fixed version 30.1.1-jre-16.0 of flink shaded guava  -->
                                    <include>org.apache.flink:flink-shaded-guava</include>
                                </includes>
                            </artifactSet>
                            <relocations>
                                <relocation>
                                    <pattern>org.apache.kafka</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.org.apache.kafka
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>org.antlr</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.org.antlr
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.fasterxml</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.fasterxml
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.google</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.google
                                    </shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.esri.geometry</pattern>
                                    <shadedPattern>org.apache.flink.cdc.connectors.shaded.com.esri.geometry</shadedPattern>
                                </relocation>
                                <relocation>
                                    <pattern>com.zaxxer</pattern>
                                    <shadedPattern>
                                        org.apache.flink.cdc.connectors.shaded.com.zaxxer
                                    </shadedPattern>
                                </relocation>
                            </relocations>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>

</project>

```

2. flink-java

```java
package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.debezium.engine.format.Json;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;

public class Main {
    private static Map<String, Integer> cache = new HashMap<>();

//    private static final String hostName = "127.0.0.1";
//    private static final String databases = "app_db";
//    private static final String tables = "app_db.orders";
//    private static final String userName = "root";
//    private static final String pwd = "123456";


//    private static final String hostName = "172.31.1.127";
//    private static final String[] databases = {"simba_os_platform", "simba_os_scheduler"};
//    private static final String[] tables = {
//            "simba_os_platform.tbl_simba_os_project"
//            , "simba_os_scheduler.tbl_simba_os_scheduler_job"
//            , "simba_os_scheduler.tbl_simba_os_scheduler_job_instance"
//    };
//    private static final String userName = "root";
//    private static final String pwd = "X7RTFoZ@9ouxP5A2J";


    private static final String hostName = "192.168.0.227";
    private static final String[] databases = {"test"};
    private static final String[] tables = {
            "test.mysqlxingneng2"
    };
    private static final String userName = "yunong";
    private static final String pwd = "startdt@123";
    public static void main(String[] args) throws Exception {

        Map<String, Object> configs = new HashMap<>();
        //转换decimal类型
        configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostName)
                .port(3306)
                .databaseList(databases) // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList(tables) // set captured table
                .username(userName)
                .password(pwd)
                .deserializer(new JsonDebeziumDeserializationSchema(false, configs)) // converts SourceRecord to JSON String
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(5000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 1 parallel source tasks
                .setParallelism(1)
                .map(line -> {
                    System.out.println(line);
                    JSONObject lineObject = JSON.parseObject(line);
                    JSONObject source = lineObject.getJSONObject("source");
                    if (source != null) {
                        String table = source.getString("table");
                        int tableCCnt = 0;
                        // 因为Parallelism为1可以这么设置，分布式不能这样设置
                        if (cache.containsKey(table)) {
                            tableCCnt = cache.get(table) + 1;
                        } else {
                            tableCCnt = 1;
                        }
                        cache.put(table, tableCCnt);
//                        System.out.printf("%s的第%d次变更\n", table, tableCCnt);
                    }



                    return line;
                }).setParallelism(1); // use parallelism 1 for sink

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
```
