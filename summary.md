# Paimon Lakehouse 项目总结

## 一、项目背景

基于微信公众号文章《Paimon实时入湖踩完三个坑，我敢说它不是啥银弹》，深入分析了 Apache Paimon 实时入湖的三个核心痛点，并构建了完整的 Lakehouse 示例项目。

## 二、三个坑详解

### 坑一：S3 账单翻了三倍（小文件问题）

**原因**：默认 append 模式未开启 FULL-COMPACTION，checkpoint 间隔太短（30秒），bucket 和并发配置不当，导致每天积累几十万个小文件。

**修复方案**：
- changelog-producer = FULL-COMPACTION
- full-compaction.delta-commits = 10
- checkpoint 间隔调到 3 分钟

**效果**：存储成本腰斩。

### 坑二：Schema 变更炸了历史分区

**原因**：ALTER TABLE ADD COLUMN 后，新旧 Schema 不兼容。Paimon 的 Schema Evolution 保证新数据能按新 Schema 写入，但不会自动重写历史分区的数据文件。当下游 StarRocks 做跨分区扫描时，Schema 不一致直接报错。

**报错信息**：Fragment must be equal to partition column count（StarRocks 抛出，非 Paimon）

**修复方案**：预留 5 个 STRING 扩展字段（ext\_field1-5），变更时只改语义不改结构。

### 坑三：AVG 差了 47 万（脏数据问题）

**原因**：上游 Kafka 混入脏数据（负数、异常大值），Flink 直接写入 Paimon 没做校验，导致聚合结果偏差。

**修复方案**：在 Flink 层加数据过滤，脏数据单独记录到 dirty\_orders 表。

## 三、Paimon 与 OLAP 引擎对接

### 为什么 Paimon 不能直接做 OLAP

Paimon 是存储格式（Table Format），不是查询引擎。缺少 MPP 分布式计算、CBO 优化器、本地缓存、物化视图。

### StarRocks 的核心优势

| 特性 | 说明 |
|------|------|
| 向量化执行引擎 | 列式处理，CPU 缓存友好，一次处理 1024 行 |
| CBO 优化器 | 基于代价选择 JOIN 顺序、Broadcast vs Shuffle |
| Data Cache | 把远端 Paimon 文件缓存到本地 SSD |
| 异步物化视图 | 预聚合 + 自动查询改写 |
| Native Parquet Reader | 原生读取湖格式文件，谓词下推 |

### 性能对比（StarRocks vs Flink SQL 直读）

| 查询 | Flink SQL | StarRocks | 提速 |
|------|-----------|-----------|------|
| COUNT(\*) 160万行 | 7-12秒 | 1.2秒 | 6-10倍 |
| TOP-10 GROUP BY | 15-25秒 | 2.4秒 | 6-10倍 |
| JOIN + 聚合 | 15-25秒 | 3.0秒 | 5-8倍 |

### CBO 与向量化详解

**CBO（基于代价的优化器）**：收集统计信息，估算每步代价，选最优执行计划。核心能力包括 JOIN 顺序选择、JOIN 策略选择、谓词下推、列裁剪、聚合下推、分区裁剪。

**向量化执行**：批量列式处理，一次处理 1024 行。两层优化独立叠加：
- 批处理（Batch）：1次函数调用处理 1024 行，减少 1024 倍函数调用
- SIMD（单次调用内部）：1条 CPU 指令处理 16 个值（AVX-512 单精度），减少 16 倍指令数
- 总吞吐提升 = 1024 × 16 = 16384 倍

## 四、Schema Evolution 深入分析

### Paimon Schema 版本机制

每次 Schema 变更在 schema/ 目录下新建 schema 文件，版本号递增。每个 snapshot 记录使用的 schema-id，每个数据文件也记录所属 schema。

### Checkpoint 恢复报错场景

1. Flink 读侧任务在 schema-0 时期做了 checkpoint
2. ALTER TABLE ADD COLUMN 生成 schema-1（不产生 snapshot，Flink 不感知）
3. 新数据按 schema-1 写入
4. Flink 任务挂了，从 checkpoint 恢复
5. 追增量时用旧 schema-0 解析新 schema-1 的文件，报 IllegalStateException

### 关键：Schema 变更不产生 snapshot

Flink 读侧任务只监听 snapshot 变更，不监听 schema 目录变更。知道有新数据（通过 snapshot），但不知道 Schema 变了。

### 解决方案

| 方案 | 做法 |
|------|------|
| schema.evolution.enabled=true | Flink source 自动感知 schema 变更 |
| 无状态重启 | 不从 checkpoint 恢复，重新从 catalog 拿最新 schema |
| 预留字段 | 建表时预留 STRING 列，变更时只改语义不改结构 |

## 五、Paimon 存储架构

Paimon 是存储格式 + 表格式，底层依赖文件系统/对象存储：

| 层级 | 技术 |
|------|------|
| 数据文件 | Parquet（默认）/ ORC |
| 元数据文件 | JSON（schema, manifest, snapshot） |
| 物理存储 | HDFS / S3 / OSS / MinIO / 本地文件系统 |

## 六、项目结构

### 项目名称

paimon-lakehouse\_project

### GitHub 仓库

https://github.com/zhao003120/paimon-lakehouse\_project

### 技术栈

Flink 1.18 + Paimon 0.8 + MinIO S3 + StarRocks 3.3

### 目录结构

```
paimon-lakehouse_project/
├── sql/                          # Flink SQL 脚本
│   ├── 00-catalog.sql             #   MinIO S3 + 建库
│   ├── 01-ods-ddl.sql             #   ODS 建表 (含三个坑修复)
│   ├── 02-mock-data.sql           #   模拟数据
│   ├── 03-dwd-ddl.sql             #   DWD 建表
│   ├── 04-ods-to-dwd.sql          #   ODS→DWD ETL
│   ├── 05-dws-ddl.sql             #   DWS 建表
│   ├── 06-dwd-to-dws.sql          #   DWD→DWS 聚合
│   ├── 07-ads-ddl.sql             #   ADS 建表
│   ├── 08-dws-to-ads.sql          #   DWS→ADS 应用层
│   ├── 09-report.sql              #   6张报表 + 全链路对账
│   ├── 10-schema-evolution.sql    #   坑二演示
│   ├── 11-checkpoint-demo.sql     #   坑二演示
│   ├── run-sql.sh                 #   Flink + StarRocks 运行脚本
│   ├── sql-defaults.yaml          #   Flink SQL Client 配置
│   └── starrocks/                 # StarRocks OLAP 查询
│       ├── 00-catalog.sql          #   创建 Paimon 外部 Catalog
│       ├── 01-report.sql           #   6张报表 (MPP查询)
│       ├── 02-benchmark.sql        #   Benchmark 对比
│       ├── 03-schema-demo.sql      #   Schema Evolution 演示
│       └── run-starrocks.sh        #   StarRocks 运行脚本
├── docker/                       # Docker 环境
│   ├── Dockerfile                 #   Flink + MySQL client
│   ├── docker-compose.yml         #   MinIO + Flink + StarRocks FE/BE
│   └── run-docker.sh              #   Docker 运行脚本
├── scripts/                      # Windows PowerShell 脚本
│   ├── run-docker.ps1             #   一键运行
│   ├── run-sql.ps1                #   SQL 运行
│   ├── start-minio.ps1            #   MinIO 启动
│   └── start-minio.sh             #   MinIO 启动 (Linux)
├── report/                       # HTML 报表
│   └── dashboard.html             #   可视化 Dashboard
└── .gitignore
```

### 数仓分层

| 层级 | 数据库 | 表 | 职责 |
|------|--------|-----|------|
| ODS | paimon\_db | orders / dirty\_orders | 原始数据（含校验、脏数据追溯） |
| DWD | dwd | dwd\_order\_detail | 明细宽表（JSON 解析、维度补全） |
| DWS | dws | dws\_order\_daily / weekly | 日汇总、周汇总 |
| ADS | ads | ads\_order\_kpi / customer\_rank / channel\_stat | KPI、排行、渠道分析 |

### 架构

```
Kafka/Source → Flink SQL (ETL) → Paimon (MinIO S3) → StarRocks (OLAP)
                写入层                存储层              查询层
```

## 七、运行方式

### Docker 一键启动

```powershell
# 启动所有服务 (MinIO + Flink + StarRocks)
.\scripts\run-docker.ps1 up

# 一键跑完: Flink 写数据 + StarRocks 查询 (推荐)
.\scripts\run-docker.ps1 warehouse+sr

# 分步执行
.\scripts\run-docker.ps1 warehouse      # Flink: 建表+写数据+ETL
.\scripts\run-docker.ps1 sr-init        # StarRocks: 初始化集群
.\scripts\run-docker.ps1 sr-report      # StarRocks: 查 6 张报表
.\scripts\run-docker.ps1 sr-benchmark   # StarRocks: 跑 Benchmark
```

### 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| MinIO | http://localhost:9001 | admin / admin123 |
| Flink | http://localhost:8081 | Web UI |
| StarRocks FE | http://localhost:8030 | Web UI |
| StarRocks SQL | localhost:9030 | mysql -h 127.0.0.1 -P 9030 -u root |

## 八、从 Java 到 SQL 的演进

项目最初使用 Java 代码实现（15 个 Java 类），后全部替换为纯 SQL 脚本（12 个 SQL 文件），无需 Maven 编译，直接通过 Flink SQL Client 执行。

| 功能 | Java 版本 | SQL 版本 |
|------|----------|---------|
| 建表 | WarehouseDDL.java | 01/03/05/07-ddl.sql |
| 模拟数据 | MockDataGenerator.java | 02-mock-data.sql |
| ODS→DWD | OdsToDwdJob.java | 04-ods-to-dwd.sql |
| DWD→DWS | DwdToDwsJob.java | 06-dwd-to-dws.sql |
| DWS→ADS | DwsToAdsJob.java | 08-dws-to-ads.sql |
| 报表查询 | ReportJob.java | 09-report.sql |
| Schema 演示 | SchemaEvolutionDemo.java | 10-schema-evolution.sql |
| Checkpoint 演示 | CheckpointRecoveryDemo.java | 11-checkpoint-demo.sql |
| StarRocks 查询 | 无 | starrocks/01-report.sql |
| Benchmark | 无 | starrocks/02-benchmark.sql |

## 九、核心结论

1. Paimon 是存储格式不是查询引擎，OLAP 查询交给 StarRocks 是正解
2. Schema Evolution 能加列但不改历史文件，跨版本扫描会出问题
3. 预留扩展字段是最稳的 Schema 变更方案
4. CBO 解决"做什么"（少干活），向量化解决"怎么干"（干得快），两者叠加是 StarRocks 快 6-10 倍的核心原因
5. 纯 SQL 方案比 Java 代码更直观、更易维护
