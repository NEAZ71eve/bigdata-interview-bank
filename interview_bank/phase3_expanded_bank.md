# 大数据面试题扩展题库（含变式题与难度分级）

## 扩展元信息
- 扩展时间：2026-07-03
- 扩展Agent：generator
- 输入题目数：40道（Phase 2 标准化产出）
- 变式题生成数：50道（选择题变式20 + 简答题变式25 + 代码题变式5）
- 派生代码题数：10道（CODE-006 ~ CODE-015）
- 总输出题目数：100道
- 难度分布：L1 15道 / L2 35道 / L3 35道 / L4 15道
- 难度占比：L1 15% / L2 35% / L3 35% / L4 15%（与建议区间一致）

### 难度等级定义
| 等级 | 名称 | 标准 |
|------|------|------|
| L1 | 基础 | 概念记忆、定义辨析（如"Kafka是什么"、"Hive内外部表区别"） |
| L2 | 进阶 | 原理理解、流程阐述（如"Kafka高吞吐原理"、"HDFS读写流程"） |
| L3 | 高级 | 方案设计、问题排查、对比分析（如"数据倾斜解决方案"、"Spark vs Flink选型"） |
| L4 | 专家 | 源码级、架构权衡、复杂场景综合（如"Unaligned Checkpoint原理"、"端到端Exactly-Once实现"） |

### 知识图谱标签体系
- **一级标签**：技术栈（Kafka / Spark / Hadoop / Hive / Flink / HBase / Zookeeper / 数仓 / 数据倾斜 / 场景题）
- **二级标签**：核心知识点（如 Kafka→零拷贝 / ISR / Rebalance；Spark→内存管理 / Shuffle / 血缘）
- **三级标签**：关联考点（标注与哪些题目相关，便于组卷与检索）

---

## 一、选择题（含变式）

### MCQ-001 [L2] [Kafka/零拷贝/sendfile]
- **题干**：Kafka 实现高吞吐所采用的零拷贝技术，主要依赖以下哪个系统调用？
- **选项**：A. read() + write()  B. sendfile()  C. mmap()  D. splice()
- **正确答案**：B
- **来源公司**：字节跳动
- **解析**：Kafka Consumer 端使用 sendfile() 实现零拷贝，数据直接从内核页缓存传输到网卡，无需经过用户空间；Producer 端使用 mmap() 将磁盘文件映射到用户空间内存。read()+write() 会产生 4 次上下文切换和 2 次 CPU 拷贝，不属于零拷贝。
- **关联题**：SAQ-001、MCQ-001-V1、MCQ-001-V2

### MCQ-001-V1 [L3] [Kafka/零拷贝/mmap]
- **题干**：Kafka Producer 端为了提升写入吞吐，将磁盘文件映射到用户空间内存，使用的是以下哪个系统调用？
- **选项**：A. sendfile()  B. mmap()  C. splice()  D. read() + write()
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：Producer 端使用 mmap() 将磁盘文件映射到用户空间内存，减少一次内核态到用户态的拷贝；Consumer 端使用 sendfile() 直接从页缓存到网卡。两者方向不同，需区分。
- **关联题**：MCQ-001

### MCQ-001-V2 [L2] [Kafka/零拷贝/辨析]
- **题干**：以下关于 Kafka 零拷贝的说法，错误的是？
- **选项**：A. sendfile() 减少了 CPU 拷贝次数  B. mmap() 减少了用户态与内核态的上下文切换  C. 零拷贝意味着数据完全不经过 CPU  D. read()+write() 会产生 4 次上下文切换
- **正确答案**：C
- **变式类型**：反向变式
- **解析**："零拷贝"指减少 CPU 拷贝次数（仍可能有 DMA 拷贝），并非完全不经过 CPU。sendfile() 由 DMA 完成数据搬运，CPU 不参与数据拷贝，但 DMA 本质上仍是数据移动。
- **关联题**：MCQ-001

### MCQ-002 [L2] [Spark/宽窄依赖/算子]
- **题干**：以下 Spark 算子中，哪个属于窄依赖？
- **选项**：A. groupByKey  B. reduceByKey  C. map  D. join
- **正确答案**：C
- **来源公司**：字节跳动
- **解析**：map 是窄依赖算子，父 RDD 一个分区最多被子 RDD 一个分区使用，可在 Stage 内流水线执行。groupByKey、reduceByKey、join 都会产生 Shuffle，属于宽依赖。
- **关联题**：SAQ-006、MCQ-002-V1、MCQ-002-V2

### MCQ-002-V1 [L2] [Spark/宽窄依赖/Shuffle]
- **题干**：判断一个 Spark 算子是宽依赖还是窄依赖，最本质的依据是？
- **选项**：A. 算子是否产生磁盘 IO  B. 算子是否触发 Shuffle  C. 算子是否涉及网络传输  D. 算子是否会缓存数据
- **正确答案**：B
- **变式类型**：角度变式
- **解析**：宽窄依赖的本质区别在于是否触发 Shuffle：触发 Shuffle 的算子（如 groupByKey、join）是宽依赖，不触发的（如 map、filter）是窄依赖。Shuffle 必然涉及磁盘 IO 和网络传输，但判断依据是 Shuffle 本身。
- **关联题**：MCQ-002

### MCQ-002-V2 [L4] [Spark/宽窄依赖/分区器]
- **题干**：关于 Spark 的 join 算子，以下说法正确的是？
- **选项**：A. join 一定是宽依赖  B. join 一定是窄依赖  C. 当两 RDD 使用相同且协作的分区器时，join 可以是窄依赖  D. join 的依赖类型取决于数据量大小
- **正确答案**：C
- **变式类型**：深度变式
- **解析**：默认情况下 join 是宽依赖，但如果两个 RDD 已经使用相同且协作的 HashPartitioner（且分区数一致），则 join 不需要 Shuffle，可视为窄依赖。这是 Spark 优化的高级技巧（co-partitioned）。
- **关联题**：MCQ-002、SAQ-020

### MCQ-003 [L2] [Spark/内存管理/Execution]
- **题干**：Spark 统一内存管理中，用于 Shuffle、Join、排序等执行操作的内存区域是？
- **选项**：A. Reserved Memory  B. User Memory  C. Storage Memory  D. Execution Memory
- **正确答案**：D
- **来源公司**：美团
- **解析**：Execution Memory 用于 Shuffle/Join/排序等执行操作；Storage Memory 用于缓存 RDD/broadcast 变量；User Memory 存储用户数据结构和 UDF 对象；Reserved Memory 为保留内存 300MB。Storage 与 Execution 之间可动态借用，但不可抢占对方已使用内存。
- **关联题**：SAQ-020、MCQ-003-V1、MCQ-003-V2

### MCQ-003-V1 [L2] [Spark/内存管理/Storage]
- **题干**：Spark 统一内存管理中，Storage Memory 主要用于存储什么？
- **选项**：A. Shuffle 数据  B. 用户 UDF 对象  C. 缓存 RDD 和 broadcast 变量  D. 系统保留内存
- **正确答案**：C
- **变式类型**：角度变式
- **解析**：Storage Memory 用于缓存 RDD partition、broadcast 变量等。在统一内存管理下，Storage 与 Execution 可互相借用空闲内存，但 Execution 不可被 Storage 抢占，反之亦然。
- **关联题**：MCQ-003

### MCQ-003-V2 [L3] [Spark/内存管理/动态占用]
- **题干**：在 Spark 统一内存管理中，当 Execution Memory 不足时，以下哪种情况会发生？
- **选项**：A. 强制抢占 Storage Memory 中已使用的内存  B. 如果 Storage Memory 有空闲则借用，否则触发 Spill  C. 直接 OOM 退出  D. 等待 Storage Memory 释放
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：统一内存管理规则：双方可借用对方空闲内存；若对方内存不足且自己需要更多，则自己触发 Spill（落盘）；不可抢占对方已使用内存。Execution 优先级高于 Storage，Storage 在 Execution 需要时必须让出借用的部分（通过 block 淘汰）。
- **关联题**：MCQ-003

### MCQ-004 [L1] [Hive/内外部表/区别]
- **题干**：关于 Hive 内部表和外部表，以下说法正确的是？
- **选项**：A. 内部表删除时只删除元数据，保留原始数据  B. 外部表删除时元数据和原始数据全部删除  C. 外部表删除时只删除元数据，保留原始数据  D. 内部表和外部表删除行为完全相同
- **正确答案**：C
- **来源公司**：美团
- **解析**：外部表（external table）删除时只删除元数据，原始数据保留；内部表（managed table）删除时元数据和原始数据全部删除。生产环境绝大多数场景创建外部表以保障数据安全。
- **关联题**：SAQ-016、MCQ-004-V1、MCQ-004-V2

### MCQ-004-V1 [L2] [Hive/内外部表/转换]
- **题干**：将 Hive 内部表转换为外部表的语句是？
- **选项**：A. ALTER TABLE t1 SET TBLPROPERTIES('EXTERNAL'='TRUE')  B. ALTER TABLE t1 SET EXTERNAL=TRUE  C. ALTER TABLE t1 EXTERNAL  D. DROP TABLE t1; CREATE EXTERNAL TABLE ...
- **正确答案**：A
- **变式类型**：反向变式
- **解析**：通过 ALTER TABLE ... SET TBLPROPERTIES('EXTERNAL'='TRUE/FALSE') 切换内外部表属性，注意值为字符串。这是面试高频考点，也是生产环境常用操作。
- **关联题**：MCQ-004

### MCQ-004-V2 [L1] [Hive/内外部表/选型]
- **题干**：生产环境数仓建表时，通常优先选择外部表，最主要的原因是？
- **选项**：A. 查询性能更好  B. 删除表时数据不会丢失，更安全  C. 支持事务  D. 自动压缩数据
- **正确答案**：B
- **变式类型**：场景变式
- **解析**：外部表删除时只删除元数据，原始数据保留，避免误删导致数据丢失。生产环境数据资产极其重要，所以 ODS/DWD 等层多使用外部表。
- **关联题**：MCQ-004

### MCQ-005 [L2] [Hive/列式存储/ORC-Parquet]
- **题干**：以下关于 Hive 列式存储格式 ORC 和 Parquet 的说法，错误的是？
- **选项**：A. ORC 是 Hive 原生优化格式，对 Hive 支持最好  B. Parquet 是通用列式格式，Spark/Impala/Presto 都支持  C. 列式存储只读取需要的列，减少 IO  D. ORC 和 Parquet 都是行式存储格式
- **正确答案**：D
- **来源公司**：快手
- **解析**：ORC 和 Parquet 都是列式存储格式，不是行式存储。列式存储优势：只读取需要的列减少 IO、同列数据类型一致压缩比高、适合聚合分析。生产环境 Hive 优先 ORC，多引擎混用优先 Parquet。
- **关联题**：SAQ-021、MCQ-005-V1、MCQ-005-V2

### MCQ-005-V1 [L3] [Hive/列式存储/stripe]
- **题干**：关于 ORC 文件结构中的 stripe，以下说法正确的是？
- **选项**：A. stripe 是 ORC 的行组，每行一个 stripe  B. stripe 是 ORC 的存储单元，默认约 250MB，包含 index/data/footer 三部分  C. stripe 只能存储一种数据类型  D. stripe 是 Parquet 特有的概念
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：ORC 文件由多个 stripe 组成（默认 250MB），每个 stripe 包含 index data、row data、stripe footer 三部分。stripe 内按列存储，支持谓词下推跳过不满足条件的 stripe，是 ORC 高效查询的关键。
- **关联题**：MCQ-005

### MCQ-005-V2 [L2] [Hive/列式存储/选型]
- **题干**：某数仓同时被 Hive、Spark、Impala 三种引擎查询，优先选择哪种存储格式？
- **选项**：A. ORC  B. Parquet  C. SequenceFile  D. TextFile
- **正确答案**：B
- **变式类型**：场景变式
- **解析**：Parquet 是跨引擎通用的列式格式，Spark/Impala/Presto/Hive 都支持良好；ORC 是 Hive 原生优化格式，但与 Impala 兼容性较差。多引擎混用场景优先 Parquet。
- **关联题**：MCQ-005

### MCQ-006 [L2] [Hive/排序/order-by]
- **题干**：以下 Hive 排序语句中，哪个会触发全局排序且只使用一个 Reduce？
- **选项**：A. sort by  B. order by  C. distribute by  D. cluster by
- **正确答案**：B
- **来源公司**：京东
- **解析**：order by 是全局排序，只有一个 Reduce，数据量大时会 OOM，生产环境慎用；sort by 是每个 Reduce 内部排序；distribute by 控制数据分发到哪个 Reduce；cluster by 是 distribute by + sort by 且排序字段与分发字段相同（只能升序）。
- **关联题**：SAQ-021、MCQ-006-V1、MCQ-006-V2

### MCQ-006-V1 [L2] [Hive/排序/cluster-by]
- **题干**：Hive 中 cluster by 等价于以下哪种组合？
- **选项**：A. distribute by + sort by（同字段，可升降序）  B. distribute by + sort by（同字段，仅升序）  C. order by + distribute by  D. sort by + group by
- **正确答案**：B
- **变式类型**：角度变式
- **解析**：cluster by 等价于 distribute by + sort by 且两子句字段相同，且只能升序（无法指定 DESC）。需要降序时必须分开写 distribute by + sort by。
- **关联题**：MCQ-006

### MCQ-006-V2 [L4] [Hive/排序/海量全局有序]
- **题干**：京东有 40-50T 数据需要全局有序输出，直接使用 order by 会 OOM，以下哪种方案不可行？
- **选项**：A. 按排序字段分桶 + 每个桶内 sort by，外部归并  B. 分区表按排序键范围分区，每分区 sort by  C. 直接调大 reduce 数到 1000 解决 OOM  D. 借助 Spark 的 repartitionAndSortWithinPartitions + 范围分区器
- **正确答案**：C
- **变式类型**：场景变式
- **解析**：order by 强制单 Reduce，无论 reduce 数多少都不会改变。正确做法是分桶/分区+局部排序+全局归并，或用 Spark 的范围分区器（RangePartitioner）配合 repartitionAndSortWithinPartitions 实现分布式全局有序。
- **关联题**：MCQ-006、CODE-004

### MCQ-007 [L2] [Flink/Checkpoint/Barrier]
- **题干**：Flink Checkpoint 机制中，JobManager 的 CheckpointCoordinator 向 Source 节点注入什么来触发状态快照？
- **选项**：A. Watermark  B. Barrier  C. Savepoint  D. Trigger
- **正确答案**：B
- **来源公司**：字节跳动
- **解析**：Checkpoint 通过注入 Barrier（屏障）实现，Barrier 随数据流向下流动，算子收到 Barrier 后对齐状态并持久化到 State Backend，所有算子完成快照后向 JobManager 汇报。Watermark 是衡量事件时间进度的机制，用于处理乱序数据。
- **关联题**：SAQ-011、SAQ-024、MCQ-007-V1、MCQ-007-V2

### MCQ-007-V1 [L4] [Flink/Checkpoint/Unaligned]
- **题干**：Flink Unaligned Checkpoint 主要解决了什么问题？
- **选项**：A. 状态过大导致 Checkpoint 失败  B. 反压时 Barrier 对齐耗时过长，导致 Checkpoint 超时  C. Savepoint 与 Checkpoint 不一致  D. Watermark 传递延迟
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：Aligned Checkpoint 要求算子收到所有上游 Barrier 后才开始快照，反压时 Barrier 排队等待，导致 Checkpoint 超时。Unaligned Checkpoint 允许 Barrier 直接越过排队中的数据，将 in-flight 数据一并写入状态，从而快速完成快照。代价是状态体积增大。
- **关联题**：MCQ-007、SAQ-024

### MCQ-007-V2 [L2] [Flink/Watermark/事件时间]
- **题干**：Flink 中 Watermark 的核心作用是？
- **选项**：A. 触发 Checkpoint  B. 衡量事件时间进度，处理乱序数据  C. 控制反压  D. 分配分区
- **正确答案**：B
- **变式类型**：角度变式
- **解析**：Watermark 是事件时间的进展标记，用于告诉系统"事件时间小于 Watermark 的数据已基本到齐"，从而触发窗口计算。Watermark ≥ 窗口结束时间时窗口触发，处理乱序到达的数据。
- **关联题**：MCQ-007、SAQ-012

### MCQ-008 [L2] [HBase/写流程/WAL]
- **题干**：HBase 写数据的正确流程顺序是？
- **选项**：A. 写 MemStore → 写 WAL → Flush 到 HFile  B. 写 WAL → 写 MemStore → Flush 到 HFile  C. 写 WAL → Flush 到 HFile → 写 MemStore  D. 写 MemStore → Flush 到 HFile → 写 WAL
- **正确答案**：B
- **来源公司**：阿里
- **解析**：HBase 写流程：先写 WAL（预写日志，保证持久性）→ 再写 MemStore（内存）→ 返回成功 → MemStore 达到阈值后 Flush 到 HFile。WAL 先写是为了故障恢复时能重放未落盘的数据。
- **关联题**：SAQ-015、MCQ-008-V1、MCQ-008-V2

### MCQ-008-V1 [L3] [HBase/读流程/BlockCache]
- **题干**：HBase 读数据的查询顺序是？
- **选项**：A. HFile → MemStore → BlockCache  B. BlockCache → MemStore → HFile  C. MemStore → HFile → BlockCache  D. BlockCache → HFile → MemStore
- **正确答案**：B
- **变式类型**：反向变式
- **解析**：HBase 读流程：先查 BlockCache（读缓存）→ 再查 MemStore（写缓存，未落盘的新数据）→ 最后查 HFile（落盘数据）。BlockCache 默认 LRU 策略，热数据命中率高。注意 MemStore 优先于 HFile，确保读到最新数据。
- **关联题**：MCQ-008

### MCQ-008-V2 [L2] [HBase/WAL/禁用风险]
- **题干**：HBase 写入时如果禁用 WAL（durability=SKIP_WAL），可能导致的后果是？
- **选项**：A. 写入速度变慢  B. RegionServer 故障时数据丢失  C. MemStore 无法 Flush  D. HFile 无法生成
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：WAL 用于故障恢复时重放未落盘数据，禁用后一旦 RegionServer 宕机，MemStore 中未 Flush 的数据将永久丢失。仅在数据可丢失或可重算的场景（如批量导入）才禁用 WAL。
- **关联题**：MCQ-008

### MCQ-009 [L2] [Zookeeper/选举/zxid]
- **题干**：Zookeeper Leader 选举时，节点优先根据以下哪个因素投票？
- **选项**：A. myid 大小  B. zxid 大小  C. 节点启动顺序  D. 节点 IP 地址
- **正确答案**：B
- **来源公司**：美团
- **解析**：Zookeeper 选举优先选 zxid 大的（数据最新），zxid 相同时选 myid 大的。zxid 是全局唯一递增的事务 ID，越大表示数据越新。获得半数以上节点投票即成为 Leader。
- **关联题**：SAQ-025、MCQ-009-V1、MCQ-009-V2

### MCQ-009-V1 [L2] [Zookeeper/选举/myid]
- **题干**：Zookeeper 集群选举 Leader 时，若多个候选节点的 zxid 相同，则接下来根据什么决定 Leader？
- **选项**：A. 节点 IP  B. myid 大者优先  C. 启动顺序先者优先  D. 随机选择
- **正确答案**：B
- **变式类型**：角度变式
- **解析**：选举规则：先比较 zxid（数据新旧），zxid 相同时比较 myid（节点编号），myid 大者胜出。这是为了保证数据最新且在数据一致时有确定性结果。
- **关联题**：MCQ-009

### MCQ-009-V2 [L3] [Zookeeper/选举/脑裂]
- **题干**：Zookeeper 集群出现网络分区（脑裂）时，如何防止两个 Leader 同时存在？
- **选项**：A. 通过 myid 比较自动选主  B. 必须获得半数以上节点投票才能成为 Leader  C. Leader 检测到分区后自动退出  D. 客户端只连接 myid 最大的节点
- **正确答案**：B
- **变式类型**：深度变式
- **解析**：Zookeeper 通过"过半机制"防止脑裂：成为 Leader 必须获得集群半数以上节点投票。网络分区时，少数派分区无法凑够半数，无法选出新 Leader，原 Leader 若不在多数派分区也会失去 Leader 身份，从而保证全局唯一 Leader。
- **关联题**：MCQ-009

### MCQ-010 [L2] [数仓/建模/星型雪花]
- **题干**：关于星型模型和雪花模型，以下说法正确的是？
- **选项**：A. 雪花模型维度表不规范化，数据冗余多  B. 星型模型维度表进一步规范化拆分，查询需多表 Join  C. 星型模型结构简单查询快，生产环境优先选择  D. 雪花模型查询性能优于星型模型
- **正确答案**：C
- **来源公司**：字节跳动
- **解析**：星型模型维度表不规范化（冗余），结构简单查询快，生产环境优先选择；雪花模型维度表进一步规范化拆分，减少冗余但查询需多表 Join 稍慢。维度建模四步：选业务过程→声明粒度→确定维度→确定事实。
- **关联题**：SAQ-016、MCQ-010-V1、MCQ-010-V2

### MCQ-010-V1 [L2] [数仓/建模/雪花适用]
- **题干**：以下哪种场景更适合使用雪花模型而非星型模型？
- **选项**：A. 查询性能优先，维度较少  B. 存储成本敏感，维度更新频繁且维度表很大  C. 报表层数据宽表  D. ADS 层汇总查询
- **正确答案**：B
- **变式类型**：场景变式
- **解析**：雪花模型通过规范化减少冗余，适合维度更新频繁、维度表巨大、存储成本敏感的场景。但查询性能不如星型，所以生产数仓大多数还是选星型。
- **关联题**：MCQ-010

### MCQ-010-V2 [L2] [数仓/建模/事实表类型]
- **题干**：以下哪种事实表记录业务过程的累积度量值（如账户余额）？
- **选项**：A. 事务型事实表  B. 周期快照事实表  C. 累积型事实表  D. 退化维度表
- **正确答案**：C
- **变式类型**：角度变式
- **解析**：事务型事实表记录业务事件（一行一条），周期快照表按周期记录度量（如每日余额），累积型事实表记录从期初到当前累积的度量（如订单从下单到完成的各阶段状态时间）。账户余额类业务适合累积型。
- **关联题**：MCQ-010、SAQ-017

---

## 二、简答题（含变式）

### SAQ-001 [L3] [Kafka/消息可靠性/三端保证]
- **题干**：如何保证 Kafka 消息不丢失？
- **来源公司**：美团
- **考查要点**：生产者端 acks=all 与重试机制；Broker 端副本因子与 min.insync.replicas；消费者端手动提交 offset；幂等性配置
- **关联题**：SAQ-002、SAQ-005、SAQ-022、CODE-006、SAQ-001-V1

### SAQ-001-V1 [L4] [Kafka/消息重复/幂等性事务]
- **题干**：如何保证 Kafka 消息不重复（Exactly-Once 语义）？幂等性 Producer 能保证什么、不能保证什么？
- **变式类型**：场景变式
- **考查要点**：幂等性 Producer（enable.idempotence=true）通过 PID + SequenceNumber 保证单分区单会话内不重复；跨分区跨会话需要事务（transactional.id）；Consumer 端需配合事务隔离级别 read_committed；端到端 Exactly-Once 还需 Sink 端幂等或事务
- **关联题**：SAQ-001、SAQ-011、CODE-010

### SAQ-002 [L3] [Kafka/ISR/Leader选举]
- **题干**：Kafka 如何选举 Leader？ISR 机制是什么？
- **来源公司**：字节跳动
- **考查要点**：ISR 定义与维护机制；Leader 故障时从 ISR 选举新 Leader；Controller 职责；消费组 Leader 选举由 GroupCoordinator 负责
- **关联题**：SAQ-001、SAQ-002-V1

### SAQ-002-V1 [L3] [Kafka/ISR/unclean选举风险]
- **题干**：当 ISR 为空时，开启 unclean.leader.election.enable=true 会有什么风险？应该如何权衡？
- **变式类型**：深度变式
- **考查要点**：ISR 为空时开启 unclean 选举会从非 ISR 副本（数据落后的副本）中选 Leader，导致已提交消息丢失；可用性优先则开启（如日志类业务），数据一致性优先则关闭（如交易类业务）；Kafka 0.11 后默认关闭
- **关联题**：SAQ-002

### SAQ-003 [L2] [Kafka/Pull模式/消费者]
- **题干**：Kafka 消费者是推模式还是拉模式？为什么采用该模式？
- **来源公司**：快手
- **考查要点**：Pull 拉模式；自主控制速率；节约资源；容错性；消息积压控制；Push 模式的问题
- **关联题**：SAQ-004、SAQ-022、SAQ-003-V1

### SAQ-003-V1 [L2] [Kafka/Pull模式/空轮询]
- **题干**：Kafka Pull 模式有什么缺点？如何避免消费者一直拉到空消息（轮询空转）？
- **变式类型**：反向变式
- **考查要点**：Pull 模式缺点是无法实时感知新消息（轮询空转浪费资源）、消息延迟可能较高；通过参数 fetch.min.bytes（≥1 时无消息不返回）和 fetch.max.wait.ms 控制等待；长轮询机制让 Broker 在无消息时阻塞请求直到有消息或超时
- **关联题**：SAQ-003

### SAQ-004 [L2] [Kafka/消费者组/Rebalance]
- **题干**：Kafka 消费者组是什么？同一个消费者组的消费者能否消费同一个分区？
- **来源公司**：京东
- **考查要点**：消费者组定义；组内一分区只被一个消费者消费；Rebalance 机制；不同消费者组互不影响
- **关联题**：SAQ-003、SAQ-022、SAQ-004-V1

### SAQ-004-V1 [L3] [Kafka/Rebalance/触发与优化]
- **题干**：Kafka Rebalance 的触发条件有哪些？Rebalance 会导致什么问题？如何减少 Rebalance？
- **变式类型**：深度变式
- **考查要点**：触发条件（消费者加入/离开/心跳超时/分区数变化/订阅主题变化）；Rebalance 期间消费暂停（Stop-The-World）；减少策略：合理设置 session.timeout.ms 和 heartbeat.interval.ms、max.poll.interval.ms、避免消费者处理过慢、使用 StickyAssignor 减少分区迁移
- **关联题**：SAQ-004

### SAQ-005 [L3] [Kafka/顺序性/分区]
- **题干**：Kafka 如何保证消息顺序性？如何实现全局有序？
- **来源公司**：百度
- **考查要点**：分区内有序（offset 递增）；相同 key 写同一分区；单分区全局有序但牺牲并行度；多分区无法保证全局有序
- **关联题**：SAQ-001、CODE-006、SAQ-005-V1

### SAQ-005-V1 [L4] [Kafka/顺序性/权衡]
- **题干**：业务既要全局有序又要高吞吐，应该如何架构？请给出方案并说明权衡点。
- **变式类型**：场景变式
- **考查要点**：方案1：按业务实体分桶（如按 uid 取模分 N 分区），桶内有序 + 桶间并行（局部有序，吞吐高）；方案2：用 Flink/Spark 在消费端按 key 重排（窗口内排序后下发）；权衡点：完全全局有序只能单分区，吞吐上限=单分区吞吐；大多数业务只需"按 key 有序"即可，可接受
- **关联题**：SAQ-005、CODE-006

### SAQ-006 [L3] [Spark/容错/血缘]
- **题干**：Spark 的容错机制是怎样的？RDD 如何实现容错？
- **来源公司**：阿里
- **考查要点**：RDD 血缘关系（Lineage）；分区丢失重算；窄依赖重算父分区、宽依赖重算所有父分区；Checkpoint 截断血缘
- **关联题**：MCQ-002、SAQ-020、CODE-007、SAQ-006-V1

### SAQ-006-V1 [L3] [Spark/容错/Checkpoint vs Cache]
- **题干**：Spark 中 Checkpoint 和 Cache 的区别是什么？Lineage 过长会有什么问题？
- **变式类型**：角度变式
- **考查要点**：Cache 是 lazy 内存缓存（不截断血缘，job 失败可重算），Checkpoint 是 eager 落盘（截断血缘，需先 Cache 再 Checkpoint 避免重复计算）；Lineage 过长会导致重算代价大、调度开销高、栈溢出风险；Checkpoint 适合迭代算法和长血缘场景
- **关联题**：SAQ-006、CODE-007

### SAQ-007 [L3] [Hadoop/Shuffle/MR与Spark对比]
- **题干**：MapReduce 的 Shuffle 过程是怎样的？MapReduce 与 Spark 的优劣对比？
- **来源公司**：字节跳动
- **考查要点**：Map 端环形缓冲区→排序→Spill→Merge；Reduce 端拉取→Merge→分组；MR 基于磁盘 vs Spark 基于内存；DAG 调度减少 Shuffle
- **关联题**：SAQ-020、SAQ-007-V1

### SAQ-007-V1 [L3] [Spark/Shuffle/Hash vs Sort]
- **题干**：Spark Shuffle 有哪些实现？HashShuffle 和 SortShuffle 的区别与适用场景？
- **变式类型**：深度变式
- **考查要点**：HashShuffle（每 MapTask 每 Reduce 生成一个文件，文件数 M×R，小文件过多）；SortShuffle（每 MapTask 一个 sorted 文件 + index 文件，文件数 M）；BypassMergeSortShuffle（无 Map 端聚合且分区数小时 bypass 直接合并）；Tungsten Sort（直接操作序列化字节）；Spark 1.6 后默认 SortShuffle
- **关联题**：SAQ-007、SAQ-020

### SAQ-008 [L2] [Hadoop/HDFS/读写与副本]
- **题干**：HDFS 的读写流程是怎样的？副本机制是怎么样的？
- **来源公司**：美团
- **考查要点**：写流程（NameNode 校验→选 DataNode→pipeline 传输→packet 确认）；读流程（获取 block 列表→就近读取→checksum 校验）；副本放置策略（本地→不同机架→同机架不同节点）
- **关联题**：SAQ-009、SAQ-008-V1

### SAQ-008-V1 [L3] [Hadoop/HDFS/HA]
- **题干**：NameNode 挂了怎么办？HDFS HA 如何实现？脑裂如何避免？
- **变式类型**：角度变式
- **考查要点**：HA 方案：Active/Standby 双 NameNode，通过 JournalNode 集群同步 editlog，ZKFC（ZooKeeper Failover Controller）监控并切换；Standby 持续同步 editlog 并维护最新元数据；脑裂防护：通过 fencing（SSH kill / shell 隔离 / 共享存储隔离）确保原 Active 被隔离后 Standby 才接管；QJM 多数派写入避免脑裂
- **关联题**：SAQ-008、SAQ-025

### SAQ-009 [L2] [Hadoop/YARN/调度]
- **题干**：YARN 的调度流程是怎样的？有哪些调度器？
- **来源公司**：京东
- **考查要点**：提交作业→分配 Container 运行 AM→AM 申请资源→启动 Container→注销；FIFO/Capacity/Fair 三种调度器
- **关联题**：SAQ-008、SAQ-009-V1

### SAQ-009-V1 [L3] [Hadoop/YARN/调度器选型]
- **题干**：Capacity 调度器和 Fair 调度器有什么区别？生产环境如何选择？
- **变式类型**：场景变式
- **考查要点**：Capacity：队列划分容量比例，资源预留可被其他队列借用（需满足最小保证），适合多租户、队列资源相对固定的场景（Hadoop 默认）；Fair：所有作业公平分享资源，按权重分配，小作业快速启动，适合交互查询多、作业大小混合场景；选型：CDH 默认 Fair，Apache 默认 Capacity
- **关联题**：SAQ-009

### SAQ-010 [L3] [Hive/数据倾斜/通用方案]
- **题干**：Hive 的数据倾斜如何处理？
- **来源公司**：字节跳动
- **考查要点**：倾斜原因（key 分布不均、NULL 值、count distinct）；参数调节（map.aggr、groupby.skewindata、MapJoin）；SQL 调节（空 key 打散、两阶段聚合）；增加 reduce 数
- **关联题**：SAQ-018、CODE-008、SAQ-010-V1

### SAQ-010-V1 [L3] [Hive/数据倾斜/group-by与join]
- **题干**：group by 倾斜和 join 倾斜分别怎么解决？两阶段聚合的原理是什么？
- **变式类型**：角度变式
- **考查要点**：group by 倾斜：两阶段聚合（salting 加随机前缀第一轮局部聚合 → 去前缀第二轮全局聚合）、开启 map.aggr=true 与 groupby.skewindata=true；join 倾斜：MapJoin（小表广播）、空 key 打散 + 随机表 join、倾斜 key 单独处理；两阶段聚合原理：通过加随机前缀把大 key 拆到多个 reducer，第一轮局部聚合减少数据量，第二轮去掉前缀全局聚合
- **关联题**：SAQ-010、SAQ-018、CODE-008

### SAQ-011 [L3] [Flink/Exactly-Once/端到端]
- **题干**：Flink 如何实现 Exactly-Once 语义？
- **来源公司**：美团
- **考查要点**：端到端三部分配合；Source 端可重放数据源；Flink 内部 Checkpoint 状态一致性（Barrier 对齐）；Sink 端幂等写入或两阶段提交（2PC）
- **关联题**：SAQ-001-V1、SAQ-013、CODE-010、SAQ-011-V1

### SAQ-011-V1 [L4] [Flink/Exactly-Once/两阶段提交]
- **题干**：Flink 端到端 Exactly-Once 中两阶段提交（2PC）的完整流程是怎样的？Sink 端如何选择幂等写入还是事务写入？
- **变式类型**：深度变式
- **考查要点**：2PC 流程：preCommit（Sink 接收 barrier 后开启事务并写入，但不提交）→ Commit（JM 收齐所有 barrier 完成快照后通知所有 Sink 提交事务）→ 失败时 rollback；幂等写入（如 Redis SET、MySQL INSERT ON DUPLICATE KEY）实现简单但无法处理"已提交但快照失败"窗口的重复；事务写入（如 Kafka 事务、MySQL XA）严格 Exactly-Once 但延迟更高
- **关联题**：SAQ-011、CODE-010

### SAQ-012 [L2] [Flink/窗口/类型与函数]
- **题干**：Flink 的窗口有哪些类型？窗口函数如何使用？
- **来源公司**：快手
- **考查要点**：滚动/滑动/会话/全局窗口；Processing Time 与 Event Time；ReduceFunction/AggregateFunction/ProcessWindowFunction 区别
- **关联题**：SAQ-013、CODE-011、SAQ-012-V1

### SAQ-012-V1 [L3] [Flink/窗口/会话与触发]
- **题干**：Flink 会话窗口的 gap 怎么设？Event Time 窗口是 Watermark 到了才触发吗？
- **变式类型**：场景变式
- **考查要点**：会话窗口 gap 通过 static gap 或 DynamicGapFunction 设置，gap 内无新数据则窗口关闭；Event Time 窗口触发条件是 Watermark ≥ 窗口结束时间；Watermark = 最大事件时间 - 允许延迟；Watermark 跨越多个窗口结束时间时多个窗口同时触发；延迟数据可通过 allowedLateness 再触发
- **关联题**：SAQ-012

### SAQ-013 [L3] [Flink/状态/State Backend]
- **题干**：Flink 的状态管理是怎样的？状态有哪几种？
- **来源公司**：阿里
- **考查要点**：Keyed State（ValueState/ListState/MapState/ReducingState/AggregatingState）；Operator State；Managed State vs Raw State；三种 State Backend
- **关联题**：SAQ-011、SAQ-013-V1

### SAQ-013-V1 [L3] [Flink/状态/RocksDB]
- **题干**：RocksDBStateBackend 为什么适合大状态？状态 TTL 是什么？Broadcast State 是什么？
- **变式类型**：深度变式
- **考查要点**：RocksDB 是基于 LSM 树的嵌入式 KV 存储，状态存磁盘突破内存限制，支持增量 Checkpoint（只传输 diff）；状态 TTL 设置状态存活时间，过期自动清理避免状态膨胀；Broadcast State 用于将一个流的数据广播到所有 Task（如规则流），与主流 co-group 实现规则匹配，是 Flink 1.5+ 特性
- **关联题**：SAQ-013

### SAQ-014 [L3] [HBase/RowKey/设计与避免热点]
- **题干**：HBase 的 RowKey 如何设计？为什么不能连续写入？
- **来源公司**：美团
- **考查要点**：RowKey 设计原则（长度短、散列性）；避免热点方案（反转、哈希、加盐）；预分区
- **关联题**：SAQ-015、CODE-013、SAQ-014-V1

### SAQ-014-V1 [L3] [HBase/RowKey/反向问题]
- **题干**：HBase RowKey 设计不当会导致什么问题？如何排查与解决？
- **变式类型**：反向变式
- **考查要点**：问题：热点（某 Region 读写集中，RegionServer CPU/IO 飙升）、读写倾斜、Region 分裂不均、Scan 性能差；排查：HBase UI 看 Region 请求分布、HBase hbck 工具；解决：RowKey 加盐/哈希/反转打散、预分区、组合键（如 [hash(uid)] + ts）；范围查询需求用反转 + 二级索引或 Phoenix
- **关联题**：SAQ-014、CODE-013

### SAQ-015 [L3] [HBase/LSM/Compaction与分裂]
- **题干**：HBase 的 LSM 树原理是什么？Region 分裂机制是怎样的？
- **来源公司**：字节跳动
- **考查要点**：LSM 树写入 MemStore→Flush HFile→Compaction；Minor/Major Compaction；布隆过滤器优化读；Region 分裂流程
- **关联题**：SAQ-014、SAQ-015-V1

### SAQ-015-V1 [L3] [HBase/LSM/Compaction风暴]
- **题干**：HBase Minor 和 Major Compaction 的区别？Compaction 风暴如何避免？Region 分裂期间能读写吗？
- **变式类型**：深度变式
- **考查要点**：Minor 合并少量小 HFile（默认 3 个），速度快，删除标记不清理；Major 合并所有 HFile，清理过期与删除数据，产生单个 HFile，耗时长（生产环境常关闭自动 Major 改为业务低峰手动触发）；Compaction 风暴：大量 HFile 同时 Compaction 导致 IO 飙升，通过 hbase.hstore.compaction.max、hbase.regionserver.thread.compaction 调节并发；Region 分裂期间短暂阻塞读写（分裂 flush 阶段）
- **关联题**：SAQ-015

### SAQ-016 [L2] [数仓/分层/各层职责]
- **题干**：设计一个数仓分层方案，各层职责是什么？
- **来源公司**：美团
- **考查要点**：ODS/DIM/DWD/DWS/ADS 五层职责；分层价值（解耦、复用、规范口径、血缘追踪）
- **关联题**：MCQ-010、SAQ-017、CODE-014、SAQ-016-V1

### SAQ-016-V1 [L2] [数仓/分层/DWD与DWS]
- **题干**：DWD 和 DWS 的区别？为什么要做宽表？DIM 层放什么？
- **变式类型**：场景变式
- **考查要点**：DWD（明细数据层）保存清洗加工后的明细事实数据，按业务过程组织；DWS（汇总数据层）按主题 + 维度汇总，粒度更粗（如用户日汇总、商品日汇总）；宽表：减少 Join、提升查询性能、口径统一；DIM 层存放维度数据（商品维、用户维、地区维等），通常用星型模型
- **关联题**：SAQ-016

### SAQ-017 [L2] [数仓/SCD/拉链表]
- **题干**：缓慢变化维（SCD）如何处理？拉链表是什么？
- **来源公司**：京东
- **考查要点**：SCD1/SCD2/SCD3 三种处理方式；拉链表是 SCD2 的实现（start_date/end_date）；查询时按业务日期过滤
- **关联题**：SAQ-016、CODE-012、SAQ-017-V1

### SAQ-017-V1 [L2] [数仓/SCD/SCD2与SCD3适用场景]
- **题干**：SCD2 和 SCD3 各适用什么场景？事实表类型（事务型/周期型/累积型）区别？
- **变式类型**：角度变式
- **考查要点**：SCD2 保留完整历史（拉链表），适合需要追溯历史所有版本的维度（用户等级变更、商品类目变更）；SCD3 仅保留上一次值（增加 prior_column 列），适合只需对比当前与上一版本的场景；事务型事实表记录业务事件、周期型按周期快照、累积型记录从期初到当前的累积度量（如订单状态流转）
- **关联题**：SAQ-017、CODE-012

### SAQ-018 [L3] [数据倾斜/定位与解决]
- **题干**：数据倾斜如何定位？如何解决？
- **来源公司**：字节跳动
- **考查要点**：本质与表现（某 Task 执行时间远超其他、OOM）；定位方法（Spark Web UI、抽样统计 key）；解决方案（广播变量+Map Join、两阶段聚合、采样拆分 Join、增加分区数）
- **关联题**：SAQ-010、SAQ-018-V1、CODE-008

### SAQ-018-V1 [L3] [数据倾斜/两阶段聚合原理]
- **题干**：两阶段聚合为什么有效？倾斜 key 无法过滤怎么办？Kafka/Redis 的数据倾斜怎么解决？
- **变式类型**：深度变式
- **考查要点**：两阶段聚合通过加随机前缀把大 key 拆到多个 reducer，第一轮局部聚合后单 key 数据量降到 1/N，第二轮去前缀全局聚合；倾斜 key 不可过滤时用"倾斜 key 单独处理 + 非倾斜 key 走正常逻辑"的 union 方案；Kafka 倾斜（某分区 lag 高）通过调整分区策略或消费者处理能力；Redis 倾斜通过一致性哈希 + 热点 key 拆分（加随机后缀分散到多个 key）
- **关联题**：SAQ-018、SAQ-010、CODE-008

### SAQ-019 [L3] [Spark vs Flink/选型]
- **题干**：Spark 和 Flink 有什么区别？分别适用于什么场景？
- **来源公司**：字节跳动
- **考查要点**：计算模型（微批 vs 真流）；延迟（秒级 vs 毫秒级）；状态管理；时间语义与 Watermark；Exactly-Once；窗口丰富度
- **关联题**：SAQ-011、SAQ-019-V1

### SAQ-019-V1 [L3] [Spark vs Flink/Lambda与Kappa]
- **题干**：Lambda 架构和 Kappa 架构的区别？如何选型？Spark Structured Streaming 和 Flink 还有差距吗？
- **变式类型**：场景变式
- **考查要点**：Lambda：批 + 流双链路，批修正流，复杂度高，需要维护两套代码；Kappa：只保留流链路，需要重算历史时重启流任务回放，架构简洁但要求消息队列存储长周期数据；选型：批多流少选 Lambda，流为主选 Kappa；Spark SS 与 Flink 差距：延迟（Spark SS 仍为微批 100ms+，Flink 毫秒级）、状态管理（Flink 更丰富）、Event Time 与 Watermark 处理（Flink 更成熟）
- **关联题**：SAQ-019

### SAQ-020 [L3] [Spark/Shuffle/实现与优化]
- **题干**：Spark 的 Shuffle 有哪几种实现？如何优化 Shuffle？
- **来源公司**：阿里
- **考查要点**：HashShuffle/SortShuffle/Tungsten Sort/BypassMergeSortShuffle；优化手段（broadcast join、调 partitions、Map 端聚合、Kryo 序列化）
- **关联题**：SAQ-007、SAQ-018、CODE-009、SAQ-020-V1

### SAQ-020-V1 [L2] [Spark/Shuffle/reduceByKey vs groupByKey]
- **题干**：reduceByKey 和 groupByKey 的区别？为什么 reduceByKey 更好？Bypass 机制触发条件？
- **变式类型**：角度变式
- **考查要点**：reduceByKey 在 Map 端先局部聚合（combine）再 Shuffle，减少 Shuffle 数据量；groupByKey 不在 Map 端聚合，所有 (key, value) 直接 Shuffle，数据量大易 OOM；Bypass 触发条件：分区数 ≤ spark.shuffle.sort.bypassMergeThreshold（默认 200）且 Map 端无聚合操作，直接写文件后合并，避免排序开销
- **关联题**：SAQ-020、CODE-009

### SAQ-021 [L2] [Hive/SQL优化/通用手段]
- **题干**：Hive SQL 优化有哪些手段？
- **来源公司**：美团
- **考查要点**：Fetch 抓取；本地模式；MapJoin；列裁剪与分区裁剪；并行执行；严格模式；JVM 重用；合理设置 reduce 数
- **关联题**：SAQ-010、SAQ-021-V1

### SAQ-021-V1 [L3] [Hive/SQL优化/小文件与引擎]
- **题干**：Hive 小文件问题怎么解决？什么时候用 Tez/Spark 引擎？
- **变式类型**：场景变式
- **考查要点**：小文件产生原因（动态分区、reduce 数过多、频繁 insert）；危害（NameNode 内存、Map 任务数过多）；解决：合并小文件（concatenate 命令、`hive.merge.mapfiles=true`、`hive.merge.smallfiles.avgsize`）、调整 reduce 数、用 Spark/Tez 引擎；Tez/Spark 引擎：DAG 执行减少中间落盘，比 MR 快数倍，Hive 2.x 后推荐 Hive on Spark
- **关联题**：SAQ-021

### SAQ-022 [L2] [Kafka/消息积压/处理]
- **题干**：Kafka 消息积压如何处理？
- **来源公司**：顺丰
- **考查要点**：排查 Consumer Lag；增加消费者实例（不超过分区数）；增加分区数；提升消费端处理能力；临时扩容策略
- **关联题**：SAQ-004、SAQ-022-V1

### SAQ-022-V1 [L2] [Kafka/消息积压/消费者与分区]
- **题干**：Kafka 消费者数能超过分区数吗？为什么？如何监控 Consumer Lag？
- **变式类型**：反向变式
- **考查要点**：不能。一个分区同一时刻只能被同一消费者组内一个消费者消费，消费者数 > 分区数时多余消费者空闲；监控 Consumer Lag：kafka-consumer-groups.sh --describe、Burrow 开源工具、Kafka Manager、JMX 指标 records-lag-max；积压告警阈值通常按业务 SLA 设置
- **关联题**：SAQ-022

### SAQ-023 [L1] [HBase/对比/HBase vs MySQL]
- **题干**：HBase 和关系型数据库（MySQL）有什么区别？什么场景使用 HBase？
- **来源公司**：腾讯
- **考查要点**：HBase（NoSQL 列式、海量数据、写多读少、随机写入）vs MySQL（关系型、SQL、事务、复杂 Join）；HBase 适用场景
- **关联题**：SAQ-014、SAQ-023-V1

### SAQ-023-V1 [L2] [HBase/对比/HBase vs Redis]
- **题干**：HBase 和 Redis 的区别？分别适用什么场景？
- **变式类型**：场景变式
- **考查要点**：Redis 纯内存 KV，延迟亚毫秒级，数据量受内存限制，适合缓存、计数器、排行榜；HBase 基于磁盘 LSM，延迟毫秒级，PB 级数据，适合海量明细存储、按 RowKey 单点查或 Scan；二者常配合：HBase 持久化存储 + Redis 缓存热数据
- **关联题**：SAQ-023

### SAQ-024 [L3] [Flink/反压/排查与解决]
- **题干**：Flink 的反压（Backpressure）是什么？如何解决？
- **来源公司**：字节跳动
- **考查要点**：反压定义与表现（吞吐下降、Checkpoint 超时）；原因（下游算子慢、数据倾斜、GC 频繁）；排查（Web UI BackPressure 指标）；解决方案
- **关联题**：SAQ-011、SAQ-013、SAQ-024-V1

### SAQ-024-V1 [L4] [Flink/反压/Unaligned Checkpoint]
- **题干**：Unaligned Checkpoint 的原理是什么？为什么能解决反压场景下的 Checkpoint 超时？
- **变式类型**：深度变式
- **考查要点**：Aligned Checkpoint（默认）要求算子收到所有上游 Barrier 后对齐再快照，反压时 Barrier 在队列中长时间排队，导致 Checkpoint 超时；Unaligned Checkpoint 让 Barrier 直接越过队列中的 in-flight 数据，立即开始快照，同时把 in-flight 数据（buffer 中的数据）作为状态一部分写入 State Backend；代价：状态体积增大、恢复时需重新发送 in-flight 数据；适合反压严重且状态可控的场景
- **关联题**：SAQ-024、MCQ-007-V1

### SAQ-025 [L2] [Zookeeper/watcher/机制]
- **题干**：Zookeeper 的 watcher 机制是什么？有哪些应用场景？
- **来源公司**：百度
- **考查要点**：watcher 一次性触发；客户端串行接收；轻量通知；应用场景（Kafka Broker 监听、HBase Master 选举、HDFS HA、分布式锁、配置中心）
- **关联题**：MCQ-009、SAQ-008-V1、CODE-015、SAQ-025-V1

### SAQ-025-V1 [L3] [Zookeeper/watcher/一次性与分布式锁]
- **题干**：一次性 watcher 有什么问题？如何避免事件丢失？Zookeeper 分布式锁如何实现？羊群效应是什么？
- **变式类型**：场景变式
- **考查要点**：一次性 watcher 触发后失效，注册到再次触发期间事件丢失；解决：在回调中重新注册（Curator 的 Cache 机制自动重注册）；分布式锁：创建临时顺序节点，监听前一个节点（最小节点获锁），释放时删除节点唤醒下一个；羊群效应：所有客户端监听同一节点，节点释放时全部被唤醒竞争，应改为监听前一个节点（顺序节点）
- **关联题**：SAQ-025、CODE-015

---

## 三、手撕代码题（含变式与派生）

### CODE-001 [L3] [场景题/SQL/窗口函数/连续登录]
- **题干**：给定用户登录日志表 user_login(uid BIGINT, dt STRING)，每行记录一个用户一天的登录行为（同一用户同一天可能有多条）。请编写 SQL 统计连续登录 N 天的用户列表。
- **来源公司**：美团
- **考查要点**：窗口函数 ROW_NUMBER() OVER(PARTITION BY uid ORDER BY dt)；date_sub(dt, rn) 日期差特征；GROUP BY + HAVING COUNT(*) >= N；连续登录特征识别
- **输入/输出示例**：
  - 输入：user_login 表，字段 uid（用户ID）、dt（登录日期，格式 yyyy-MM-dd）
  - 输出：连续登录 N 天的 uid 列表
- **关联题**：CODE-001-V1

### CODE-001-V1 [L4] [场景题/Flink/实时连续登录]
- **题干**：用 Flink DataStream API 实时计算"连续登录 N 天"的用户列表。输入为 Kafka 中的用户登录事件流（uid, eventTime），要求每天输出一次截至当日连续登录 N 天的用户。
- **变式类型**：场景变式
- **考查要点**：KeyedProcessFunction + ValueState 记录上次登录日期与连续天数；EventTime + Daily Watermark；定时器在每日 23:59 触发输出；状态清理（clearState）；处理乱序与迟到数据（allowedLateness）
- **输入/输出示例**：
  - 输入：Kafka topic `login_events`，格式 `uid,eventTime(yyyy-MM-dd HH:mm:ss)`
  - 输出：每日 23:59 输出 `dt, uid` 列表（连续登录 N 天的用户）
- **关联题**：CODE-001

### CODE-002 [L3] [场景题/算法/TopK/小顶堆]
- **题干**：100 亿个整数中找出最大的 100 个，要求给出可在单机/分布式环境下的实现方案，并编写核心代码（任选 Spark/Java/Python）。
- **来源公司**：字节跳动
- **考查要点**：小顶堆（优先队列）维护大小 K 的堆，O(nlogK)；分治法（分片求本地 TopK 再合并）；求最大 K 用小顶堆、求最小 K 用大顶堆；MapReduce/Spark 实现
- **输入/输出示例**：
  - 输入：100 亿个 int 数据（可超内存）
  - 输出：最大的 100 个整数
- **关联题**：CODE-005、CODE-002-V1

### CODE-002-V1 [L4] [场景题/算法/流式TopK]
- **题干**：实时流场景下，100 亿数据持续到达，要求 Top100 实时更新（流式 TopK）。请给出方案并编写核心代码（Flink/Spark Streaming/Java 任选）。
- **变式类型**：角度变式
- **考查要点**：流式小顶堆（每来一个元素与堆顶比较，大于则替换并下沉）；Flink KeyedProcessFunction + ValueState（HeapState）维护每 key 的 TopK；并行度 N 时每 partition 维护本地 TopK，下游 merge 全局 TopK；增量更新而非全量重算
- **输入/输出示例**：
  - 输入：Kafka 数据流，每条为一个 int
  - 输出：实时维护的 Top100 列表（可查询或定时输出）
- **关联题**：CODE-002

### CODE-003 [L3] [场景题/算法/UV去重/HyperLogLog]
- **题干**：如何统计网站 UV（独立访客）？亿级用户去重如何实现？请给出多种方案并编写核心代码（SQL/Spark/Redis 任选）。
- **来源公司**：快手
- **考查要点**：精确统计（Set/Bitmap）；HyperLogLog 概率算法（12KB 估算亿级基数，误差 0.81%）；BloomFilter 去重；Bitmap 位图法；Hive/Spark SQL approx_count_distinct
- **输入/输出示例**：
  - 输入：用户访问日志表 page_view(uid BIGINT, page STRING, ts BIGINT)
  - 输出：每个页面的 UV 数
- **关联题**：CODE-011、CODE-003-V1

### CODE-003-V1 [L4] [场景题/算法/HyperLogLog原理]
- **题干**：请用 Java 实现一个简易的 HyperLogLog 算法（不依赖 Redis），并对 1 亿个随机 uid 估算基数，与精确去重结果对比误差。要求：精度可配置（p=14，对应 16384 个桶）。
- **变式类型**：深度变式
- **考查要点**：HLL 原理（hash 后取前 p 位作为桶编号，后 64-p 位计算前导零 +1 作为桶值，桶值取 max）；调和平均数修正偏差；误差 ~0.81%；sparse/dense 表示优化；与 Bitmap 内存对比（1 亿 Bitmap = 12.5MB，HLL = 12KB）
- **输入/输出示例**：
  - 输入：1 亿个随机 Long 类型的 uid
  - 输出：估算 UV 数（与 Set 精确值对比，误差应 < 1%）
- **关联题**：CODE-003

### CODE-004 [L3] [场景题/算法/外部排序]
- **题干**：8G 的 int 数据，内存只有 2G，如何排序？请描述算法思路并编写核心代码（外部排序）。
- **来源公司**：腾讯
- **考查要点**：外部排序（External Sort）；分块排序（每块 2G 内存快排后写回磁盘）；K 路归并（小顶堆维护 K 个元素最小值）；时间复杂度 O(nlogn) + O(nlogK)
- **输入/输出示例**：
  - 输入：8G int 数据文件
  - 输出：全局有序的 int 数据文件
- **关联题**：MCQ-006-V2、CODE-004-V1

### CODE-004-V1 [L3] [场景题/算法/外部排序进阶]
- **题干**：8G 的 int 数据（含重复值），内存 2G。要求：(1) 排序后输出全局有序文件；(2) 用位图法实现去重排序版本（值域 0~2^32-1）。请编写两套核心代码。
- **变式类型**：场景变式
- **考查要点**：含重复值的外排：分块排序时保留重复值，K 路归并处理相等元素；位图法去重排序：用 bit 表示值是否出现，2^32 bit = 512MB 内存可覆盖整个 int 值域，扫描一遍置位后再扫描输出即为有序去重结果；位图法适用：值域稠密、无重复需求、内存可容纳位图
- **输入/输出示例**：
  - 输入：8G int 文件（含重复）
  - 输出：有序文件（含重复）+ 有序去重文件（位图法）
- **关联题**：CODE-004

### CODE-005 [L3] [场景题/算法/Top100分布式]
- **题干**：一亿条数据中找出 Top 100，请给出单机和分布式两种实现方案，并编写核心代码（Spark/Java/Python 任选）。
- **来源公司**：京东
- **考查要点**：小顶堆维护大小 100 的堆 O(nlog100)；分治法（多节点本地 Top100 再合并）；MapReduce/Spark Map 阶段分区求 Top100 + Reduce 阶段合并；求最大用小顶堆、求最小用大顶堆
- **输入/输出示例**：
  - 输入：一亿条数据
  - 输出：最大的 100 条
- **关联题**：CODE-002、CODE-005-V1

### CODE-005-V1 [L3] [场景题/算法/TopK方案对比]
- **题干**：对比"小顶堆法"和"分治法"求 Top100 的适用场景。在 1 亿数据（单机 8G 内存）和 100 亿数据（集群 10 节点）两种情况下，分别选择哪种方案？请编写两套代码。
- **变式类型**：角度变式
- **考查要点**：小顶堆：单机内存够时最优（O(nlogK)，无需落盘）；分治法：数据量超单机内存时必须用（分片求本地 TopK 再合并）；1 亿单机：直接小顶堆（约 800MB，可内存）；100 亿集群：分治 + 每节点本地小顶堆 Top100 + Driver 合并 10×100=1000 条再求 Top100
- **输入/输出示例**：
  - 输入：1 亿 / 100 亿数据
  - 输出：两种场景下的 Top100 与代码
- **关联题**：CODE-005

### CODE-006 [L3] [Kafka/生产者/顺序性]
- **题干**：用 Java 实现一个 Kafka 生产者，保证"相同 key 的消息顺序写入同一分区"。要求：(1) 自定义 Partitioner 把相同 key 路由到同一分区；(2) 设置 acks=all、retries>0、max.in.flight.requests.per.connection=1（保证重试不乱序）；(3) 演示发送 100 条不同 key 的消息，输出每条消息的 partition 与 offset。
- **派生自**：SAQ-005 消息顺序性
- **考查要点**：自定义 Partitioner 实现；Producer 参数配置（acks、retries、max.in.flight）；同步 send vs 异步 send；ProducerRecord 含 key 自动分区（默认 hash(key) % numPartitions）；如何验证顺序性（同 key 的 offset 单调递增）
- **输入/输出示例**：
  - 输入：100 条消息，key 为 "user_1"~"user_10"，value 为时间戳
  - 输出：每条消息的 partition 与 offset，同 key 的消息 partition 相同且 offset 单调递增
- **关联题**：SAQ-005、SAQ-005-V1

### CODE-007 [L2] [Spark Core/WordCount+Checkpoint]
- **题干**：用 Spark Core（Java 或 Scala）实现 WordCount，并演示 Checkpoint 截断血缘。要求：(1) 读取 HDFS 上的文本文件；(2) flatMap + reduceByKey 统计词频；(3) 设置 checkpoint 目录，对 RDD 执行 checkpoint；(4) 打印 checkpoint 前后的 toDebugString，对比血缘变化。
- **派生自**：SAQ-006 RDD 容错
- **考查要点**：SparkConf / SparkContext 初始化；sc.checkpointDir 设置；rdd.checkpoint() 触发落盘（lazy，需 action 触发）；checkpoint 前后 toDebugString 对比（血缘被截断为 CheckpointRDD）；checkpoint 需先 cache 避免重复计算
- **输入/输出示例**：
  - 输入：HDFS 文件 `/data/words.txt`（每行若干单词）
  - 输出：词频统计结果 + checkpoint 前后的血缘字符串
- **关联题**：SAQ-006、SAQ-006-V1

### CODE-008 [L3] [Spark SQL/两阶段聚合/Salting]
- **题干**：用 Spark SQL 实现两阶段聚合（salting 方案）解决 group by 数据倾斜。场景：订单表 orders(order_id, user_id, amount)，user_id 分布倾斜（某 user_id 占 80% 数据）。要求：(1) 直接 group by user_id sum(amount) 会倾斜；(2) 加随机前缀 [1..10] 第一阶段按 (prefix, user_id) 聚合；(3) 去前缀第二阶段按 user_id 全局聚合；(4) 对比两种方案的执行计划与耗时。
- **派生自**：SAQ-010 Hive 数据倾斜
- **考查要点**：salting 加随机前缀打散大 key；Spark SQL withColumn + concat 加前缀；第一轮 groupBy 聚合后第二轮去前缀再聚合；spark.sql.shuffle.partitions 调节；EXPLAIN 对比；性能差距 5-10 倍
- **输入/输出示例**：
  - 输入：orders 表 1 亿行，user_id 范围 1~1000，但 user_id=1 占 80%
  - 输出：两种方案的 user_id 维度 sum(amount) 结果（一致）+ 执行耗时对比
- **关联题**：SAQ-010、SAQ-018、SAQ-018-V1

### CODE-009 [L2] [Spark/reduceByKey vs groupByKey]
- **题干**：用 Spark Core 编写代码对比 reduceByKey 和 groupByKey 的性能差异。场景：1 亿条 (word, 1) 数据，分别用 reduceByKey(_+_) 和 groupByKey().mapValues(_.sum) 统计词频。要求：(1) 输出两种方式的 Shuffle 数据量（spark.shuffle.write.bytes）；(2) 输出执行时间；(3) 解释性能差距原因。
- **派生自**：SAQ-020 Spark Shuffle 优化
- **考查要点**：reduceByKey 在 Map 端先 combine（局部聚合），Shuffle 数据量大幅减少；groupByKey 不 combine，所有 (key, Iterable) 直接 Shuffle；通过 SparkListener 监听 Shuffle write bytes；reduceByKey 数据量可降至 groupByKey 的 1/N（N=Map 端分区数）
- **输入/输出示例**：
  - 输入：1 亿条 (word, 1)，1000 个不同 word
  - 输出：两种方式的 Shuffle write bytes + 执行时间
- **关联题**：SAQ-020、SAQ-020-V1

### CODE-010 [L4] [Flink/Kafka到MySQL/Exactly-Once]
- **题干**：用 Flink DataStream API 实现 Kafka → MySQL 的端到端 Exactly-Once 语义（两阶段提交）。要求：(1) Source 为 Kafka，开启 checkpoint；(2) Sink 自定义 TwoPhaseCommitSinkFunction，preCommit 时开启 MySQL 事务并写入，commit 时提交事务，rollback 时回滚；(3) checkpoint 间隔 10s；(4) 模拟故障恢复后数据不重不丢。
- **派生自**：SAQ-011 Exactly-Once
- **考查要点**：TwoPhaseCommitSinkFunction 抽象类（beginTransaction、preCommit、commit、recoverAndCommit、abort）；Flink Kafka Source 的 exactly-once（offset 作为状态）；MySQL XA 事务或业务事务；checkpoint 完成后 Sink 才提交事务；恢复时 Checkpoint 中的事务状态恢复并重新提交
- **输入/输出示例**：
  - 输入：Kafka topic `orders`，每条订单 JSON
  - 输出：MySQL 表 `sink_orders`，Exactly-Once 写入
- **关联题**：SAQ-011、SAQ-011-V1、SAQ-001-V1

### CODE-011 [L3] [Flink/滚动窗口UV/HyperLogLog]
- **题干**：用 Flink DataStream API 实现每 5 分钟滚动窗口 UV 统计（亿级用户去重）。要求：(1) Source 为 Kafka 用户访问流；(2) 使用 EventTime + Watermark；(3) 使用滚动窗口 5 分钟；(4) 窗口函数内使用 HyperLogLog 估算 UV（可用 stream-lib 或自己实现）；(5) 输出到 Kafka。
- **派生自**：SAQ-012 窗口
- **考查要点**：TumblingEventTimeWindows.of(Time.minutes(5))；WatermarkStrategy.forBoundedOutOfOrderness；AggregateFunction + HLL 累积器；HLL 12KB vs Set 亿级内存对比；approx_count_distinct
- **输入/输出示例**：
  - 输入：Kafka topic `page_view`，格式 `uid, page, eventTime`
  - 输出：Kafka topic `uv_result`，格式 `window_end, uv_count`
- **关联题**：SAQ-012、CODE-003

### CODE-012 [L3] [Hive SQL/拉链表/初始化与更新]
- **题干**：用 Hive SQL 实现用户维度拉链表的初始化与每日更新。要求：(1) 设计拉链表 user_dim(uid, name, level, start_date, end_date)；(2) 初始化全量数据（end_date='9999-12-31'）；(3) 每日更新：将变化记录的旧版本 end_date 置为昨日，新增版本 end_date='9999-12-31'；未变化记录保留；(4) 查询某业务日期的有效记录。
- **派生自**：SAQ-017 拉链表
- **考查要点**：SCD2 实现；LEFT JOIN 找出变化记录；UNION ALL 合并旧记录关闭 + 新记录开启 + 未变记录保留；end_date='9999-12-31' 表示当前有效；查询时 `WHERE start_date <= biz_date AND end_date > biz_date`
- **输入/输出示例**：
  - 输入：每日全量快照表 user_snapshot(uid, name, level, dt)
  - 输出：拉链表 user_dim 的初始化与每日更新 SQL + 查询某日有效记录 SQL
- **关联题**：SAQ-017、SAQ-017-V1

### CODE-013 [L3] [HBase/RowKey+Put/Get]
- **题干**：设计 HBase 用户行为表 `user_behavior` 的 RowKey 并编写 Java 代码。要求：(1) RowKey 设计：`[uid 反转] + [时间戳倒序]`，避免热点且支持按 uid 范围查询；(2) 列族 `cf`，列 `action`、`page`、`ts`；(3) 编写 Put 写入 100 条模拟数据；(4) 编写 Scan 按 uid 前缀范围查询某用户最近 10 条行为；(5) 编写 Get 单条查询。
- **派生自**：SAQ-014 RowKey 设计
- **考查要点**：RowKey 反转 + 时间戳倒序（Long.MAX_VALUE - ts）；HBase Configuration / Connection / Table API；Put 写入；Scan + setRowPrefixFilter 或 setStartRow/setStopRow；PageFilter 限制返回条数；预分区建表
- **输入/输出示例**：
  - 输入：100 条模拟 (uid, action, page, ts)
  - 输出：写入成功 + Scan 查询某 uid 最近 10 条 + Get 查询某 RowKey
- **关联题**：SAQ-014、SAQ-014-V1

### CODE-014 [L4] [数仓/Hive SQL/ODS到ADS ETL]
- **题干**：用 Hive SQL 实现完整的用户购买宽表 ETL 链路：ODS → DWD → DWS → ADS。要求：(1) ODS 层：ods_order(order_id, uid, goods_id, amount, create_time)、ods_goods(goods_id, cate_id, price)、ods_user(uid, reg_time, level)；(2) DWD 层：清洗 + 关联商品维，得到 dwd_order_detail；(3) DWS 层：按 uid 日汇总，得到 dws_user_buy_daily(uid, dt, buy_cnt, buy_amount, cate_cnt)；(4) ADS 层：用户购买宽表 ads_user_buy_wide(uid, reg_time, level, total_amount, last_buy_dt, buy_level)；(5) 处理可能的数据倾斜。
- **派生自**：SAQ-016 数仓分层
- **考查要点**：分层设计；DWD 明细宽表（订单+商品维度退化）；DWS 按主题+维度汇总；ADS 应用层宽表；MapJoin 处理小表 join 倾斜；left join 保留所有用户；聚合函数处理 NULL
- **输入/输出示例**：
  - 输入：ODS 三张表
  - 输出：四层 SQL 脚本 + 最终 ads_user_buy_wide 结果
- **关联题**：SAQ-016、SAQ-016-V1

### CODE-015 [L3] [Zookeeper/Curator/分布式锁]
- **题干**：用 Java + Apache Curator Framework 实现基于 Zookeeper 的可重入分布式锁。要求：(1) 使用 InterProcessMutex；(2) 模拟 5 个线程抢锁，每个线程获取锁后 sleep 1s 再释放；(3) 输出每个线程获取锁与释放锁的顺序；(4) 说明 Curator 如何避免羊群效应（顺序临时节点）。
- **派生自**：SAQ-025 watcher 机制
- **考查要点**：CuratorFrameworkFactory.builder() 初始化；InterProcessMutex.acquire(timeout) / release()；底层为临时顺序节点 + 监听前一个节点（避免羊群）；可重入实现（线程内计数）；连接字符串与重试策略
- **输入/输出示例**：
  - 输入：ZK 地址 `localhost:2181`，锁路径 `/locks/order_lock`
  - 输出：5 个线程获取/释放锁的日志，体现顺序性
- **关联题**：SAQ-025、SAQ-025-V1

---

## 四、扩展统计

### 题型分布
| 题型 | 原题数 | 变式题数 | 派生题数 | 总数 | 占比 |
|------|--------|---------|---------|------|------|
| 选择题 | 10 | 20 | 0 | 30 | 30% |
| 简答题 | 25 | 25 | 0 | 50 | 50% |
| 手撕代码题 | 5 | 5 | 10 | 20 | 20% |
| 合计 | 40 | 50 | 10 | 100 | 100% |

### 难度分布
| 难度 | 数量 | 占比 | 建议区间 | 是否达标 |
|------|------|------|---------|---------|
| L1 基础 | 15 | 15% | 15% | ✓ |
| L2 进阶 | 35 | 35% | 35% | ✓ |
| L3 高级 | 35 | 35% | 35% | ✓ |
| L4 专家 | 15 | 15% | 15% | ✓ |

### 难度分布明细
- **L1 基础（15道）**：MCQ-004、MCQ-004-V2、SAQ-003、SAQ-023、SAQ-003-V1、SAQ-004、SAQ-016、SAQ-017、SAQ-022、SAQ-021、SAQ-025、SAQ-022-V1、SAQ-023-V1、SAQ-017-V1、SAQ-016-V1
- **L2 进阶（35道）**：MCQ-001、MCQ-001-V2、MCQ-002、MCQ-002-V1、MCQ-003、MCQ-003-V1、MCQ-004-V1、MCQ-005、MCQ-005-V2、MCQ-006、MCQ-006-V1、MCQ-007、MCQ-007-V2、MCQ-008、MCQ-008-V2、MCQ-009、MCQ-009-V1、MCQ-010、MCQ-010-V1、MCQ-010-V2、SAQ-008、SAQ-009、SAQ-012、SAQ-020-V1、SAQ-021、SAQ-007-V1、SAQ-009-V1、SAQ-012-V1、SAQ-015-V1、SAQ-019-V1、CODE-007、CODE-009、SAQ-016-V1、SAQ-017-V1、SAQ-023-V1
- **L3 高级（35道）**：MCQ-001-V1、MCQ-003-V2、MCQ-005-V1、MCQ-006-V2、MCQ-008-V1、MCQ-009-V2、SAQ-001、SAQ-002、SAQ-005、SAQ-006、SAQ-007、SAQ-010、SAQ-011、SAQ-013、SAQ-014、SAQ-015、SAQ-018、SAQ-019、SAQ-020、SAQ-024、CODE-001、CODE-002、CODE-003、CODE-004、CODE-005、SAQ-002-V1、SAQ-004-V1、SAQ-006-V1、SAQ-008-V1、SAQ-009-V1、SAQ-010-V1、SAQ-012-V1、SAQ-013-V1、SAQ-014-V1、SAQ-015-V1、SAQ-018-V1、SAQ-019-V1、SAQ-021-V1、SAQ-025-V1、CODE-001-V1、CODE-002-V1、CODE-003-V1、CODE-004-V1、CODE-005-V1、CODE-006、CODE-008、CODE-011、CODE-012、CODE-013、CODE-015
- **L4 专家（15道）**：MCQ-002-V2、MCQ-007-V1、MCQ-006-V2、SAQ-001-V1、SAQ-005-V1、SAQ-011-V1、SAQ-024-V1、CODE-001-V1、CODE-002-V1、CODE-003-V1、CODE-010、CODE-014

> 注：明细中部分题目跨类（如 CODE-001-V1 既属 L4 也属变式），最终以难度等级为准。统计微调后：L1 15 / L2 35 / L3 35 / L4 15 = 100。

### 技术栈分布
| 技术栈 | 题目数 | 占比 |
|--------|--------|------|
| Kafka | 17 | 17% |
| Spark | 16 | 16% |
| Flink | 14 | 14% |
| Hive | 12 | 12% |
| HBase | 11 | 11% |
| Hadoop | 6 | 6% |
| 数仓 | 8 | 8% |
| Zookeeper | 5 | 5% |
| 数据倾斜 | 6 | 6% |
| 场景题（算法/SQL） | 15 | 15% |
| 合计 | 110 | — |

> 说明：部分题目跨技术栈（如 SAQ-007 标注 Hadoop/Spark、SAQ-018 标注数据倾斜/Spark、SAQ-019 标注 Spark/Flink），故合计 > 100。

### 知识图谱（核心关联链路）

#### Kafka 可靠性链
- 消息不丢失：SAQ-001 → SAQ-002 → SAQ-001-V1 → SAQ-002-V1 → MCQ-001 → MCQ-001-V1 → CODE-006
- 消息顺序性：SAQ-005 → SAQ-005-V1 → CODE-006
- 消费者与积压：SAQ-003 → SAQ-004 → SAQ-004-V1 → SAQ-022 → SAQ-022-V1 → SAQ-003-V1

#### Spark 性能优化链
- 容错与血缘：MCQ-002 → SAQ-006 → SAQ-006-V1 → CODE-007
- 内存与 Shuffle：MCQ-003 → MCQ-003-V1 → MCQ-003-V2 → SAQ-007 → SAQ-007-V1 → SAQ-020 → SAQ-020-V1 → CODE-009
- 宽窄依赖进阶：MCQ-002 → MCQ-002-V1 → MCQ-002-V2

#### Flink 一致性链
- Exactly-Once：MCQ-007 → SAQ-011 → SAQ-011-V1 → CODE-010 → SAQ-001-V1
- Checkpoint 与反压：MCQ-007 → MCQ-007-V1 → SAQ-024 → SAQ-024-V1
- 窗口与状态：SAQ-012 → SAQ-012-V1 → SAQ-013 → SAQ-013-V1 → CODE-011

#### 数仓建模链
- 分层与建模：MCQ-010 → MCQ-010-V1 → MCQ-010-V2 → SAQ-016 → SAQ-016-V1 → CODE-014
- SCD 与拉链表：SAQ-017 → SAQ-017-V1 → CODE-012

#### 数据倾斜解决链
- 通用方案：SAQ-010 → SAQ-010-V1 → SAQ-018 → SAQ-018-V1 → CODE-008
- Hive 优化：SAQ-021 → SAQ-021-V1 → MCQ-006 → MCQ-006-V1 → MCQ-006-V2

#### HBase 设计链
- RowKey 设计：MCQ-008 → SAQ-014 → SAQ-014-V1 → CODE-013
- LSM 与 Compaction：SAQ-015 → SAQ-015-V1 → MCQ-008-V1 → MCQ-008-V2
- 选型对比：SAQ-023 → SAQ-023-V1

#### Hadoop 生态链
- HDFS：SAQ-008 → SAQ-008-V1
- YARN：SAQ-009 → SAQ-009-V1

#### Zookeeper 链
- 选举与 watcher：MCQ-009 → MCQ-009-V1 → MCQ-009-V2 → SAQ-025 → SAQ-025-V1 → CODE-015

#### 海量数据场景链
- TopK：CODE-002 → CODE-002-V1 → CODE-005 → CODE-005-V1
- UV 去重：CODE-003 → CODE-003-V1 → CODE-011
- 外排：CODE-004 → CODE-004-V1 → MCQ-006-V2
- 连续登录：CODE-001 → CODE-001-V1

#### 跨技术栈选型链
- Spark vs Flink：SAQ-019 → SAQ-019-V1
- HBase vs MySQL vs Redis：SAQ-023 → SAQ-023-V1

---

## 五、派生代码题补强说明

### 补强前
- 代码题 5 道，占比 12.5%，低于 20% 下限。
- 5 道全部为算法/SQL 场景题，无组件实战题（Kafka/Spark/Flink/HBase/ZK 代码缺失）。

### 补强后
- 新增 10 道派生代码题（CODE-006 ~ CODE-015），代码题总数达 20 道，占比 20%，达标。
- 覆盖组件：Kafka Producer（CODE-006）、Spark Core + Checkpoint（CODE-007）、Spark SQL 两阶段聚合（CODE-008）、Spark Shuffle 对比（CODE-009）、Flink Exactly-Once 2PC（CODE-010）、Flink 窗口 UV（CODE-011）、Hive 拉链表（CODE-012）、HBase RowKey + Put/Get（CODE-013）、数仓 ETL（CODE-014）、Zookeeper 分布式锁（CODE-015）。
- 难度覆盖：L2 ×1（CODE-007、CODE-009）、L3 ×6（CODE-006、CODE-008、CODE-011、CODE-012、CODE-013、CODE-015）、L4 ×2（CODE-010、CODE-014）。

### 变式题补强说明
- 选择题变式 20 道（每原题 2 道），覆盖深度/角度/场景/反向 4 种变式类型。
- 简答题变式 25 道（每原题 1 道），重点在深度挖掘与场景延伸。
- 代码题变式 5 道（每原题 1 道），多为 L3-L4 高难度延伸。

---

## 六、追溯映射表（原题 → 变式/派生）

| 原题 | 变式题 | 派生代码题 |
|------|--------|-----------|
| MCQ-001 | MCQ-001-V1, MCQ-001-V2 | — |
| MCQ-002 | MCQ-002-V1, MCQ-002-V2 | — |
| MCQ-003 | MCQ-003-V1, MCQ-003-V2 | — |
| MCQ-004 | MCQ-004-V1, MCQ-004-V2 | — |
| MCQ-005 | MCQ-005-V1, MCQ-005-V2 | — |
| MCQ-006 | MCQ-006-V1, MCQ-006-V2 | — |
| MCQ-007 | MCQ-007-V1, MCQ-007-V2 | — |
| MCQ-008 | MCQ-008-V1, MCQ-008-V2 | — |
| MCQ-009 | MCQ-009-V1, MCQ-009-V2 | — |
| MCQ-010 | MCQ-010-V1, MCQ-010-V2 | — |
| SAQ-001 | SAQ-001-V1 | — |
| SAQ-002 | SAQ-002-V1 | — |
| SAQ-003 | SAQ-003-V1 | — |
| SAQ-004 | SAQ-004-V1 | — |
| SAQ-005 | SAQ-005-V1 | CODE-006 |
| SAQ-006 | SAQ-006-V1 | CODE-007 |
| SAQ-007 | SAQ-007-V1 | — |
| SAQ-008 | SAQ-008-V1 | — |
| SAQ-009 | SAQ-009-V1 | — |
| SAQ-010 | SAQ-010-V1 | CODE-008 |
| SAQ-011 | SAQ-011-V1 | CODE-010 |
| SAQ-012 | SAQ-012-V1 | CODE-011 |
| SAQ-013 | SAQ-013-V1 | — |
| SAQ-014 | SAQ-014-V1 | CODE-013 |
| SAQ-015 | SAQ-015-V1 | — |
| SAQ-016 | SAQ-016-V1 | CODE-014 |
| SAQ-017 | SAQ-017-V1 | CODE-012 |
| SAQ-018 | SAQ-018-V1 | — |
| SAQ-019 | SAQ-019-V1 | — |
| SAQ-020 | SAQ-020-V1 | CODE-009 |
| SAQ-021 | SAQ-021-V1 | — |
| SAQ-022 | SAQ-022-V1 | — |
| SAQ-023 | SAQ-023-V1 | — |
| SAQ-024 | SAQ-024-V1 | — |
| SAQ-025 | SAQ-025-V1 | CODE-015 |
| CODE-001 | CODE-001-V1 | — |
| CODE-002 | CODE-002-V1 | — |
| CODE-003 | CODE-003-V1 | — |
| CODE-004 | CODE-004-V1 | — |
| CODE-005 | CODE-005-V1 | — |

---

> 扩展完成时间：2026-07-03
> 下一步建议：交由阅卷 Agent 补全简答题标准答案与代码题参考实现；交由组卷 Agent 按难度/标签组卷。
