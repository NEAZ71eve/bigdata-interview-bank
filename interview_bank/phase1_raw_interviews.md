# 大数据面经原始数据集

## 采集元信息
- 采集时间：2026-07-03
- 数据来源：牛客网面经帖 + 补充搜索（牛客网大数据面经汇总帖、大数据面试题V3.0、各技术栈专项面经帖）
- 覆盖技术栈：Hadoop / Zookeeper / Hive / Kafka / Spark / Flink / HBase / 数仓 / 数据倾斜 / 海量数据场景题
- 覆盖公司：字节跳动、美团、快手、阿里、京东、百度、腾讯、OPPO、微众、SHEIN、顺丰、贝壳、美的
- 原始题目总数：40道
- 采集人：collector（爬虫采集Agent）

> 说明：本数据集为原始采集层（Phase1），未做去重与标准化，仅做精准过滤（剔除纯Java后端八股/JVM/Spring/前端/测试无关内容）。每道题标注技术栈、来源公司、出现频率、题目原文、网友答案摘要与追问。

---

## 题目列表

### RAW-001
- **题目**：Kafka 是如何实现高吞吐的？介绍一下零拷贝。
- **技术栈**：Kafka
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：四大核心机制：①顺序读写磁盘（消息追加写，顺序写多数情况比随机写内存还快）；②Partition并行处理（多分区分布在不同Broker，磁盘间并行）；③充分利用页缓存Page Cache（Broker写磁盘只是写Page Cache）；④零拷贝技术（sendfile，减少内核缓存到用户缓存的CPU拷贝，磁盘→内核缓存→网卡）。
- **追问**：零拷贝具体用的是什么系统调用？DMA是什么？Page Cache数据未落盘会丢吗？

### RAW-002
- **题目**：如何保证 Kafka 的消息不丢失？
- **技术栈**：Kafka
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：从生产者、Broker、消费者三端保证。生产者端：设置 acks=all/-1（ISR所有副本写入成功才响应），开启重试；Broker端：配置副本因子 replication.factor>=3，min.insync.replicas>=2，开启幂等性避免重复；消费者端：关闭自动提交offset，手动提交，处理完业务逻辑后再提交。
- **追问**：acks=1 和 acks=all 的区别？幂等性只能保证什么？多分区能保证全局有序吗？

### RAW-003
- **题目**：Kafka 如何选举 leader？ISR 机制是什么？
- **技术栈**：Kafka
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：ISR（In-Sync Replicas）是与Leader保持同步的副本集合，落后太多的副本会被移出ISR，追上后重新加入。Leader故障时，从ISR集合中选择第一个存活的副本作为新Leader（优先级副本）。Controller负责监听Broker存活状态并触发Leader选举。消费组Leader的选举由GroupCoordinator负责，第一个加入消费组的消费者即为leader。
- **追问**：ISR为空怎么办？unclean.leader.election.enable=true有什么风险？Controller是怎么选出来的？

### RAW-004
- **题目**：Kafka 消费者是推模式还是拉模式？为什么？
- **技术栈**：Kafka
- **来源公司**：快手
- **出现频率**：中频
- **网友答案摘要**：Pull拉模式。消费者主动拉取Kafka服务器的消息。好处：①自主控制速率，根据自身处理能力决定拉取速率；②节约资源，避免服务器主动推送造成资源消耗；③容错性，出错可重新拉取相同消息重试；④消息积压控制。Push模式的问题：多分区同时推送会导致消费能力有限时数据积压。
- **追问**：拉模式有什么缺点？如何避免消费者一直拉到空消息（轮询空转）？

### RAW-005
- **题目**：Kafka 消费者组是什么？同一个消费者组的消费者能消费同一个分区吗？
- **技术栈**：Kafka
- **来源公司**：京东
- **出现频率**：中频
- **网友答案摘要**：消费者组是多个消费者组成的逻辑组，组内每个消费者负责不同分区，一个分区只能被组内一个消费者消费（保证顺序性和负载均衡）。某实例挂掉时，其他实例自动承担其分区（Rebalance）。不同消费者组之间互不影响，可各自消费完整数据。
- **追问**：Rebalance触发条件有哪些？Rebalance有什么问题？如何减少Rebalance？

### RAW-006
- **题目**：Kafka 如何保证消息的顺序性？如何实现全局有序？
- **技术栈**：Kafka
- **来源公司**：百度
- **出现频率**：中频
- **网友答案摘要**：分区内有序（消息写入分区后offset递增不可修改）。保证顺序性方案：①相同key的消息写入同一分区（按key分区，key的hash值%partition数）；②单分区可实现全局有序（但牺牲并行度，吞吐低）。多分区无法保证全局有序，幂等性也只能保证单分区消息有序不重复。
- **追问**：业务需要全局有序又需要高吞吐怎么权衡？

### RAW-007
- **题目**：Spark 的 transformation 和 action 有什么区别？宽依赖和窄依赖？Stage 怎么划分？
- **技术栈**：Spark
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：transformation是转换算子（map/flatMap/filter等），懒加载，只记录依赖关系不立即执行；action是行动算子（collect/count/save等），触发作业提交执行。窄依赖：父RDD一个分区最多被子RDD一个分区使用（map/filter）；宽依赖：父RDD一个分区被子RDD多个分区使用（groupByKey/reduceByKey），会产生Shuffle。Stage划分：DAGScheduler从后往前遍历RDD，遇到宽依赖就断开划分Stage，每个Stage内都是窄依赖可流水线并行执行。
- **追问**：为什么要设计宽窄依赖？窄依赖为什么可以流水线执行？列举常见的宽依赖算子。

### RAW-008
- **题目**：Spark 的内存管理是怎样的？
- **技术栈**：Spark
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：Spark内存分为四块（统一内存管理）：①Reserved Memory（保留内存，300MB）；②User Memory（用户内存，存储用户数据结构和UDF中的对象）；③Storage Memory（存储内存，缓存RDD/broadcast变量）；④Execution Memory（执行内存，Shuffle/Join/排序等）。Storage和Execution之间可动态借用，但一方内存不足时不能抢占对方已使用的内存。可通过spark.memory.fraction、spark.memory.storageFraction调参。
- **追问**：Spark内存溢出怎么排查？如何调优？堆外内存是什么？

### RAW-009
- **题目**：Spark 的容错机制是怎样的？RDD 如何实现容错？
- **技术栈**：Spark
- **来源公司**：阿里
- **出现频率**：中频
- **网友答案摘要**：RDD通过血缘关系（Lineage）实现容错。RDD是不可变的分布式数据集，记录了父RDD的依赖关系和转换算子。当某个分区数据丢失时，Spark根据DAG重新计算丢失的分区（不需回滚整个作业）。窄依赖只需重算父分区，宽依赖需重算所有父分区（可借助Checkpoint截断血缘）。Checkpoint将RDD持久化到HDFS，截断血缘，适合 lineage 过长时使用。
- **追问**：Checkpoint和Cache的区别？Cache是lazy还是eager？ lineage过长有什么问题？

### RAW-010
- **题目**：MapReduce 的 Shuffle 过程？MapReduce 与 Spark 的优劣对比？
- **技术栈**：Hadoop / Spark
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：Shuffle过程：Map端输出数据→环形缓冲区（默认100MB，80%溢写）→排序（快排，按key分区排序）→Spill到磁盘→Merge合并成一个大文件；Reduce端从Map端拉取数据（HTTP）→Merge合并→分组（相同key交给同一Reduce处理）→Reduce处理。MapReduce基于磁盘，每次Shuffle都落盘，磁盘IO高；Spark基于内存，中间结果可缓存内存，DAG调度减少Shuffle，迭代计算性能远超MR。但Spark内存消耗大，MR更稳定适合超大规模离线批处理。
- **追问**：Shuffle为什么是性能瓶颈？Map端combiner有什么作用？Spark Shuffle有哪些实现（HashSort）？

### RAW-011
- **题目**：HDFS 的读写流程是怎样的？副本机制是怎么样的？
- **技术栈**：Hadoop
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：写流程：客户端向NameNode请求上传文件→NameNode检查目录/权限/是否已存在，返回可上传→客户端请求上传第一个block，NameNode返回DataNode列表（按机架感知策略选3个节点，默认副本数3）→客户端与第一个DataNode建立pipeline，第一个再与第二个建立，第二个与第三个建立（pipeline流水线）→数据以packet为单位逐级传输并ack确认→block传输完成，关闭连接，继续下一个block。读流程：客户端向NameNode请求下载文件→NameNode返回文件block列表及所在DataNode地址（按距离排序）→客户端就近选择DataNode建立连接读取block→校验checksum→拼装完整文件。副本放置策略：第一个副本本地，第二个副本不同机架，第三个副本与第二个同机架不同节点。
- **追问**：机架感知是什么？为什么第二个副本放不同机架？NameNode挂了怎么办？HDFS如何保证数据完整性？

### RAW-012
- **题目**：YARN 的调度流程是怎样的？有哪些调度器？
- **技术栈**：Hadoop
- **来源公司**：京东
- **出现频率**：中频
- **网友答案摘要**：YARN调度流程：客户端提交作业→ResourceManager分配第一个Container（运行ApplicationMaster）→ApplicationMaster向ResourceManager注册并申请资源→ResourceManager返回资源列表→ApplicationMaster与NodeManager通信启动Container运行Task→Task运行完毕，ApplicationMaster向ResourceManager注销。三种调度器：①FIFO Scheduler（先进先出，单队列）；②Capacity Scheduler（多队列，容量保证，Hadoop默认）；③Fair Scheduler（公平共享，多队列，所有作业平均分配资源）。
- **追问**：Capacity和Fair调度器的区别？如何配置队列？作业失败如何重试？

### RAW-013
- **题目**：Hive 的内部表和外部表有什么区别？生产环境怎么选？
- **技术栈**：Hive
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：核心区别在删除时的行为：内部表（managed table）删除时元数据和原始数据全部删除；外部表（external table）删除时只删除元数据，原始数据保留。生产环境绝大多数场景创建外部表（多人共同使用数据，数据安全），只有自己使用的临时表才创建内部表。建表语句：CREATE EXTERNAL TABLE ... LOCATION '/path'。
- **追问**：外部表能不能转内部表？分区表和分桶表的区别？

### RAW-014
- **题目**：Hive 的数据倾斜如何处理？
- **技术栈**：Hive / 数据倾斜
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：倾斜原因：①key分布不均匀；②业务数据本身特性（如大量NULL值）；③SQL语句造成（如count distinct）。解决方法：①参数调节：开启Map端聚合 hive.map.aggr=true 和 hive.groupby.skewindata=true（两阶段聚合）；开启MapJoin hive.auto.convert.join=true（小表加载到内存在Map端join）；②SQL调节：小表join大表用MapJoin；大表join大表对空key过滤或给空key赋随机数打散；count distinct大量相同特殊值单独处理NULL再union；③增加reduce数；④选用join key分布最均匀的表作为驱动表。
- **追问**：group by倾斜和join倾斜分别怎么解决？两阶段聚合的原理是什么？

### RAW-015
- **题目**：Hive 的 ORC 和 Parquet 文件格式有什么区别？为什么用列式存储？
- **技术栈**：Hive
- **来源公司**：快手
- **出现频率**：中频
- **网友答案摘要**：都是列式存储格式。列式存储优势：①只读取需要的列，减少IO；②同列数据类型一致，压缩比高（字典编码/游程编码/RLE）；③适合聚合分析。ORC（Optimized Row Columnar）：Hive原生优化格式，压缩比高，内置索引（min/max/bloom filter），对Hive支持最好。Parquet：通用列式格式，Spark/Impala/Presto都支持，嵌套数据结构支持好。生产环境Hive优先ORC，多引擎混用优先Parquet。
- **追问**：列式存储有什么缺点？什么场景适合行式存储？ORC的stripe是什么？

### RAW-016
- **题目**：Hive 的四个 by（order by / sort by / distribute by / cluster by）有什么区别？生产环境用哪个？
- **技术栈**：Hive
- **来源公司**：京东
- **出现频率**：中频
- **网友答案摘要**：order by：全局排序，只有一个Reduce，数据量大时会OOM，生产环境慎用；sort by：每个Reduce内部排序，不是全局有序；distribute by：控制数据分发到哪个Reduce（按字段hash）；cluster by：distribute by + sort by，且排序字段和分发字段相同（只能升序）。生产环境 sort by + distribute by 组合用得最多，在分区内部排序效果好。
- **追问**：京东40-50T数据用order by会怎样？如何实现全局有序？

### RAW-017
- **题目**：Flink 的 Checkpoint 机制是怎样的？Watermark 是什么？
- **技术栈**：Flink
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：Checkpoint是Flink实现容错的核心机制，本质是所有任务状态在某个时间点的一份快照。流程：JobManager的CheckpointCoordinator向Source节点注入Barrier（屏障）→Barrier随数据流向下流动→算子收到Barrier后对齐状态并持久化到State Backend→所有算子完成快照后向JobManager汇报成功。Watermark（水位线）是衡量事件时间进度的机制，表示小于该Watermark的数据都已到达，用于处理乱序数据。Watermark = 最大事件时间 - 允许最大延迟时间。当Watermark推进到窗口结束时间，窗口触发计算。
- **追问**：Checkpoint和Savepoint的区别？Barrier对齐有什么问题？Aligned Checkpoint vs Unaligned Checkpoint？Watermark如何传递？

### RAW-018
- **题目**：Flink 如何实现 Exactly-Once 语义？
- **技术栈**：Flink
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：端到端Exactly-Once需要三部分配合：①Source端：可重放的数据源（如Kafka，记录offset）；②Flink内部：Checkpoint机制保证状态一致性（Barrier对齐，Chandy-Lamport算法）；③Sink端：幂等写入（如Redis/MySQL按主键覆盖）或两阶段提交（2PC，如Kafka事务）。Flink Kafka Producer通过事务提交，Checkpoint成功后才提交事务，失败则回滚。
- **追问**：两阶段提交的流程？At-Least-Once和Exactly-Once的区别？Sink端幂等和事务怎么选？

### RAW-019
- **题目**：Flink 的窗口有哪些类型？窗口函数怎么用？
- **技术栈**：Flink
- **来源公司**：快手
- **出现频率**：中频
- **网友答案摘要**：窗口类型：①滚动窗口（Tumbling Window，固定大小不重叠）；②滑动窗口（Sliding Window，固定大小可重叠，有滑动步长）；③会话窗口（Session Window，基于数据活跃度，无活动间隔超时关闭）；④全局窗口（Global Window，需配合Trigger）。按时间语义分：Processing Time窗口和Event Time窗口。窗口函数：ReduceFunction（增量聚合，两两合并）、AggregateFunction（增量聚合，更灵活）、ProcessWindowFunction（全量聚合，可获取窗口元数据，性能低）。建议用 Reduce/Aggregate + ProcessWindowFunction 组合。
- **追问**：滚动和滑动窗口的区别？会话窗口的gap怎么设？Event Time窗口 Watermark 到了才触发吗？

### RAW-020
- **题目**：Flink 的状态管理是怎样的？状态有哪几种？
- **技术栈**：Flink
- **来源公司**：阿里
- **出现频率**：中频
- **网友答案摘要**：Flink是有状态的流处理框架。状态分类：①Keyed State（键控状态，只能用在KeyedStream上）：ValueState、ListState、MapState、ReducingState、AggregatingState；②Operator State（算子状态，如Kafka Source记录offset）：ListState。按存储分：Managed State（Flink管理，推荐）和 Raw State（原始字节）。State Backend：MemoryStateBackend（内存，测试用）、FsStateBackend（HDFS，生产用）、RocksDBStateBackend（本地RocksDB+HDFS，支持大状态和增量Checkpoint，生产推荐）。
- **追问**：RocksDBStateBackend为什么适合大状态？状态TTL是什么？Broadcast State是什么？

### RAW-021
- **题目**：HBase 的架构和读写流程是怎样的？
- **技术栈**：HBase
- **来源公司**：阿里
- **出现频率**：高频
- **网友答案摘要**：架构：HMaster（管理元数据、负载均衡、Region分配）、RegionServer（管理Region、处理读写）、ZooKeeper（存储meta表位置、Master选举、Region定位）。写流程：客户端→ZooKeeper获取meta表所在RegionServer→扫描meta表定位目标Region→写入WAL（预写日志，保证持久性）→写入MemStore（内存）→返回成功→MemStore达到阈值Flush到HFile。读流程：定位Region→先查BlockCache（读缓存）→再查MemStore→最后查HFile→合并返回。HBase适合写多读少、随机写入场景。
- **追问**：WAL有什么用？MemStore什么时候Flush？HFile是什么？BlockCache和MemStore的关系？

### RAW-022
- **题目**：HBase 的 RowKey 如何设计？为什么不能连续写入？
- **技术栈**：HBase
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：RowKey设计原则：①长度尽量短（10-100字节），节省存储和缓存；②散列性，避免热点（数据按RowKey字典序排序存储，连续写入会集中到某个Region造成热点）。避免热点方案：①反转RowKey（手机号反转）；②哈希（对RowKey取hash前缀）；③加盐（在RowKey前加随机前缀）。预分区：建表时指定多个Region，避免数据都写入一个Region。
- **追问**：热点问题有什么危害？预分区怎么做？RowKey反转有什么缺点？如何范围查询？

### RAW-023
- **题目**：HBase 的 LSM 树原理？Region 分裂机制？
- **技术栈**：HBase
- **来源公司**：字节跳动
- **出现频率**：中频
- **网友答案摘要**：LSM树（Log-Structured Merge Tree）：写入先到MemStore（内存），Flush到磁盘成HFile，多个HFile通过Minor Compaction（小合并，合并小文件）和Major Compaction（大合并，合并所有文件，删除带删除标记的数据）合并。读写分离，写内存极快，读需要合并MemStore+多个HFile（用布隆过滤器优化）。Region分裂：当Region大小超过阈值（默认10GB）自动分裂为两个，分裂过程：ZK上报→HMaster协调→RegionServer执行分裂→创建子Region→更新meta表→负载均衡。
- **追问**：Minor和Major Compaction的区别？Compaction有什么问题？Region分裂期间能读写吗？如何避免分裂风暴？

### RAW-024
- **题目**：Zookeeper 的选举机制和 ZAB 协议？
- **技术栈**：Zookeeper
- **来源公司**：美团
- **出现频率**：中频
- **网友答案摘要**：ZAB协议（Zookeeper Atomic Broadcast）是为ZooKeeper设计的崩溃可恢复的原子广播协议，包含Leader选举、消息广播（类似两阶段提交）、崩溃恢复三个阶段。Leader选举：节点初始LOOKING状态，互相发送投票（myid, zxid），优先选zxid大的（数据新），zxid相同选myid大的。过半机制：获得半数以上节点投票即成为Leader。Leader选举触发：集群启动、Leader宕机、网络分区。消息广播：Leader收到写请求，分配全局唯一递增zxid，发proposal给follower，半数ack后发commit。
- **追问**：zxid是什么？为什么选zxid大的？脑裂怎么解决？Observer是什么？

### RAW-025
- **题目**：设计一个数仓分层方案。各层职责是什么？
- **技术栈**：数据仓库
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：常见5层分层：①ODS（原始数据层）：保持数据原貌不做修改，起备份作用，创建分区表防全表扫描，数据压缩；②DIM（维度层）：存储业务维度信息（商品/客户/地区），供共享复用；③DWD（明细层）：基于ODS+DIM做清洗整合，构建最细粒度明细事实表，可适当宽表化；④DWS（汇总层）：以分析主题为驱动，构建公共粒度汇总指标宽表，口径一致；⑤ADS（应用层）：面向特定应用场景的最终汇总指标，供BI报表。分层价值：解耦、复用、规范口径、便于血缘追踪。
- **追问**：DWD和DWS的区别？为什么要做宽表？DIM层放什么？数据从ODS到ADS的流转流程？

### RAW-026
- **题目**：维度建模的星型模型和雪花模型有什么区别？如何选择？
- **技术栈**：数据仓库
- **来源公司**：字节跳动
- **出现频率**：中频
- **网友答案摘要**：星型模型：一张事实表周围连接多张维度表，维度表不规范化（冗余），结构简单查询快，维护成本低但数据冗余。雪花模型：维度表进一步规范化（拆分），减少冗余但结构复杂，查询需多表Join稍慢。星座模型：多张事实表共享维度表，适合复杂业务。生产环境优先星型模型（查询性能优先，宽表化趋势），雪花模型适合维度层级多且更新频繁的场景。维度建模四步：选业务过程→声明粒度→确定维度→确定事实。
- **追问**：事实表有哪几种类型？退化维是什么？一致性维度是什么？

### RAW-027
- **题目**：缓慢变化维（SCD）怎么处理？拉链表是什么？
- **技术栈**：数据仓库
- **来源公司**：京东
- **出现频率**：中频
- **网友答案摘要**：缓慢变化维（Slowly Changing Dimension）是维度属性随时间缓慢变化。处理方式：①SCD1：直接覆盖旧值（不保留历史）；②SCD2：新增一行，加有效时间和版本标识（start_date/end_date，is_current），保留完整历史，最常用；③SCD3：新增一列记录旧值（只保留上一次历史）。拉链表是SCD2的实现方式，通过start_date/end_date标记每条记录的有效期，能保存历史快照、去除重复、节约空间。查询时按业务日期过滤 start_date<=date<end_date。
- **追问**：拉链表如何初始化和更新？SCD2和SCD3各适用什么场景？事实表类型（事务型/周期型/累积型）区别？

### RAW-028
- **题目**：数据倾斜怎么定位？怎么解决？
- **技术栈**：数据倾斜 / Spark
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：本质：大量数据被发送到同一个Task/Reduce，导致消息堆积。表现：某个Task执行时间远超其他Task，甚至OOM。定位方法：①看Spark Web UI，找执行时间长的Task；②抽样统计key分布，找数据量最大的key。解决（Spark）：①广播变量+Map Join（小表广播到内存规避Shuffle）；②两阶段聚合（给key打随机前缀局部聚合，再去前缀全局聚合，解决聚合倾斜）；③采样拆分Join（倾斜key单独打散扩容N份，另一方对应key扩N倍，分别Join再Union）；④增加分区数/调参（spark.sql.shuffle.partitions）。Hive：开启MapJoin、两阶段聚合、空key处理。
- **追问**：Kafka/Redis的数据倾斜怎么解决？两阶段聚合为什么有效？倾斜key无法过滤怎么办？

### RAW-029
- **题目**：统计「连续登录 N 天的用户」怎么写 SQL？
- **技术栈**：场景题 / SQL
- **来源公司**：美团
- **出现频率**：高频
- **网友答案摘要**：方法一（窗口函数+日期差）：用 ROW_NUMBER() OVER(PARTITION BY uid ORDER BY dt) 排序，用 date_sub(dt, rn) 计算日期差，相同日期差的为连续登录，再group by uid, date_diff having count(*)>=N。方法二（自连接）：用户表自连接，条件是同一用户且日期差连续。方法三（Spark）：用DataFrame窗口函数同样思路。关键：利用"连续登录日期减去排序序号相等"这一特征。
- **追问**：如果要求连续登录且每天有多次登录怎么去重？如何统计连续登录最大天数？用Flink实时计算怎么做？

### RAW-030
- **题目**：海量数据求 TopK？100亿个数找最大的100个？
- **技术栈**：场景题 / 算法
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：①小顶堆（优先队列）：维护大小为K的小顶堆，遍历数据，比堆顶大则替换堆顶并调整，最后堆中就是TopK，时间复杂度O(nlogK)，空间O(K)，适合单机内存够放堆；②分治+堆：数据分片到多个节点，每节点用小顶堆求本地TopK，再合并各节点TopK求全局TopK，适合数据量超内存；③MapReduce/Spark：Map阶段每个分区求本地TopK，Reduce阶段合并。注意求最大K个用小顶堆（堆顶是最小值），求最小K个用大顶堆。
- **追问**：堆和快排topK哪个快？为什么不用大顶堆存所有数据？如果K也很大怎么办？

### RAW-031
- **题目**：如何统计网站 UV（独立访客）？亿级用户去重怎么做？
- **技术栈**：场景题 / 算法
- **来源公司**：快手
- **出现频率**：中频
- **网友答案摘要**：①精确统计：用Set/Bitmap存用户ID，但亿级用户内存大；②HyperLogLog（HLL）：概率算法，固定12KB内存可估算亿级基数，误差约0.81%，Redis的PFADD/PFCOUNT支持，适合不要求精确的UV统计；③BloomFilter（布隆过滤器）：判断元素是否存在，有误判率但无漏判，适合去重判断"一定不存在"或"可能存在"；④Bitmap：用户ID映射到位图，1位表示1用户，1亿用户约12MB，精确且省空间。Hive/Spark SQL内置 approx_count_distinct 函数基于HLL。
- **追问**：HyperLogLog原理？BloomFilter能不能删除元素？Bitmap有什么限制？

### RAW-032
- **题目**：Spark 和 Flink 有什么区别？什么场景用哪个？
- **技术栈**：Spark / Flink
- **来源公司**：字节跳动
- **出现频率**：高频
- **网友答案摘要**：①计算模型：Spark Streaming是微批处理（Micro-batching，把流切成小批次RDD），Flink是真流处理（逐条处理）；②延迟：Spark秒级延迟，Flink毫秒级延迟；③状态管理：Flink有完善的状态管理和Checkpoint，Spark Streaming状态管理较弱；④时间语义：Flink支持Event Time/Processing Time/Ingestion Time，Watermark处理乱序，Spark Streaming时间语义较弱；⑤Exactly-Once：Flink端到端Exactly-Once，Spark Streaming基于WAL是At-Least-Once（Structured Streaming改进）；⑥窗口：Flink窗口丰富（滚动/滑动/会话），Spark窗口相对简单。实时性要求高选Flink，批处理和Lambda架构选Spark。
- **追问**：Spark Structured Streaming和Flink还有差距吗？Lambda架构和Kappa架构的区别？

### RAW-033
- **题目**：Spark 的 Shuffle 有哪几种？如何优化 Shuffle？
- **技术栈**：Spark
- **来源公司**：阿里
- **出现频率**：中频
- **网友答案摘要**：Shuffle Write实现：①HashShuffle（早期，每个Map Task为每个Reduce Task生成一个文件，文件数M*R，小文件过多）；②SortShuffle（1.2+，Map Task输出排序后写一个data文件+一个index文件，文件数M，默认）；③Tungsten SortShuffle（1.4+，基于内存页直接序列化排序，避免对象开销）；④BypassMergeSortShuffle（不需要排序/聚合，直接写再合并）。优化：①减少Shuffle（用broadcast join替代shuffle join）；②调整 spark.sql.shuffle.partitions（默认200，大数据量需调大）；③开启Map端聚合（reduceByKey优于groupByKey）；④调整Shuffle内存参数；⑤使用Kryo序列化。
- **追问**：reduceByKey和groupByKey的区别？为什么reduceByKey更好？Bypass机制触发条件？

### RAW-034
- **题目**：Hive SQL 优化有哪些手段？
- **技术栈**：Hive
- **来源公司**：美团
- **出现频率**：中频
- **网友答案摘要**：①Fetch抓取：简单查询（SELECT字段、过滤、LIMIT）不走MapReduce，hive.fetch.task.conversion=more；②本地模式：小数据集单机处理，hive.exec.mode.local.auto=true；③表优化：小表join大表用MapJoin，大表join大表空key过滤/打散；④列裁剪：只读需要的列，少用SELECT *；⑤分区裁剪：WHERE带分区字段，避免全表扫描；⑥并行执行：hive.exec.parallel=true；⑦严格模式：hive.mapred.mode=strict，禁止无LIMIT的ORDER BY、笛卡尔积、无分区字段的查询；⑧JVM重用：amlljob复用JVM减少启动开销；⑨合理设置reduce数；⑩数据倾斜调参。
- **追问**：如何查看SQL执行计划（EXPLAIN）？小文件问题怎么解决？什么时候用Tez/Spark引擎？

### RAW-035
- **题目**：Kafka 消息积压怎么处理？
- **技术栈**：Kafka
- **来源公司**：顺丰
- **出现频率**：中频
- **网友答案摘要**：消息积压指消费速度跟不上生产速度。排查：①看消费者滞后（Consumer Lag），kafka-consumer-groups.sh查看；②看消费端是否有异常/卡住；③看下游处理是否变慢。解决：①增加消费者实例（注意不能超过分区数，一个分区只能被组内一个消费者消费）；②增加分区数（需保证下游能处理）；③提升消费端处理能力（批处理、异步化、线程池）；④临时扩容：跳过堆积消息先处理最新消息（需业务允许）；⑤如果是消费端处理慢，优化处理逻辑/DB写入；⑥下游瓶颈扩容下游。长期：监控告警Consumer Lag，合理评估分区数和消费者数。
- **追问**：消费者数能超过分区数吗？为什么？如何监控Consumer Lag？

### RAW-036
- **题目**：HBase 和关系型数据库（MySQL）有什么区别？什么场景用 HBase？
- **技术栈**：HBase
- **来源公司**：腾讯
- **出现频率**：低频
- **网友答案摘要**：HBase：NoSQL列式存储，适合海量数据（亿级+）、写多读少、随机写入、稀疏数据、无复杂查询（只按RowKey查/范围查）场景；强一致性、高可用、可伸缩。MySQL：关系型，支持SQL、事务、复杂Join，适合数据量适中、强一致性、复杂查询场景。HBase适用：订单/消息明细存储、用户画像标签、实时推荐特征、监控数据。不适用：复杂分析查询（用Hive/Spark）、小数据量CRUD（用MySQL）。
- **追问**：HBase支持事务吗？HBase为什么不适合分析查询？HBase和Redis的区别？

### RAW-037
- **题目**：Flink 的反压（Backpressure）是什么？怎么解决？
- **技术栈**：Flink
- **来源公司**：字节跳动
- **出现频率**：中频
- **网友答案摘要**：反压是指下游处理速度慢于上游，导致数据在Buffer中堆积，反向传导给上游，使上游也被阻塞的现象。表现：作业吞吐下降，Checkpoint超时失败。原因：①下游算子处理慢（复杂计算、外部IO瓶颈）；②数据倾斜；③GC频繁。排查：看Flink Web UI的BackPressure指标（High/Moderate/OK），定位反压节点。解决：①优化慢算子（异步IO、批量写入）；②解决数据倾斜；③调大网络缓冲（taskmanager.network.memory）；④增加并行度；⑤用Unaligned Checkpoint缓解反压下的Checkpoint超时。注意：反压是症状不是根因，要定位真正的瓶颈算子。
- **追问**：反压和Checkpoint超时的关系？Unaligned Checkpoint原理？如何定位是哪个算子慢？

### RAW-038
- **题目**：Zookeeper 的 watcher 机制是什么？有哪些应用场景？
- **技术栈**：Zookeeper
- **来源公司**：百度
- **出现频率**：低频
- **网友答案摘要**：watcher机制：客户端在ZNode上注册watcher，当ZNode数据变化或子节点变化时，ZooKeeper通知客户端。特点：①一次性触发（触发后失效，需重新注册）；②客户端串行接收事件；③轻量（只通知事件类型和路径，不传数据内容）。应用场景：①Kafka用ZK监听Broker存活（旧版，新版Kraft移除ZK依赖）；②HBase Master选举和Region定位；③HDFS HA NameNode主备切换；④分布式锁（创建临时节点，监听前一个节点）；⑤配置中心（监听配置节点变化）。注意：watcher是一次性的，可能导致事件丢失，需在收到通知后立即重新注册。
- **追问**：一次性watcher有什么问题？如何避免事件丢失？Zookeeper分布式锁怎么实现？羊群效应是什么？

### RAW-039
- **题目**：8G 的 int 数据，内存只有 2G，如何排序？
- **技术栈**：场景题 / 算法
- **来源公司**：腾讯
- **出现频率**：中频
- **网友答案摘要**：外部排序（External Sort）：①分块：将8G数据分成4块（每块2G），每块加载到内存用快排/堆排排序，写回磁盘生成4个有序子文件；②归并：对4个有序子文件做K路归并（K=4），每个文件读一块到内存，用小顶堆维护K个元素的最小值，输出最小值并从对应文件补充，最终合并成全局有序文件。时间复杂度：分块排序O(nlogn) + 归并O(nlogK)。也可用MapReduce：Map阶段分片排序，Reduce阶段归并。
- **追问**：K路归并为什么用堆不用数组遍历？如果数据有重复怎么办？位图法适用什么场景？

### RAW-040
- **题目**：一亿条数据找 Top 100？运维场景的海量数据处理。
- **技术栈**：场景题 / 算法
- **来源公司**：京东
- **出现频率**：中频
- **网友答案摘要**：①小顶堆：维护大小100的小顶堆，遍历一亿数据，比堆顶大则替换，O(nlog100)，单机可解；②分治：一亿数据分到多个节点，每节点求本地Top100，合并所有本地Top100再求全局Top100；③MapReduce/Spark：Map阶段分区求Top100，Reduce阶段合并；④如果数据范围已知且密集，可用Bitmap或计数排序。注意：求最大100个用小顶堆（堆顶最小，比堆顶大才入堆）；求最小100个用大顶堆。
- **追问**：堆法和分治法各适用什么场景？如果Top100要实时更新怎么做（流式TopK）？

---

## 附：采集统计

| 技术栈 | 题目数 | 题号范围 |
|--------|--------|----------|
| Kafka | 6 | RAW-001~006, 035 |
| Spark | 6 | RAW-007~010, 032, 033 |
| Hadoop（HDFS/MR/YARN） | 3 | RAW-010, 011, 012 |
| Hive | 5 | RAW-013~016, 034 |
| Flink | 5 | RAW-017~020, 037 |
| HBase | 3 | RAW-021~023, 036 |
| Zookeeper | 2 | RAW-024, 038 |
| 数据仓库 | 3 | RAW-025~027 |
| 数据倾斜 | 2 | RAW-014, 028 |
| 场景题（SQL/算法） | 5 | RAW-029~031, 039, 040 |

> 注：部分题目跨技术栈（如RAW-010同时涉及Hadoop与Spark，RAW-028同时涉及数据倾斜与Spark），统计时归入主技术栈，故上表部分题号重复出现。

## 附：来源公司分布

| 公司 | 题目数 |
|------|--------|
| 字节跳动 | 9 |
| 美团 | 8 |
| 阿里 | 3 |
| 快手 | 3 |
| 京东 | 3 |
| 百度 | 2 |
| 腾讯 | 2 |
| 顺丰 | 1 |
| SHEIN | 0（报告中提及但搜索未补充到具体题目） |
| 微众 | 0（报告中提及但搜索未补充到具体题目） |
| OPPO | 0（报告中提及但搜索未补充到具体题目） |
| 美的 | 0（报告中提及但搜索未补充到具体题目） |
| 贝壳 | 0（报告中提及但搜索未补充到具体题目） |

---

> 采集完成时间：2026-07-03
> 下一步建议：交由去重Agent进行跨题去重、标准化，再交由内容Agent补全标准答案与难度分级。
