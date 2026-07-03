# 大数据面试题简答题标准答案

## 阅卷元信息
- 阅卷时间：2026-07-03
- 阅卷Agent：saq-grader
- 简答题总数：50道（原题25 + 变式25）
- 难度分布：L1 1道 / L2 16道 / L3 29道 / L4 4道

---

## SAQ-001 [L3] [Kafka/消息可靠性/三端保证]
**题干**：如何保证 Kafka 消息不丢失？

### 标准答案

保证 Kafka 消息不丢失需要从**生产者端、Broker 端、消费者端**三端协同配置。

#### 1. 生产者端
- 使用 `acks=all`（或 -1）：消息必须写入所有 ISR 副本后才返回成功，而非仅 Leader 写入
- 开启重试 `retries=Integer.MAX_VALUE`，并配合 `delivery.timeout.ms` 控制总重试时间上限
- 开启幂等性 `enable.idempotence=true`，避免重试导致重复（通过 PID + SequenceNumber 去重）
- 限制 `max.in.flight.requests.per.connection ≤ 5`，保证重试时不乱序（幂等性开启后可放宽到 5，未开启则必须为 1）
- 关键参数：`acks=all`, `retries=Integer.MAX_VALUE`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`

#### 2. Broker 端
- 副本因子 `replication.factor=3`：每个分区 3 个副本，容忍 2 个副本故障
- 最小同步副本数 `min.insync.replicas=2`：ISR 中至少 2 个副本写入才认为提交成功
- 禁止非 ISR 副本成为 Leader `unclean.leader.election.enable=false`：防止数据落后的副本成为 Leader 导致已提交消息丢失
- 关键参数：`replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`

#### 3. 消费者端
- 关闭自动提交 `enable.auto.commit=false`，改为**手动提交 offset**
- 消费完成后手动 `commitSync()`（同步提交，保证可靠性）或 `commitAsync()`（异步提交，性能更好但可能失败重试导致乱序）
- 生产环境推荐"处理一批 → 同步提交一次"的模式，在可靠性和性能间权衡
- 关键参数：`enable.auto.commit=false`

#### 追问简答
- Q: acks=1 和 acks=all 的区别？  A: acks=1 仅 Leader 写入即响应；acks=all 需所有 ISR 副本写入才响应，更安全但延迟更高
- Q: 幂等性只能保证什么？  A: 仅保证单分区单会话内的去重，跨分区/跨会话需事务机制

#### 生产实践注意点
- acks=all 会降低吞吐约 10%-30%，生产环境需压测权衡
- min.insync.replicas 设为 2 而非 1，防止 ISR 仅剩 Leader 时单点故障导致数据丢失
- 消费者手动提交时要注意"先处理再提交"顺序，处理失败要重试而非跳过
- 生产环境推荐使用事务 Producer（transactional.id）实现跨分区的 Exactly-Once

---

## SAQ-002 [L3] [Kafka/ISR/Leader选举]
**题干**：Kafka 如何选举 Leader？ISR 机制是什么？

### 标准答案

#### 1. ISR 机制定义
- ISR（In-Sync Replicas，同步副本集）：与 Leader 副本数据保持"同步"的副本集合，包含 Leader 自身
- ISR 维护由 Leader 负责：Leader 跟踪每个 Follower 的同步进度（fetch offset），超过 `replica.lag.time.max.ms`（默认 10s）未追上 Leader 的 Follower 会被移出 ISR
- ISR 动态变化：Follower 追上后重新加入 ISR，Leader 故障时优先从 ISR 选举新 Leader

#### 2. Broker Leader 选举（Controller 主导）
- Kafka 集群中有一个 Broker 被选为 **Controller**（通过 ZooKeeper 选举）
- Controller 职责：监控 Broker 存活状态、管理分区 Leader 选举、管理副本重分配
- Leader 故障流程：
  1. Controller 通过 ZooKeeper 的 `/brokers/ids` watch 感知 Broker 宕机
  2. Controller 从该 Broker 上所有分区的 ISR 中按顺序选第一个存活的副本作为新 Leader
  3. Controller 更新 ZooKeeper 元数据并向所有 Broker 发送 LeaderAndIsrRequest
  4. 新 Leader 开始接收读写，Follower 开始同步

#### 3. Controller 选举
- 集群启动时，各 Broker 尝试在 ZooKeeper 的 `/controller` 临时节点创建成功者成为 Controller
- Controller 故障时该临时节点消失，其他 Broker 竞争创建新的 Controller

#### 4. 消费组 Leader 选举
- 由 **GroupCoordinator**（某 Broker 上的协调器）负责
- 消费者加入组时，Coordinator 选第一个加入的消费者作为 Consumer Leader
- Consumer Leader 负责分配分区策略（Range/RoundRobin/Sticky），Coordinator 将方案下发给组内消费者

#### 追问简答
- Q: ISR 为空时怎么办？  A: 受 `unclean.leader.election.enable` 控制，开启则从非 ISR 选 Leader（可能丢数据），关闭则等待 ISR 恢复
- Q: Controller 宕机会怎样？  A: ZooKeeper 临时节点消失，其他 Broker 竞争成为新 Controller，集群短暂不可管理但读写不受影响

#### 生产实践注意点
- `replica.lag.time.max.ms` 不宜设置过大，否则落后副本长期留在 ISR 中，故障切换时可能丢数据
- 生产环境推荐 `min.insync.replicas=2` + `unclean.leader.election.enable=false` 保证数据安全
- Controller 切换期间分区 Leader 选举暂停，应监控 Controller 切换频率

---

## SAQ-003 [L2] [Kafka/Pull模式/消费者]
**题干**：Kafka 消费者是推模式还是拉模式？为什么采用该模式？

### 标准答案

#### 1. Kafka 消费者采用 Pull（拉）模式
- Consumer 主动调用 `poll()` 方法向 Broker 拉取消息，而非 Broker 主动推送

#### 2. 采用 Pull 模式的原因
- **自主控制消费速率**：Consumer 根据自身处理能力决定拉取频率和批量大小，避免 Push 模式下消费不及被压垮
- **批量拉取提升吞吐**：Consumer 可一次拉取多条消息批量处理，减少网络往返开销
- **消费进度自主管理**：Consumer 自主提交 offset，可实现精确的位点控制和重复消费
- **容错性好**：消费失败可不提交 offset，下次重新拉取重试；Broker 无需关心消费状态
- **节约 Broker 资源**：Broker 无需维护每个消费者的推送状态和心跳，只需被动响应拉取请求

#### 3. Push 模式的问题（对比）
- Broker 推送速率不受 Consumer 控制，消费慢的 Consumer 容易被压垮（缓冲区溢出）
- Broker 需维护每个 Consumer 的推送状态，复杂度高
- 难以实现批量推送的精确控制

#### 追问简答
- Q: Pull 模式有什么缺点？  A: 实时性略差（轮询间隔内消息有延迟），无消息时空轮询浪费资源（通过长轮询机制缓解）

---

## SAQ-004 [L2] [Kafka/消费者组/Rebalance]
**题干**：Kafka 消费者组是什么？同一个消费者组的消费者能否消费同一个分区？

### 标准答案

#### 1. 消费者组定义
- 消费者组（Consumer Group）：一组共同完成数据消费的逻辑单元，组内消费者共同消费订阅主题的所有分区
- 核心规则：**一个分区同一时刻只能被同一个消费者组内的一个消费者消费**
- 不同消费者组之间互不影响，各自独立消费全量数据（广播订阅模型）

#### 2. 分区分配规则
- 若消费者数 < 分区数：部分消费者消费多个分区
- 若消费者数 = 分区数：每个消费者消费一个分区
- 若消费者数 > 分区数：多余消费者空闲（不消费任何分区）

#### 3. Rebalance 机制
- 当消费者组成员变化（加入/退出/故障）或分区数变化时，触发 Rebalance 重新分配分区
- Rebalance 期间消费暂停（Stop-The-World），所有消费者停止消费等待重新分配
- 分配策略：Range（按范围）、RoundRobin（轮询）、Sticky（粘性，尽量保持原分配）、CooperativeSticky（增量 Rebalance）

#### 追问简答
- Q: 消费者数能超过分区数吗？  A: 可以，但多余消费者空闲，不会消费任何分区，所以生产环境消费者数 ≤ 分区数

---

## SAQ-005 [L3] [Kafka/顺序性/分区]
**题干**：Kafka 如何保证消息顺序性？如何实现全局有序？

### 标准答案

#### 1. Kafka 顺序性保证机制
- **分区内有序**：Kafka 仅保证单分区内消息按写入顺序存储（offset 单调递增），消费时按 offset 顺序消费
- **多分区不保证全局有序**：不同分区之间无顺序保证，因为分区分布在不同 Broker 上，写入和消费并行进行

#### 2. 保证业务顺序性的方案
- **相同 key 写同一分区**：Producer 发送时指定 key（如 orderId、userId），Kafka 默认按 `hash(key) % numPartitions` 路由到固定分区
- 自定义 Partitioner 实现更灵活的路由逻辑
- 关键参数：`acks=all`，`max.in.flight.requests.per.connection=1`（未开幂等性时）或 `enable.idempotence=true`（允许 5，重试不乱序）

#### 3. 全局有序方案
- **单分区方案**：整个 Topic 只设 1 个分区，所有消息写入同一分区，消费端单线程消费
  - 优点：严格全局有序
  - 缺点：吞吐极低（单分区上限约 10MB/s），无法水平扩展
- **多分区方案**：无法实现严格全局有序，只能通过下游处理（如 Flink/Spark 按 key 重排）实现"按 key 有序"

#### 4. 业务实践
- 大多数业务只需"**按 key 有序**"（如同一订单的状态变更有序），而非全局有序
- 典型场景：订单状态流转（创建→支付→发货→签收），按 orderId 路由到同一分区即可

#### 追问简答
- Q: 为什么要 max.in.flight.requests.per.connection=1？  A: 防止重试时后续消息先成功导致乱序；开启幂等性后可放宽到 5

#### 生产实践注意点
- 单分区全局有序吞吐瓶颈明显，生产环境慎用
- 按 key 路由可能导致数据倾斜（某 key 数据量过大），需结合业务设计合理的 key 粒度

---

## SAQ-006 [L3] [Spark/容错/血缘]
**题干**：Spark 的容错机制是怎样的？RDD 如何实现容错？

### 标准答案

#### 1. RDD 容错核心：血缘关系（Lineage）
- RDD 是不可变分布式数据集，通过记录血缘关系（DAG）实现容错
- 某个分区数据丢失时，Spark 根据血缘关系**重新计算**丢失的分区，而非做数据复制
- 血缘记录：每个 RDD 知道自己的父 RDD 和转换算子，形成有向无环图（DAG）

#### 2. 宽窄依赖与重算代价
- **窄依赖**（ Narrow Dependency）：父 RDD 一个分区最多被子 RDD 一个分区使用（如 map、filter）
  - 分区丢失只需重算对应的父分区，代价小
- **宽依赖**（Wide Dependency / Shuffle 依赖）：父 RDD 一个分区被子 RDD 多个分区使用（如 groupByKey、join）
  - 分区丢失需重算所有父分区，代价大（因为无法定位具体父分区）

#### 3. Checkpoint 机制
- 对于血缘过长的 RDD，可设置 Checkpoint 将数据持久化到 HDFS，**截断血缘**
- Checkpoint 后该 RDD 变为 CheckpointRDD，父 RDD 可被回收
- Checkpoint 是 eager 操作，需通过 action 触发
- 推荐先 `cache()` 再 `checkpoint()`，避免 Checkpoint 时重复计算

#### 4. Cache/Persist 机制
- `persist()` / `cache()` 将 RDD 缓存到内存或磁盘，加速重复计算
- 不截断血缘：缓存丢失后仍可重算
- 存储级别：MEMORY_ONLY、MEMORY_AND_DISK、DISK_ONLY 等

#### 追问简答
- Q: Cache 和 Checkpoint 的区别？  A: Cache 不截断血缘（lazy 缓存，丢失可重算），Checkpoint 截断血缘（eager 落盘，需先 Cache 避免重复计算）

#### 生产实践注意点
- 血缘过长（如迭代算法 PageRank、ML 训练）建议定期 Checkpoint，避免重算代价爆炸和调度栈溢出
- 宽依赖前的 RDD 建议 Cache，避免 Shuffle 数据丢失后重算所有父分区

---

## SAQ-007 [L3] [Hadoop/Shuffle/MR与Spark对比]
**题干**：MapReduce 的 Shuffle 过程是怎样的？MapReduce 与 Spark 的优劣对比？

### 标准答案

#### 1. MapReduce Shuffle 流程

**Map 端**：
1. MapTask 输出数据先写入**环形缓冲区**（默认 100MB，`mapreduce.task.io.sort.mb`）
2. 缓冲区达到阈值（默认 80%，`mapreduce.map.sort.spill.percent`）时触发 **Spill**（溢写）
3. Spill 前对数据按分区 + key 排序，可选 Combiner 局部聚合
4. 多次 Spill 产生多个临时文件，最终 **Merge** 合并成一个排序好的输出文件
5. 可选压缩减少 IO

**Reduce 端**：
1. ReduceTask 通过 HTTP 拉取所有 MapTask 中属于自己的分区数据
2. 拉取的数据先写入内存缓冲区，溢写后落盘
3. 多个文件 **Merge** 合并（归并排序），保持按 key 有序
4. 按 key 分组后调用 `reduce()` 函数处理

#### 2. MapReduce vs Spark 对比

| 维度 | MapReduce | Spark |
|------|-----------|-------|
| 计算模型 | Map→Shuffle→Reduce 两阶段 | DAG 多阶段，可链式执行 |
| 数据存储 | 基于磁盘，每阶段结果落盘 | 基于内存，中间结果可缓存 |
| 迭代效率 | 差，每次迭代重读磁盘 | 好，内存缓存加速迭代 |
| 容错 | Task 失败重试 | 血缘重算 + Cache |
| 编程模型 | 仅 Map/Reduce 两接口 | 丰富的算子（map/filter/join 等） |
| 启动延迟 | JVM 启动慢（每次 Task 新 JVM） | 可复用 Executor |
| 适用场景 | 超大规模离线批处理、磁盘密集 | 内存充足的迭代/交互/批处理 |

#### 追问简答
- Q: Spark 为什么比 MR 快？  A: 基于内存的 DAG 执行减少中间落盘，丰富的算子减少 Shuffle 次数，Executor 复用减少启动开销

#### 生产实践注意点
- MR 在超大数据集（TB+）且内存不足时仍稳定可靠，Spark 在内存不足时会 Spill 到磁盘，性能下降明显
- Spark 的 Shuffle 实现经历过 HashShuffle → SortShuffle 演进，1.6 后默认 SortShuffle

---

## SAQ-008 [L2] [Hadoop/HDFS/读写与副本]
**题干**：HDFS 的读写流程是怎样的？副本机制是怎么样的？

### 标准答案

#### 1. HDFS 写流程
1. Client 向 NameNode 请求上传文件，NameNode 校验权限、目录是否存在、文件是否已存在
2. NameNode 返回可写的 DataNode 列表（按副本放置策略选 3 个节点）
3. Client 与第一个 DataNode 建立 pipeline，第一个 DataNode 再与第二个 DataNode 建立 pipeline，依次类推（pipeline 链式传输）
4. 数据以 **packet**（默认 64KB）为单位流式传输，每个 packet 经过 pipeline 各节点并逐级 ACK 确认
5. 所有 packet 传输完成后，DataNode 向 NameNode 汇报 block 接收完成
6. Client 关闭连接，NameNode 提交元数据

**示意图描述**：Client → DN1 → DN2 → DN3（pipeline 链式），ACK 确认反向返回 DN3 → DN2 → DN1 → Client

#### 2. HDFS 读流程
1. Client 向 NameNode 请求获取文件的 block 列表（含每个 block 的副本位置）
2. NameNode 返回 block 元数据（按距离 Client 最近排序）
3. Client 优先选择**最近的 DataNode**（同节点 > 同机架 > 跨机架）建立连接
4. 直接与 DataNode 通信读取数据（流式传输），校验 checksum
5. 读取完一个 block 后切换到下一个 block 的最近副本
6. 文件读取完成后关闭连接

#### 3. 副本机制
- 默认副本数 3（`dfs.replication=3`）
- **副本放置策略**（机架感知）：
  1. 第一个副本：优先放 Client 所在节点（若 Client 在集群内）
  2. 第二个副本：放在**不同机架**的节点（防机架故障）
  3. 第三个副本：放在第二个副本**同机架的不同节点**（平衡容错与网络开销）
- 副本数可按文件级别设置（热数据多副本，冷数据可降低）

#### 追问简答
- Q: 为什么第二个副本放不同机架？  A: 防止整个机架故障（电源/交换机）导致数据丢失，保证容错性

---

## SAQ-009 [L2] [Hadoop/YARN/调度]
**题干**：YARN 的调度流程是怎样的？有哪些调度器？

### 标准答案

#### 1. YARN 调度流程
1. **提交作业**：Client 向 ResourceManager（RM）提交应用，RM 分配第一个 Container
2. **启动 ApplicationMaster**：在 Container 中启动应用的 AM，AM 负责作业的资源申请和任务监控
3. **AM 申请资源**：AM 向 RM 的 Scheduler 申请资源（通过心跳上报资源需求）
4. **分配 Container**：RM 根据调度策略分配 Container，返回 Container 列表给 AM
5. **启动 Task**：AM 与对应 NodeManager（NM）通信，在 Container 中启动 Task（MapTask/ReduceTask）
6. **Task 执行与汇报**：Task 执行过程中向 AM 汇报进度状态
7. **作业完成注销**：所有 Task 完成后，AM 向 RM 注销自己，释放资源

#### 2. 三种调度器

| 调度器 | 特点 | 适用场景 |
|--------|------|---------|
| **FIFO** | 单队列先进先出，小作业易被大作业阻塞 | 单用户、简单场景 |
| **Capacity** | 多队列划分容量比例，队列间可借用空闲资源，保证最小资源 | 多租户、资源相对固定（Apache 默认） |
| **Fair** | 所有作业公平分享资源，按权重分配，小作业快速启动 | 交互查询多、作业大小混合（CDH 默认） |

#### 追问简答
- Q: Capacity 和 Fair 的区别？  A: Capacity 按队列容量比例分配，保证最小资源；Fair 按作业权重公平分配，小作业启动快

---

## SAQ-010 [L3] [Hive/数据倾斜/通用方案]
**题干**：Hive 的数据倾斜如何处理？

### 标准答案

#### 1. 数据倾斜本质
- 某 key 数据量远超其他 key，导致 reduce 处理时某个 Task 执行时间极长甚至 OOM
- 表现：Job 进度长期卡在 99%，少数 Task 耗时是其他 Task 的数倍，某 reduce 处理数据量远超均值

#### 2. 倾斜原因
- **key 分布不均**：如某 user_id 占 80% 数据
- **NULL 值过多**：join 时 NULL 被分到同一 reduce
- **count(distinct)**：所有去重值被分到单个 reduce
- **关联键类型不一致**：导致 join 时无法正确匹配，部分数据集中

#### 3. 解决方案

**参数调节**：
- `hive.map.aggr=true`：开启 Map 端聚合，减少 Shuffle 数据量
- `hive.groupby.skewindata=true`：开启两阶段聚合（自动加随机前缀）
- `hive.skewjoin.key=100000` + `hive.skewjoin.mapjoin.xml`：倾斜 join 转 MapJoin

**SQL 调节**：
- **空 key 打散**：`SELECT ... FROM t1 LEFT JOIN t2 ON CASE WHEN t1.key IS NULL THEN concat('null_', rand()) ELSE t1.key END = t2.key`
- **两阶段聚合**（手动 salting）：
  1. 第一阶段：加随机前缀 `[1..N]` 按 `(prefix, key)` group by 局部聚合
  2. 第二阶段：去掉前缀按 `key` group by 全局聚合
- **MapJoin**：小表广播到所有 Map 端，避免 reduce 端 join（`/*+ MAPJOIN(small_table) */`）
- **倾斜 key 单独处理**：过滤出倾斜 key 单独 union 处理，其余走正常逻辑

**增加 reduce 数**：
- `set mapreduce.job.reduces=N`：增加 reduce 数分散数据（对均匀分布有效，对极端倾斜效果有限）

#### 追问简答
- Q: groupby.skewindata=true 的原理？  A: 自动两阶段聚合，第一阶段加随机前缀局部聚合，第二阶段去前缀全局聚合
- Q: count(distinct) 倾斜怎么办？  A: 用 `group by + count` 子查询替代，或用 `approx_count_distinct`（HyperLogLog）

#### 生产实践注意点
- 倾斜 key 无法过滤时，用"倾斜 key 单独处理 + 非倾斜 key 正常逻辑"的 union 方案
- join 倾斜优先用 MapJoin（小表场景），大表 join 大表用 salting 打散
- 生产环境建议开启 `hive.map.aggr=true` 和 `hive.groupby.skewindata=true` 作为默认配置

---

## SAQ-011 [L3] [Flink/Exactly-Once/端到端]
**题干**：Flink 如何实现 Exactly-Once 语义？

### 标准答案

#### 1. 端到端 Exactly-Once 三部分配合

**Source 端**：
- 数据源支持可重放（如 Kafka，可重置 offset）
- Checkpoint 时将读取 offset 作为状态保存
- 故障恢复后从 Checkpoint 中的 offset 重新消费，保证数据不丢不重

**Flink 内部（State 一致性）**：
- 通过 **Checkpoint + Barrier** 机制实现状态一致性
- JobManager 的 CheckpointCoordinator 向 Source 注入 Barrier
- Barrier 随数据流流动，算子收到 Barrier 后：
  - **Aligned Checkpoint**：等待所有上游 Barrier 对齐，快照状态到 State Backend，向下游发送 Barrier
  - **Unaligned Checkpoint**：Barrier 直接越过 in-flight 数据，立即快照（含 in-flight 数据）
- 所有算子完成快照后向 JM 汇报，JM 确认 Checkpoint 成功

**Sink 端**：
- **幂等写入**：如 Redis SET、MySQL INSERT ON DUPLICATE KEY UPDATE，重复写入不产生副作用
- **事务写入（两阶段提交 2PC）**：
  1. `preCommit`：Sink 接收 Barrier 后开启事务并写入数据，但不提交
  2. `commit`：JM 确认 Checkpoint 成功后通知所有 Sink 提交事务
  3. `rollback`：Checkpoint 失败时回滚事务
- 实现：继承 `TwoPhaseCommitSinkFunction`

#### 2. 关键配置
- `enableCheckpointing(interval)`：开启 Checkpoint，间隔通常 10s-60s
- `CheckpointingMode.EXACTLY_ONCE`：设置为精确一次语义
- State Backend：MemoryStateBackend / FsStateBackend / RocksDBStateBackend
- 关键参数：`checkpoint interval`, `minPauseBetweenCheckpoints`, `tolerableCheckpointFailureNumber`

#### 追问简答
- Q: 幂等写入和事务写入怎么选？  A: 幂等写入实现简单但无法处理"已提交但快照失败"窗口的重复；事务写入严格 Exactly-Once 但延迟更高
- Q: Sink 两阶段提交详见？  A: preCommit 开启事务写入 → JM 收齐 barrier 后 commit → 失败时 rollback（详见 SAQ-011-V1）

#### 生产实践注意点
- Checkpoint 间隔不宜过短（增加开销）或过长（恢复时重算数据多），通常 30s-60s
- RocksDB State Backend 适合大状态场景，支持增量 Checkpoint
- Kafka 事务 Sink 需配置 `transactional.id` 和 `isolation.level=read_committed`

---

## SAQ-012 [L2] [Flink/窗口/类型与函数]
**题干**：Flink 的窗口有哪些类型？窗口函数如何使用？

### 标准答案

#### 1. 窗口类型

| 窗口类型 | 特点 | 适用场景 |
|---------|------|---------|
| **Tumbling Window（滚动）** | 固定大小，不重叠，无间隔 | 每分钟统计 PV/UV |
| **Sliding Window（滑动）** | 固定大小，可重叠，有滑动步长 | 每小时统计过去 24 小时数据 |
| **Session Window（会话）** | 无固定大小，由 gap 决定，无数据时关闭 | 用户会话分析 |
| **Global Window（全局）** | 所有数据一个窗口，需自定义触发器 | 自定义触发逻辑 |

#### 2. 时间语义
- **Processing Time**：处理时间，机器系统时间，延迟低但结果不确定
- **Event Time**：事件时间，数据本身携带的时间，结果确定但需处理乱序
- **Ingestion Time**：摄入时间，数据进入 Flink 的时间

#### 3. 窗口函数

| 函数类型 | 特点 | 适用场景 |
|---------|------|---------|
| **ReduceFunction** | 增量聚合，输入输出类型相同，性能高 | 求和、最大值 |
| **AggregateFunction** | 增量聚合，输入输出类型可不同，灵活 | 计算平均值、UV |
| **ProcessWindowFunction** | 全量聚合，缓存窗口所有数据，性能低但可获取窗口上下文 | 需要窗口元信息的复杂逻辑 |
| **ProcessWindowFunction + 增量聚合** | 结合两者，增量聚合 + 最终处理 | 既有性能又有上下文 |

#### 4. 使用示例
```
// 滚动窗口 + 增量聚合
data.keyBy(x => x.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new MyAggregateFunction())

// 会话窗口 + ProcessWindowFunction
data.keyBy(x => x.key)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .process(new MyProcessWindowFunction())
```

#### 追问简答
- Q: ReduceFunction 和 AggregateFunction 区别？  A: Reduce 输入输出类型相同，Aggregate 可不同类型且更灵活
- Q: Event Time 窗口何时触发？  A: Watermark ≥ 窗口结束时间时触发

---

## SAQ-013 [L3] [Flink/状态/State Backend]
**题干**：Flink 的状态管理是怎样的？状态有哪几种？

### 标准答案

#### 1. 状态分类

**按作用域**：
- **Keyed State**（键控状态）：绑定到 key，只能在 KeyedStream 上使用，每个 key 独立状态
- **Operator State**（算子状态）：绑定到算子，不依赖 key，如 Kafka Source 的 offset 状态

**按管理方式**：
- **Managed State**（托管状态）：由 Flink 框架管理，支持序列化和 Checkpoint，推荐使用
- **Raw State**（原始状态）：用户自行管理字节数组，灵活但不推荐

#### 2. Keyed State 类型

| 类型 | 说明 | 典型场景 |
|------|------|---------|
| **ValueState** | 单值状态，每 key 一个值 | 记录用户上次登录时间 |
| **ListState** | 列表状态，每 key 一个列表 | 累积用户访问记录 |
| **MapState** | 映射状态，每 key 一个 Map | 统计每用户每页面 PV |
| **ReducingState** | 归约状态，新值与旧值 reduce | 累计求和 |
| **AggregatingState** | 聚合状态，输入输出类型可不同 | 计算平均值 |

#### 3. Operator State
- 实现 `CheckpointedFunction` 接口，在 `snapshotState` 和 `initializeState` 中管理状态
- 典型场景：Kafka Source 记录消费 offset、Broadcast State（广播状态）

#### 4. 三种 State Backend

| State Backend | 状态存储 | 特点 | 适用场景 |
|--------------|---------|------|---------|
| **MemoryStateBackend** | 内存（JVM 堆） | 速度快，状态受内存限制，Checkppoint 存 JM 内存 | 测试、小状态 |
| **FsStateBackend** | 内存（TaskManager 堆） + Checkpoint 到 HDFS | 状态在内存，快照落盘 | 中等状态、生产环境 |
| **RocksDBStateBackend** | RocksDB（磁盘 + 内存缓存） + Checkpoint 到 HDFS | 支持大状态（超内存），支持增量 Checkpoint，速度稍慢 | 大状态、生产推荐 |

#### 追问简答
- Q: RocksDB 为什么适合大状态？  A: 基于 LSM 树的嵌入式 KV 存储，状态存磁盘突破内存限制，支持增量 Checkpoint
- Q: Managed State 和 Raw State 区别？  A: Managed 由 Flink 管理序列化，支持 Checkpoint；Raw 用户自行管理，灵活但维护成本高

#### 生产实践注意点
- 生产环境推荐 RocksDBStateBackend，状态超过内存时唯一选择
- 状态需合理设置 TTL 避免无限膨胀（`StateTtlConfig`）
- Keyed State 的 ValueState 需通过 `RuntimeContext` 获取，需在 `RichFunction` 中使用

---

## SAQ-014 [L3] [HBase/RowKey/设计与避免热点]
**题干**：HBase 的 RowKey 如何设计？为什么不能连续写入？

### 标准答案

#### 1. RowKey 设计原则
- **长度短**：RowKey 存储在每个 KV 中，越短内存占用越小，建议 ≤ 50 字节
- **散列性**：RowKey 应均匀分布，避免集中写入某 Region 导致热点
- **唯一性**：RowKey 唯一标识一行数据
- **排序性**：RowKey 按字典序存储，设计时考虑 Scan 查询需求

#### 2. 为什么不能连续写入
- HBase 按 RowKey 字典序排序存储，Region 按 RowKey 范围划分
- 连续 RowKey（如自增 ID、时间戳）会集中写入最后一个 Region，导致**热点**（Hotspotting）
- 该 RegionServer 承载所有写入压力，CPU/IO/网络飙升，其他节点空闲
- 热点还导致 Region 分裂不均，影响整体性能

#### 3. 避免热点的方案

| 方案 | 做法 | 优点 | 缺点 |
|------|------|------|------|
| **反转（Reversing）** | RowKey 反转（如 uid=123 → 321） | 散列性好 | 牺牲范围查询 |
| **哈希（Hashing）** | RowKey 前加 `hash(key) % N` | 散列均匀 | 牺牲范围查询 |
| **加盐（Salting）** | RowKey 前加随机前缀 `[0..N)` | 散列均匀 | Scan 需查所有前缀 |
| **组合键** | `[hash(uid)] + [ts]` | 兼顾散列和按 uid 范围查询 | 设计复杂 |

#### 4. 预分区
- 建表时指定分区边界，避免所有数据初始写入一个 Region
- `create 't', 'cf', SPLITS => ['1','2','3','4','5','6','7','8','9']`
- 配合哈希方案，分区数与哈希取模数一致

#### 追问简答
- Q: 既想散列又想范围查询怎么办？  A: 用组合键 `[hash(uid)] + [ts]`，或建二级索引（Phoenix/Elasticsearch）
- Q: 如何排查热点？  A: HBase UI 查看 Region 请求分布，hbck 工具检查 Region 分布

#### 生产实践注意点
- 预分区数通常 16-64 个，根据数据量和集群规模决定
- RowKey 反转方案适合"前缀相似后缀不同"的场景（如手机号）
- 范围查询需求强烈时考虑 Phoenix 二级索引或 Elasticsearch

---

## SAQ-015 [L3] [HBase/LSM/Compaction与分裂]
**题干**：HBase 的 LSM 树原理是什么？Region 分裂机制是怎样的？

### 标准答案

#### 1. LSM 树原理
- LSM（Log-Structured Merge-Tree）树是一种写优化数据结构，将随机写转化为顺序写
- 写入流程：
  1. 数据先写 **WAL**（预写日志，保证持久性）
  2. 再写 **MemStore**（内存，按 RowKey 排序）
  3. 返回成功
  4. MemStore 达到阈值后 **Flush** 到 HFile（磁盘，有序文件）

**示意图描述**：WAL → MemStore（内存有序）→ Flush → HFile（磁盘有序）→ Compaction 合并

#### 2. Compaction 机制
- **Minor Compaction**：合并少量小 HFile（默认 3 个），速度快，不清理删除标记和过期数据
- **Major Compaction**：合并所有 HFile，清理删除标记和过期数据（TTL），产生单个大 HFile，耗时长
- 触发条件：HFile 数量达阈值（`hbase.hstore.compactionThreshold` 默认 3）

#### 3. 布隆过滤器优化读
- HBase 在读时需合并 MemStore + 多个 HFile
- 每个 HFile 关联一个 BloomFilter，快速判断某 RowKey 是否在该 HFile 中
- 避免扫描所有 HFile，显著提升随机读性能

#### 4. Region 分裂机制
- Region 是 HBase 表的分片单元，按 RowKey 范围划分
- 当 Region 大小达到阈值（`hbase.hregion.max.filesize`，默认 10GB）时自动**分裂**（Split）
- 分裂流程：
  1. Region 下线，停止服务
  2. 按 SplitPoint 将 Region 分成两个子 Region
  3. 子 Region 上线，分配到不同 RegionServer
- 分裂期间该 Region **短暂阻塞读写**（Flush 阶段）

#### 追问简答
- Q: Minor 和 Major 的区别？  A: Minor 合并少量小 HFile 不清理删除标记；Major 合并所有 HFile 清理过期数据，耗时长
- Q: Compaction 风暴如何避免？  A: 调节 `hbase.hstore.compaction.max` 限制并发，生产环境关闭自动 Major 改为低峰手动触发

#### 生产实践注意点
- 生产环境常关闭自动 Major Compaction（`hbase.hregion.majorcompaction=0`），改为业务低峰期手动触发
- Region 分裂会短暂阻塞，可预分区减少分裂频率
- 布隆过滤器配置 `ROW` 或 `ROWCOL`，根据查询模式选择

---

## SAQ-016 [L2] [数仓/分层/各层职责]
**题干**：设计一个数仓分层方案，各层职责是什么？

### 标准答案

#### 1. 标准五层数仓架构

| 层级 | 名称 | 职责 | 数据特性 |
|------|------|------|---------|
| **ODS** | 操作数据层 | 贴源层，原始数据落地，基本不处理 | 原始数据、可能含噪声 |
| **DIM** | 维度层 | 维度数据存储，缓慢变化维处理 | 维度表、星型模型 |
| **DWD** | 明细数据层 | 清洗加工后的明细事实数据，按业务过程组织 | 明细宽表、规范化 |
| **DWS** | 汇总数据层 | 按主题+维度汇总，粒度更粗 | 聚合数据、按日/周汇总 |
| **ADS** | 应用数据层 | 面向应用的指标结果，直接供 BI 报表使用 | 高度汇总、指标结果 |

#### 2. 分层价值
- **解耦**：上层不直接依赖业务系统，业务变化时只需调整 ODS
- **复用**：DWD/DWS 被多个 ADS 引用，避免重复 ETL
- **规范口径**：统一数据加工逻辑，保证指标一致性
- **血缘追踪**：清晰的数据流转链路，便于问题排查和影响分析
- **性能优化**：预计算 DWS 层，加速 ADS 查询

#### 3. 数据流向
- ODS → DWD（清洗、脱敏、维度退化）→ DWS（按主题汇总）→ ADS（指标计算）→ BI/报表
- DIM 层供 DWD/DWS/ADS 引用

#### 追问简答
- Q: DWD 和 DWS 的区别？  A: DWD 存明细事实数据按业务过程组织，DWS 按主题+维度汇总粒度更粗
- Q: 为什么要做宽表？  A: 减少 Join、提升查询性能、统一口径

---

## SAQ-017 [L2] [数仓/SCD/拉链表]
**题干**：缓慢变化维（SCD）如何处理？拉链表是什么？

### 标准答案

#### 1. 缓慢变化维（SCD）处理方式

| 类型 | 处理方式 | 特点 | 适用场景 |
|------|---------|------|---------|
| **SCD1** | 直接覆盖旧值 | 不保留历史 | 不重要的维度变更 |
| **SCD2** | 新增一行记录，加有效时间区间 | 保留完整历史 | 需追溯历史的维度 |
| **SCD3** | 增加历史列，仅保留上一次值 | 仅保留上一版本 | 只需对比当前与上一版 |

#### 2. 拉链表（SCD2 实现）
- 拉链表通过 `start_date` 和 `end_date` 两个字段记录每条记录的有效时间区间
- 当前有效记录：`start_date <= biz_date AND end_date = '9999-12-31'`
- 历史记录：`start_date <= biz_date AND end_date > biz_date`

#### 3. 拉链表更新流程
1. 比较每日全量快照与拉链表当前记录
2. **变化记录**：关闭旧版本（`end_date = 昨日`），新增版本（`start_date = 今日, end_date = 9999-12-31`）
3. **未变化记录**：保留不变（`end_date = 9999-12-31`）
4. **新增记录**：直接插入（`start_date = 今日, end_date = 9999-12-31`）

#### 4. 查询示例
```sql
-- 查询某业务日期的有效记录
SELECT * FROM user_dim
WHERE start_date <= '2026-06-01' AND end_date > '2026-06-01';
```

#### 追问简答
- Q: SCD2 和 SCD3 适用场景？  A: SCD2 需追溯所有历史版本（用户等级变更）；SCD3 仅需对比当前与上一版
- Q: 拉链表如何查询某日数据？  A: `WHERE start_date <= biz_date AND end_date > biz_date`

---

## SAQ-018 [L3] [数据倾斜/定位与解决]
**题干**：数据倾斜如何定位？如何解决？

### 标准答案

#### 1. 数据倾斜本质与表现
- 本质：某 key 数据量远超其他 key，导致 reduce 处理时某 Task 数据量过大
- 表现：
  - Job 进度长期卡在 99%（少数 Task 未完成）
  - 某 Task 执行时间是其他 Task 的 5-10 倍
  - 某 Task 处理数据量远超均值（如单 Task 处理 80% 数据）
  - 严重时 Task OOM 退出

#### 2. 定位方法
- **Spark Web UI**：查看 Stage 的 Task 分布，找耗时/数据量异常的 Task
- **抽样统计 key**：`df.select("key").rdd.map(x => (x, 1)).reduceByKey(_+_).sortBy(_._2, false).take(10)` 找 top key
- **查看 Shuffle Read**：Spark UI 查看 Task 的 Shuffle Read Bytes，找异常大的 Task
- **日志分析**：查看 Executor 日志中 OOM 或长耗时 Task 的处理 key

#### 3. 解决方案

**方案1：广播变量 + Map Join**
- 小表广播到所有 Executor，在 Map 端完成 join，避免 reduce 端 Shuffle
- Spark：`broadcast(small_df).join(big_df)` 或 `/*+ BROADCAST(small_table) */`
- 适用：小表（< 2GB）join 大表

**方案2：两阶段聚合（Salting）**
- 第一阶段：给倾斜 key 加随机前缀 `[1..N]`，按 `(prefix, key)` 聚合（局部聚合）
- 第二阶段：去掉前缀，按 `key` 聚合（全局聚合）
- 原理：随机前缀将大 key 拆到 N 个 reducer，局部聚合后数据量降到 1/N
- 适用：group by 倾斜

**方案3：采样拆分 Join**
- 采样找出倾斜 key，单独处理
- 倾斜 key：加随机前缀 + 另一表膨胀 N 倍加 `[1..N]` 后缀 join
- 非倾斜 key：正常 join
- 两者 union

**方案4：增加分区数**
- `spark.sql.shuffle.partitions=400`（默认 200）
- 对均匀分布有效，对极端倾斜效果有限

#### 追问简答
- Q: 两阶段聚合为什么有效？  A: 随机前缀把大 key 拆到多个 reducer 局部聚合，数据量降到 1/N 后再全局聚合
- Q: 倾斜 key 无法过滤怎么办？  A: 用"倾斜 key 单独处理 + 非倾斜 key 正常逻辑"的 union 方案

#### 生产实践注意点
- 先定位再解决，盲目调参（如增加 reduce 数）对极端倾斜无效
- join 倾斜优先尝试 MapJoin（小表场景）
- Kafka 倾斜（某分区 lag 高）通过调整分区策略或消费者处理能力解决
- Redis 倾斜通过一致性哈希 + 热点 key 拆分（加随机后缀分散到多个 key）

---

## SAQ-019 [L3] [Spark vs Flink/选型]
**题干**：Spark 和 Flink 有什么区别？分别适用于什么场景？

### 标准答案

#### 1. 核心对比

| 维度 | Spark | Flink |
|------|-------|-------|
| **计算模型** | 微批（Micro-Batch） | 真流（Native Streaming） |
| **延迟** | 秒级（Spark Streaming 100ms+） | 毫秒级 |
| **状态管理** | 较弱（Structured Streaming 有改进） | 丰富（Keyed State/Operator State/RocksDB） |
| **时间语义** | Processing Time + Event Time | Processing Time + Event Time + Ingestion Time |
| **Watermark** | 较简单 | 成熟完善（含迟到数据处理） |
| **Exactly-Once** | 基于 Checkpoint，Sink 端幂等 | 端到端（Source 可重放 + 2PC Sink） |
| **窗口** | 滚动/滑动/会话 | 滚动/滑动/会话/全局 + 自定义触发器 |
| **API 风格** | DataFrame/SQL 为主 | DataStream/SQL 为主 |
| **生态** | 批处理生态成熟（Spark SQL/MLlib/GraphX） | 流处理生态领先 |
| **资源管理** | YARN/K8s/Standalone | YARN/K8s/Standalone/Mesos |

#### 2. 适用场景

**Spark 适用**：
- 离线批处理 ETL（T+1 数仓）
- 交互式分析（Spark SQL）
- 机器学习（MLlib）
- 图计算（GraphX）
- 对延迟要求不高的近实时场景（Structured Streaming）

**Flink 适用**：
- 实时流处理（毫秒级延迟）
- 复杂事件处理（CEP）
- 实时数仓（DWD/DWS 实时化）
- 状态计算（如实时累计 UV/GMV）
- 需要严格 Exactly-Once 的场景

#### 3. 选型建议
- 批多流少 → Spark（生态成熟、SQL 强大）
- 流为主 → Flink（流处理原生、状态管理强）
- 混合场景 → 批用 Spark + 流用 Flink（Lambda 架构）或全 Flink（Kappa 架构）

#### 追问简答
- Q: Spark Structured Streaming 和 Flink 还有差距吗？  A: 延迟（Spark SS 微批 100ms+ vs Flink 毫秒级）、状态管理（Flink 更丰富）、Event Time/Watermark（Flink 更成熟）

#### 生产实践注意点
- Spark 微批模型在小数据量高频场景下延迟较高，不适合毫秒级需求
- Flink 的状态管理在大状态场景下需配合 RocksDB 和增量 Checkpoint
- 实时数仓趋势：Flink 替代 Spark Streaming 做 DWD/DWS 实时化

---

## SAQ-020 [L3] [Spark/Shuffle/实现与优化]
**题干**：Spark 的 Shuffle 有哪几种实现？如何优化 Shuffle？

### 标准答案

#### 1. Spark Shuffle 实现

| 实现 | 文件数 | 特点 | 版本 |
|------|--------|------|------|
| **HashShuffle** | M×R | 每 MapTask 每 Reduce 生成一个文件，小文件过多 | Spark 1.x 以前 |
| **SortShuffle** | M | 每 MapTask 一个 sorted 文件 + index 文件，文件数减少 | Spark 1.6 后默认 |
| **BypassMergeSortShuffle** | M | 无 Map 端聚合且分区数小时，直接写文件后合并，避免排序开销 | SortShuffle 优化 |
| **Tungsten Sort** | M | 直接操作序列化字节，内存效率高 | 性能优化 |

#### 2. Bypass 触发条件
- 分区数 ≤ `spark.shuffle.sort.bypassMergeThreshold`（默认 200）
- 且 Map 端无聚合操作
- 满足时直接写文件后合并，避免排序开销

#### 3. Shuffle 优化手段

**减少 Shuffle 数据量**：
- **broadcast join**：小表广播避免 reduce 端 join（`broadcast(small_df)`）
- **Map 端聚合**：用 `reduceByKey` 替代 `groupByKey`（Map 端 combine 减少数据量）
- **过滤无用数据**：Shuffle 前过滤无用列和行（列裁剪、谓词下推）

**调整并行度**：
- `spark.sql.shuffle.partitions=400`（默认 200，根据数据量调整）
- 分区数过多小文件多，过少单 Task 数据大易 OOM

**序列化优化**：
- 使用 Kryo 序列化（`spark.serializer=org.apache.spark.serializer.KryoSerializer`）
- 注册自定义类提升序列化速度

**内存与文件配置**：
- `spark.shuffle.memoryFraction`：Shuffle 内存比例
- `spark.shuffle.io.maxRetries`：Shuffle 拉取重试次数
- `spark.sql.adaptive.enabled=true`：自适应调整 Shuffle 分区数（AQE）

#### 追问简答
- Q: reduceByKey 和 groupByKey 的区别？  A: reduceByKey 在 Map 端先 combine 减少 Shuffle 数据量；groupByKey 不 combine，所有数据直接 Shuffle，易 OOM
- Q: Bypass 机制触发条件？  A: 分区数 ≤ 200 且 Map 端无聚合时触发，直接写文件后合并

#### 生产实践注意点
- 生产环境优先用 SortShuffle（默认），开启 AQE 自适应调整分区
- `reduceByKey` 总是优于 `groupByKey`（除非需要完整 Iterable）
- Shuffle 是 Spark 性能瓶颈，优先用 broadcast join 减少 Shuffle

---

## SAQ-021 [L2] [Hive/SQL优化/通用手段]
**题干**：Hive SQL 优化有哪些手段？

### 标准答案

#### 1. Fetch 抓取
- `hive.fetch.task.conversion=more`：简单查询（SELECT、LIMIT、字段过滤）不走 MapReduce，直接读取文件
- 避免简单查询启动 MR 作业的开销

#### 2. 本地模式
- `hive.exec.mode.local.auto=true`：小数据量作业在本地执行，不分配 YARN 资源
- 触发条件：`hive.exec.mode.local.auto.inputbytes`（默认 128MB）、`hive.exec.mode.local.auto.input.files`（默认 4）

#### 3. MapJoin
- 小表广播到所有 Map 端，避免 reduce 端 join
- `/*+ MAPJOIN(small_table) */` 或 `hive.auto.convert.join=true`（自动判断）
- `hive.mapjoin.smalltable.filesize`（默认 25MB）：小表阈值

#### 4. 列裁剪与分区裁剪
- **列裁剪**：只 SELECT 需要的列，减少 IO（Hive 默认开启 `hive.optimize.cp=true`）
- **分区裁剪**：WHERE 条件加分区字段过滤，只扫描需要的分区

#### 5. 并行执行
- `hive.exec.parallel=true`：无依赖的 Stage 并行执行
- `hive.exec.parallel.thread.number`：并行度（默认 8）

#### 6. 严格模式
- `hive.mapred.mode=strict`：禁止全表扫描（必须加分区过滤）、禁止 order by 不加 limit、禁止笛卡尔积

#### 7. JVM 重用
- `mapreduce.job.jvm.numtasks`：JVM 复用执行多个 Task，减少启动开销（Hadoop 2.x 后已弱化）

#### 8. 合理设置 reduce 数
- `mapreduce.job.reduces=N`：根据数据量调整
- 经验公式：reduce 数 = 数据量 / 每个reduce处理量（默认 256MB）

#### 追问简答
- Q: 小文件问题怎么解决？  A: 合并小文件（`hive.merge.mapfiles=true`、`concatenate` 命令）、调整 reduce 数、用 Spark/Tez 引擎

---

## SAQ-022 [L2] [Kafka/消息积压/处理]
**题干**：Kafka 消息积压如何处理？

### 标准答案

#### 1. 排查 Consumer Lag
- `kafka-consumer-groups.sh --describe --group <group>`：查看每个分区的 lag
- 监控工具：Burrow、Kafka Manager、JMX 指标 `records-lag-max`
- 告警阈值：按业务 SLA 设置（如 lag > 10000 告警）

#### 2. 增加消费者实例
- 同一消费者组内增加消费者数（不超过分区数）
- 消费者数 > 分区数时多余消费者空闲（一个分区只能被组内一个消费者消费）

#### 3. 增加分区数
- `kafka-topics.sh --alter --topic <topic> --partitions N`：增加分区数
- 注意：增加分区后无法保证全局顺序性（新分区与旧分区无序）
- 需同步增加消费者数利用新分区

#### 4. 提升消费端处理能力
- 优化消费逻辑：批量处理替代单条处理、异步化非关键路径
- 增加消费者并行度：多线程消费（注意 offset 提交线程安全）
- 优化下游存储：批量写入、连接池

#### 5. 临时扩容策略
- 临时增加消费者实例（如 K8s 弹性扩容）
- 临时增加分区数 + 消费者数
- 降级非关键消费逻辑（如跳过部分非必要处理）

#### 追问简答
- Q: 消费者数能超过分区数吗？  A: 不能有效消费。多余消费者空闲，因为一个分区只能被组内一个消费者消费
- Q: 如何监控 Consumer Lag？  A: kafka-consumer-groups.sh、Burrow、Kafka Manager、JMX 指标 records-lag-max

#### 生产实践注意点
- 积压根因往往是消费端处理慢而非 Kafka 本身，优先优化消费逻辑
- 多线程消费时需手动管理 offset 提交，避免线程间顺序问题

---

## SAQ-023 [L1] [HBase/对比/HBase vs MySQL]
**题干**：HBase 和关系型数据库（MySQL）有什么区别？什么场景使用 HBase？

### 标准答案

#### 1. HBase vs MySQL 对比

| 维度 | HBase | MySQL |
|------|-------|-------|
| **数据模型** | NoSQL 列式存储（KV） | 关系型，表结构固定 |
| **查询语言** | API/SQL（Phoenix） | 标准 SQL |
| **事务** | 单行强一致，无多行事务 | ACID 事务 |
| **Join** | 不支持复杂 Join | 支持复杂 Join |
| **扩展性** | 水平扩展（PB 级） | 垂直扩展为主（TB 级） |
| **写入性能** | 写多读少，百万 TPS | 中等，受索引约束 |
| **查询模式** | 按 RowKey 单点查、Scan 范围查 | 复杂查询、多条件组合 |
| **Schema** | 列动态可变 | 列固定，DDL 修改成本高 |

#### 2. HBase 适用场景
- 海量数据存储（TB+，PB 级）
- 写多读少的场景（如日志、订单流水、用户行为）
- 按 RowKey 随机读写
- 数据列动态变化
- 不需要复杂 SQL 和多表 Join

#### 3. MySQL 适用场景
- 数据量适中（GB-TB 级）
- 需要 ACID 事务保证
- 复杂查询和多表关联
- 结构化固定的业务数据（如订单、用户、账户）

#### 追问简答
- Q: HBase 和 Redis 的区别？  A: Redis 纯内存 KV 亚毫秒级延迟但数据量受内存限制；HBase 基于磁盘 LSM 毫秒级延迟支持 PB 级数据

---

## SAQ-024 [L3] [Flink/反压/排查与解决]
**题干**：Flink 的反压（Backpressure）是什么？如何解决？

### 标准答案

#### 1. 反压定义与表现
- 反压：下游算子处理速度慢于上游，导致数据在算子间缓冲区堆积，反向传导给上游
- 表现：
  - 吞吐量下降
  - Checkpoint 超时或失败（Barrier 排队等待）
  - Web UI 显示算子处于 High Backpressure 状态
  - 数据延迟增大

#### 2. 反压原因
- **下游算子处理慢**：复杂计算、外部 IO（数据库、RPC）阻塞
- **数据倾斜**：某 key 数据量过大，对应 Task 成为瓶颈
- **GC 频繁**：JVM Full GC 导致 Task 停顿
- **资源不足**：CPU/内存/网络达到瓶颈
- **窗口缓存过多**：大窗口缓存大量数据

#### 3. 排查方法
- **Flink Web UI**：BackPressure 指标（HIGH/LOW），查看各算子反压状态
- **Metrics**：`outPoolUsage`（输出缓冲区使用率）高 + `inPoolUsage`（输入缓冲区使用率）低 = 下游瓶颈
- **火焰图（Flame Graph）**：Flink 1.13+ 支持，定位算子内耗时方法
- **GC 日志**：检查 Full GC 频率和耗时
- **Checkpoint 时长**：Checkpoint 超时往往是反压的信号

#### 4. 解决方案

**提升处理能力**：
- 增加并行度（`setParallelism`）
- 优化算子逻辑：减少外部 IO、批量处理
- 异步化：`AsyncFunction` 异步查询外部系统

**解决数据倾斜**：
- KeyBy 前加盐打散 + 二次聚合
- 调整分区策略

**资源优化**：
- 增加 TaskManager 内存
- 优化 GC 参数（G1GC）
- 增加网络缓冲区（`taskmanager.network.memory.fraction`）

**反压容错**：
- 开启 Unaligned Checkpoint 解决反压下 Checkpoint 超时
- 限制缓冲区大小避免 OOM

#### 追问简答
- Q: Unaligned Checkpoint 原理？  A: Barrier 直接越过 in-flight 数据立即快照，适合反压场景（详见 SAQ-024-V1）
- Q: 反压如何监控？  A: Web UI BackPressure 指标、Metrics 的 outPoolUsage/inPoolUsage、火焰图

#### 生产实践注意点
- 反压根因通常是下游算子慢或数据倾斜，优先定位瓶颈算子
- 外部 IO（如查询 MySQL）用 `AsyncFunction` 异步化，避免阻塞
- 反压严重时 Checkpoint 超时，考虑 Unaligned Checkpoint

---

## SAQ-025 [L2] [Zookeeper/watcher/机制]
**题干**：Zookeeper 的 watcher 机制是什么？有哪些应用场景？

### 标准答案

#### 1. Watcher 机制
- 客户端注册 Watcher 监听节点变化（创建、删除、数据变更、子节点变更）
- 节点变化时 ZooKeeper **通知**注册的客户端
- Watcher 是**一次性触发**：触发后失效，需重新注册才能继续监听

#### 2. Watcher 特性
- **一次性触发**：事件发生后 Watcher 失效，需在回调中重新注册
- **客户端串行接收**：同一客户端的 Watcher 通知串行处理，保证顺序
- **轻量通知**：通知只含事件类型和节点路径，不含数据内容，需客户端主动获取
- **最终一致性**：客户端收到通知时节点可能已变化，需重新读取最新数据

#### 3. 应用场景

| 场景 | Watcher 监听对象 | 说明 |
|------|----------------|------|
| **Kafka Broker 监控** | `/brokers/ids` | Broker 上下线感知 |
| **HBase Master 选举** | `/master` | Master 故障时触发选举 |
| **HDFS HA** | ZKFC 监控 NameNode | NameNode 故障时切换 |
| **分布式锁** | 前一个临时节点 | 节点删除时唤醒下一个等待者 |
| **配置中心** | 配置节点 | 配置变更时通知客户端更新 |
| **服务发现** | 服务注册节点 | 服务上下线感知 |

#### 追问简答
- Q: 一次性 Watcher 有什么问题？  A: 触发后失效，注册到再次触发期间事件可能丢失；解决：在回调中重新注册（Curator 的 Cache 机制自动重注册）
- Q: 分布式锁如何避免羊群效应？  A: 用临时顺序节点 + 监听前一个节点，而非所有客户端监听同一节点

#### 生产实践注意点
- 一次性 Watcher 需在回调中重新注册，否则会漏掉事件
- 生产环境推荐用 Curator 的 NodeCache/PathChildrenCache 自动重注册
- 分布式锁用临时顺序节点避免羊群效应

---

## SAQ-001-V1 [L4] [Kafka/消息重复/幂等性事务]
**题干**：如何保证 Kafka 消息不重复（Exactly-Once 语义）？幂等性 Producer 能保证什么、不能保证什么？

### 标准答案

#### 1. 幂等性 Producer
- 配置：`enable.idempotence=true`
- 原理：Producer 获得 PID（Producer ID），每条消息携带 SequenceNumber
- Broker 端去重：Leader 按 `<PID, Partition, SequenceNumber>` 去重，重复消息被丢弃
- **能保证**：单分区单会话内不重复（PID 在 Producer 重启后变化）
- **不能保证**：
  - 跨分区不重复（多分区无序保证）
  - 跨会话不重复（PID 变化后无法去重）
  - 端到端 Exactly-Once（Consumer 仍可能重复消费）

#### 2. 事务机制（跨分区跨会话）
- 配置：`transactional.id=<唯一ID>`（跨会话保持一致性）
- Producer 流程：
  1. `initTransactions()`：初始化事务协调器
  2. `beginTransaction()`：开启事务
  3. `send()` + `sendOffsetsToTransaction()`：发送消息并提交消费 offset
  4. `commitTransaction()`：提交事务（所有分区原子提交）
  5. `abortTransaction()`：回滚事务（故障时）
- Broker 端：事务日志记录事务状态，所有分区要么全部可见要么全部不可见

#### 3. Consumer 端配合
- `isolation.level=read_committed`：只读取已提交的事务消息
- `read_uncommitted`（默认）：读取所有消息（含未提交）

#### 4. 端到端 Exactly-Once
- Source 端：可重放数据源（如 Kafka，offset 作为事务的一部分提交）
- Flink/Spark 内部：Checkpoint 状态一致性
- Sink 端：
  - **幂等写入**：Redis SET、MySQL INSERT ON DUPLICATE KEY（重复写入无副作用）
  - **事务写入**：Kafka 事务、MySQL XA、两阶段提交（2PC）
- 关键：offset 作为状态保存，故障恢复后从 Checkpoint offset 重新消费 + Sink 幂等/事务保证不重

#### 5. Kafka Streams 事务
- Kafka Streams 内置 Exactly-Once（`processing.guarantee=exactly_once_v2`）
- 自动处理事务提交，无需手动编码

#### 追问简答
- Q: 幂等性 Producer 能保证什么？  A: 仅保证单分区单会话内不重复，跨分区/跨会话需事务
- Q: 事务 Producer 如何保证跨会话一致？  A: `transactional.id` 跨会话不变，Broker 通过 epoch 号防止"僵尸 Producer"提交旧事务

#### 生产实践注意点
- 幂等性 Producer 性能开销小（约 5%），生产环境建议默认开启
- 事务 Producer 性能开销较大（吞吐降 20%+），仅在严格 Exactly-Once 场景使用
- Consumer 必须设置 `read_committed`，否则事务无效
- 端到端 Exactly-Once 还需 Sink 端支持幂等或事务，否则仍有重复

---

## SAQ-002-V1 [L3] [Kafka/ISR/unclean选举风险]
**题干**：当 ISR 为空时，开启 unclean.leader.election.enable=true 会有什么风险？应该如何权衡？

### 标准答案

#### 1. ISR 为空时的风险
- ISR 为空表示所有 Follower 都落后于 Leader
- 开启 `unclean.leader.election.enable=true` 时，从**非 ISR 副本**（数据落后的副本）中选 Leader
- 风险：
  1. **已提交消息丢失**：新 Leader 数据落后，未追上 Leader 的已提交消息（在旧 Leader 上但不在新 Leader 上）丢失
  2. **数据不一致**：不同副本数据进度不同，选举后可能出现数据冲突
  3. **Consumer offset 错乱**：offset 可能回退，导致重复消费或跳过消息

#### 2. 权衡维度

| 场景 | 推荐配置 | 理由 |
|------|---------|------|
| **数据一致性优先**（交易、金融） | `unclean.leader.election.enable=false` | 宁可不可用也不丢数据，等待 ISR 恢复 |
| **可用性优先**（日志、监控） | `unclean.leader.election.enable=true` | 接受少量数据丢失换取服务可用 |
| **Kafka 0.11+ 默认** | `false` | 官方认为数据安全更重要 |

#### 3. 生产环境最佳实践
- `unclean.leader.election.enable=false`（默认）
- `replication.factor=3` + `min.insync.replicas=2`：容忍 1 个副本故障，ISR 至少 2 个
- 监控 ISR 缩减告警：ISR 数量 < `min.insync.replicas` 时告警，及时处理
- 合理设置 `replica.lag.time.max.ms`（默认 10s），避免落后副本长期留在 ISR

#### 4. ISR 为空时的正确处理
1. 确认 ISR 为空原因（网络分区、Follower 故障、负载过高）
2. 优先恢复 Follower 同步，等待 ISR 重建
3. 若 ISR 无法恢复且必须恢复服务：
   - 临时开启 unclean 选举（接受数据丢失）
   - 或人工介入修复 Follower 数据后重建 ISR

#### 追问简答
- Q: Kafka 0.11 后为什么默认关闭 unclean 选举？  A: 官方认为数据安全比可用性更重要，避免已提交消息丢失
- Q: 如何监控 ISR 缩减？  A: JMX 指标 `UnderReplicatedPartitions`，ISR 数量 < 副本因子时告警

#### 生产实践注意点
- 交易类业务严禁开启 unclean 选举
- 日志类业务可接受开启，但需明确告知业务方数据可能丢失
- ISR 缩减告警是生产环境必备监控项

---

## SAQ-003-V1 [L2] [Kafka/Pull模式/空轮询]
**题干**：Kafka Pull 模式有什么缺点？如何避免消费者一直拉到空消息（轮询空转）？

### 标准答案

#### 1. Pull 模式缺点
- **实时性略差**：Consumer 主动轮询，新消息到达后需等下次 poll 才能消费，存在延迟
- **空轮询浪费资源**：无消息时 poll 返回空，Consumer 频繁轮询消耗 CPU 和网络资源
- **延迟与吞吐权衡**：轮询间隔短延迟低但开销大，间隔长延迟高但开销小

#### 2. 避免空轮询的机制

**长轮询（Long Polling）**：
- Broker 端支持：Consumer poll 请求时若 Broker 无消息，不立即返回，而是阻塞等待
- 直到有新消息或超时（`fetch.max.wait.ms`）才返回
- 兼顾实时性和资源效率

**关键参数**：
- `fetch.min.bytes`（默认 1）：Broker 返回前需累积的最小数据量
  - 设为 ≥ 1 时，无消息时 Broker 阻塞等待（不立即返回空）
  - 设为较大值（如 1024）可减少小批次返回，提升吞吐但增加延迟
- `fetch.max.wait.ms`（默认 500ms）：配合 `fetch.min.bytes`，Broker 等待最大时间
  - 超时后即使数据量不足 `fetch.min.bytes` 也返回
- `fetch.max.bytes`（默认 50MB）：单次返回最大数据量

#### 3. 生产环境配置建议
- 实时性要求高：`fetch.min.bytes=1`, `fetch.max.wait.ms=100ms`
- 吞吐优先：`fetch.min.bytes=1024`, `fetch.max.wait.ms=500ms`
- 大部分场景默认配置（`fetch.min.bytes=1`, `fetch.max.wait.ms=500ms`）即可

#### 追问简答
- Q: 长轮询如何工作？  A: Broker 无消息时阻塞 poll 请求，有消息或超时后才返回，避免空轮询
- Q: fetch.min.bytes 设大有什么影响？  A: 减少小批次返回提升吞吐，但增加延迟

---

## SAQ-004-V1 [L3] [Kafka/Rebalance/触发与优化]
**题干**：Kafka Rebalance 的触发条件有哪些？Rebalance 会导致什么问题？如何减少 Rebalance？

### 标准答案

#### 1. Rebalance 触发条件
- **消费者加入组**：新消费者上线
- **消费者离开组**：消费者主动关闭、Consumer 被 kill
- **消费者心跳超时**：`session.timeout.ms` 内未收到心跳，Coordinator 认为消费者故障
- **消费者处理超时**：两次 poll 间隔超过 `max.poll.interval.ms`，被 Coordinator 踢出组
- **分区数变化**：Topic 增加分区
- **订阅主题变化**：消费者订阅的主题列表变化（正则匹配新增 Topic）

#### 2. Rebalance 导致的问题
- **Stop-The-World**：Rebalance 期间所有消费者**停止消费**，等待重新分配
- **消费延迟**：Rebalance 耗时期间消息积压
- **重复消费风险**：Rebalance 前未提交的 offset 可能在恢复后重复消费
- **频繁 Rebalance 影响稳定性**：每次 Rebalance 消费暂停数秒到数十秒

#### 3. 减少 Rebalance 的策略

**合理设置心跳参数**：
- `session.timeout.ms`（默认 10s）：心跳超时，适当调大避免误判（如 30s）
- `heartbeat.interval.ms`（默认 3s）：心跳间隔，通常为 `session.timeout.ms` 的 1/3
- 确保心跳线程独立于业务处理线程，不受处理耗时影响

**合理设置 poll 参数**：
- `max.poll.interval.ms`（默认 5min）：两次 poll 最大间隔，需大于单批处理时间
- `max.poll.records`（默认 500）：单次 poll 最大记录数，处理慢时调小（如 100）
- 确保单批处理时间 < `max.poll.interval.ms`

**使用 StickyAssignor**：
- `partition.assignment.strategy=org.apache.kafka.clients.consumer.StickyAssignor`
- 尽量保持原分配方案，减少分区迁移
- CooperativeStickyAssignor 支持增量 Rebalance（不停止所有消费者）

**避免消费者频繁上下线**：
- 消费者优雅关闭（`consumer.close()` 主动离开）
- 避免在 K8s 中频繁滚动更新
- 消费者处理慢时优化逻辑而非重启

#### 追问简答
- Q: max.poll.interval.ms 和 session.timeout.ms 的区别？  A: 前者是两次 poll 最大间隔（处理超时），后者是心跳超时（连接故障）
- Q: StickyAssignor 有什么优势？  A: 尽量保持原分配，减少分区迁移和重复消费

#### 生产实践注意点
- 消费者处理慢时优先调小 `max.poll.records` 而非调大 `max.poll.interval.ms`
- 心跳线程独立于业务处理，不受处理耗时影响
- CooperativeStickyAssignor（Kafka 2.4+）支持增量 Rebalance，减少 Stop-The-World

---

## SAQ-005-V1 [L4] [Kafka/顺序性/权衡]
**题干**：业务既要全局有序又要高吞吐，应该如何架构？请给出方案并说明权衡点。

### 标准答案

#### 1. 问题本质
- Kafka 仅保证单分区内有序，多分区无全局顺序保证
- 全局有序 → 单分区 → 吞吐上限 = 单分区吞吐（约 10MB/s）
- 高吞吐 → 多分区并行 → 无法全局有序
- 二者本质矛盾，需根据业务需求权衡

#### 2. 方案1：局部有序 + 桶间并行（推荐）
- **设计**：按业务实体分桶（如按 uid 取模分 N 分区），相同 key 写同一分区
- **保证**：桶内（同 key）有序，桶间无序
- **吞吐**：N 分区并行，吞吐 ≈ N × 单分区吞吐
- **适用**：绝大多数业务（订单状态、用户行为、消息流）

```
Producer → 按 uid % N 路由 → Partition 0（uid1, uid11... 有序）
                          → Partition 1（uid2, uid12... 有序）
                          → Partition N（uidN, uid... 有序）
```

#### 3. 方案2：消费端按 key 重排
- **设计**：多分区并行消费 + 下游（Flink/Spark）按 key 窗口排序后下发
- **保证**：窗口内按 key 有序（窗口间可能无序）
- **吞吐**：消费端多分区并行，排序在下游
- **适用**：可容忍窗口延迟的业务（如每分钟排序一次）

```
Kafka（多分区无序）→ Flink KeyBy(uid) → 窗口内排序 → 下发
```

#### 4. 方案3：单分区 + 异步刷盘优化
- **设计**：单分区保证全局有序，优化单分区吞吐
- **优化手段**：
  - 增大 `batch.size`（默认 16KB → 1MB）
  - 增大 `linger.ms`（默认 0 → 10ms）
  - 使用 SSD + 高性能网卡
  - Producer 端异步发送 + 批量确认
- **吞吐上限**：优化后约 50-100MB/s（仍远低于多分区）
- **适用**：严格全局有序且数据量不大的场景（如审计日志）

#### 5. 方案4：分层架构
- **设计**：热数据单分区有序 + 冷数据多分区并行
- 实时层：单分区保证有序（低延迟）
- 批处理层：多分区并行处理（高吞吐）
- 最终一致：批处理修正实时层
- **适用**：Lambda 架构思想，兼顾有序和高吞吐

#### 6. 权衡点

| 方案 | 有序性 | 吞吐 | 延迟 | 复杂度 | 适用场景 |
|------|--------|------|------|--------|---------|
| 单分区 | 全局有序 | 极低 | 低 | 低 | 严格全局有序 |
| 局部有序（分桶） | 按 key 有序 | 高 | 低 | 中 | 绝大多数业务 |
| 消费端重排 | 窗口内有序 | 高 | 中 | 高 | 可容忍窗口延迟 |
| 分层架构 | 实时层有序 | 高 | 低 | 极高 | Lambda 架构 |

#### 追问简答
- Q: 大多数业务需要全局有序吗？  A: 不需要。按 key 有序（局部有序）已满足绝大多数业务（如订单状态流转按 orderId 有序）
- Q: 单分区吞吐上限多少？  A: 默认配置约 10MB/s，优化后约 50-100MB/s

#### 生产实践注意点
- 优先评估业务是否真需全局有序，99% 场景按 key 有序即可
- 按 key 路由需注意数据倾斜（某 key 数据量过大），可二次分桶
- 消费端重排方案需下游支持状态管理（Flink Keyed State）

---

## SAQ-006-V1 [L3] [Spark/容错/Checkpoint vs Cache]
**题干**：Spark 中 Checkpoint 和 Cache 的区别是什么？Lineage 过长会有什么问题？

### 标准答案

#### 1. Cache vs Checkpoint 对比

| 维度 | Cache（persist） | Checkpoint |
|------|-----------------|------------|
| **存储位置** | 内存/磁盘（TaskManager 本地） | HDFS（可靠分布式存储） |
| **血缘关系** | 不截断，RDD 仍可重算 | 截断，变为 CheckpointRDD |
| **触发方式** | lazy（需 action 触发） | eager（需 action 触发，但立即落盘） |
| **生命周期** | Job 结束后自动清理 | 需手动清理，持久存在 |
| **故障恢复** | Executor 故障后 Cache 丢失，需重算 | HDFS 持久，直接读取 |
| **适用场景** | 短期重复计算 | 长血缘截断、迭代算法 |

#### 2. Cache 特点
- `persist()` / `cache()` 将 RDD 缓存到内存或磁盘
- 不截断血缘：Cache 失效后仍可根据血缘重算
- 存储级别：MEMORY_ONLY、MEMORY_AND_DISK、DISK_ONLY 等
- Job 结束后自动清理（`unpersist()` 可手动清理）

#### 3. Checkpoint 特点
- `sc.setCheckpointDir("hdfs://...")` 设置 Checkpoint 目录
- `rdd.checkpoint()` 标记 Checkpoint
- 需通过 action 触发（如 `rdd.count()`）
- **截断血缘**：Checkpoint 后 RDD 变为 CheckpointRDD，父 RDD 可被回收
- 推荐先 `cache()` 再 `checkpoint()`：避免 Checkpoint 时重复计算（Checkpoint 是单独的 Job）

#### 4. Lineage 过长的问题
- **重算代价大**：分区丢失时需从血缘起点重算，耗时随血缘长度增长
- **调度开销高**：DAG 调度器需遍历长血缘，调度时间增加
- **栈溢出风险**：递归遍历血缘可能触发 StackOverflowError
- **内存占用**：长血缘的元数据占用 Driver 内存

#### 5. 适用场景
- **Cache**：短期重复计算（如多次 action 的同一 RDD）、Shuffle 前缓存避免重算
- **Checkpoint**：迭代算法（PageRank、ML 训练）、长血缘场景、关键中间结果持久化

#### 追问简答
- Q: 为什么 Checkpoint 前要先 Cache？  A: Checkpoint 会启动单独 Job 重算 RDD，先 Cache 避免重复计算
- Q: Lineage 过长为什么会栈溢出？  A: DAG 调度递归遍历血缘，深度过大时栈溢出

#### 生产实践注意点
- 迭代算法建议每 N 轮 Checkpoint 一次（如 PageRank 每 10 轮）
- Checkpoint 目录需配置 HDFS 高可用路径
- Cache 优先 MEMORY_AND_DISK，避免内存不足时直接重算

---

## SAQ-007-V1 [L3] [Spark/Shuffle/Hash vs Sort]
**题干**：Spark Shuffle 有哪些实现？HashShuffle 和 SortShuffle 的区别与适用场景？

### 标准答案

#### 1. Spark Shuffle 演进

| 版本 | 默认实现 | 说明 |
|------|---------|------|
| Spark 1.0 | HashShuffle | 每 MapTask 每 Reduce 一个文件，M×R 文件 |
| Spark 1.1 | SortShuffle 引入 | 每 MapTask 一个文件 + index |
| Spark 1.6 | SortShuffle 默认 | 替代 HashShuffle |
| Spark 2.0 | SortShuffle 唯一 | HashShuffle 移除 |

#### 2. HashShuffle（已废弃）
- **机制**：每 MapTask 为每个 Reduce 生成一个文件，共 M×R 个文件
- **文件数**：M（MapTask 数）× R（Reduce 数）
- **优点**：实现简单，无排序开销
- **缺点**：小文件过多（M×R 可能达数百万），磁盘 IO 和内存开销大
- **优化**：Consolidated HashShuffle（合并文件，文件数降为 Core×R）

#### 3. SortShuffle（默认）
- **机制**：
  1. MapTask 数据先写入内存缓冲区
  2. 缓冲区满后按 Partition ID 排序，Spill 到磁盘
  3. 多次 Spill 文件 Merge 成一个 sorted 数据文件 + 一个 index 文件
  4. Reduce 端通过 index 文件定位分区数据
- **文件数**：M（每 MapTask 2 个文件）
- **优点**：文件数少，支持排序，内存效率高
- **缺点**：排序开销（但通常可接受）

#### 4. BypassMergeSortShuffle（SortShuffle 优化）
- **触发条件**：
  1. 分区数 ≤ `spark.shuffle.sort.bypassMergeThreshold`（默认 200）
  2. Map 端无聚合操作（如无 reduceByKey 的 combine）
- **机制**：直接写分区文件后合并，避免排序开销
- **文件数**：M（每 MapTask 一个合并文件）

#### 5. Tungsten Sort（Unsafe 优化）
- 直接操作序列化字节，绕过 JVM 对象
- 内存效率高，减少 GC 压力
- 适合大数据量 Shuffle

#### 6. 适用场景

| 实现 | 适用场景 | 不适用场景 |
|------|---------|-----------|
| HashShuffle | 已废弃，不推荐 | 任何生产场景 |
| SortShuffle | 通用场景，默认 | 分区数极小且无聚合 |
| BypassMergeSortShuffle | 分区数 ≤ 200 且无 Map 端聚合 | 有 Map 端聚合 |
| Tungsten Sort | 大数据量、序列化优化场景 | 需要反序列化处理 |

#### 追问简答
- Q: 为什么 HashShuffle 被淘汰？  A: M×R 小文件过多，磁盘 IO 和内存开销大
- Q: Bypass 何时触发？  A: 分区数 ≤ 200 且无 Map 端聚合时，直接写文件合并避免排序

#### 生产实践注意点
- Spark 2.0+ 默认 SortShuffle，无需手动切换
- `spark.shuffle.sort.bypassMergeThreshold` 可调大（如 400）减少排序开销
- 大数据量 Shuffle 关注 `spark.shuffle.file.buffer`（默认 32KB）和 `spark.shuffle.io.retryWait`

---

## SAQ-008-V1 [L3] [Hadoop/HDFS/HA]
**题干**：NameNode 挂了怎么办？HDFS HA 如何实现？脑裂如何避免？

### 标准答案

#### 1. HDFS HA 方案
- **Active/Standby 双 NameNode**：一主一备，Active 提供服务，Standby 同步元数据热备
- 故障时 Standby 切换为 Active，实现高可用

#### 2. 元数据同步
- **JournalNode 集群**（QJM，Quorum Journal Manager）：
  - 3 或 5 个 JournalNode 组成集群（奇数，多数派写入）
  - Active NameNode 的 editlog 写入 JournalNode 集群
  - Standby NameNode 从 JournalNode 读取 editlog 并应用，保持元数据同步
- **NFS 共享存储**（旧方案）：
  - Active 写 editlog 到 NFS，Standby 从 NFS 读取
  - 单点故障风险，已不推荐

#### 3. 故障切换流程
- **ZKFC（ZooKeeper Failover Controller）**：每个 NameNode 节点运行一个 ZKFC 进程
  1. ZKFC 监控 NameNode 健康状态
  2. Active NameNode 故障时，ZKFC 在 ZooKeeper 的锁节点失效
  3. Standby ZKFC 获取锁，触发切换
  4. Standby 成为 Active，原 Active 被隔离（fencing）
  5. DataNode 向新 Active 汇报 block 信息

#### 4. 脑裂防护（Fencing）
- 脑裂风险：原 Active 未真正故障（如 GC 停顿、网络抖动），两个 NameNode 同时认为自己是 Active
- 防护措施：
  1. **SSH kill**：ZKFC 通过 SSH 杀死原 Active 进程
  2. **Shell 隔离**：自定义脚本隔离原 Active（如禁用网络端口）
  3. **QJM 多数派**：editlog 写入需 JournalNode 多数派确认，原 Active 无法获得多数派写入，自动降级
- QJM 天然防脑裂：两个 NameNode 无法同时获得 JournalNode 多数派，只有一方能写入 editlog

#### 5. 客户端容错
- 客户端配置 `dfs.ha.nameservices` 指向逻辑 NameService
- 自动故障转移：客户端感知 Active 切换，自动重连新 Active

#### 追问简答
- Q: Standby NameNode 如何保持元数据最新？  A: 从 JournalNode 集群读取 editlog 并应用，持续同步
- Q: QJM 如何防止脑裂？  A: editlog 写入需 JournalNode 多数派确认，两个 NameNode 无法同时获得多数派

#### 生产实践注意点
- JournalNode 部署 3 或 5 个（奇数），与 ZooKeeper 可共用节点
- fencing 配置必须可靠，SSH kill 失败时需有备用隔离手段
- 监控 ZKFC 和 NameNode 健康状态，避免误切换

---

## SAQ-009-V1 [L3] [Hadoop/YARN/调度器选型]
**题干**：Capacity 调度器和 Fair 调度器有什么区别？生产环境如何选择？

### 标准答案

#### 1. Capacity 调度器
- **机制**：队列划分容量比例，每个队列有最小保证资源和最大容量限制
- **资源借用**：队列空闲资源可被其他队列借用，但需满足最小保证
- **层次队列**：支持多级队列（如 `root.production.etl`, `root.production.adhoc`）
- **特点**：
  - 资源预留可预测（最小保证）
  - 多租户隔离好
  - 适合资源相对固定的场景
- **Apache Hadoop 默认**

#### 2. Fair 调度器
- **机制**：所有作业公平分享资源，按权重（pool/fairShare）分配
- **特点**：
  - 小作业快速启动（无需等待大作业释放）
  - 支持资源池优先级和权重
  - 适合交互查询多、作业大小混合场景
- **CDH 默认**

#### 3. 对比

| 维度 | Capacity | Fair |
|------|----------|------|
| **分配方式** | 队列容量比例 | 作业权重公平 |
| **小作业启动** | 需等队列资源 | 快速启动 |
| **资源借用** | 队列间借用（保证最小） | 全局公平分享 |
| **多租户** | 队列隔离强 | 资源池隔离 |
| **适用场景** | 资源固定、多租户 | 作业混合、交互查询 |
| **默认环境** | Apache Hadoop | CDH |

#### 4. 选型建议

| 场景 | 推荐 | 理由 |
|------|------|------|
| **多租户、队列资源固定** | Capacity | 资源隔离好，保证最小资源 |
| **交互查询多、作业大小混合** | Fair | 小作业快速启动 |
| **生产 ETL + 临时查询混合** | Capacity（分层队列） | ETL 高优先级队列 + 临时查询低优先级队列 |
| **CDH 环境** | Fair（默认） | 与 CDH 生态集成好 |

#### 追问简答
- Q: Capacity 的资源借用如何工作？  A: 队列空闲资源可被其他队列借用，但需满足原队列最小保证，原队列需要时可回收
- Q: Fair 如何保证小作业快速启动？  A: 全局公平分享，小作业无需等待大作业释放即可获得资源

#### 生产实践注意点
- Capacity 配置最小资源保证 + 最大容量限制，避免某队列抢占所有资源
- Fair 配置资源池权重，重要业务高权重
- 生产环境推荐 Capacity（多租户隔离更好），交互查询场景用 Fair

---

## SAQ-010-V1 [L3] [Hive/数据倾斜/group-by与join]
**题干**：group by 倾斜和 join 倾斜分别怎么解决？两阶段聚合的原理是什么？

### 标准答案

#### 1. group by 倾斜解决方案

**方案1：两阶段聚合（Salting）**
- 第一阶段：给 key 加随机前缀 `[1..N]`，按 `(prefix, key)` group by 局部聚合
- 第二阶段：去掉前缀，按 `key` group by 全局聚合
- SQL 示例：
```sql
-- 第一阶段：加随机前缀局部聚合
SELECT split_key, key, sum(cnt) as cnt
FROM (
  SELECT concat(cast(rand()*10 as int), '_', key) as split_key, key, count(*) as cnt
  FROM table GROUP BY concat(cast(rand()*10 as int), '_', key), key
) t GROUP BY split_key, key;

-- 第二阶段：去前缀全局聚合
SELECT key, sum(cnt) FROM above GROUP BY key;
```

**方案2：参数自动开启**
- `hive.map.aggr=true`：开启 Map 端聚合（combine）
- `hive.groupby.skewindata=true`：自动两阶段聚合（Hive 自动加随机前缀）

**方案3：count(distinct) 优化**
- `count(distinct key)` 倾斜时改用 `group by + count`：
```sql
-- 倾斜写法
SELECT count(distinct user_id) FROM table;
-- 优化写法
SELECT count(*) FROM (SELECT user_id FROM table GROUP BY user_id) t;
```

#### 2. join 倾斜解决方案

**方案1：MapJoin（小表广播）**
- 小表广播到所有 Map 端，避免 reduce 端 join
- `/*+ MAPJOIN(small_table) */` 或 `hive.auto.convert.join=true`
- 适用：小表（< 25MB）join 大表

**方案2：空 key 打散**
- join 时 NULL 值被分到同一 reduce，打散 NULL：
```sql
SELECT * FROM t1
LEFT JOIN t2 ON CASE WHEN t1.key IS NULL THEN concat('null_', rand()) ELSE t1.key END = t2.key;
```

**方案3：倾斜 key 单独处理**
- 采样找出倾斜 key，单独 union 处理：
```sql
-- 倾斜 key 单独 MapJoin
SELECT /*+ MAPJOIN(t2) */ t1.key, t2.val FROM t1 JOIN t2 ON t1.key = t2.key AND t1.key = 'skew_key'
UNION ALL
-- 非倾斜 key 正常 join
SELECT t1.key, t2.val FROM t1 JOIN t2 ON t1.key = t2.key AND t1.key <> 'skew_key';
```

**方案4：参数调节**
- `hive.skewjoin.key=100000`：单个 key 超过 10 万条触发倾斜处理
- `hive.skewjoin.mapjoin.xml`：倾斜 join 转 MapJoin

#### 3. 两阶段聚合原理
- **问题**：单 key 数据量过大，单个 reducer 处理所有该 key 数据
- **解决**：
  1. 加随机前缀 `[0..N-1]`，将大 key 拆成 N 个子 key
  2. N 个子 key 分到 N 个 reducer，每个 reducer 处理 1/N 数据量
  3. 第一阶段局部聚合后，每个子 key 数据量大幅减少
  4. 第二阶段去掉前缀，合并 N 个子 key 为原 key，全局聚合
- **效果**：单 key 数据量从 M 降到 M/N（第一阶段）再降到 M/N（第二阶段）

#### 追问简答
- Q: groupby.skewindata=true 的原理？  A: 自动两阶段聚合，第一阶段加随机前缀局部聚合，第二阶段去前缀全局聚合
- Q: join 倾斜优先用什么方案？  A: 小表用 MapJoin，大表用空 key 打散或倾斜 key 单独处理

#### 生产实践注意点
- 两阶段聚合的 N 值（前缀数）根据倾斜程度调整，通常 10-100
- join 倾斜优先 MapJoin（小表场景），大表 join 大表用 salting 打散
- 生产环境建议默认开启 `hive.map.aggr=true` 和 `hive.groupby.skewindata=true`

---

## SAQ-011-V1 [L4] [Flink/Exactly-Once/两阶段提交]
**题干**：Flink 端到端 Exactly-Once 中两阶段提交（2PC）的完整流程是怎样的？Sink 端如何选择幂等写入还是事务写入？

### 标准答案

#### 1. 两阶段提交（2PC）完整流程

**阶段一：preCommit（预提交）**
1. JobManager 的 CheckpointCoordinator 向 Source 注入 Barrier
2. Barrier 随数据流流动，每个算子收到 Barrier 后对齐状态
3. Sink 算子收到 Barrier 后：
   - 调用 `beginTransaction()` 开启外部系统事务（如 Kafka 事务、MySQL XA 事务）
   - 将本 Checkpoint 周期内的数据写入外部系统（事务内，未提交）
   - 调用 `preCommit()` 标记预提交（事务仍不对外可见）
4. Sink 向 JobManager 汇报 Checkpoint 完成（状态已快照，事务已预提交）

**阶段二：Commit（正式提交）**
1. JobManager 收到**所有算子**的 Checkpoint 完成确认
2. JobManager 确认 Checkpoint 成功，向所有 Sink 发送 Commit 通知
3. Sink 收到 Commit 通知后调用 `commit()` 提交外部事务
4. 事务提交后数据对外可见，Checkpoint 完成

**失败处理：Rollback（回滚）**
1. 若任一算子 Checkpoint 失败或超时，JobManager 标记 Checkpoint 失败
2. JobManager 通知所有 Sink 回滚
3. Sink 调用 `abort()` 回滚外部事务，预提交的数据被丢弃
4. Flink 从上一个成功 Checkpoint 恢复，重新消费数据

**故障恢复：**
1. Flink 从最近成功的 Checkpoint 恢复状态
2. Sink 的 `recoverAndCommit()` 重新提交 Checkpoint 中记录的事务（故障前 preCommit 但未 commit 的事务）
3. 保证故障恢复后数据不重不丢

#### 2. 2PC 流程示意图
```
JM --barrier--> Source --barrier--> Map --barrier--> Sink
                                                  |
                              1. beginTransaction (开启事务)
                              2. 写入数据 (事务内)
                              3. preCommit (预提交)
                              4. 汇报 Checkpoint 完成
                                                  |
JM 收齐所有算子确认 --commit--> Sink.commit() (提交事务，数据可见)

失败时:
JM --rollback--> Sink.abort() (回滚事务，数据丢弃)
```

#### 3. 幂等写入 vs 事务写入

| 维度 | 幂等写入 | 事务写入（2PC） |
|------|---------|---------------|
| **实现** | Redis SET、MySQL INSERT ON DUPLICATE KEY | Kafka 事务、MySQL XA、自定义 2PC |
| **复杂度** | 简单 | 复杂（需实现 TwoPhaseCommitSinkFunction） |
| **延迟** | 低（直接写入可见） | 较高（需等 Commit 才可见） |
| **严格性** | 非严格 Exactly-Once | 严格 Exactly-Once |
| **窗口期风险** | "已提交但快照失败"窗口内重复 | 无（事务回滚保证不重复） |

#### 4. 幂等写入的窗口期风险
- 场景：Sink 写入数据（幂等）→ 快照失败 → 恢复后重新消费 → 重新写入
- 幂等写入在"已写入但快照失败"窗口内的重复数据：
  - 若 key 不变：幂等覆盖，无副作用
  - 若 key 变化（如含时间戳）：重复写入产生重复数据
- 事务写入无此问题：preCommit 数据未提交，快照失败时回滚

#### 5. 选择建议

| 场景 | 推荐 | 理由 |
|------|------|------|
| **Sink 支持事务**（Kafka、MySQL XA） | 事务写入 | 严格 Exactly-Once |
| **Sink 仅支持幂等**（Redis、MySQL UPSERT） | 幂等写入 | 简单高效，大部分场景够用 |
| **Sink 无幂等无事务**（如打印日志） | 无法保证 Exactly-Once | 至少 Once |

#### 追问简答
- Q: preCommit 和 commit 的区别？  A: preCommit 预提交（数据写入但不可见），commit 正式提交（数据对外可见）
- Q: 幂等写入有什么风险？  A: "已提交但快照失败"窗口内重复数据，若 key 含时间戳等变化因素则产生重复

#### 生产实践注意点
- 事务写入延迟较高（需等 Commit），对延迟敏感场景可考虑幂等写入
- Kafka 事务 Sink 需配置 `transactional.id` 和 `isolation.level=read_committed`
- 故障恢复时 `recoverAndCommit` 必须可靠执行，否则 preCommit 数据悬挂
- Checkpoint 间隔影响事务提交频率，间隔过长导致事务长时间未提交

---

## SAQ-012-V1 [L3] [Flink/窗口/会话与触发]
**题干**：Flink 会话窗口的 gap 怎么设？Event Time 窗口是 Watermark 到了才触发吗？

### 标准答案

#### 1. 会话窗口 gap 设置
- **静态 gap**：`EventTimeSessionWindows.withGap(Time.minutes(10))`，固定 10 分钟无数据则窗口关闭
- **动态 gap**：`EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<T>() { ... })`，根据数据内容动态决定 gap
- 选择依据：
  - 用户行为统一（如所有用户相同 session 时长）→ 静态 gap
  - 不同用户不同 session 时长（如 VIP 用户更长）→ 动态 gap
- 典型值：电商 session 30 分钟，App 使用 session 5-10 分钟

#### 2. Event Time 窗口触发条件
- **触发条件**：`Watermark ≥ 窗口结束时间`
- Watermark 计算：`Watermark = 最大事件时间 - 允许延迟（allowedLateness）`
- 注意：不是"Watermark 到了才触发"，而是 Watermark 推进到窗口结束时间时触发

#### 3. Watermark 触发机制详解
- Watermark 是事件时间的进展标记，表示"事件时间 ≤ Watermark 的数据已基本到齐"
- Watermark 跨越多个窗口结束时间时，**多个窗口同时触发**
- 示例：
  - 窗口1：[00:00, 00:10)，窗口2：[00:10, 00:20)
  - Watermark 从 00:05 推进到 00:15（如某条数据 eventTime=00:25，延迟 10s）
  - 窗口1（结束时间 00:10）和窗口2（结束时间 00:20）都不会立即触发
  - 当 Watermark ≥ 00:10 时窗口1触发
  - 当 Watermark ≥ 00:20 时窗口2触发

#### 4. 延迟数据处理
- `allowedLateness(Time.minutes(5))`：允许数据迟到 5 分钟
- 延迟数据（eventTime < Watermark 但在 allowedLateness 内）到达时：
  - 重新触发对应窗口计算（窗口状态未清理）
- 超过 allowedLateness 的数据：
  - 丢弃或侧输出（`sideOutputLateData`）

#### 5. 触发器（Trigger）
- 默认触发器：EventTimeTrigger（Watermark ≥ 窗口结束时间时触发）
- 自定义触发器：可实现 `Trigger` 接口自定义触发逻辑
  - 如 CountTrigger（元素数量触发）、ProcessingTimeTrigger（处理时间触发）
  - 或组合触发（如"数量或时间任一满足"）

#### 追问简答
- Q: 会话窗口 gap 怎么设？  A: 静态 `withGap(Time.minutes(10))` 或动态 `withDynamicGap()`，根据业务场景
- Q: Watermark 如何计算？  A: Watermark = 最大事件时间 - 允许延迟，表示事件时间进展

#### 生产实践注意点
- Watermark 策略选择 `forBoundedOutOfOrderness`（有界乱序）适合大多数场景
- `allowedLateness` 不宜过大，否则窗口状态长期保留导致内存压力
- 会话窗口 gap 过大会导致窗口合并开销大，需根据业务合理设置

---

## SAQ-013-V1 [L3] [Flink/状态/RocksDB]
**题干**：RocksDBStateBackend 为什么适合大状态？状态 TTL 是什么？Broadcast State 是什么？

### 标准答案

#### 1. RocksDBStateBackend 适合大状态的原因
- **RocksDB 是基于 LSM 树的嵌入式 KV 存储**，状态存磁盘突破内存限制
- 写入流程：数据先写 MemTable（内存）→ 满后 Flush 到 SSTable（磁盘）→ Compaction 合并
- 读流程：MemTable → BlockCache（读缓存）→ SSTable（磁盘）
- 优势：
  1. 状态大小不受 TaskManager 内存限制，仅受磁盘限制（可支持 TB 级状态）
  2. 支持增量 Checkpoint（只传输 diff），减少 Checkpoint 数据量
  3. 状态持久化到磁盘，TaskManager 故障后可从 Checkpoint 恢复
- 劣势：读写需经过 RocksDB 序列化/反序列化，速度稍慢于内存

#### 2. 状态 TTL（Time-To-Live）
- 作用：设置状态存活时间，过期自动清理，避免状态无限膨胀
- 配置：
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.days(7))  // 7 天后过期
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 创建和写入时更新
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 不返回过期值
    .cleanupInRocksdbCompactFilter(1000)  // RocksDB Compaction 时清理
    .build();
```
- 清理策略：
  - `cleanupFullSnapshot`：全量快照时清理（默认，但不减少增量 Checkpoint 数据量）
  - `cleanupIncrementally`：增量清理（每次状态访问时检查）
  - `cleanupInRocksdbCompactFilter`：RocksDB Compaction 时清理（推荐，后台清理）

#### 3. Broadcast State
- 作用：将一个流的数据**广播到所有 Task**，与主流 co-group 实现规则匹配
- 典型场景：
  - 规则流广播：如风控规则、用户画像标签，广播到所有 Task 与事件流匹配
  - 配置动态更新：配置流广播，主流动态应用最新配置
- API：
```java
// 规则流广播
BroadcastStream<Rule> ruleStream = rules.broadcast(ruleStateDescriptor);
// 主流 connect 广播流
dataStream.connect(ruleStream)
    .process(new BroadcastProcessFunction<Event, Rule, Result>() {
        @Override
        public void processElement(Event value, ReadOnlyContext ctx, Collector<Result> out) {
            // 读取广播状态进行匹配
            Rule rule = ctx.getBroadcastState(ruleStateDescriptor).get("rule");
        }
        @Override
        public void processBroadcastElement(Rule value, Context ctx, Collector<Result> out) {
            // 更新广播状态
            ctx.getBroadcastState(ruleStateDescriptor).put("rule", value);
        }
    });
```
- Flink 1.5+ 特性
- 广播状态与 Keyed State 区别：广播状态在所有 Task 上有相同副本，Keyed State 按 key 分区

#### 追问简答
- Q: RocksDB 为什么支持增量 Checkpoint？  A: 基于 LSM 树的 SSTable 不可变，只需传输新增的 SSTable 文件（diff）
- Q: Broadcast State 与 Keyed State 区别？  A: 广播状态所有 Task 相同副本，Keyed State 按 key 分区

#### 生产实践注意点
- 大状态（> 内存）必须用 RocksDBStateBackend
- 状态 TTL 配合 `cleanupInRocksdbCompactFilter` 后台清理，避免状态膨胀
- Broadcast State 适合规则流，注意规则更新频率不宜过高（避免频繁广播开销）

---

## SAQ-014-V1 [L3] [HBase/RowKey/反向问题]
**题干**：HBase RowKey 设计不当会导致什么问题？如何排查与解决？

### 标准答案

#### 1. RowKey 设计不当导致的问题
- **热点（Hotspotting）**：某 Region 读写集中，RegionServer CPU/IO/网络飙升
  - 原因：连续 RowKey（自增 ID、时间戳）集中写入最后一个 Region
- **读写倾斜**：部分 Region 请求量大，其他 Region 空闲
- **Region 分裂不均**：热点 Region 频繁分裂，冷 Region 不分裂
- **Scan 性能差**：RowKey 无法支持范围查询，需全表扫描
- **数据分布不均**：某 Region 数据量远超其他

#### 2. 排查方法
- **HBase UI**：查看 Region 请求分布（Requests per Region），找请求量异常的 Region
- **HBase hbck 工具**：检查 Region 分布是否均匀、是否有未分配 Region
- **HBase shell**：`status 'detailed'` 查看 RegionServer 状态
- **监控指标**：RegionServer 的 readRequestCount、writeRequestCount 分布
- **日志分析**：RegionServer 日志中 Compaction/Split 频率异常

#### 3. 解决方案

**方案1：RowKey 加盐（Salting）**
- RowKey 前加随机前缀 `[0..N)`，分散到 N 个 Region
- 缺点：Scan 需查所有前缀，性能下降

**方案2：RowKey 哈希（Hashing）**
- RowKey 前加 `hash(key) % N`，均匀分布
- 缺点：牺牲范围查询

**方案3：RowKey 反转（Reversing）**
- RowKey 反转（如手机号 13800001234 → 43210000831）
- 适合前缀相似后缀不同的场景

**方案4：组合键**
- `[hash(uid)] + [ts]`：兼顾散列和按 uid 范围查询
- `[uid 反转] + [Long.MAX_VALUE - ts]`：散列 + 时间倒序

**方案5：预分区**
- 建表时指定分区边界，避免初始热点
- `create 't', 'cf', SPLITS => ['1','2',...,'9']`
- 配合哈希方案，分区数与哈希取模数一致

**方案6：二级索引**
- 范围查询需求强烈时用 Phoenix 二级索引或 Elasticsearch
- RowKey 主索引 + 二级索引支持多维度查询

#### 追问简答
- Q: 如何排查热点 Region？  A: HBase UI 查看 Region 请求分布，hbck 工具检查 Region 分布
- Q: 既想散列又想范围查询怎么办？  A: 组合键 `[hash(uid)] + [ts]` 或二级索引

#### 生产实践注意点
- RowKey 设计阶段就要考虑散列性，避免事后重建表
- 预分区数通常 16-64 个，根据数据量和集群规模决定
- 热点问题优先通过 RowKey 重新设计解决，而非单纯增加 RegionServer

---

## SAQ-015-V1 [L3] [HBase/LSM/Compaction风暴]
**题干**：HBase Minor 和 Major Compaction 的区别？Compaction 风暴如何避免？Region 分裂期间能读写吗？

### 标准答案

#### 1. Minor vs Major Compaction

| 维度 | Minor Compaction | Major Compaction |
|------|-----------------|-----------------|
| **合并范围** | 少量小 HFile（默认 3 个） | 所有 HFile |
| **速度** | 快 | 慢（分钟到小时级） |
| **删除标记** | 不清理 | 清理（删除标记 + TTL 过期数据） |
| **输出** | 合并后的 HFile | 单个大 HFile |
| **触发条件** | HFile 数达阈值（默认 3） | 定期触发（默认 7 天）或手动 |
| **资源消耗** | 小 | 大（IO/CPU/网络） |
| **参数** | `hbase.hstore.compactionThreshold=3` | `hbase.hregion.majorcompaction=604800000`（7天） |

#### 2. Compaction 风暴
- **定义**：大量 HFile 同时 Compaction，导致 IO/CPU/网络飙升，影响读写性能
- **原因**：
  1. 写入高峰期频繁 Flush 产生大量小 HFile
  2. Major Compaction 自动触发，多个 Region 同时执行
  3. Compaction 队列堆积，RegionServer 资源耗尽

#### 3. 避免 Compaction 风暴的策略

**限制 Compaction 并发**：
- `hbase.regionserver.thread.compaction.small=1`：Minor Compaction 线程数
- `hbase.regionserver.thread.compaction.large=1`：Major Compaction 线程数
- 限制并发避免资源争抢

**限制 Compaction 吞吐**：
- `hbase.regionserver.throughput.lower.bound=10MB/s`：Compaction 最低吞吐
- `hbase.regionserver.throughput.higher.bound=20MB/s`：Compaction 最高吞吐
- 压低吞吐避免影响读写

**关闭自动 Major Compaction**：
- `hbase.hregion.majorcompaction=0`：关闭自动 Major
- 手动在业务低峰期触发：`major_compact 'table_name'`

**调优 Compaction 参数**：
- `hbase.hstore.compaction.max=10`：单次 Minor 最多合并 HFile 数
- `hbase.hstore.compactionThreshold=3`：触发 Minor 的 HFile 阈值
- `hbase.hstore.blockingStoreFiles=16`：HFile 超过此值阻塞写入

#### 4. Region 分裂期间的读写
- **分裂期间短暂阻塞读写**：
  1. Region 下线（停止服务）
  2. Flush MemStore 到 HFile
  3. 按 SplitPoint 分裂为两个子 Region
  4. 子 Region 上线，分配到 RegionServer
- 阻塞时间通常几秒到几十秒
- 分裂完成后读写恢复，客户端自动重连新 Region

#### 追问简答
- Q: 为什么关闭自动 Major Compaction？  A: Major 耗时长且资源消耗大，生产环境改为低峰期手动触发避免影响业务
- Q: Region 分裂阻塞多久？  A: 通常几秒到几十秒（Flush + 分裂阶段）

#### 生产实践注意点
- 生产环境关闭自动 Major Compaction，低峰期手动触发或脚本调度
- 监控 Compaction 队列长度和 HFile 数量，及时处理堆积
- 预分区减少 Region 分裂频率，避免分裂期间阻塞业务

---

## SAQ-016-V1 [L2] [数仓/分层/DWD与DWS]
**题干**：DWD 和 DWS 的区别？为什么要做宽表？DIM 层放什么？

### 标准答案

#### 1. DWD vs DWS

| 维度 | DWD（明细数据层） | DWS（汇总数据层） |
|------|------------------|------------------|
| **数据粒度** | 明细（每条业务事件一行） | 汇总（按主题+维度聚合） |
| **组织方式** | 按业务过程组织（如下单、支付、发货） | 按主题+维度组织（如用户日汇总、商品日汇总） |
| **数据量** | 大（明细数据） | 小（聚合后） |
| **查询性能** | 较慢（需聚合） | 快（预计算） |
| **典型表** | dwd_order_detail（订单明细） | dws_user_buy_daily（用户日购买汇总） |
| **职责** | 清洗加工后的明细事实数据 | 按主题+维度汇总的指标数据 |

#### 2. 为什么要做宽表
- **减少 Join**：多张维度表退化到事实表，查询无需多表关联
- **提升查询性能**：预计算汇总指标，查询直接读取结果
- **统一口径**：DWS 层统一指标计算逻辑，避免各应用重复计算口径不一致
- **加速 ADS 层**：ADS 层基于 DWS 宽表进一步计算，减少重复聚合
- **降低开发成本**：下游复用 DWS 宽表，避免重复 ETL

#### 3. DIM 层内容
- 存放维度数据（维度表）
- 典型维度：
  - 用户维（uid, name, age, level, reg_time）
  - 商品维（goods_id, name, cate_id, price）
  - 地区维（region_id, name, level）
  - 时间维（dt, week, month, quarter）
- 维度建模通常用**星型模型**（维度表不规范化，冗余换查询性能）
- 缓慢变化维（SCD）处理：SCD1/SCD2/SCD3，拉链表实现 SCD2

#### 追问简答
- Q: DWD 和 DWS 的粒度区别？  A: DWD 明细粒度（每事件一行），DWS 汇总粒度（按主题+维度聚合）
- Q: 星型模型和雪花模型？  A: 星型维度表不规范化查询快，雪花维度表规范化减少冗余但查询需多 Join

---

## SAQ-017-V1 [L2] [数仓/SCD/SCD2与SCD3适用场景]
**题干**：SCD2 和 SCD3 各适用什么场景？事实表类型（事务型/周期型/累积型）区别？

### 标准答案

#### 1. SCD2 vs SCD3 适用场景

| 类型 | 处理方式 | 保留历史 | 适用场景 | 典型例子 |
|------|---------|---------|---------|---------|
| **SCD2** | 新增记录，加时间区间 | 完整历史 | 需追溯所有历史版本 | 用户等级变更、商品类目变更 |
| **SCD3** | 增加历史列 | 仅上一版本 | 只需对比当前与上一版 | 用户最近一次等级变更 |

**SCD2 适用场景**：
- 需要追溯维度所有变更历史（如用户从 V1 → V2 → V3 的完整路径）
- 审计要求严格，需还原任意时间点的维度状态
- 实现：拉链表（start_date, end_date）

**SCD3 适用场景**：
- 只需对比当前值与上一次值（如"用户当前等级 vs 上次等级"）
- 历史版本不需要完整保留
- 实现：增加 `prior_level` 列

#### 2. 事实表类型

| 类型 | 定义 | 特点 | 典型例子 |
|------|------|------|---------|
| **事务型事实表** | 记录业务事件，一行一条 | 粒度细，实时性高 | 订单表（每订单一行）、登录日志 |
| **周期快照事实表** | 按周期记录度量值 | 固定周期，粒度粗 | 每日账户余额、每日库存 |
| **累积型事实表** | 记录从期初到当前的累积度量 | 包含业务全生命周期字段 | 订单状态流转（下单→支付→发货→签收各阶段时间） |

**累积型事实表示例**：
```
order_id | user_id | create_time | pay_time | ship_time | receive_time | total_amount
1001     | u1      | 2026-07-01  | 2026-07-01 | 2026-07-02 | 2026-07-04  | 100
```
- 每个阶段时间字段随业务进展更新，最终包含完整生命周期

#### 追问简答
- Q: 累积型事实表和周期快照的区别？  A: 累积型记录业务全生命周期各阶段状态，周期快照按周期记录度量值（如每日余额）
- Q: 账户余额适合哪种事实表？  A: 周期快照（每日记录余额）或累积型（记录期初到当前的累积值）

---

## SAQ-018-V1 [L3] [数据倾斜/两阶段聚合原理]
**题干**：两阶段聚合为什么有效？倾斜 key 无法过滤怎么办？Kafka/Redis 的数据倾斜怎么解决？

### 标准答案

#### 1. 两阶段聚合原理
- **问题本质**：单 key 数据量过大，单个 reducer 处理所有该 key 数据，成为瓶颈
- **解决思路**：将大 key 拆到多个 reducer，局部聚合后全局聚合

**详细流程**：
1. **第一阶段（局部聚合）**：
   - 给每条数据的 key 加随机前缀 `[0..N-1]`，大 key 被拆成 N 个子 key
   - N 个子 key 分到 N 个 reducer，每个 reducer 处理 1/N 数据量
   - 每个 reducer 内按 `(prefix, key)` 聚合，单子 key 数据量从 M 降到 M/N
2. **第二阶段（全局聚合）**：
   - 去掉随机前缀，合并 N 个子 key 为原 key
   - 按 `key` 聚合，输入数据量已大幅减少（每 key 仅 N 条中间结果）
   - 全局聚合快速完成

**为什么有效**：
- 第一阶段：大 key 拆分到 N 个 reducer，并行处理，单 reducer 负载降低 N 倍
- 第二阶段：数据量从 M 降到 M/N（局部聚合后），全局聚合无瓶颈
- 总体：将单 reducer 的串行处理变为 N reducer 的并行处理

#### 2. 倾斜 key 无法过滤的方案
- **方案1：倾斜 key 单独处理 + union**
  - 采样找出倾斜 key（如 top 10）
  - 倾斜 key：单独处理（如加随机前缀 + 另一表膨胀）
  - 非倾斜 key：正常逻辑
  - 两者 union 合并

```sql
-- 倾斜 key 单独处理（加前缀 + 膨胀）
SELECT t1.key, sum(t1.val * t2.expanded_val)
FROM (
  SELECT concat(cast(rand()*10 as int), '_', key) as key, val FROM skewed_table WHERE key IN ('skew1', 'skew2')
) t1
JOIN (
  SELECT concat(cast(id as int), '_', key) as key, 1.0/10 as expanded_val FROM expand_table WHERE key IN ('skew1', 'skew2') LATERAL VIEW explode(array(0,1,2,...,9)) t AS id
) t2 ON t1.key = t2.key
GROUP BY t1.key
UNION ALL
-- 非倾斜 key 正常处理
SELECT key, sum(val) FROM skewed_table WHERE key NOT IN ('skew1', 'skew2') GROUP BY key;
```

#### 3. Kafka 数据倾斜
- **表现**：某分区 Consumer Lag 远高于其他分区
- **原因**：
  - 分区策略不合理（如按 key 分区，某 key 数据量大）
  - 消费者处理能力不均
- **解决**：
  - 调整分区策略：按 key 取模 + 随机打散
  - 增加分区数分散数据
  - 优化消费者处理能力（多线程、批量处理）
  - 临时方案：单独消费者处理倾斜分区

#### 4. Redis 数据倾斜
- **表现**：某 Redis 节点 CPU/内存使用率远高于其他节点
- **原因**：热点 key（如某商品详情、某明星微博）
- **解决**：
  - **一致性哈希 + 虚拟节点**：数据均匀分布到所有节点
  - **热点 key 拆分**：加随机后缀分散到多个 key（如 `goods_123` → `goods_123_0` ~ `goods_123_9`），读时随机选一个 + 写时全部更新
  - **多级缓存**：本地缓存 + Redis，减少 Redis 访问
  - **读写分离**：读流量分散到从节点

#### 追问简答
- Q: 两阶段聚合的 N 值怎么选？  A: 根据倾斜程度，通常 10-100，N 越大并行度越高但第二阶段聚合开销也增加
- Q: Redis 热点 key 拆分的缺点？  A: 写入需更新所有副本 key，一致性维护复杂

#### 生产实践注意点
- 两阶段聚合 N 值需压测调优，过大增加第二阶段开销，过小并行度不足
- 倾斜 key 单独处理方案需定期更新倾斜 key 列表（数据分布可能变化）
- Redis 热点 key 拆分需配合监控自动发现热点

---

## SAQ-019-V1 [L3] [Spark vs Flink/Lambda与Kappa]
**题干**：Lambda 架构和 Kappa 架构的区别？如何选型？Spark Structured Streaming 和 Flink 还有差距吗？

### 标准答案

#### 1. Lambda 架构
- **设计**：批处理层（Batch Layer）+ 流处理层（Speed Layer）+ 服务层（Serving Layer）
- **批处理层**：Hadoop/Spark 离线批处理全量数据，生成批处理视图（准确但延迟高，T+1）
- **流处理层**：Storm/Spark Streaming 实时处理增量数据，生成实时视图（低延迟但可能不精确）
- **服务层**：合并批处理视图和实时视图，对外提供查询（批处理修正流处理的误差）
- **优点**：批处理保证准确性，流处理保证实时性
- **缺点**：
  - 需维护两套代码（批 + 流），逻辑需保持一致
  - 数据一致性难保证（批流结果可能不一致）
  - 运维复杂度高

#### 2. Kappa 架构
- **设计**：只保留流处理层，批处理视为流的回放
- **核心思想**：消息队列（如 Kafka）存储长周期数据，需要重算历史时重启流任务回放
- **优点**：
  - 一套代码（流处理），维护简单
  - 数据一致性天然保证（同一逻辑）
  - 架构简洁
- **缺点**：
  - 要求消息队列存储长周期数据（Kafka 保留 7-30 天）
  - 回放历史时需重新处理全量数据，耗时
  - 流处理框架需支持事件时间语义

#### 3. Lambda vs Kappa 对比

| 维度 | Lambda | Kappa |
|------|--------|-------|
| **计算层** | 批 + 流双链路 | 仅流 |
| **代码维护** | 两套（批流一致难） | 一套 |
| **数据一致性** | 难（批流可能不一致） | 天然一致 |
| **历史重算** | 批处理直接重算 | 流任务回放 |
| **复杂度** | 高 | 低 |
| **适用场景** | 批多流少 | 流为主 |

#### 4. 选型建议
- **批多流少** → Lambda（批处理为主，流处理补充实时性）
- **流为主** → Kappa（流处理统一批流）
- **混合场景** → 批用 Spark + 流用 Flink（Lambda 变种）或全 Flink（Kappa）

#### 5. Spark Structured Streaming vs Flink 差距

| 维度 | Spark SS | Flink |
|------|----------|-------|
| **计算模型** | 微批（Micro-Batch） | 真流（Native Streaming） |
| **延迟** | 100ms+ | 毫秒级 |
| **状态管理** | 较弱（基于 Checkpoint） | 丰富（Keyed State/RocksDB） |
| **Event Time** | 支持 | 成熟（Watermark + allowedLateness） |
| **Exactly-Once** | Sink 端幂等 | 端到端（2PC） |
| **窗口** | 滚动/滑动/会话 | 滚动/滑动/会话/全局 + 自定义 |
| **生态** | 批处理生态强 | 流处理生态领先 |

- **结论**：Spark SS 在延迟、状态管理、Event Time 处理上仍落后于 Flink，但批处理生态更成熟
- 流优先选 Flink，批优先选 Spark

#### 追问简答
- Q: Kappa 如何重算历史？  A: 重启流任务从 Kafka 早期 offset 回放全量数据
- Q: Spark SS 为什么延迟高？  A: 微批模型，每批处理间隔 100ms+，无法做到毫秒级

#### 生产实践注意点
- 实时数仓趋势：Kappa 架构（Flink 统一批流），但批处理场景 Spark 仍有优势
- Lambda 架构需严格保证批流逻辑一致，建议用同一套 SQL 模板生成批流代码
- Kafka 消息保留时间需足够长（Kappa 架构回放需求）

---

## SAQ-020-V1 [L2] [Spark/Shuffle/reduceByKey vs groupByKey]
**题干**：reduceByKey 和 groupByKey 的区别？为什么 reduceByKey 更好？Bypass 机制触发条件？

### 标准答案

#### 1. reduceByKey vs groupByKey

| 维度 | reduceByKey | groupByKey |
|------|-------------|------------|
| **Map 端聚合** | 有（combine） | 无 |
| **Shuffle 数据量** | 小（局部聚合后） | 大（全量数据 Shuffle） |
| **内存压力** | 小 | 大（易 OOM） |
| **返回类型** | (key, 聚合值) | (key, Iterable[值]) |
| **性能** | 优 | 差 |
| **适用场景** | 求和、计数、最大值 | 需要完整 Iterable |

#### 2. reduceByKey 更好的原因
- **Map 端 combine**：reduceByKey 在 Map 端先局部聚合（如求和：相同 key 先在本地求和）
- **Shuffle 数据量减少**：局部聚合后只 Shuffle 聚合结果，而非全量数据
  - 例：1 亿条 (word, 1)，1000 个 word，reduceByKey 先在 Map 端合并为 1000 个 (word, count)，Shuffle 仅 1000 条
  - groupByKey 全量 Shuffle 1 亿条数据
- **内存压力小**：reduce 端接收的是聚合后的数据，不易 OOM
- **网络 IO 少**：Shuffle 数据量小，网络传输少

#### 3. 性能对比示例
- 场景：1 亿条 (word, 1)，1000 个不同 word
- reduceByKey：
  - Map 端 combine：1000 个 (word, count)
  - Shuffle 数据量：约几 KB（1000 条）
  - 执行时间：快
- groupByKey：
  - 无 Map 端聚合：1 亿条 (word, 1)
  - Shuffle 数据量：约 800MB（1 亿条）
  - 执行时间：慢，可能 OOM

#### 4. Bypass 机制触发条件
- **条件1**：分区数 ≤ `spark.shuffle.sort.bypassMergeThreshold`（默认 200）
- **条件2**：Map 端无聚合操作（如无 reduceByKey 的 combine）
- 满足时：直接写分区文件后合并，避免排序开销
- 不满足时：走标准 SortShuffle（需排序）

#### 5. 何时用 groupByKey
- 需要**完整 Iterable** 进行复杂处理时（如求中位数、需遍历所有值）
- 数据量小且不影响性能时
- 大部分场景应优先 reduceByKey / aggregateByKey

#### 追问简答
- Q: 为什么 reduceByKey 比 groupByKey 好？  A: reduceByKey 在 Map 端 combine 减少 Shuffle 数据量，groupByKey 全量 Shuffle 易 OOM
- Q: Bypass 何时触发？  A: 分区数 ≤ 200 且无 Map 端聚合时，直接写文件合并避免排序

#### 生产实践注意点
- 优先用 `reduceByKey` 或 `aggregateByKey`，避免 `groupByKey`
- 需要聚合后转换类型用 `aggregateByKey`（输入输出类型可不同）
- Bypass 阈值 `spark.shuffle.sort.bypassMergeThreshold` 可调大（如 400）减少排序

---

## SAQ-021-V1 [L3] [Hive/SQL优化/小文件与引擎]
**题干**：Hive 小文件问题怎么解决？什么时候用 Tez/Spark 引擎？

### 标准答案

#### 1. 小文件产生原因
- **动态分区插入**：每个分区产生文件，分区数多时小文件爆炸
- **reduce 数过多**：reduce 数 > 数据量/256MB 时产生小文件
- **频繁 insert**：小批量频繁 insert 产生大量小文件
- **Map 端输出**：MapTask 输出文件数 = MapTask 数，数据量小时文件小

#### 2. 小文件危害
- **NameNode 内存压力**：每个文件占 NameNode 约 150 字节元数据，小文件过多耗尽 NameNode 内存
- **MapTask 数过多**：下游作业每个小文件启动一个 MapTask，资源浪费
- **查询性能差**：大量小文件随机 IO，HDFS 读取效率低
- **任务启动慢**：MapTask 数量多，调度和启动开销大

#### 3. 解决方案

**合并小文件**：
- `hive.merge.mapfiles=true`：Map 端输出后合并小文件
- `hive.merge.mapredfiles=true`：Reduce 端输出后合并小文件
- `hive.merge.smallfiles.avgsize=16MB`：平均文件大小低于此值触发合并
- `hive.merge.size.per.task=256MB`：合并后目标文件大小
- `ALTER TABLE t CONCATENATE`：手动合并 ORC/RCFile 小文件

**调整 reduce 数**：
- `mapreduce.job.reduces=N`：根据数据量调整，避免 reduce 过多
- 经验公式：reduce 数 = 数据量 / 256MB

**Map 端合并**：
- `hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat`：Map 端合并小文件作为输入
- `mapreduce.input.fileinputformat.split.maxsize=256000000`：合并后 split 大小

**使用 Spark/Tez 引擎**：
- DAG 执行减少中间落盘，小文件产生概率低
- Spark 默认输出文件数 = 分区数，可通过 `coalesce(N)` 合并

#### 4. Tez/Spark 引擎选择

| 引擎 | 执行模型 | 优势 | 适用场景 |
|------|---------|------|---------|
| **MR** | Map→Shuffle→Reduce 两阶段 | 稳定、成熟 | 超大规模、传统批处理 |
| **Tez** | DAG 多阶段 | 减少 intermediate 落盘，比 MR 快 2-3 倍 | Hive on Tez（Hortonworks 默认） |
| **Spark** | DAG + 内存缓存 | 内存计算，比 MR 快 5-10 倍 | Hive on Spark（Hive 2.x 推荐） |

**何时用 Tez/Spark**：
- 多阶段 ETL（MR 中间结果多次落盘）→ Tez/Spark 减少落盘
- 交互式查询（低延迟需求）→ Spark 内存计算
- 复杂 SQL（多 Join/子查询）→ Tez/Spark DAG 优化
- **Hive 2.x 后推荐 Hive on Spark**（性能最优）

#### 追问简答
- Q: 小文件如何合并？  A: `hive.merge.mapfiles=true` 自动合并，或 `ALTER TABLE t CONCATENATE` 手动合并
- Q: Tez 为什么比 MR 快？  A: DAG 执行减少中间落盘，多阶段任务在内存中链式执行

#### 生产实践注意点
- 动态分区插入后定期合并小文件
- 监控 HDFS 小文件数量（< 1MB 的文件占比）
- Hive on Spark 需调整 `spark.executor.instances` 和 `spark.executor.memory`

---

## SAQ-022-V1 [L2] [Kafka/消息积压/消费者与分区]
**题干**：Kafka 消费者数能超过分区数吗？为什么？如何监控 Consumer Lag？

### 标准答案

#### 1. 消费者数能否超过分区数
- **可以配置，但多余消费者空闲**
- 原因：同一消费者组内，一个分区同一时刻只能被一个消费者消费
- 消费者数 > 分区数时，多余消费者不消费任何分区，处于空闲状态
- 生产环境建议：消费者数 ≤ 分区数，避免资源浪费

**分配规则**：
- 消费者数 < 分区数：部分消费者消费多个分区
- 消费者数 = 分区数：每个消费者消费一个分区
- 消费者数 > 分区数：多余消费者空闲

#### 2. 为什么一个分区只能被一个消费者消费
- 保证消费顺序性：若多个消费者消费同一分区，无法保证消息顺序处理
- 简化 offset 管理：一个分区一个消费者，offset 提交无冲突
- 不同消费者组之间互不影响：每组独立消费全量数据

#### 3. Consumer Lag 监控

**命令行工具**：
```bash
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group>
# 输出：TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID
```

**开源监控工具**：
- **Burrow**：LinkedIn 开源，专门监控 Kafka Consumer Lag，支持多集群、告警
- **Kafka Manager**（CMAK）：Yahoo 开源，Web UI 查看 Lag
- **Kafka UI**：开源 Web 管理界面
- **Confluent Control Center**：商业版，全面监控

**JMX 指标**：
- `records-lag-max`：最大 Lag（分区级）
- `records-lag-avg`：平均 Lag
- `bytes-consumed-rate`：消费速率
- 可集成 Prometheus + Grafana 监控

**告警阈值**：
- 按业务 SLA 设置（如 lag > 10000 告警）
- 按时间设置（如 lag 持续增长 5 分钟告警）
- 按百分比设置（如 lag > log-end-offset 的 10% 告警）

#### 追问简答
- Q: 消费者数超过分区数会怎样？  A: 多余消费者空闲，不消费任何分区
- Q: Consumer Lag 如何告警？  A: Burrow + Prometheus，按业务 SLA 设置阈值（如 lag > 10000）

#### 生产实践注意点
- 消费者数 = 分区数时利用率最高
- Consumer Lag 监控是必备项，建议集成 Prometheus + Grafana
- Lag 持续增长时优先优化消费逻辑，而非单纯增加消费者

---

## SAQ-023-V1 [L2] [HBase/对比/HBase vs Redis]
**题干**：HBase 和 Redis 的区别？分别适用什么场景？

### 标准答案

#### 1. HBase vs Redis 对比

| 维度 | HBase | Redis |
|------|-------|-------|
| **存储介质** | 磁盘（LSM 树） | 纯内存（可选持久化） |
| **数据模型** | 列式 KV（RowKey + ColumnFamily） | KV/Hash/List/Set/ZSet |
| **延迟** | 毫秒级（1-10ms） | 亚毫秒级（< 1ms） |
| **数据量** | PB 级（磁盘扩展） | GB-TB 级（受内存限制） |
| **持久性** | 强（WAL + HFile 落盘） | 可选（RDB/AOF，故障可能丢数据） |
| **查询模式** | RowKey 单点查、Scan 范围查 | 丰富数据结构操作 |
| **扩展性** | 水平扩展（Region 分裂） | 集群分片（Cluster） |
| **一致性** | 强一致 | 最终一致（集群模式） |
| **成本** | 低（磁盘） | 高（内存） |

#### 2. 适用场景

**Redis 适用**：
- 缓存热数据（如商品详情、用户信息）
- 计数器（如点赞数、浏览数）
- 排行榜（ZSet 有序集合）
- 会话管理（Session 存储）
- 实时排行榜、最新列表
- 分布式锁

**HBase 适用**：
- 海量明细数据存储（如订单流水、用户行为日志）
- 按 RowKey 随机读写
- 时间序列数据（如监控指标、传感器数据）
- 大数据生态集成（Hadoop/Spark/Flink）

#### 3. 配合使用模式
- **HBase 持久化存储 + Redis 缓存热数据**
  - 写入：先写 HBase（持久化）→ 异步写 Redis（缓存）
  - 读取：先查 Redis（命中则返回）→ 未命中查 HBase → 回填 Redis
  - 缓存淘汰：LRU 策略或 TTL 过期
- 适用：海量数据 + 低延迟访问（如用户画像、商品详情）

#### 追问简答
- Q: Redis 为什么快？  A: 纯内存操作 + 单线程避免锁竞争 + IO 多路复用
- Q: HBase 和 Redis 如何配合？  A: HBase 持久化存储 + Redis 缓存热数据，读先 Redis 再 HBase

#### 生产实践注意点
- Redis 数据量受内存限制，热数据才放 Redis
- HBase 适合写多读少，Redis 适合读多写少
- 配合使用时注意缓存一致性（先写 DB 再删缓存）

---

## SAQ-024-V1 [L4] [Flink/反压/Unaligned Checkpoint]
**题干**：Unaligned Checkpoint 的原理是什么？为什么能解决反压场景下的 Checkpoint 超时？

### 标准答案

#### 1. Aligned Checkpoint（默认）的问题
- **流程**：算子收到所有上游 Barrier 后对齐，再快照状态
- **反压场景问题**：
  1. 下游算子处理慢，Barrier 在输入缓冲区队列中排队等待
  2. Barrier 排队时间随反压程度增长
  3. Checkpoint 超时（`checkpoint.timeout`），失败
  4. 频繁 Checkpoint 失败导致作业不稳定，甚至触发重启

**示意图**：
```
上游 → [数据1, 数据2, ..., Barrier, 数据N] → 下游（处理慢，Barrier 排队）
                                            ↑ Barrier 长时间无法对齐
```

#### 2. Unaligned Checkpoint 原理
- **核心思想**：Barrier 不等待对齐，直接越过 in-flight 数据（缓冲区中的数据），立即开始快照
- **流程**：
  1. 算子收到 Barrier 后立即开始快照状态（不等待其他上游 Barrier）
  2. 将 in-flight 数据（输入缓冲区中已收到但未处理的数据）作为状态的一部分写入 State Backend
  3. 向下游发送 Barrier
  4. 所有算子并行快照，无需对齐等待

**示意图**：
```
上游 → [数据1, 数据2, ..., Barrier, 数据N] → 下游
                              ↓ Barrier 直接越过
下游立即快照：当前状态 + in-flight 数据（数据1, 数据2, ...）
```

#### 3. 为什么能解决反压下的 Checkpoint 超时
- **Aligned**：Barrier 需等待所有上游对齐，反压时排队时间长 → 超时
- **Unaligned**：Barrier 直接越过队列，无需等待 → 快速完成快照
- Checkpoint 时间从"等待对齐 + 快照"降为"仅快照"（加上 in-flight 数据写入）

#### 4. 代价与限制
- **状态体积增大**：in-flight 数据作为状态写入，状态大小 = 原状态 + 缓冲区数据量
  - 反压严重时缓冲区数据多，状态膨胀明显
  - 需要更多存储空间和更长的恢复时间
- **恢复开销**：恢复时需重新发送 in-flight 数据
- **不适用场景**：
  - 状态极大且反压不严重（Aligned 更优）
  - 算子间缓冲区数据量大（状态膨胀严重）

#### 5. 配置与使用
- 开启：`env.getCheckpointConfig().enableUnalignedCheckpoints();`
- Flink 1.11+ 支持
- 前提：State Backend 支持（RocksDB 支持，MemoryStateBackend 有限制）
- 建议搭配增量 Checkpoint 使用，减少状态传输量

#### 6. Aligned vs Unaligned 对比

| 维度 | Aligned | Unaligned |
|------|---------|-----------|
| **Barrier 处理** | 等待所有上游对齐 | 直接越过 in-flight 数据 |
| **反压场景** | Barrier 排队，Checkpoint 超时 | 立即快照，快速完成 |
| **状态大小** | 仅当前状态 | 当前状态 + in-flight 数据 |
| **恢复开销** | 小 | 大（需重放 in-flight 数据） |
| **适用场景** | 正常场景、状态大 | 反压严重、状态可控 |
| **Flink 版本** | 默认 | 1.11+ |

#### 追问简答
- Q: Unaligned Checkpoint 为什么状态变大？  A: in-flight 数据（缓冲区中未处理的数据）作为状态写入
- Q: 什么时候用 Unaligned？  A: 反压严重导致 Checkpoint 超时，且状态可控的场景

#### 生产实践注意点
- 反压严重且 Checkpoint 频繁超时时开启 Unaligned
- 监控状态大小变化，状态膨胀过大时考虑优化反压根因
- Unaligned 不能解决反压本身，只是保证 Checkpoint 不超时
- 搭配增量 Checkpoint（RocksDB）减少状态传输

---

## SAQ-025-V1 [L3] [Zookeeper/watcher/一次性与分布式锁]
**题干**：一次性 watcher 有什么问题？如何避免事件丢失？Zookeeper 分布式锁如何实现？羊群效应是什么？

### 标准答案

#### 1. 一次性 Watcher 的问题
- **问题**：Watcher 触发后失效，注册到再次触发期间的事件可能丢失
- **场景**：
  1. 客户端注册 Watcher 监听节点 `/config`
  2. 节点 `/config` 变化，Watcher 触发，客户端收到通知
  3. 客户端处理通知期间，节点 `/config` 再次变化
  4. 由于 Watcher 已失效，第二次变化未被捕获，事件丢失
- **风险**：配置更新丢失、服务状态变更未感知、分布式锁唤醒失败

#### 2. 避免事件丢失的方案

**方案1：回调中重新注册**
- 在 Watcher 回调处理逻辑中重新注册 Watcher
- 时间窗口：回调执行到重新注册之间仍有极短窗口可能丢事件
- 示例：
```java
Stat stat = zk.exists("/config", new Watcher() {
    @Override
    public void process(WatchedEvent event) {
        // 处理事件
        handleEvent(event);
        // 重新注册 Watcher
        zk.exists("/config", this);
    }
});
```

**方案2：Curator Cache 机制**
- Apache Curator 提供 `NodeCache`、`PathChildrenCache`、`TreeCache`
- 自动重注册 Watcher，封装事件丢失风险
- 生产环境推荐：
```java
NodeCache nodeCache = new NodeCache(client, "/config");
nodeCache.getListenable().addListener(() -> {
    ChildData data = nodeCache.getCurrentData();
    // 处理节点变化
});
nodeCache.start(true);  // true 表示启动时立即注册 Watcher
```

#### 3. ZooKeeper 分布式锁实现

**方案1：临时节点（简单版，有羊群效应）**
1. 所有客户端尝试在 `/locks` 下创建临时节点 `lock`
2. 创建成功者获得锁
3. 未创建成功者注册 Watcher 监听 `/locks/lock`
4. 持锁客户端释放锁（删除节点）
5. 所有等待客户端被唤醒，竞争创建节点
- **问题**：羊群效应（所有客户端竞争，只有一个成功，其余再次等待）

**方案2：临时顺序节点（推荐，无羊群效应）**
1. 客户端在 `/locks` 下创建临时顺序节点 `/locks/node_00000001`
2. 获取 `/locks` 下所有子节点，排序
3. 若自己是序号最小的节点，获得锁
4. 否则监听**前一个节点**（比自己序号小 1 的节点）
5. 前一个节点删除时（持锁者释放锁），自己被唤醒
6. 再次检查自己是否最小，若是则获得锁
- **优点**：避免羊群效应，只有前一个节点的 Watcher 被触发

**代码示例（Curator）**：
```java
InterProcessMutex lock = new InterProcessMutex(client, "/locks/order_lock");
try {
    if (lock.acquire(5, TimeUnit.SECONDS)) {
        // 获得锁，执行业务逻辑
        doBusiness();
    }
} finally {
    lock.release();
}
```

#### 4. 羊群效应
- **定义**：所有客户端监听同一节点，节点变化时所有客户端被唤醒竞争，但只有一个成功
- **问题**：
  1. 大量唤醒和竞争浪费资源（ZooKeeper 通知压力 + 客户端竞争压力）
  2. 网络流量突增
  3. 性能下降
- **解决**：用临时顺序节点 + 监听前一个节点，每次只唤醒一个客户端

#### 追问简答
- Q: 一次性 Watcher 如何避免事件丢失？  A: 回调中重新注册，或用 Curator Cache 自动重注册
- Q: 羊群效应如何避免？  A: 临时顺序节点 + 监听前一个节点，每次只唤醒一个客户端

#### 生产实践注意点
- 生产环境优先用 Curator 的 `InterProcessMutex` 实现分布式锁（封装好、可重入）
- 临时顺序节点方案避免羊群效应，性能优于简单临时节点方案
- Watcher 推荐用 Curator Cache 自动重注册，避免手动重注册的窗口风险
- 分布式锁需处理锁超时（避免持锁者宕机导致死锁）和可重入

---

## 阅卷完成总结

### 产出统计
- 简答题总数：50 道（原题 25 + 变式 25）
- 难度分布：L1 1 道 / L2 16 道 / L3 29 道 / L4 4 道
- 技术栈覆盖：Kafka、Spark、Hadoop、Hive、Flink、HBase、Zookeeper、数仓、数据倾斜

### 质量达成
- 结构化：每道答案分点阐述，含标题层级
- 难度匹配：L1（3-5点）/ L2（5-8点）/ L3（8-12点含生产经验）/ L4（12点以上含源码级/架构权衡）
- 关键参数：每道题标注真实配置参数
- 对比表格：Spark vs Flink、HBase vs MySQL、SCD2 vs SCD3、Lambda vs Kappa 等
- 示意图描述：HDFS pipeline、Flink Barrier 对齐、LSM 树、2PC 流程等
- 追问简答：每道题 1-2 句精炼回答
- 生产实践注意点：L3/L4 题均含踩坑点和生产经验

### 技术准确性
- Kafka：acks/all、ISR、幂等性 PID+SequenceNumber、事务 transactional.id
- Spark：宽窄依赖、SortShuffle、Bypass 触发条件、Cache vs Checkpoint
- Flink：Barrier 对齐、Unaligned Checkpoint、2PC 流程、状态 TTL、Broadcast State
- HBase：LSM 树、Compaction、RowKey 设计、Region 分裂
- Hive：数据倾斜两阶段聚合、小文件合并、SCD 拉链表
- Zookeeper：Watcher 一次性、临时顺序节点分布式锁、羊群效应
