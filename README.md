# 大数据面试题库 · bigdata-interview-bank

> 从一份真实面经报告出发，5 个 AI Agent 协作跑完四阶段流水线，自动产出覆盖 9 大技术栈、4 级难度、3 种题型的完整面试题库。
>
> **112 道题 + 74 道标准答案 + Harness 量化质检 ALL PASS**

---

## 一、项目成果

| 维度 | 数量 |
|------|------|
| 题目总数 | 112 道（原题 40 + 变式 60 + L1 补全 12） |
| 选择题 | 36 道（含正确答案与解析） |
| 简答题 | 54 道（含标准答案） |
| 手撕代码题 | 22 道（含可运行参考实现） |
| 已补全答案 | 74 道（简答 54 + 代码 22） |
| 难度分级 | L1 基础 15 / L2 进阶 38 / L3 高级 47 / L4 专家 12 |
| 技术栈覆盖 | Kafka · Spark · Hadoop · Hive · Flink · HBase · Zookeeper · 数仓 · 数据倾斜 |

---

## 二、项目架构 · 多 Agent 协作流水线

本项目采用**职责拆分式多 Agent 流水线架构**，将大数据面经从采集、清洗、结构化、扩增出题到智能阅卷、量化质检进行分层解耦，五大智能角色各司其职、串行联动、可独立迭代。

### 五大 Agent 职责

| Agent | 角色 | 职责 | 产出文件 |
|-------|------|------|---------|
| **Collector** | 爬虫采集 | 定向抓取牛客网等平台的大数据开发真实面试复盘，过滤无关岗位，提取题目原文、面试场景、高频公司 | `phase1_raw_interviews.md` |
| **Cleaner** | 标准化整理 | 分类三大题型、清洗冗余、设计选择题选项、统一格式 | `phase2_standardized_questions.md` |
| **Generator** | 智能出题 | 变式出题（角度/场景/反向设问）、派生代码题、难度标注、知识图谱关联 | `phase3_expanded_bank.md` |
| **Saq-grader** | 简答阅卷 | 简答题纠错、标准化重构、剔除过时知识点，以"原理+流程+优缺点+场景+话术"模板输出 | `phase4_saq_answers.md` |
| **Code-grader** | 代码阅卷 | 代码题落地实现、逻辑校验、注释与复杂度分析、测试用例 | `phase4_code_solutions.md` |

### 流水线阶段

```
Phase 1 采集        Phase 2 标准化      Phase 3 扩展        Phase 4 阅卷         Phase 5 质检
Collector  ───→   Cleaner     ───→   Generator  ───→   Saq-grader ┐
(爬虫+搜索)        (分类+清洗)        (变式+派生)         Code-grader┘──→  Harness 引擎
                                                          (并行)           (四维评测)
```

---

## 三、Harness 量化评测体系（项目核心亮点）

区别于普通 AI 出题项目，本项目搭建专属 **Harness 题库质量评测引擎**，通过四大量化维度全自动校验题库质量，实现劣质题目自动识别、题库质量可量化。

运行 `python interview_bank/harness_engine.py` 即可执行全量评测，输出 PASS/FAIL 判定 + 修复清单。

| 维度 | 评测内容 | 阈值 | 当前状态 |
|------|---------|------|---------|
| **H1 题目重复率** | 题干 Jaccard 相似度全量比对 | 高相似对占比 ≤ 5% | ✅ 0 重复 |
| **H2 答案准确性** | 真实参数/API 标注密度 + 复杂度覆盖率 | 参数密度 ≥ 2/题 | ✅ 7.4/题 |
| **H3 考点覆盖率** | 9 大技术栈分布 + 缺失检测 | 无缺失栈 | ✅ 全覆盖 |
| **H4 难度均衡度** | L1-L4 占比 vs 目标区间 | 偏差 ≤ ±7% | ✅ 全达标 |

---

## 四、文件结构

```
bigdata-interview-bank/
├── README.md                          # 本文件（项目总介绍）
├── overview.md                        # 项目概览
├── 牛客网各开发岗位面经汇总报告.md      # 数据源（面经采集原始报告）
└── interview_bank/                    # 题库核心目录
    ├── README.md                      # 题库总览索引
    ├── phase1_raw_interviews.md       # Phase 1: 原始面经采集（36KB）
    ├── phase2_standardized_questions.md  # Phase 2: 40道标准化题目（28KB）
    ├── phase3_expanded_bank.md        # Phase 3: 112道扩展题库（73KB）
    ├── phase4_saq_answers.md          # Phase 4: 54道简答题标准答案（125KB）
    ├── phase4_code_solutions.md       # Phase 4: 22道代码题参考实现（107KB）
    ├── harness_engine.py              # Phase 5: Harness评测引擎
    ├── harness_report.md              # 最新质检报告
    ├── harness_verify.py              # 早期核验脚本（已被harness_engine.py取代）
    ├── architecture_verification_report.md  # 架构实现核验报告
    └── upload_to_github.py            # GitHub API上传工具（token从环境变量读取）
```

---

## 五、难度等级定义

| 等级 | 名称 | 标准 | 占比 | 适用人群 |
|------|------|------|------|---------|
| L1 | 基础 | 概念记忆、定义辨析（如"Kafka 是什么"） | 13.4% | 零基础入门 |
| L2 | 进阶 | 原理理解、流程阐述（如"HDFS 读写流程"） | 33.9% | 初级岗面试 |
| L3 | 高级 | 方案设计、问题排查、对比分析（如"数据倾斜解决方案"） | 42.0% | 中高级岗面试 |
| L4 | 专家 | 源码级、架构权衡、复杂场景综合（如"端到端 Exactly-Once"） | 10.7% | 大厂高级岗冲刺 |

---

## 六、答案质量基线

### 简答题（54 道）
每道答案均包含：
1. **结构化分点**（L1: 3-5 点 / L2: 5-8 点 / L3: 8-12 点 / L4: 12 点以上）
2. **真实配置参数**（如 `acks=all`、`min.insync.replicas=2`、`spark.sql.shuffle.partitions`）
3. **对比表格**（260+ 行，涵盖 Spark vs Flink、SCD2 vs SCD3、Lambda vs Kappa 等）
4. **追问简答**（每题 1-2 个面试追问点）
5. **生产实践注意点**（L3/L4 题均含踩坑经验）

### 代码题（22 道）
每道实现均包含：
1. **可运行代码**（无占位符/TODO，API 调用准确）
2. **中文注释**（解释"为什么这样写"）
3. **复杂度分析**（算法题含时间+空间复杂度）
4. **测试用例**（正常 + 边界情况）
5. **方案对比**（适用处含多方案优劣对比）
6. **追问简答**（2-3 个面试追问点）

---

## 七、快速开始

### 环境要求
- Python 3.8+（运行 Harness 评测引擎）

### 运行质检
```bash
cd interview_bank
python harness_engine.py
# 输出四维评测结果，生成 harness_report.md
# 退出码 0 = ALL PASS，1 = 存在 FAIL 项
```

### 使用场景
- **日常刷题**：按 `phase3_expanded_bank.md` 顺序刷，L1→L2→L3→L4
- **模拟面试**：选择题 30 + 简答 10（L2-L3）+ 代码 2（1 L3 + 1 L4）
- **查漏补缺**：按技术栈标签检索薄弱项
- **面试官出题**：选择题可直接作为笔试题，简答题截取分点作为评分参考

---

## 八、项目核心创新点

- **岗位定向精准适配**：专为大数据开发岗定制，全自动过滤非相关面经
- **多 Agent 分工协同流水线**：五大智能角色解耦协作，从原始素材到成品题库全自动落地
- **量化题库质量体系**：自研 Harness 多维评测引擎，质量从主观感受变为可量化指标
- **变式出题突破瓶颈**：基于真实高频考点衍生新题，解决传统题库原题枯竭问题
- **全自动纠错汰旧**：阅卷 Agent 修正网传错误答案、淘汰过时技术知识点

---

## 九、技术栈

| 类别 | 技术 |
|------|------|
| Agent 编排 | WorkBuddy 多 Agent 团队协作（TeamCreate + Agent + SendMessage） |
| 评测引擎 | Python（re / json / urllib，零第三方依赖） |
| 文档输出 | Markdown 标准化文档 |
| 版本控制 | Git + GitHub（Contents API 推送） |
| 数据源 | 牛客网大数据开发面经 + WebSearch 补充 |

---

## 十、协作团队

| Agent | 职责 | 产出 |
|-------|------|------|
| collector | 采集原始面经、WebSearch 补充 | phase1_raw_interviews.md |
| cleaner | 去重、分类、标注难度、设计选项 | phase2_standardized_questions.md |
| generator | 生成变式题、派生代码题、知识图谱 | phase3_expanded_bank.md |
| saq-grader | 50 道简答题阅卷补全标准答案 | phase4_saq_answers.md |
| code-grader | 20 道代码题阅卷补全参考实现 | phase4_code_solutions.md |

> 项目由 WorkBuddy AI Agent 团队协作完成，全程零人工干预出题与阅卷。

---

## License

MIT
