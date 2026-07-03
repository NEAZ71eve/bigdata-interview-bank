# 简历项目描述

> 提供 3 个版本：精简版（简历用）、详细版（项目栏用）、英文版（外企/英文简历用）。

---

## 一、精简版（简历项目栏，3-4 行）

### 中文版

**大数据面试题库自动化生产系统** | 个人项目 | GitHub: NEAZ71eve/bigdata-interview-bank

设计并实现 5 Agent 协作流水线（采集→标准化→扩展→并行阅卷→量化质检），全自动产出 112 道题 + 74 道标准答案，覆盖 9 大技术栈 4 级难度。自研 Harness 四维评测引擎（重复率/答案准确性/覆盖率/难度均衡度），实现质量可量化与闭环修复，四维 ALL PASS。

### 英文版

**Big Data Interview Bank · Automated Production Pipeline** | Personal Project | GitHub: NEAZ71eve/bigdata-interview-bank

Designed a 5-agent collaborative pipeline (collection → standardization → expansion → parallel grading → quantitative QA) that auto-produces 112 questions + 74 standard answers across 9 tech stacks. Built a custom Harness evaluation engine (4 dimensions: duplication/accuracy/coverage/balance) enabling measurable quality and closed-loop remediation — all four dimensions PASS.

---

## 二、详细版（项目栏展开，含技术栈与量化成果）

### 中文版

**大数据面试题库自动化生产系统** | 个人项目 | 2026.07 | Python · Multi-Agent · GitHub API

**项目背景**：针对大数据面经"题目杂乱、答案过时、质量无标准"三大痛点，构建全自动题库生产与质检体系。

**技术架构**：
- 设计 5 Agent 协作流水线（collector/cleaner/generator/saq-grader/code-grader），五阶段串行联动 + 阶段内并行，主理人中转消息避免上下文污染
- 自研 Harness 量化评测引擎（Python 零依赖），四维度自动评测：Jaccard 重复率检测、真实参数密度核验、9 大技术栈覆盖率统计、L1-L4 难度均衡度校验
- GitHub Contents API 逐文件推送绕过网络阻断，支持 PASS/FAIL 判定 + 修复清单 + CI 退出码

**量化成果**：
- 全自动产出 112 道题 + 74 道标准答案，覆盖 Kafka/Spark/Flink/Hive/Hadoop/HBase/ZK/数仓/数据倾斜
- Harness 闭环修复：首次评测 L1 偏差 -12%（FAIL）→ 补题后 -1.6%（PASS），四维全部达标
- 答案质量：参数密度 7.4/题、对比表格 260 行、代码复杂度标注覆盖 204%、重复率 0
- 全流水线 54 分钟完成，5 Agent 零人工干预

### 英文版

**Big Data Interview Bank · Automated Production Pipeline** | Personal Project | 2026.07 | Python · Multi-Agent · GitHub API

**Background**: Tackled three pain points in big-data interview prep: scattered questions, outdated answers, no quality standards.

**Architecture**:
- Designed a 5-agent collaborative pipeline (collector / cleaner / generator / saq-grader / code-grader) with serial phases + intra-phase parallelism; team-lead relays all messages to avoid context pollution
- Built a custom Harness evaluation engine (zero-dependency Python) with 4 quantitative dimensions: Jaccard duplication detection, real-parameter density check, 9-stack coverage analysis, L1-L4 balance verification
- Used GitHub Contents API for file-by-file push to bypass network blocks; engine outputs PASS/FAIL verdicts + fix lists + CI exit codes

**Quantified Results**:
- Auto-produced 112 questions + 74 standard answers covering Kafka/Spark/Flink/Hive/Hadoop/HBase/ZK/DW/data-skew
- Closed-loop remediation: first run L1 deviation -12% (FAIL) → post-fix -1.6% (PASS), all 4 dimensions passed
- Answer quality: 7.4 real params/question, 260 comparison table rows, 204% complexity annotation coverage, 0% duplication
- Full pipeline completed in 54 minutes, 5 agents, zero manual intervention

---

## 三、关键词标签（供 ATS 系统匹配）

```
Python | Multi-Agent | LLM Orchestration | Automated Pipeline | 
Quantitative QA | Harness Evaluation | GitHub API | CI/CD |
Big Data | Kafka | Spark | Flink | Hive | Hadoop | HBase |
Data Warehouse | Data Skew | Interview Question Bank
```

---

## 四、一句话亮点（面试开场白用）

> "我做了一个大数据面试题库的自动化生产系统——5 个 AI Agent 协作跑流水线，配套自研的量化评测引擎，让题库质量从主观感受变成可度量的指标，最终四维质检全部 PASS。"
