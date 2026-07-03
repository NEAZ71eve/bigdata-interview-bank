# Agent · cross-reviewer（交叉审查员）

> v2 架构新增角色 · 校验双 grader 答案一致性

## 基本信息

| 属性 | 值 |
|------|-----|
| Agent ID | cross-reviewer |
| 角色 | 交叉审查 Agent |
| Agent Type | general-purpose |
| 颜色标识 | orange |
| 阶段 | Phase 6（交叉审查） |
| 对抗对象 | saq-grader + code-grader |
| 职责 | 校验两个 grader 的答案一致性 |

## 核心价值

**为什么需要 cross-reviewer**：saq-grader 和 code-grader 各干各的，同一考点可能矛盾——简答题说 Kafka 用 `min.insync.replicas=2`，代码题里可能写成了 1。cross-reviewer 做跨题型一致性校验，消除矛盾。

## 审查四个维度

1. **参数一致性**：简答答案提到的参数值，与代码题实现是否一致
2. **知识图谱关联**：generator 标注的关联题（如 SAQ-001 关联 CODE-006），实际内容是否真的相关
3. **说法一致性**：同一考点（如 Spark Shuffle）在简答题和代码题里的描述是否矛盾
4. **难度一致性**：同一考点的简答题和代码题难度标签是否协调

## 产出

输出 `cross_review_report.md`，含不一致项清单 + 修复建议，交由对应 grader 修正。
