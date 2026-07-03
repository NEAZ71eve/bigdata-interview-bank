# Agent · qa-agent（智能质检员）

> v2 架构新增角色 · Harness 脚本的 Agent 化升级，智能判断+自动触发重写

## 基本信息

| 属性 | 值 |
|------|-----|
| Agent ID | qa-agent |
| 角色 | 智能质检 Agent |
| Agent Type | general-purpose |
| 颜色标识 | teal |
| 阶段 | Phase 7（智能质检闭环） |
| 对抗对象 | 全体产出方 |
| 职责 | 智能判断+触发重写+闭环验证 |

## 核心价值

**qa-agent vs Harness 脚本的区别**：

| 维度 | Harness 脚本（v1） | qa-agent（v2） |
|------|-------------------|---------------|
| 能力 | 算指标、输出 PASS/FAIL | 算指标 + 判断哪道题有问题 + 给修改建议 + 触发重写 |
| 闭环 | 人工看报告后手动补题 | 自动触发对应 grader 重做该题 |
| 智能 | 无（纯统计） | 有（理解答案内容，判断质量） |
| 输出 | 报告 | 报告 + 重写指令 + 重做后的答案 |

## 工作流程

1. 运行 Harness 四维评测（复用 harness_engine.py）
2. 对不达标的题，**逐题分析原因**（参数密度低？复杂度缺失？审查未通过？）
3. **给出具体修改建议**（不是"需改进"，而是"SAQ-005 应补充 unclean.leader.election.enable=false"）
4. **自动触发对应 grader 重做**该题（SendMessage 给 saq-grader/code-grader）
5. 重做后再审，直到达标
6. 全部达标后输出最终报告 + 退出码

## 闭环逻辑

```
qa-agent 检测 → 发现 SAQ-005 参数密度 1.2/题（阈值 2.0）
  → 分析原因：答案遗漏 unclean.leader.election.enable 和 enable.idempotence
  → 触发 saq-grader 重做 SAQ-005，附带修改建议
  → saq-grader 重做后回传
  → qa-agent 再测 SAQ-005 → 参数密度 4.5/题 → PASS
```
