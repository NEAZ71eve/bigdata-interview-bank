#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
大数据面试题库 · Harness 量化评测引擎（流水线 Phase 5 独立质检阶段）
================================================================
功能：对题库全量内容做四维量化评测，输出 PASS/FAIL 判定 + 修复清单。

四大量化维度：
  H1. 题目重复率检测（题干 Jaccard 相似度）
  H2. 答案准确性核验（参数/API 标注密度 + 完整性检查）
  H3. 考点覆盖率评测（9大技术栈分布 + 缺失检测）
  H4. 难度均衡度评测（L1-L4 占比 vs 目标区间）

输出：
  - 控制台实时报告
  - harness_report.md（可读报告，含修复清单）
  - 退出码：0=全部PASS / 1=存在FAIL项

用法：
  python harness_engine.py [--fix]   # --fix 输出修复清单（默认也输出）
"""
import re
import os
import sys
import json
from collections import Counter, defaultdict
from datetime import datetime

BANK_DIR = os.path.dirname(os.path.abspath(__file__))
PHASE3 = os.path.join(BANK_DIR, "phase3_expanded_bank.md")
PHASE4_SAQ = os.path.join(BANK_DIR, "phase4_saq_answers.md")
PHASE4_CODE = os.path.join(BANK_DIR, "phase4_code_solutions.md")
REPORT_FILE = os.path.join(BANK_DIR, "harness_report.md")

# 评测阈值配置
THRESHOLDS = {
    "duplication": {
        "similarity_threshold": 0.5,     # 相似度 >= 此值判定为重复
        "max_high_sim_ratio": 0.05,      # 高相似对占比上限 5%
    },
    "answer_quality": {
        "min_saq_param_density": 2.0,    # 简答题每道至少 2 个参数命中
        "min_code_complexity_ratio": 0.8,# 代码题复杂度标注覆盖率
        "min_saq_table_rows": 100,       # 简答题对比表格最少行数
    },
    "coverage": {
        "required_stacks": ["Kafka", "Spark", "Hive", "Flink", "HBase",
                            "Zookeeper", "数仓", "数据倾斜"],
        "min_per_stack": 3,              # 每个技术栈最少题数
    },
    "difficulty": {
        "targets": {"L1": 15, "L2": 35, "L3": 35, "L4": 15},  # 目标占比 %
        "tolerance": 7,                 # 允许偏差 ±7%
    }
}

# 答案质量关键词（真实配置参数 / API）
QUALITY_PATTERNS = [
    r'acks\s*=\s*all', r'min\.insync\.replicas', r'spark\.sql\.shuffle\.partitions',
    r'checkpoint', r'watermark', r'stateful', r'ValueState', r'TwoPhaseCommit',
    r'RocksDB', r'Unaligned', r'offset', r'reduceByKey', r'groupByKey',
    r'SCD[23]', r'Lambda', r'Kappa', r'LSM', r'Compaction', r'Barrier',
    r'fsimage', r'edits', r'SecondaryNameNode', r'Partitioner',
]


def read_file(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def extract_questions(phase3_text):
    """提取所有题目：[(qid, qtype, level, tags, stem)]"""
    pattern = re.compile(
        r'^### (MCQ-\S+|SAQ-\S+|CODE-\S+)\s*\[(L[1-4])\]\s*\[([^\]]+)\]\s*$',
        re.MULTILINE
    )
    questions = []
    for m in pattern.finditer(phase3_text):
        qid = m.group(1)
        level = m.group(2)
        tags = m.group(3)
        qtype = qid.split("-")[0]
        after = phase3_text[m.end():m.end()+500]
        stem_m = re.search(r'\*\*题干\*\*[：:]\s*(.+?)(?:\n-|\n\n|\n###|\Z)', after, re.S)
        stem = stem_m.group(1).strip() if stem_m else ""
        questions.append((qid, qtype, level, tags, stem))
    return questions


def jaccard_sim(a, b):
    def bigrams(s):
        s = re.sub(r'\s+', '', s)
        return set(s[i:i+2] for i in range(len(s)-1)) if len(s) > 1 else {s}
    sa, sb = bigrams(a), bigrams(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


# ============ H1: 题目重复率检测 ============
def check_duplication(questions):
    threshold = THRESHOLDS["duplication"]["similarity_threshold"]
    high_sim_pairs = []
    sim_distribution = {"<0.3": 0, "0.3-0.5": 0, ">=0.5": 0}

    for i in range(len(questions)):
        for j in range(i+1, len(questions)):
            qi, qj = questions[i], questions[j]
            if not qi[4] or not qj[4]:
                continue
            sim = jaccard_sim(qi[4], qj[4])
            if sim < 0.3:
                sim_distribution["<0.3"] += 1
            elif sim < threshold:
                sim_distribution["0.3-0.5"] += 1
            else:
                sim_distribution[">=0.5"] += 1
                high_sim_pairs.append((qi[0], qj[0], round(sim, 3)))

    total_pairs = sum(sim_distribution.values())
    high_ratio = sim_distribution[">=0.5"] / total_pairs if total_pairs else 0
    passed = (len(high_sim_pairs) == 0 and
              high_ratio <= THRESHOLDS["duplication"]["max_high_sim_ratio"])

    return {
        "name": "H1 题目重复率检测",
        "passed": passed,
        "metrics": {
            "总比对对数": total_pairs,
            "相似度分布": sim_distribution,
            "高相似对(>=%.1f)数量" % threshold: len(high_sim_pairs),
            "高相似占比": f"{high_ratio:.2%}",
        },
        "issues": high_sim_pairs[:20],
        "fixes": [f"删除或改写重复题对: {p[0]} <-> {p[1]} (相似度{p[2]})" 
                  for p in high_sim_pairs[:10]] if high_sim_pairs else []
    }


# ============ H2: 答案准确性核验 ============
def check_answer_quality(saq_text, code_text, questions):
    saq_count = sum(1 for q in questions if q[1] == "SAQ")
    code_count = sum(1 for q in questions if q[1] == "CODE")

    saq_param_hits = sum(len(re.findall(p, saq_text, re.I)) for p in QUALITY_PATTERNS)
    code_param_hits = sum(len(re.findall(p, code_text, re.I)) for p in QUALITY_PATTERNS)
    saq_param_density = saq_param_hits / saq_count if saq_count else 0

    table_rows = len(re.findall(r'^\|.*\|$', saq_text, re.M))
    code_blocks = len(re.findall(r'```', code_text)) // 2
    complexity_notes = len(re.findall(r'复杂度', code_text))
    complexity_ratio = complexity_notes / code_count if code_count else 0

    issues = []
    fixes = []

    if saq_param_density < THRESHOLDS["answer_quality"]["min_saq_param_density"]:
        issues.append(f"简答题参数密度 {saq_param_density:.1f}/题 低于阈值 {THRESHOLDS['answer_quality']['min_saq_param_density']}")
        fixes.append("补充简答题真实配置参数（如 acks=all、checkpoint.interval 等）")

    if complexity_ratio < THRESHOLDS["answer_quality"]["min_code_complexity_ratio"]:
        issues.append(f"代码题复杂度标注覆盖率 {complexity_ratio:.1%} 低于阈值 {THRESHOLDS['answer_quality']['min_code_complexity_ratio']:.0%}")
        fixes.append("为缺少复杂度分析的代码题补充时间/空间复杂度标注")

    if table_rows < THRESHOLDS["answer_quality"]["min_saq_table_rows"]:
        issues.append(f"对比表格行数 {table_rows} 低于阈值 {THRESHOLDS['answer_quality']['min_saq_table_rows']}")
        fixes.append("补充技术对比表格（如 Spark vs Flink、SCD2 vs SCD3）")

    passed = len(issues) == 0

    return {
        "name": "H2 答案准确性核验",
        "passed": passed,
        "metrics": {
            "简答题数": saq_count,
            "代码题数": code_count,
            "简答题参数命中": saq_param_hits,
            "简答题参数密度(题)": f"{saq_param_density:.1f}",
            "代码题参数命中": code_param_hits,
            "对比表格行数": table_rows,
            "代码块数": code_blocks,
            "复杂度标注数": complexity_notes,
            "复杂度覆盖率": f"{complexity_ratio:.1%}",
        },
        "issues": issues,
        "fixes": fixes,
    }


# ============ H3: 考点覆盖率评测 ============
def check_coverage(questions):
    stacks = ["Kafka", "Spark", "Hadoop", "HDFS", "YARN", "MapReduce", "Hive",
              "Flink", "HBase", "Zookeeper", "数仓", "数据倾斜", "场景题"]
    coverage = defaultdict(int)
    for q in questions:
        for s in stacks:
            if s in q[3]:
                coverage[s] += 1

    # 归并 Hadoop 子项
    hadoop_total = coverage["Hadoop"] + coverage["HDFS"] + coverage["YARN"] + coverage["MapReduce"]

    required = THRESHOLDS["coverage"]["required_stacks"]
    min_per = THRESHOLDS["coverage"]["min_per_stack"]
    missing = [s for s in required if coverage.get(s, 0) == 0]
    weak = [s for s in required if 0 < coverage.get(s, 0) < min_per]

    issues = []
    fixes = []
    for s in missing:
        issues.append(f"技术栈 [{s}] 覆盖为 0，完全缺失")
        fixes.append(f"补充 [{s}] 技术栈题目（至少 {min_per} 道）")
    for s in weak:
        issues.append(f"技术栈 [{s}] 仅 {coverage[s]} 道，低于最低 {min_per} 道")
        fixes.append(f"补充 [{s}] 技术栈题目至 {min_per} 道以上")

    passed = len(missing) == 0

    return {
        "name": "H3 考点覆盖率评测",
        "passed": passed,
        "metrics": {
            "技术栈分布": dict(coverage),
            "Hadoop族合计": hadoop_total,
            "缺失技术栈": missing if missing else "无",
            "薄弱技术栈": weak if weak else "无",
        },
        "issues": issues,
        "fixes": fixes,
    }


# ============ H4: 难度均衡度评测 ============
def check_difficulty(questions):
    levels = Counter(q[2] for q in questions)
    total = sum(levels.values())
    targets = THRESHOLDS["difficulty"]["targets"]
    tolerance = THRESHOLDS["difficulty"]["tolerance"]

    results = {}
    issues = []
    fixes = []
    all_pass = True

    for l in ["L1", "L2", "L3", "L4"]:
        cnt = levels.get(l, 0)
        pct = round(cnt / total * 100, 1) if total else 0
        target = targets[l]
        dev = round(pct - target, 1)
        passed = abs(dev) <= tolerance
        results[l] = {"count": cnt, "pct": pct, "target": target, "deviation": dev, "passed": passed}
        if not passed:
            all_pass = False
            direction = "不足" if dev < 0 else "偏多"
            issues.append(f"{l}: {cnt}道 ({pct}%) 目标{target}% 偏差{dev:+}% {direction}")
            if dev < 0:
                need = int((target - pct) / 100 * total)
                fixes.append(f"补充 {need} 道 {l} 题目")
            else:
                need = int((pct - target) / 100 * total)
                fixes.append(f"精简或提升 {need} 道 {l} 题目难度")

    return {
        "name": "H4 难度均衡度评测",
        "passed": all_pass,
        "metrics": {
            "总题数": total,
            "L1": results["L1"],
            "L2": results["L2"],
            "L3": results["L3"],
            "L4": results["L4"],
        },
        "issues": issues,
        "fixes": fixes,
    }


# ============ 报告生成 ============
def generate_report(results, questions):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_pass = all(r["passed"] for r in results)
    total_issues = sum(len(r["issues"]) for r in results)
    total_fixes = sum(len(r["fixes"]) for r in results)

    lines = [
        f"# 大数据面试题库 · Harness 质检报告",
        f"",
        f"> 评测时间：{timestamp}",
        f"> 评测引擎：harness_engine.py（Phase 5 独立质检阶段）",
        f"> 题库总量：{len(questions)} 道",
        f"> 总体判定：{'✅ ALL PASS' if all_pass else '⚠️ 存在 FAIL 项'}",
        f"> 问题数：{total_issues} ｜ 修复建议数：{total_fixes}",
        f"",
        f"---",
        f"",
    ]

    # 汇总表
    lines.append("## 评测汇总")
    lines.append("")
    lines.append("| 维度 | 判定 | 关键指标 | 问题数 |")
    lines.append("|------|------|---------|--------|")
    for r in results:
        status = "✅ PASS" if r["passed"] else "❌ FAIL"
        key_metric = list(r["metrics"].values())[0] if r["metrics"] else "-"
        lines.append(f"| {r['name']} | {status} | {key_metric} | {len(r['issues'])} |")
    lines.append("")

    # 各维度详情
    for r in results:
        lines.append(f"## {r['name']}")
        lines.append(f"**判定：{'✅ PASS' if r['passed'] else '❌ FAIL'}**")
        lines.append("")
        lines.append("### 指标")
        for k, v in r["metrics"].items():
            lines.append(f"- **{k}**: {v}")
        lines.append("")

        if r["issues"]:
            lines.append("### ⚠️ 发现的问题")
            for i, issue in enumerate(r["issues"], 1):
                lines.append(f"{i}. {issue}")
            lines.append("")

        if r["fixes"]:
            lines.append("### 🔧 修复建议")
            for i, fix in enumerate(r["fixes"], 1):
                lines.append(f"{i}. {fix}")
            lines.append("")

        lines.append("---")
        lines.append("")

    # 题型×难度交叉表
    cross = defaultdict(lambda: Counter())
    for q in questions:
        cross[q[1]][q[2]] += 1
    lines.append("## 题型×难度交叉表")
    lines.append("")
    lines.append("| 题型 | L1 | L2 | L3 | L4 | 合计 |")
    lines.append("|------|----|----|----|----|------|")
    for t in ["MCQ", "SAQ", "CODE"]:
        c = cross[t]
        row = f"| {t} | {c.get('L1',0)} | {c.get('L2',0)} | {c.get('L3',0)} | {c.get('L4',0)} | {sum(c.values())} |"
        lines.append(row)
    total_row = f"| **合计** |"
    for l in ["L1", "L2", "L3", "L4"]:
        total = sum(cross[t].get(l, 0) for t in ["MCQ", "SAQ", "CODE"])
        total_row += f" **{total}** |"
    total_row += f" **{len(questions)}** |"
    lines.append(total_row)
    lines.append("")

    # 退出建议
    lines.append("## 下一步建议")
    if all_pass:
        lines.append("- 题库质量全部达标，可进入组卷/发布阶段")
    else:
        lines.append("- 按修复建议逐项修复后重新运行 `python harness_engine.py`")
        lines.append("- 重点关注 ❌ FAIL 项，直到全部 ✅ PASS")
    lines.append("")

    report = "\n".join(lines)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)
    return report


def main():
    print("=" * 60)
    print("大数据面试题库 · Harness 量化评测引擎")
    print("=" * 60)

    # 读取文件
    phase3 = read_file(PHASE3)
    saq = read_file(PHASE4_SAQ)
    code = read_file(PHASE4_CODE)

    questions = extract_questions(phase3)
    print(f"\n题库总量: {len(questions)} 道")
    by_type = Counter(q[1] for q in questions)
    print(f"题型分布: {dict(by_type)}")

    # 执行四维评测
    results = []
    print("\n" + "-" * 40)
    print("执行评测...")
    print("-" * 40)

    r1 = check_duplication(questions)
    results.append(r1)
    print(f"\n[{r1['name']}] {'✅ PASS' if r1['passed'] else '❌ FAIL'}")
    for k, v in r1["metrics"].items():
        print(f"  {k}: {v}")

    r2 = check_answer_quality(saq, code, questions)
    results.append(r2)
    print(f"\n[{r2['name']}] {'✅ PASS' if r2['passed'] else '❌ FAIL'}")
    for k, v in r2["metrics"].items():
        print(f"  {k}: {v}")

    r3 = check_coverage(questions)
    results.append(r3)
    print(f"\n[{r3['name']}] {'✅ PASS' if r3['passed'] else '❌ FAIL'}")
    for k, v in r3["metrics"].items():
        print(f"  {k}: {v}")

    r4 = check_difficulty(questions)
    results.append(r4)
    print(f"\n[{r4['name']}] {'✅ PASS' if r4['passed'] else '❌ FAIL'}")
    for l in ["L1", "L2", "L3", "L4"]:
        d = r4["metrics"][l]
        status = "✓" if d["passed"] else "✗"
        print(f"  {l}: {d['count']}道 ({d['pct']}%) 目标{d['target']}% 偏差{d['deviation']:+} {status}")

    # 生成报告
    report = generate_report(results, questions)
    print(f"\n{'=' * 60}")
    all_pass = all(r["passed"] for r in results)
    if all_pass:
        print(f"总体判定: ✅ ALL PASS")
    else:
        fail_count = sum(1 for r in results if not r["passed"])
        print(f"总体判定: ⚠️ {fail_count} 项 FAIL，需修复")
    print(f"报告已生成: {REPORT_FILE}")
    print(f"{'=' * 60}")

    # 退出码
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
