"""
大数据面试题库架构核验脚本
核验 Harness 评测体系的四大量化维度：
1. 题目重复率检测（题干文本相似度）
2. 答案准确性核验（统计层面：参数/API 标注密度）
3. 考点覆盖率评测（9大技术栈分布）
4. 难度均衡度评测（L1-L4 占比）
"""
import re
import os
from collections import Counter, defaultdict

BANK_DIR = r"C:\Users\21516\WorkBuddy\2026-07-03-14-50-58\interview_bank"

def read_file(name):
    with open(os.path.join(BANK_DIR, name), encoding="utf-8") as f:
        return f.read()

def extract_questions(phase3_text):
    """提取所有题目：返回 [(qid, level, tags_str, stem)]"""
    # 匹配 ### QID [Lx] [tags] 标题行，下一行 - **题干**：...
    pattern = re.compile(
        r'^### (MCQ-\S+|SAQ-\S+|CODE-\S+)\s*\[(L[1-4])\]\s*\[([^\]]+)\]\s*$',
        re.MULTILINE
    )
    questions = []
    for m in pattern.finditer(phase3_text):
        qid, level, tags = m.group(1), m.group(2), m.group(3)
        # 找题干：标题行之后找 - **题干**：
        after = phase3_text[m.end():m.end()+500]
        stem_m = re.search(r'\*\*题干\*\*[：:]\s*(.+?)(?:\n-|\n\n|\n###|\Z)', after, re.S)
        stem = stem_m.group(1).strip() if stem_m else ""
        questions.append((qid, level, tags, stem))
    return questions

def jaccard_sim(a, b):
    """简易 Jaccard 相似度：按字符 bigram"""
    def bigrams(s):
        s = re.sub(r'\s+', '', s)
        return set(s[i:i+2] for i in range(len(s)-1)) if len(s) > 1 else {s}
    sa, sb = bigrams(a), bigrams(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def check_duplication(questions):
    """题目重复率检测"""
    high_sim_pairs = []
    max_sim_per_q = {}
    for i in range(len(questions)):
        for j in range(i+1, len(questions)):
            qi, qj = questions[i], questions[j]
            # 只比对题干（非空）
            if not qi[3] or not qj[3]:
                continue
            sim = jaccard_sim(qi[3], qj[3])
            if sim >= 0.5:
                high_sim_pairs.append((qi[0], qj[0], round(sim, 3)))
            max_sim_per_q[qi[0]] = max(max_sim_per_q.get(qi[0], 0), sim)
            max_sim_per_q[qj[0]] = max(max_sim_per_q.get(qj[0], 0), sim)
    return high_sim_pairs, max_sim_per_q

def check_difficulty(questions):
    """难度均衡度评测"""
    levels = Counter(q[1] for q in questions)
    total = sum(levels.values())
    return {l: (levels[l], round(levels[l]/total*100, 1)) for l in ["L1","L2","L3","L4"] if levels[l]}, total

def check_coverage(questions):
    """考点覆盖率评测：9大技术栈"""
    stacks = ["Kafka","Spark","Hadoop","HDFS","YARN","MapReduce","Hive","Flink",
              "HBase","Zookeeper","数仓","数据倾斜","场景题"]
    coverage = defaultdict(int)
    for q in questions:
        tags = q[2]
        for s in stacks:
            if s in tags:
                coverage[s] += 1
    # 归并 Hadoop 子项
    hadoop_total = coverage["Hadoop"] + coverage["HDFS"] + coverage["YARN"] + coverage["MapReduce"]
    return dict(coverage), hadoop_total

def check_answer_quality(saq_text, code_text):
    """答案准确性核验（统计层面）"""
    # 真实配置参数密度
    param_patterns = [
        r'acks\s*=\s*all', r'min\.insync\.replicas', r'spark\.sql\.shuffle\.partitions',
        r'checkpoint', r'watermark', r'stateful', r'ValueState', r'TwoPhaseCommit',
        r' RocksDB', r'Unaligned', r'offset'
    ]
    saq_params = sum(len(re.findall(p, saq_text, re.I)) for p in param_patterns)
    code_params = sum(len(re.findall(p, code_text, re.I)) for p in param_patterns)
    # 对比表格行数
    table_rows = len(re.findall(r'^\|.*\|$', saq_text, re.M))
    # 代码块数
    code_blocks = len(re.findall(r'```', code_text)) // 2
    # 复杂度标注数
    complexity = len(re.findall(r'复杂度', code_text))
    return {
        "saq_param_hits": saq_params,
        "code_param_hits": code_params,
        "saq_table_rows": table_rows,
        "code_blocks": code_blocks,
        "complexity_notes": complexity
    }

def main():
    phase3 = read_file("phase3_expanded_bank.md")
    saq = read_file("phase4_saq_answers.md")
    code = read_file("phase4_code_solutions.md")

    questions = extract_questions(phase3)
    print(f"=== 题目提取 ===")
    print(f"总题目数: {len(questions)}")
    by_type = Counter(q[0].split("-")[0] for q in questions)
    print(f"题型分布: {dict(by_type)}")

    print(f"\n=== Harness 1: 题目重复率检测 ===")
    high_sim, max_sim = check_duplication(questions)
    sim_dist = Counter()
    for qid, s in max_sim.items():
        if s < 0.3: sim_dist["<0.3 (低)"] += 1
        elif s < 0.5: sim_dist["0.3-0.5 (中)"] += 1
        elif s < 0.7: sim_dist["0.5-0.7 (高)"] += 1
        else: sim_dist[">=0.7 (极高)"] += 1
    print(f"相似度分布: {dict(sim_dist)}")
    print(f"高相似对(>=0.5)数量: {len(high_sim)}")
    if high_sim:
        print(f"高相似对示例(前10):")
        for p in sorted(high_sim, key=lambda x:-x[2])[:10]:
            print(f"  {p[0]} <-> {p[1]}: {p[2]}")

    print(f"\n=== Harness 2: 答案准确性核验（统计层面）===")
    aq = check_answer_quality(saq, code)
    for k, v in aq.items():
        print(f"  {k}: {v}")

    print(f"\n=== Harness 3: 考点覆盖率评测 ===")
    cov, hadoop_total = check_coverage(questions)
    print(f"技术栈分布(原始): {cov}")
    print(f"Hadoop族(HDFS+YARN+MR+Hadoop)合计: {hadoop_total}")
    # 检查是否有覆盖为0的技术栈
    expected = ["Kafka","Spark","Hive","Flink","HBase","Zookeeper","数仓","数据倾斜"]
    missing = [s for s in expected if cov.get(s,0)==0]
    print(f"缺失技术栈: {missing if missing else '无'}")

    print(f"\n=== Harness 4: 难度均衡度评测 ===")
    diff, total = check_difficulty(questions)
    print(f"难度分布(总数{total}):")
    target = {"L1":15,"L2":35,"L3":35,"L4":15}
    for l in ["L1","L2","L3","L4"]:
        if l in diff:
            cnt, pct = diff[l]
            tgt = target[l]
            dev = round(pct - tgt, 1)
            status = "✓" if abs(dev) <= 5 else "△"
            print(f"  {l}: {cnt}道 ({pct}%) 目标{tgt}% 偏差{dev:+} {status}")
        else:
            print(f"  {l}: 0道 (0%) 目标{tgt}% 偏差{-tgt:+} ✗")

    # 题型×难度交叉
    print(f"\n=== 题型×难度交叉表 ===")
    cross = defaultdict(lambda: Counter())
    for q in questions:
        qtype = q[0].split("-")[0]
        cross[qtype][q[1]] += 1
    print(f"{'题型':<8} {'L1':<6} {'L2':<6} {'L3':<6} {'L4':<6} {'合计':<6}")
    for t in ["MCQ","SAQ","CODE"]:
        c = cross[t]
        row = f"{t:<8}"
        for l in ["L1","L2","L3","L4"]:
            row += f" {c.get(l,0):<5}"
        row += f" {sum(c.values()):<5}"
        print(row)

if __name__ == "__main__":
    main()
