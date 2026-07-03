"""
通过 GitHub Contents API 逐文件上传，支持空仓库初始化。
仅走 api.github.com（已验证可达）。

用法：
  set GITHUB_TOKEN=ghp_xxx
  python upload_to_github.py

注意：请勿将含 token 的版本提交到 git 仓库。
"""
import base64
import json
import os
import urllib.request
import urllib.error
import time

TOKEN = os.environ.get("GITHUB_TOKEN", "")
OWNER = "NEAZ71eve"
REPO = "bigdata-interview-bank"
API = "https://api.github.com"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = [
    (".gitignore", ".gitignore"),
    ("overview.md", "overview.md"),
    ("interview_bank/README.md", "interview_bank/README.md"),
    ("interview_bank/architecture_verification_report.md", "interview_bank/architecture_verification_report.md"),
    ("interview_bank/harness_verify.py", "interview_bank/harness_verify.py"),
    ("interview_bank/phase2_standardized_questions.md", "interview_bank/phase2_standardized_questions.md"),
    ("interview_bank/phase1_raw_interviews.md", "interview_bank/phase1_raw_interviews.md"),
    ("interview_bank/phase3_expanded_bank.md", "interview_bank/phase3_expanded_bank.md"),
    ("interview_bank/phase4_saq_answers.md", "interview_bank/phase4_saq_answers.md"),
    ("interview_bank/phase4_code_solutions.md", "interview_bank/phase4_code_solutions.md"),
]

def api_call(method, path, data=None, retry=3):
    url = f"{API}{path}"
    headers = {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "interview-bank-uploader",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    for attempt in range(retry):
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            try:
                err_json = json.loads(err_body) if err_body else {}
            except Exception:
                err_json = {"raw": err_body}
            if e.code in (422, 403) and attempt < retry - 1:
                time.sleep(2 * (attempt + 1))
                continue
            return e.code, err_json
        except Exception as e:
            if attempt < retry - 1:
                time.sleep(2 * (attempt + 1))
                continue
            return 0, {"error": str(e)}

def upload_file(rel_path, abs_path, commit_msg):
    with open(abs_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")
    status, resp = api_call("PUT", f"/repos/{OWNER}/{REPO}/contents/{rel_path}", {
        "message": commit_msg,
        "content": content,
        "branch": "main"
    })
    return status, resp

def main():
    if not TOKEN:
        print("ERROR: 请先设置 GITHUB_TOKEN 环境变量")
        print("  set GITHUB_TOKEN=ghp_your_token_here")
        return

    print(f"=== GitHub Contents API Uploader ===")
    print(f"Repo: {OWNER}/{REPO}")
    print(f"Files: {len(FILES)}")

    success = 0
    failed = []
    for i, (rel_path, abs_path) in enumerate(FILES, 1):
        full_path = os.path.join(PROJECT_ROOT, abs_path)
        if not os.path.exists(full_path):
            print(f"  [{i}/{len(FILES)}] {rel_path} - SKIP (文件不存在)")
            continue
        size_kb = os.path.getsize(full_path) / 1024
        commit_msg = f"feat: add {rel_path}"
        print(f"  [{i}/{len(FILES)}] {rel_path} ({size_kb:.1f}KB)...", end=" ", flush=True)
        status, resp = upload_file(rel_path, full_path, commit_msg)
        if status in (200, 201):
            commit_sha = resp.get("commit", {}).get("sha", "?")[:7]
            print(f"OK commit={commit_sha}")
            success += 1
        else:
            msg = resp.get("message", str(resp))[:80]
            print(f"FAIL [{status}] {msg}")
            failed.append(rel_path)
        if i < len(FILES):
            time.sleep(0.5)

    print(f"\n成功: {success}/{len(FILES)}")
    if failed:
        print(f"失败: {failed}")
    print(f"\n访问: https://github.com/{OWNER}/{REPO}")

if __name__ == "__main__":
    main()
