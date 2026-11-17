#!/usr/bin/env python3
"""入口：

  python3 run.py serve       启动删除登记服务(8601) + 控制台(8600)
  python3 run.py init-data   生成并封存虚构原始包（6 包 × 12 条）
  python3 run.py verify      校验封存完整性（原始包哈希 + 审计链）
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import pipeline, registry, web  # noqa: E402
from app.common import ensure_dirs  # noqa: E402

REGISTRY_PORT = int(os.environ.get("REGISTRY_PORT", "8601"))
WEB_PORT = int(os.environ.get("WEB_PORT", "8600"))


def cmd_serve():
    ensure_dirs()
    t = threading.Thread(target=registry.serve, args=(REGISTRY_PORT,), daemon=True)
    t.start()
    web.serve(WEB_PORT)


def cmd_init_data():
    ensure_dirs()
    info = pipeline.generate_packages()
    print(f"已封存 {info['packages']} 个原始包，共 {info['records']} 条虚构记录")


def cmd_verify():
    ensure_dirs()
    report = pipeline.verify_all()
    for c in report["checks"]:
        print(("✓" if c["ok"] else "✗"), c["item"], "—", c["detail"])
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"
    {"serve": cmd_serve, "init-data": cmd_init_data, "verify": cmd_verify}.get(
        cmd, lambda: sys.exit(f"未知命令: {cmd}"))()
