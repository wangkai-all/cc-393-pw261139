"""共享基础设施：数据目录、原子写、哈希、追加式审计链。

数据分两个世界，物理上分开存放：

  data/sealed/          历史封存（审计侧）——只增不改，写后只读
    raw/                原始包（封存时计算 sha256 记入 manifest 与审计链）
    withdrawn/          被撤回的可分发版本（从可分发区单向迁入，留档）
    audit.jsonl         哈希链审计日志（每条含前一条的哈希，篡改可检测）

  data/distributable/   可分发区（消费侧）——删除语义只作用在这里
    published/          当前可分发结果
    index.json          当前生效版本索引

  data/state/           运行态（登记服务镜像、回补运行、待发布结果）
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from pathlib import Path

DATA_ROOT = Path(os.environ.get("PIPELINE_DATA_DIR", "")) if os.environ.get("PIPELINE_DATA_DIR") else Path(__file__).resolve().parent.parent / "data"

SEALED = DATA_ROOT / "sealed"
RAW_DIR = SEALED / "raw"
WITHDRAWN_DIR = SEALED / "withdrawn"
AUDIT_LOG = SEALED / "audit.jsonl"

STATE_DIR = DATA_ROOT / "state"
PENDING_DIR = STATE_DIR / "pending"
RUNS_FILE = STATE_DIR / "runs.json"
REGISTRY_MIRROR = STATE_DIR / "registry.json"

DIST_DIR = DATA_ROOT / "distributable"
PUBLISHED_DIR = DIST_DIR / "published"
DIST_INDEX = DIST_DIR / "index.json"

GENESIS = "0" * 64


def ensure_dirs() -> None:
    for d in (RAW_DIR, WITHDRAWN_DIR, PENDING_DIR, PUBLISHED_DIR):
        d.mkdir(parents=True, exist_ok=True)


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(p: Path, default=None):
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def write_json_atomic(p: Path, obj, mode: int | None = None) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1, sort_keys=True)
            f.write("\n")
        os.replace(tmp, p)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    if mode is not None:
        os.chmod(p, mode)


def seal_file(p: Path) -> None:
    """封存：去掉写权限。封存区文件写后只读，任何后续修改都会留下痕迹。"""
    os.chmod(p, 0o444)


# ---------------------------------------------------------------- 审计链

_audit_lock = threading.Lock()


def _audit_tail_hash() -> str:
    if not AUDIT_LOG.exists():
        return GENESIS
    last = GENESIS
    with open(AUDIT_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    last = json.loads(line)["hash"]
                except (KeyError, json.JSONDecodeError):
                    break
    return last


def audit(event_type: str, payload: dict) -> dict:
    """追加一条哈希链审计记录。审计日志在封存区，只增不改。"""
    ensure_dirs()
    with _audit_lock:
        prev = _audit_tail_hash()
        body = {"ts": now(), "type": event_type, "payload": payload, "prev": prev}
        entry = dict(body)
        entry["hash"] = sha256_bytes(canonical(body))
        # 审计日志靠哈希链防篡改（而非文件权限），因此可以持续追加
        with open(AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        return entry


def audit_tail(n: int = 100) -> list[dict]:
    if not AUDIT_LOG.exists():
        return []
    lines = AUDIT_LOG.read_text(encoding="utf-8").splitlines()
    out = []
    for line in lines[-n:]:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            out.append({"type": "CORRUPT_LINE", "raw": line})
    return out


def verify_audit_chain() -> dict:
    """重放整条哈希链。任何一环对不上都说明封存区被篡改。"""
    if not AUDIT_LOG.exists():
        return {"ok": True, "entries": 0, "detail": "审计日志为空"}
    prev = GENESIS
    n = 0
    with open(AUDIT_LOG, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                return {"ok": False, "entries": n, "detail": f"第 {lineno} 行不是合法 JSON"}
            body = {k: entry[k] for k in ("ts", "type", "payload", "prev")}
            if entry.get("prev") != prev:
                return {"ok": False, "entries": n, "detail": f"第 {lineno} 行 prev 指针断链"}
            if entry.get("hash") != sha256_bytes(canonical(body)):
                return {"ok": False, "entries": n, "detail": f"第 {lineno} 行哈希不匹配（内容被改）"}
            prev = entry["hash"]
            n += 1
    return {"ok": True, "entries": n, "detail": f"{n} 条记录，哈希链完整"}
