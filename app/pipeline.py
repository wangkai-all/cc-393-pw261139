"""核心域逻辑：原始包封存、区间回补、删除屏障发布、修订扫描、完整性校验。

协调协议（为什么不能只在回补开始时查一次名单）：

  删除的线性化点 = 登记服务分配版本号（POST /v1/deletions 返回）。
  发布的线性化点 = 在 _PUBLISH_LOCK 内向登记服务读取的一致性快照版本。

  1. 回补 PREPARE：读快照 v_start —— 只用于计算期过滤，不作安全保证。
  2. 回补 FINALIZE：读 deletions since v_start，把计算期间到达的删除过滤掉，
     结果带“删除屏障”版本 v_end 进入待发布。
  3. 发布 PUBLISH（持 _PUBLISH_LOCK）：再读 deletions since barrier。
     屏障 == 当前版本 → 直接发布；有新删除 → 在屏障处过滤后按新版本发布；
     登记服务不可达 → fail-closed 阻断，可恢复。
  4. 修订扫描（登记删除后、持同一把 _PUBLISH_LOCK）：把已发布结果中含
     已删记录的版本撤回（迁入封存区留档），发布过滤后的修订版。

  因为 (3) 和 (4) 互斥，且删除先登记后扫描，所以“计算完成与发布之间”
  乃至“发布进行中”到达的删除，要么被屏障快照看到，要么被修订扫描兜底，
  不存在漏网窗口。登记服务离线时 (3)(4) 都无法取得安全证明 → 一律阻断。
"""
from __future__ import annotations

import http.client
import json
import os
import random
import threading
from pathlib import Path

from . import common
from .common import (
    AUDIT_LOG, DIST_INDEX, PENDING_DIR, PUBLISHED_DIR, RAW_DIR, RUNS_FILE,
    WITHDRAWN_DIR, audit, ensure_dirs, now, read_json, seal_file,
    sha256_file, write_json_atomic,
)

REGISTRY_PORT = int(os.environ.get("REGISTRY_PORT", "8601"))


# ------------------------------------------------------------ 登记服务客户端

class RegistryUnavailable(Exception):
    """登记服务不可达或明确拒绝服务（503）。调用方必须 fail-closed。"""


class RegistryClient:
    def __init__(self, port: int = REGISTRY_PORT):
        self.port = port

    def _req(self, method: str, path: str, body: dict | None = None) -> dict:
        payload = json.dumps(body).encode() if body is not None else None
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
            conn.request(method, path, body=payload,
                         headers={"Content-Type": "application/json"} if payload else {})
            resp = conn.getresponse()
            data = resp.read()
        except (OSError, http.client.HTTPException) as e:
            raise RegistryUnavailable(f"连接失败: {e}") from e
        finally:
            try:
                conn.close()
            except Exception:
                pass
        try:
            obj = json.loads(data) if data else {}
        except json.JSONDecodeError:
            raise RegistryUnavailable(f"登记服务返回非法响应 (HTTP {resp.status})")
        if resp.status == 503:
            raise RegistryUnavailable(obj.get("detail", "登记服务离线 (HTTP 503)"))
        if resp.status >= 400:
            raise RegistryUnavailable(f"登记服务错误 HTTP {resp.status}: {obj}")
        return obj

    def status(self) -> dict:
        return self._req("GET", "/v1/status")

    def deletions_since(self, version: int) -> dict:
        return self._req("GET", f"/v1/deletions?since={version}")

    def add_deletion(self, record_id: str) -> dict:
        return self._req("POST", "/v1/deletions", {"record_id": record_id})

    def set_offline(self, flag: bool) -> dict:
        return self._req("POST", f"/v1/admin/{'offline' if flag else 'online'}")


_client = None


def client() -> RegistryClient:
    global _client
    if _client is None:
        _client = RegistryClient()
    return _client


# ------------------------------------------------------------ 运行态存储

_LOCK = threading.RLock()          # 运行态（runs/pending）互斥
_PUBLISH_LOCK = threading.Lock()   # 发布临界区与修订扫描互斥（见模块 docstring）


def _load_runs() -> dict:
    return read_json(RUNS_FILE, {}) or {}


def _save_runs(runs: dict) -> None:
    write_json_atomic(RUNS_FILE, runs)


def _get_run(run_id: str) -> dict | None:
    return _load_runs().get(run_id)


def _put_run(run: dict) -> None:
    with _LOCK:
        runs = _load_runs()
        runs[run["id"]] = run
        _save_runs(runs)


def _run_event(run: dict, msg: str) -> None:
    run.setdefault("events", []).append({"ts": now(), "msg": msg})


def _pending_path(result_id: str) -> Path:
    return PENDING_DIR / f"{result_id}.json"


def _load_pending(result_id: str) -> dict | None:
    return read_json(_pending_path(result_id), None)


def _save_pending(res: dict) -> None:
    write_json_atomic(_pending_path(res["id"]), res)


def _load_index() -> dict:
    return read_json(DIST_INDEX, {}) or {}


def _save_index(idx: dict) -> None:
    write_json_atomic(DIST_INDEX, idx)


# ------------------------------------------------------------ 原始包封存

STATIONS = ["青崖站", "白泽站", "梧丘站", "澜沧站", "栖霞站", "望舒站", "临渊站", "折柳站"]
ROUTES = ["北环", "南环", "东直", "西直", "山线", "水线"]


def manifest() -> dict:
    return read_json(RAW_DIR / "manifest.json", {}) or {}


def generate_packages(n_packages: int = 6, per_package: int = 12) -> dict:
    """生成虚构的配送事件原始包并封存。已存在封存包时拒绝（封存区不可覆盖）。"""
    ensure_dirs()
    if any(RAW_DIR.glob("pkg-*.json")):
        raise RuntimeError("封存区已有原始包，拒绝覆盖：封存数据只增不改")
    m = {}
    for p in range(1, n_packages + 1):
        pkg_id = f"pkg-{p:04d}"
        rng = random.Random(p * 7919)
        records = []
        for s in range(1, per_package + 1):
            records.append({
                "record_id": f"R{p:04d}-{s:02d}",
                "station": STATIONS[rng.randrange(len(STATIONS))],
                "route": ROUTES[rng.randrange(len(ROUTES))],
                "weight_kg": round(rng.uniform(0.2, 30.0), 2),
                "parcels": rng.randint(1, 9),
                "event_ts": f"2026-10-{p:02d}T{rng.randrange(8, 20):02d}:{rng.randrange(60):02d}:00",
            })
        pkg = {"package": pkg_id, "kind": "fictional-delivery-events", "records": records}
        path = RAW_DIR / f"{pkg_id}.json"
        write_json_atomic(path, pkg)
        digest = sha256_file(path)
        seal_file(path)
        m[pkg_id] = {"sha256": digest, "records": len(records), "sealed_at": now()}
        audit("RAW_PACKAGE_SEALED", {"package": pkg_id, "sha256": digest, "records": len(records)})
    write_json_atomic(RAW_DIR / "manifest.json", m)
    seal_file(RAW_DIR / "manifest.json")
    audit("MANIFEST_SEALED", {"sha256": sha256_file(RAW_DIR / "manifest.json"), "packages": len(m)})
    return {"packages": len(m), "records": sum(v["records"] for v in m.values())}


# ------------------------------------------------------------ 完整性

class IntegrityError(Exception):
    pass


def _verify_package(pkg_id: str) -> dict:
    m = manifest()
    if pkg_id not in m:
        raise IntegrityError(f"{pkg_id} 不在封存清单中")
    path = RAW_DIR / f"{pkg_id}.json"
    if not path.exists():
        raise IntegrityError(f"{pkg_id} 文件缺失")
    actual = sha256_file(path)
    if actual != m[pkg_id]["sha256"]:
        raise IntegrityError(f"{pkg_id} 哈希不匹配：封存 {m[pkg_id]['sha256'][:16]}… ≠ 当前 {actual[:16]}…")
    return m[pkg_id]


def verify_all() -> dict:
    """全量校验：原始包哈希、清单哈希对审计锚点、审计链。"""
    checks = []
    ok = True
    m = manifest()
    for pkg_id, meta in sorted(m.items()):
        path = RAW_DIR / f"{pkg_id}.json"
        if not path.exists():
            checks.append({"item": pkg_id, "ok": False, "detail": "文件缺失"})
            ok = False
            continue
        actual = sha256_file(path)
        good = actual == meta["sha256"]
        ok = ok and good
        checks.append({"item": pkg_id, "ok": good,
                       "detail": "哈希一致" if good else f"被篡改！封存 {meta['sha256'][:16]}… ≠ 当前 {actual[:16]}…"})
    # 清单本身对审计锚点
    anchor = None
    if AUDIT_LOG.exists():
        for line in AUDIT_LOG.read_text(encoding="utf-8").splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("type") == "MANIFEST_SEALED":
                anchor = e["payload"]["sha256"]
    if anchor:
        good = sha256_file(RAW_DIR / "manifest.json") == anchor
        ok = ok and good
        checks.append({"item": "manifest.json", "ok": good,
                       "detail": "清单哈希与审计锚点一致" if good else "清单被篡改（与审计锚点不符）"})
    chain = common.verify_audit_chain()
    ok = ok and chain["ok"]
    checks.append({"item": "audit.jsonl", "ok": chain["ok"], "detail": chain["detail"]})
    return {"ok": ok, "checks": checks, "verified_at": now()}


# ------------------------------------------------------------ 区间回补

def _next_run_id(runs: dict) -> str:
    n = 0
    for rid in runs:
        if rid.startswith("run-"):
            n = max(n, int(rid.split("-")[1]))
    return f"run-{n + 1:04d}"


def start_backfill(lo: int, hi: int) -> dict:
    """发起区间回补。计算在后台线程进行，状态机推进全部落盘可观测。"""
    ensure_dirs()
    m = manifest()
    if not m:
        raise RuntimeError("封存区没有原始包，请先生成数据")
    max_pkg = max(int(k.split("-")[1]) for k in m)
    if lo < 1 or hi > max_pkg or lo > hi:
        raise RuntimeError(f"区间 [{lo},{hi}] 非法：可用包 1..{max_pkg}")
    with _LOCK:
        runs = _load_runs()
        run = {
            "id": _next_run_id(runs), "range": [lo, hi], "state": "PREPARE",
            "created_at": now(), "events": [], "blocked": None, "recovered": None,
            "v_start": None, "barrier": None, "result_id": None, "scratch": None,
        }
        runs[run["id"]] = run
        _save_runs(runs)
    audit("BACKFILL_STARTED", {"run": run["id"], "range": [lo, hi]})
    t = threading.Thread(target=_run_backfill, args=(run["id"],), daemon=True)
    t.start()
    return _get_run(run["id"])


def _block_run(run: dict, phase: str, reason: str) -> None:
    run["state"] = f"BLOCKED_{phase}"
    run["blocked"] = {"phase": phase, "reason": reason, "at": now(),
                      "recovery": "删除登记服务恢复在线后点击“恢复”"}
    _run_event(run, f"阻断于 {phase}：{reason}")
    _put_run(run)
    audit("BACKFILL_BLOCKED", {"run": run["id"], "phase": phase, "reason": reason})


def _run_backfill(run_id: str) -> None:
    run = _get_run(run_id)
    lo, hi = run["range"]
    # ---- PREPARE：拿名单快照。拿不到就 fail-closed，不用过期快照冒险。
    try:
        snap = client().deletions_since(0)
    except RegistryUnavailable as e:
        _block_run(run, "PREPARE", f"删除登记服务不可达：{e}")
        return
    run["v_start"] = snap["version"]
    run["state"] = "COMPUTING"
    _run_event(run, f"名单快照 v{snap['version']}，开始读取封存包 [{lo},{hi}]")
    _put_run(run)
    # ---- COMPUTE：读封存包前先校验哈希。篡改不能用来掩盖处理问题。
    deleted = {d["record_id"] for d in snap["deletions"]}
    rows, sources = [], []
    try:
        for p in range(lo, hi + 1):
            pkg_id = f"pkg-{p:04d}"
            meta = _verify_package(pkg_id)
            data = read_json(RAW_DIR / f"{pkg_id}.json")
            for rec in data["records"]:
                if rec["record_id"] not in deleted:
                    rows.append(rec)
            sources.append({"package": pkg_id, "sha256": meta["sha256"], "verified": True})
    except IntegrityError as e:
        run["state"] = "FAILED_INTEGRITY"
        _run_event(run, f"完整性校验失败：{e}")
        _put_run(run)
        audit("INTEGRITY_FAILED", {"run": run_id, "detail": str(e)})
        return
    run["scratch"] = {"rows": rows, "sources": sources}
    run["state"] = "COMPUTED"
    _run_event(run, f"计算完成：{len(rows)} 条记录（已按 v{run['v_start']} 过滤）")
    _put_run(run)
    _finalize(run_id)


def _finalize(run_id: str) -> None:
    """FINALIZE：把“计算期间”到达的删除过滤掉，给结果盖上删除屏障版本。"""
    run = _get_run(run_id)
    scratch = run.get("scratch") or {}
    rows, sources = scratch.get("rows", []), scratch.get("sources", [])
    try:
        snap = client().deletions_since(run["v_start"])
    except RegistryUnavailable as e:
        _block_run(run, "FINALIZE", f"删除登记服务不可达：{e}")
        return
    late = {d["record_id"] for d in snap["deletions"]}
    dropped = sorted(r["record_id"] for r in rows if r["record_id"] in late)
    rows = [r for r in rows if r["record_id"] not in late]
    result_id = f"res-{run_id}"
    res = {
        "id": result_id, "run_id": run_id, "range": run["range"],
        "rows": rows, "barrier": snap["version"],
        "source_packages": sources,
        "dropped_at_finalize": dropped,
        "created_at": now(), "blocked": None, "recovered": None,
    }
    _save_pending(res)
    run["barrier"] = snap["version"]
    run["result_id"] = result_id
    run["scratch"] = None
    run["state"] = "PENDING"
    _run_event(run, f"定稿：屏障 v{snap['version']}，计算期后删除 {len(dropped)} 条已过滤，待发布")
    _put_run(run)
    audit("BACKFILL_FINALIZED", {"run": run_id, "result": result_id,
                                 "barrier": snap["version"], "rows": len(rows),
                                 "dropped_at_finalize": dropped})


def resume_run(run_id: str) -> dict:
    """恢复被阻断的回补运行，并把恢复依据写进运行记录与审计链。"""
    run = _get_run(run_id)
    if not run:
        raise RuntimeError(f"运行 {run_id} 不存在")
    state = run["state"]
    if state == "BLOCKED_PREPARE":
        run["recovered"] = {"at": now(), "basis": "人工触发恢复：重新获取名单快照并重启计算"}
        run["blocked"] = None
        run["state"] = "PREPARE"
        _put_run(run)
        threading.Thread(target=_run_backfill, args=(run_id,), daemon=True).start()
    elif state == "BLOCKED_FINALIZE":
        try:
            snap = client().deletions_since(run["v_start"])
        except RegistryUnavailable as e:
            raise RegistryUnavailable(f"登记服务仍不可达，无法恢复：{e}")
        run["recovered"] = {"at": now(),
                            "basis": f"登记服务恢复在线，当前名单 v{snap['version']}；据此续跑定稿"}
        run["blocked"] = None
        run["state"] = "COMPUTED"
        _put_run(run)
        audit("BACKFILL_RESUMED", {"run": run_id, "basis": run["recovered"]["basis"]})
        _finalize(run_id)
    else:
        raise RuntimeError(f"运行状态 {state} 不需要恢复")
    return _get_run(run_id)


# ------------------------------------------------------------ 发布（删除屏障）

def _aggregates(rows: list[dict]) -> dict:
    agg: dict[str, dict] = {}
    for r in rows:
        a = agg.setdefault(r["station"], {"events": 0, "parcels": 0, "weight_kg": 0.0})
        a["events"] += 1
        a["parcels"] += r["parcels"]
        a["weight_kg"] = round(a["weight_kg"] + r["weight_kg"], 2)
    return dict(sorted(agg.items()))


def _next_version(idx: dict, result_id: str) -> int:
    entry = idx.get(result_id)
    return (entry["version"] + 1) if entry else 1


def _write_published(res: dict, note: str, dropped: list[str]) -> dict:
    idx = _load_index()
    version = _next_version(idx, res["id"])
    pub = {
        "result_id": res["id"], "version": version, "range": res["range"],
        "barrier": res["barrier"], "rows": res["rows"],
        "aggregates": _aggregates(res["rows"]),
        "lineage": sorted(r["record_id"] for r in res["rows"]),
        "source_packages": res["source_packages"],
        "published_at": now(), "publish_note": note,
        "dropped_at_barrier": dropped,
    }
    fname = f"{res['id']}@v{version}.json"
    write_json_atomic(PUBLISHED_DIR / fname, pub)
    idx[res["id"]] = {"version": version, "file": fname, "published_at": pub["published_at"],
                      "barrier": res["barrier"], "records": len(res["rows"]), "retracted": False}
    _save_index(idx)
    audit("PUBLISHED", {"result": res["id"], "version": version, "barrier": res["barrier"],
                        "records": len(res["rows"]), "note": note, "dropped_at_barrier": dropped})
    return pub


def publish_pending(result_id: str) -> dict:
    """发布屏障：发布前必须与登记服务对齐版本。不可达 → fail-closed 阻断。"""
    res = _load_pending(result_id)
    if not res:
        raise RuntimeError(f"待发布结果 {result_id} 不存在")
    with _PUBLISH_LOCK:
        try:
            snap = client().deletions_since(res["barrier"])
        except RegistryUnavailable as e:
            res["blocked"] = {"phase": "PUBLISH", "at": now(),
                              "reason": f"删除登记服务不可达，无法证明结果干净：{e}",
                              "recovery": "登记服务恢复后重试发布"}
            _save_pending(res)
            audit("PUBLISH_BLOCKED", {"result": result_id, "reason": res["blocked"]["reason"]})
            return {"ok": False, "blocked": True, "reason": res["blocked"]["reason"]}
        v_now = snap["version"]
        lineage = {r["record_id"] for r in res["rows"]}
        late = [d for d in snap["deletions"] if d["record_id"] in lineage]
        dropped = sorted(d["record_id"] for d in late)
        note = "barrier_ok" if v_now == res["barrier"] else "verified_clean"
        if dropped:
            gone = {d["record_id"] for d in late}
            res["rows"] = [r for r in res["rows"] if r["record_id"] not in gone]
            note = "late_deletions_filtered"
            audit("PUBLISH_BARRIER_FILTER", {"result": result_id, "dropped": dropped,
                                             "from_barrier": res["barrier"], "to_barrier": v_now})
        if res.get("blocked"):
            res["recovered"] = {"at": now(),
                                "basis": f"登记服务恢复，重读名单 v{v_now}（原屏障 v{res['barrier']}），"
                                         f"增量删除 {len(snap['deletions'])} 条"
                                         f"（命中本结果 {len(dropped)} 条）后放行"}
            audit("PUBLISH_RECOVERED", {"result": result_id, "basis": res["recovered"]["basis"]})
        res["barrier"] = v_now
        res["blocked"] = None
        pub = _write_published(res, note, dropped)
        _pending_path(result_id).unlink(missing_ok=True)
        run = _get_run(res["run_id"])
        if run:
            run["state"] = "PUBLISHED"
            run["barrier"] = v_now
            _run_event(run, f"已发布 v{pub['version']}（屏障 v{v_now}，{note}）")
            _put_run(run)
        return {"ok": True, "blocked": False, "version": pub["version"], "note": note,
                "dropped_at_barrier": dropped}


def publish_all() -> list[dict]:
    out = []
    for f in sorted(PENDING_DIR.glob("res-*.json")):
        out.append({"result": f.stem, **publish_pending(f.stem)})
    return out


# ------------------------------------------------------------ 删除登记 + 修订扫描

def _withdraw(result_id: str, version: int, reason: str) -> None:
    """把可分发版本单向迁入封存区留档。撤回不是删除：留档供审计。"""
    src = PUBLISHED_DIR / f"{result_id}@v{version}.json"
    if not src.exists():
        return
    digest = sha256_file(src)
    dst = WITHDRAWN_DIR / src.name
    os.replace(src, dst)
    seal_file(dst)
    audit("VERSION_WITHDRAWN", {"result": result_id, "version": version,
                                "sha256": digest, "reason": reason})


def _revision_sweep() -> list[dict]:
    """对已发布结果按完整名单重过滤。与发布临界区互斥（调用方持锁语义见 docstring）。"""
    revisions = []
    snap = client().deletions_since(0)  # 调用本函数前登记服务刚应答过，仍包一层防御
    deleted = {d["record_id"] for d in snap["deletions"]}
    idx = _load_index()
    for result_id, entry in list(idx.items()):
        if entry.get("retracted"):
            continue
        pub = read_json(PUBLISHED_DIR / entry["file"], None)
        if not pub:
            continue
        hit = sorted(rid for rid in pub["lineage"] if rid in deleted)
        if not hit:
            continue
        rows = [r for r in pub["rows"] if r["record_id"] not in deleted]
        _withdraw(result_id, entry["version"], f"删除修订：{', '.join(hit)}")
        if rows:
            res = {"id": result_id, "range": pub["range"], "rows": rows,
                   "barrier": snap["version"], "source_packages": pub["source_packages"]}
            new_pub = _write_published(res, "revision_after_deletion", hit)
            revisions.append({"result": result_id, "dropped": hit,
                              "from_version": entry["version"], "to_version": new_pub["version"]})
            audit("REVISION_CREATED", {"result": result_id, "dropped": hit,
                                       "from_version": entry["version"],
                                       "to_version": new_pub["version"],
                                       "barrier": snap["version"]})
        else:
            idx = _load_index()
            idx[result_id] = {**entry, "retracted": True, "retracted_at": now()}
            _save_index(idx)
            revisions.append({"result": result_id, "dropped": hit,
                              "from_version": entry["version"], "to_version": None})
            audit("RESULT_RETRACTED", {"result": result_id, "dropped": hit,
                                       "reason": "全部记录均已删除"})
    return revisions


def register_deletion(record_id: str) -> dict:
    """登记删除并立即修订可分发区。登记服务离线 → 删除不被受理（不静默丢失）。

    删除登记成功但修订扫描失败（服务在两者之间宕掉）时，扫描延期，
    由恢复流程（sweep_after_recovery）补扫——已登记删除绝不丢。
    """
    entry = client().add_deletion(record_id)  # 离线时抛 RegistryUnavailable
    audit("DELETION_REGISTERED", entry)
    try:
        with _PUBLISH_LOCK:
            revisions = _revision_sweep()
    except RegistryUnavailable as e:
        audit("SWEEP_DEFERRED", {"record_id": record_id, "reason": str(e)})
        return {"entry": entry, "revisions": [], "sweep": "deferred"}
    return {"entry": entry, "revisions": revisions, "sweep": "done"}


def sweep_after_recovery() -> list[dict]:
    """登记服务从离线恢复后调用：补扫可分发区，兜住离线期间延期的修订。"""
    with _PUBLISH_LOCK:
        revisions = _revision_sweep()
    audit("RECOVERY_SWEEP", {"revisions": revisions})
    return revisions


# ------------------------------------------------------------ 状态快照（供 UI）

def snapshot() -> dict:
    ensure_dirs()
    try:
        st = client().status()
        reg = {"reachable": True, **st}
        # 离线时绝不展示本地缓存冒充权威名单
        deletions = None if st.get("offline") else client().deletions_since(0)["deletions"]
    except RegistryUnavailable as e:
        reg = {"reachable": False, "error": str(e)}
        deletions = None
    m = manifest()
    published = []
    idx = _load_index()
    for result_id, entry in sorted(idx.items()):
        item = {"result_id": result_id, **entry}
        if not entry.get("retracted"):
            pub = read_json(PUBLISHED_DIR / entry["file"], {}) or {}
            item["aggregates"] = pub.get("aggregates", {})
            item["publish_note"] = pub.get("publish_note")
            item["dropped_at_barrier"] = pub.get("dropped_at_barrier", [])
        published.append(item)
    pendings = []
    for f in sorted(PENDING_DIR.glob("res-*.json")):
        res = read_json(f, {}) or {}
        pendings.append({
            "id": res["id"], "run_id": res["run_id"], "range": res["range"],
            "rows": len(res.get("rows", [])), "barrier": res.get("barrier"),
            "created_at": res.get("created_at"),
            "blocked": res.get("blocked"), "recovered": res.get("recovered"),
            "dropped_at_finalize": res.get("dropped_at_finalize", []),
        })
    withdrawn = []
    for f in sorted(WITHDRAWN_DIR.glob("*.json")):
        withdrawn.append({"file": f.name, "sha256": sha256_file(f)[:16] + "…"})
    return {
        "registry": reg,
        "deletions": deletions,
        "packages": [{"package": k, **v} for k, v in sorted(m.items())],
        "runs": sorted(_load_runs().values(), key=lambda r: r["id"]),
        "pendings": pendings,
        "published": published,
        "withdrawn": withdrawn,
        "audit": common.audit_tail(80),
    }
