#!/usr/bin/env python3
"""端到端场景测试：删除登记服务 / 回补进程 / 发布端 三方协调。

覆盖：
  1. 正常回补 + 发布（屏障对齐）
  2. 删除命中已发布结果 → 修订版 + 旧版撤回封存
  3. 删除发生在“计算完成与发布之间”→ 发布屏障过滤
  4. 登记服务离线 → 发布 fail-closed 阻断；恢复后带依据放行
  5. 离线期间删除不受理（不静默丢失）
  6. 回补在 PREPARE / FINALIZE 阶段遇离线 → 阻断 → 恢复续跑
  7. 删除幂等
  8. 篡改原始包 → 完整性失败且留痕，恢复原始包也抹不掉审计记录
  9. 审计链校验
运行：python3 tests/test_scenarios.py
"""
import os
import sys
import tempfile
import threading
import time

TMP = tempfile.mkdtemp(prefix="pipeline-test-")
os.environ["PIPELINE_DATA_DIR"] = TMP
os.environ["REGISTRY_PORT"] = "18601"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import common, pipeline, registry  # noqa: E402

PASS = 0


def ok(name, cond, detail=""):
    global PASS
    assert cond, f"FAIL: {name} {detail}"
    PASS += 1
    print(f"  ✓ {name}")


def wait_run(rid, prefixes, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = pipeline._get_run(rid)
        if any(run["state"].startswith(p) for p in prefixes):
            return run
        time.sleep(0.05)
    raise AssertionError(f"{rid} 未在超时内进入 {prefixes}，当前 {run['state']}")


def audit_types():
    return [e["type"] for e in common.audit_tail(10000)]


def main():
    srv = registry.make_server(18601)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            pipeline.client().status()
            break
        except pipeline.RegistryUnavailable:
            time.sleep(0.05)

    print("场景 0：生成封存原始包")
    info = pipeline.generate_packages(6, 12)
    ok("6 包 72 条", info["packages"] == 6 and info["records"] == 72)
    ok("完整性校验通过", pipeline.verify_all()["ok"])

    print("场景 1：回补 1–3 并发布（无删除，屏障对齐）")
    run = pipeline.start_backfill(1, 3)
    run = wait_run(run["id"], ["PENDING"])
    rid1 = run["result_id"]
    res = pipeline.publish_pending(rid1)
    ok("发布成功", res["ok"] and res["note"] == "barrier_ok", str(res))
    ok("屏障 v0", res["version"] == 1)
    idx = pipeline._load_index()
    ok("可分发索引在册", idx[rid1]["records"] == 36)

    print("场景 2：删除命中已发布结果 → 修订 + 撤回留档")
    pub = common.read_json(common.PUBLISHED_DIR / idx[rid1]["file"])
    victim = pub["lineage"][5]
    out = pipeline.register_deletion(victim)
    ok("登记版本 v1", out["entry"]["version"] == 1)
    ok("产生 1 个修订", len(out["revisions"]) == 1 and out["revisions"][0]["to_version"] == 2)
    idx = pipeline._load_index()
    ok("索引指向 v2", idx[rid1]["version"] == 2)
    pub2 = common.read_json(common.PUBLISHED_DIR / idx[rid1]["file"])
    ok("v2 已不含被删记录", victim not in pub2["lineage"])
    ok("旧版已迁入封存区", (common.WITHDRAWN_DIR / f"{rid1}@v1.json").exists())
    ok("可分发区无旧版", not (common.PUBLISHED_DIR / f"{rid1}@v1.json").exists())
    ok("审计含撤回与修订", {"VERSION_WITHDRAWN", "REVISION_CREATED"} <= set(audit_types()))

    print("场景 3：删除发生在计算完成与发布之间 → 发布屏障过滤")
    run = wait_run(pipeline.start_backfill(1, 6)["id"], ["PENDING"])
    rid2 = run["result_id"]
    pend = pipeline._load_pending(rid2)
    # 挑一条只在 4–6 号包（不在已发布的 1–3 区间内）的记录，确保扫描不波及已发布结果
    late_victim = next(r["record_id"] for r in pend["rows"] if r["record_id"] >= "R0004")
    out = pipeline.register_deletion(late_victim)  # 此时结果在待发布，扫描不动它
    ok("待发布不被扫描改动", out["revisions"] == [])
    res = pipeline.publish_pending(rid2)
    ok("屏障处过滤后发布", res["ok"] and res["note"] == "late_deletions_filtered", str(res))
    ok("被删记录在屏障丢弃清单", late_victim in res["dropped_at_barrier"])
    pub = common.read_json(common.PUBLISHED_DIR / pipeline._load_index()[rid2]["file"])
    ok("已发布结果不含该记录", late_victim not in pub["lineage"])
    ok("审计含 PUBLISH_BARRIER_FILTER", "PUBLISH_BARRIER_FILTER" in audit_types())

    print("场景 4：登记服务离线 → 发布 fail-closed；恢复后带依据放行")
    run = wait_run(pipeline.start_backfill(4, 6)["id"], ["PENDING"])
    rid3 = run["result_id"]
    pipeline.client().set_offline(True)
    res = pipeline.publish_pending(rid3)
    ok("发布被阻断", not res["ok"] and res["blocked"])
    pend = pipeline._load_pending(rid3)
    ok("阻断原因可见", "不可达" in pend["blocked"]["reason"] or "离线" in pend["blocked"]["reason"])
    ok("审计含 PUBLISH_BLOCKED", "PUBLISH_BLOCKED" in audit_types())
    ok("可分发区没有该结果", rid3 not in pipeline._load_index())
    pipeline.client().set_offline(False)
    pipeline.sweep_after_recovery()
    res = pipeline.publish_pending(rid3)
    ok("恢复后发布成功", res["ok"])
    run = pipeline._get_run(run["id"])
    ok("恢复依据已记录", run["state"] == "PUBLISHED")

    print("场景 5：离线期间删除不受理")
    pipeline.client().set_offline(True)
    before = pipeline.client().status()["version"]
    try:
        pipeline.register_deletion("R0005-05")
        raise AssertionError("离线时删除不应被受理")
    except pipeline.RegistryUnavailable:
        pass
    pipeline.client().set_offline(False)
    after = pipeline.client().status()["version"]
    ok("版本号未推进（删除未登记）", before == after)

    print("场景 6：回补在 PREPARE / FINALIZE 遇离线 → 阻断 → 恢复")
    pipeline.client().set_offline(True)
    run = pipeline.start_backfill(1, 2)
    run = wait_run(run["id"], ["BLOCKED_PREPARE"])
    ok("阻断于 PREPARE", run["blocked"]["phase"] == "PREPARE")
    pipeline.client().set_offline(False)
    pipeline.resume_run(run["id"])
    run = wait_run(run["id"], ["PENDING"])
    ok("恢复后定稿待发布", run["result_id"] is not None)
    # FINALIZE 阻断：手工构造一个已计算未定的运行
    runs = pipeline._load_runs()
    runs["run-9001"] = {
        "id": "run-9001", "range": [1, 1], "state": "COMPUTED", "v_start": 0,
        "scratch": {"rows": [{"record_id": "R0001-01", "station": "青崖站",
                              "route": "北环", "weight_kg": 1.0, "parcels": 1,
                              "event_ts": "2026-10-01T08:00:00"}],
                    "sources": [{"package": "pkg-0001", "sha256": "x", "verified": True}]},
        "events": [], "blocked": None, "recovered": None, "barrier": None,
        "result_id": None, "created_at": common.now(),
    }
    pipeline._save_runs(runs)
    pipeline.client().set_offline(True)
    pipeline._finalize("run-9001")
    ok("阻断于 FINALIZE", pipeline._get_run("run-9001")["state"] == "BLOCKED_FINALIZE")
    pipeline.client().set_offline(False)
    pipeline.resume_run("run-9001")
    run = pipeline._get_run("run-9001")
    ok("恢复续跑定稿", run["state"] == "PENDING" and run["recovered"] is not None)
    ok("恢复依据含版本比对", "名单 v" in run["recovered"]["basis"])

    print("场景 7：删除幂等")
    e1 = pipeline.register_deletion("R0006-06")["entry"]
    e2 = pipeline.register_deletion("R0006-06")["entry"]
    ok("重复删除同版本", e1["version"] == e2["version"] and e2["duplicate"])

    print("场景 8：篡改原始包 → 完整性失败且留痕")
    pkg_path = common.RAW_DIR / "pkg-0002.json"
    original = pkg_path.read_bytes()
    os.chmod(pkg_path, 0o644)
    pkg_path.write_bytes(original.replace(b'"parcels": 1', b'"parcels": 99', 1))
    os.chmod(pkg_path, 0o444)
    run = wait_run(pipeline.start_backfill(2, 2)["id"], ["FAILED_INTEGRITY"])
    ok("回补因篡改中止", "INTEGRITY_FAILED" in audit_types())
    report = pipeline.verify_all()
    ok("校验报告篡改", not report["ok"] and any("pkg-0002" == c["item"] and not c["ok"] for c in report["checks"]))
    os.chmod(pkg_path, 0o644)
    pkg_path.write_bytes(original)
    os.chmod(pkg_path, 0o444)
    ok("恢复后校验通过", pipeline.verify_all()["ok"])
    ok("篡改痕迹留在审计链（无法掩盖）", "INTEGRITY_FAILED" in audit_types())

    print("场景 9：审计链与 Web 冒烟")
    chain = common.verify_audit_chain()
    ok("审计链完整", chain["ok"], chain["detail"])
    from http.server import ThreadingHTTPServer
    from urllib.request import urlopen
    from app.web import Handler
    web_srv = ThreadingHTTPServer(("127.0.0.1", 18600), Handler)
    threading.Thread(target=web_srv.serve_forever, daemon=True).start()
    body = urlopen("http://127.0.0.1:18600/", timeout=5).read().decode()
    ok("页面可访问", "删除屏障" in body)
    state = __import__("json").loads(urlopen("http://127.0.0.1:18600/api/state", timeout=5).read())
    ok("状态接口正常", state["registry"]["reachable"] and len(state["published"]) >= 3)
    web_srv.shutdown()
    srv.shutdown()
    print(f"\n全部通过：{PASS} 项断言")


if __name__ == "__main__":
    main()
