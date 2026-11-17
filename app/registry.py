"""删除登记服务（Deletion Registry）。

唯一权威的删除名单来源。回补进程与发布端都通过 HTTP 向它查询，
任何一方拿不到明确答复都必须 fail-closed（阻断），不得自行放行。

接口：
  GET  /v1/status            控制面：永远应答，报告 online/offline
  GET  /v1/version           数据面：当前名单版本（单调递增）
  GET  /v1/deletions?since=N 数据面：版本 N 之后登记的删除
  POST /v1/deletions         数据面：登记删除 {"record_id": "..."}（幂等）
  POST /v1/admin/offline     控制面：模拟服务离线（数据面全部 503）
  POST /v1/admin/online      控制面：恢复在线

“离线”用 503 模拟进程崩溃/网络分区：数据面请求一律失败，
控制面（status/admin）保持可达以便演示恢复。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from . import common


class RegistryCore:
    """登记状态：版本号 + 墓碑表。持久化到 state/registry.json。"""

    def __init__(self):
        self._lock = threading.Lock()
        st = common.read_json(common.REGISTRY_MIRROR, None) or {}
        self.version: int = st.get("version", 0)
        self.tombstones: dict[str, dict] = st.get("tombstones", {})
        self.offline: bool = st.get("offline", False)

    def _persist(self):
        common.write_json_atomic(common.REGISTRY_MIRROR, {
            "version": self.version,
            "tombstones": self.tombstones,
            "offline": self.offline,
        })

    def add(self, record_id: str) -> dict:
        """幂等：同一记录重复删除返回首次登记的版本，不重复推进版本号。"""
        with self._lock:
            if record_id in self.tombstones:
                e = self.tombstones[record_id]
                return {"record_id": record_id, "version": e["version"], "duplicate": True}
            self.version += 1
            entry = {"record_id": record_id, "version": self.version, "registered_at": common.now()}
            self.tombstones[record_id] = entry
            self._persist()
            return {**entry, "duplicate": False}

    def deletions_since(self, since: int) -> dict:
        with self._lock:
            items = [e for e in self.tombstones.values() if e["version"] > since]
            items.sort(key=lambda e: e["version"])
            return {"version": self.version, "deletions": items}

    def current_version(self) -> int:
        with self._lock:
            return self.version

    def set_offline(self, flag: bool):
        with self._lock:
            self.offline = flag
            self._persist()

    def is_offline(self) -> bool:
        with self._lock:
            return self.offline


CORE = RegistryCore()


def _json_response(h: BaseHTTPRequestHandler, code: int, obj: dict):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    h.send_response(code)
    h.send_header("Content-Type", "application/json; charset=utf-8")
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    server_version = "DeletionRegistry/1.0"

    def log_message(self, *args):  # 静默
        pass

    def _offline_guard(self) -> bool:
        """数据面守卫：离线时一律 503，调用方必须视为“无法证明安全”。"""
        if CORE.is_offline():
            _json_response(self, 503, {
                "error": "registry_offline",
                "detail": "删除登记服务离线：无法证明任何结果不含已删除记录",
            })
            return True
        return False

    def do_GET(self):
        path = urlparse(self.path).path
        qs = parse_qs(urlparse(self.path).query)
        if path == "/v1/status":
            _json_response(self, 200, {
                "offline": CORE.is_offline(),
                "version": CORE.current_version(),
                "tombstones": len(CORE.tombstones),
            })
            return
        if self._offline_guard():
            return
        if path == "/v1/version":
            _json_response(self, 200, {"version": CORE.current_version()})
        elif path == "/v1/deletions":
            since = int(qs.get("since", ["0"])[0])
            _json_response(self, 200, CORE.deletions_since(since))
        else:
            _json_response(self, 404, {"error": "not_found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/v1/admin/offline":
            CORE.set_offline(True)
            _json_response(self, 200, {"offline": True})
            return
        if path == "/v1/admin/online":
            CORE.set_offline(False)
            _json_response(self, 200, {"offline": False})
            return
        if self._offline_guard():
            return
        if path == "/v1/deletions":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
            except (ValueError, json.JSONDecodeError):
                _json_response(self, 400, {"error": "bad_request"})
                return
            rid = (body.get("record_id") or "").strip()
            if not rid:
                _json_response(self, 400, {"error": "record_id_required"})
                return
            _json_response(self, 200, CORE.add(rid))
        else:
            _json_response(self, 404, {"error": "not_found"})


def make_server(port: int) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    return srv


def serve(port: int):
    srv = make_server(port)
    print(f"[registry] 删除登记服务监听 127.0.0.1:{port}")
    srv.serve_forever()
