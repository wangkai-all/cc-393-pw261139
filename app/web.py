"""Web 层：操作页面 + 编排 API（端口 8600）。

页面能力（对应任务要求）：
  · 提交删除请求          —— 登记服务离线时明确“未受理”，不静默丢失
  · 发起区间回补          —— 指定原始包区间，后台状态机推进
  · 让删除服务短暂离线     —— 一键离线/上线，演示 fail-closed 与恢复
  · 查看被阻断的结果及恢复依据 —— 阻断原因、恢复条件、恢复时的版本比对全部可见
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import pipeline
from .pipeline import RegistryUnavailable

PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>回补 / 删除屏障 控制台</title>
<style>
  :root { --ok:#1a7f37; --bad:#cf222e; --warn:#9a6700; --line:#d0d7de; --bg:#f6f8fa; }
  * { box-sizing: border-box; }
  body { font: 14px/1.5 -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 0; background: var(--bg); color: #1f2328; }
  header { background: #0a3069; color: #fff; padding: 10px 20px; display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
  header h1 { font-size: 16px; margin: 0; }
  .badge { padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
  .b-ok { background: var(--ok); color: #fff; } .b-bad { background: var(--bad); color: #fff; }
  .b-warn { background: #bf8700; color: #fff; } .b-grey { background: #6e7781; color: #fff; }
  main { padding: 16px 20px; display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  section { background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }
  section.wide { grid-column: 1 / -1; }
  h2 { font-size: 14px; margin: 0 0 8px; display: flex; align-items: center; gap: 8px; }
  h2 .tag { font-size: 11px; color: #57606a; font-weight: 400; }
  table { border-collapse: collapse; width: 100%; font-size: 12.5px; }
  th, td { border: 1px solid var(--line); padding: 4px 8px; text-align: left; vertical-align: top; }
  th { background: var(--bg); }
  button { font: inherit; font-size: 12.5px; padding: 3px 10px; border: 1px solid var(--line);
           border-radius: 6px; background: #fff; cursor: pointer; }
  button:hover { background: var(--bg); }
  button.primary { background: #1f6feb; border-color: #1f6feb; color: #fff; }
  button.danger { background: #cf222e; border-color: #cf222e; color: #fff; }
  input { font: inherit; font-size: 12.5px; padding: 3px 8px; border: 1px solid var(--line); border-radius: 6px; }
  .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin: 6px 0; }
  .mono { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 12px; }
  .muted { color: #57606a; } .ok { color: var(--ok); } .bad { color: var(--bad); } .warn { color: var(--warn); }
  .block-note { background: #fff8e6; border: 1px solid #eac54f; border-radius: 6px; padding: 4px 8px; margin-top: 4px; font-size: 12px; }
  .recover-note { background: #ddf4e4; border: 1px solid #4ac26b; border-radius: 6px; padding: 4px 8px; margin-top: 4px; font-size: 12px; }
  .sealed-zone { border-left: 4px solid #6e7781; }
  .dist-zone { border-left: 4px solid #1f6feb; }
  #audit { max-height: 260px; overflow: auto; }
  #toast { position: fixed; right: 16px; bottom: 16px; max-width: 420px; }
  .toast { background: #1f2328; color: #fff; padding: 8px 14px; border-radius: 8px; margin-top: 8px; font-size: 13px; }
  .toast.err { background: var(--bad); }
  #verify-result { white-space: pre-wrap; font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1>区间回补 · 删除屏障 控制台</h1>
  <span id="reg-badge" class="badge b-grey">登记服务 …</span>
  <span id="reg-ver" class="mono" style="color:#cdd9e5"></span>
  <button class="danger" onclick="registryToggle(true)">让删除服务离线</button>
  <button class="primary" onclick="registryToggle(false)">恢复上线</button>
  <span class="muted" style="color:#9db4d0">离线期间：删除不受理、回补/发布 fail-closed 阻断</span>
</header>
<main>
  <section>
    <h2>操作 <span class="tag">删除登记 / 区间回补 / 数据生成</span></h2>
    <div class="row">
      <b>提交删除</b><input id="del-rid" placeholder="记录号 如 R0002-03" size="16">
      <button class="primary" onclick="submitDeletion()">登记删除</button>
      <span class="muted">登记成功即触发可分发区修订扫描</span>
    </div>
    <div class="row">
      <b>区间回补</b><input id="bf-lo" type="number" value="1" min="1" style="width:64px"> —
      <input id="bf-hi" type="number" value="3" min="1" style="width:64px">
      <button class="primary" onclick="startBackfill()">发起回补</button>
      <span class="muted">计算 → 定稿（盖屏障）→ 待发布</span>
    </div>
    <div class="row">
      <b>数据</b><button onclick="genData()">生成封存原始包（6 包）</button>
      <button onclick="verify()">校验封存完整性</button>
      <button onclick="publishAll()">发布全部待发布</button>
    </div>
    <div id="verify-result"></div>
  </section>

  <section>
    <h2>删除名单 <span class="tag">来自登记服务（权威源）</span></h2>
    <div id="deletions"></div>
  </section>

  <section class="wide">
    <h2>回补运行 <span class="tag">状态机：PREPARE → COMPUTE → FINALIZE → PENDING → PUBLISHED</span></h2>
    <div id="runs"></div>
  </section>

  <section class="dist-zone">
    <h2>可分发区 · 待发布 <span class="tag">发布前必须通过删除屏障</span></h2>
    <div id="pendings"></div>
  </section>

  <section class="dist-zone">
    <h2>可分发区 · 已发布 <span class="tag">当前生效版本</span></h2>
    <div id="published"></div>
  </section>

  <section class="sealed-zone">
    <h2>封存区 · 原始包 <span class="tag">只增不改，读取前校验哈希</span></h2>
    <div id="packages"></div>
  </section>

  <section class="sealed-zone">
    <h2>封存区 · 撤回版本留档 <span class="tag">从可分发区单向迁入</span></h2>
    <div id="withdrawn"></div>
  </section>

  <section class="wide sealed-zone">
    <h2>封存区 · 审计链 <span class="tag">哈希链，只增不改</span></h2>
    <div id="audit"></div>
  </section>
</main>
<div id="toast"></div>
<script>
function toast(msg, err) {
  const d = document.createElement('div');
  d.className = 'toast' + (err ? ' err' : '');
  d.textContent = msg;
  document.getElementById('toast').appendChild(d);
  setTimeout(() => d.remove(), 6000);
}
async function api(path, body) {
  const r = await fetch(path, {method: 'POST', headers: {'Content-Type': 'application/json'},
                               body: JSON.stringify(body || {})});
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
  return j;
}
const esc = s => String(s ?? '').replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const STATE_CN = {PREPARE:'准备', COMPUTING:'计算中', COMPUTED:'已计算', BLOCKED_PREPARE:'阻断·准备',
  BLOCKED_FINALIZE:'阻断·定稿', PENDING:'待发布', PUBLISHED:'已发布', FAILED_INTEGRITY:'完整性失败'};
function stateBadge(s) {
  const cls = s.startsWith('BLOCKED') || s.startsWith('FAILED') ? 'b-bad'
            : s === 'PUBLISHED' ? 'b-ok' : s === 'PENDING' ? 'b-warn' : 'b-grey';
  return `<span class="badge ${cls}">${STATE_CN[s] || s}</span>`;
}
function blockedHtml(b) {
  return b ? `<div class="block-note">⛔ <b>阻断</b> ${esc(b.at)}<br>原因：${esc(b.reason)}<br>恢复条件：${esc(b.recovery)}</div>` : '';
}
function recoveredHtml(r) {
  return r ? `<div class="recover-note">✅ <b>恢复</b> ${esc(r.at)}<br>依据：${esc(r.basis)}</div>` : '';
}

async function refresh() {
  let s;
  try { s = await (await fetch('/api/state')).json(); }
  catch (e) { return; }
  const reg = s.registry;
  const badge = document.getElementById('reg-badge');
  if (reg.reachable && !reg.offline) { badge.className = 'badge b-ok'; badge.textContent = '登记服务在线'; }
  else if (reg.reachable && reg.offline) { badge.className = 'badge b-bad'; badge.textContent = '登记服务离线（模拟故障）'; }
  else { badge.className = 'badge b-bad'; badge.textContent = '登记服务不可达'; }
  document.getElementById('reg-ver').textContent = reg.reachable ? `名单版本 v${reg.version} · 墓碑 ${reg.tombstones}` : '';

  document.getElementById('deletions').innerHTML = s.deletions === null
    ? '<p class="bad">登记服务离线：权威名单不可读。页面绝不展示本地缓存冒充名单。</p>'
    : (s.deletions.length ? '<table><tr><th>记录号</th><th>名单版本</th><th>登记时间</th></tr>' +
        s.deletions.map(d => `<tr><td class="mono">${esc(d.record_id)}</td><td>v${d.version}</td><td>${esc(d.registered_at)}</td></tr>`).join('') + '</table>'
        : '<p class="muted">暂无删除登记</p>');

  document.getElementById('runs').innerHTML = s.runs.length ? '<table><tr><th>运行</th><th>区间</th><th>状态</th><th>屏障</th><th>时间线</th><th>操作</th></tr>' +
    s.runs.map(r => `<tr><td class="mono">${r.id}</td><td>pkg ${r.range[0]}–${r.range[1]}</td>
      <td>${stateBadge(r.state)}${blockedHtml(r.blocked)}${recoveredHtml(r.recovered)}</td>
      <td>${r.barrier === null ? '—' : 'v' + r.barrier}</td>
      <td class="mono">${(r.events || []).map(e => `${esc(e.ts.slice(11))} ${esc(e.msg)}`).join('<br>')}</td>
      <td>${r.state.startsWith('BLOCKED_') ? `<button class="primary" onclick="resumeRun('${r.id}')">恢复</button>` : ''}</td></tr>`).join('') + '</table>'
    : '<p class="muted">尚无回补运行</p>';

  document.getElementById('pendings').innerHTML = s.pendings.length ? '<table><tr><th>结果</th><th>区间</th><th>记录数</th><th>屏障</th><th>状态</th><th>操作</th></tr>' +
    s.pendings.map(p => `<tr><td class="mono">${p.id}</td><td>pkg ${p.range[0]}–${p.range[1]}</td><td>${p.rows}</td><td>v${p.barrier}</td>
      <td>${blockedHtml(p.blocked)}${recoveredHtml(p.recovered)}
          ${p.dropped_at_finalize.length ? `<span class="warn">定稿时过滤 ${p.dropped_at_finalize.length} 条</span>` : ''}</td>
      <td><button class="primary" onclick="publishOne('${p.id}')">发布</button></td></tr>`).join('') + '</table>'
    : '<p class="muted">无待发布结果</p>';

  document.getElementById('published').innerHTML = s.published.length ? '<table><tr><th>结果</th><th>版本</th><th>屏障</th><th>记录数</th><th>聚合（站点 → 事件/件数/重量）</th><th>说明</th></tr>' +
    s.published.map(p => p.retracted
      ? `<tr><td class="mono">${p.result_id}</td><td colspan="5" class="bad">已全部撤回（所有记录被删除），留档于封存区</td></tr>`
      : `<tr><td class="mono">${p.result_id}</td><td>v${p.version}</td><td>v${p.barrier}</td><td>${p.records}</td>
        <td class="mono">${Object.entries(p.aggregates || {}).map(([k, a]) => `${esc(k)}: ${a.events}/${a.parcels}/${a.weight_kg}kg`).join('<br>')}</td>
        <td>${esc(p.publish_note || '')}${(p.dropped_at_barrier || []).length ? `<br><span class="warn">屏障过滤: ${p.dropped_at_barrier.join(', ')}</span>` : ''}</td></tr>`).join('') + '</table>'
    : '<p class="muted">尚无已发布结果</p>';

  document.getElementById('packages').innerHTML = s.packages.length ? '<table><tr><th>包</th><th>记录数</th><th>sha256</th><th>封存时间</th></tr>' +
    s.packages.map(p => `<tr><td class="mono">${p.package}</td><td>${p.records}</td>
      <td class="mono">${p.sha256.slice(0, 20)}…</td><td>${esc(p.sealed_at)}</td></tr>`).join('') + '</table>'
    : '<p class="muted">尚未生成原始包</p>';

  document.getElementById('withdrawn').innerHTML = s.withdrawn.length ? '<table><tr><th>留档文件</th><th>sha256</th></tr>' +
    s.withdrawn.map(w => `<tr><td class="mono">${esc(w.file)}</td><td class="mono">${esc(w.sha256)}</td></tr>`).join('') + '</table>'
    : '<p class="muted">无撤回版本</p>';

  document.getElementById('audit').innerHTML = '<table><tr><th>时间</th><th>事件</th><th>内容</th><th>链哈希</th></tr>' +
    s.audit.slice().reverse().map(e => `<tr><td class="mono">${esc(e.ts || '')}</td><td class="mono">${esc(e.type)}</td>
      <td class="mono">${esc(JSON.stringify(e.payload || e.raw || ''))}</td>
      <td class="mono">${e.hash ? e.hash.slice(0, 12) + '…' : '—'}</td></tr>`).join('') + '</table>';
}

async function submitDeletion() {
  const rid = document.getElementById('del-rid').value.trim();
  if (!rid) return toast('请输入记录号', true);
  try {
    const j = await api('/api/deletions', {record_id: rid});
    const rev = j.revisions.length ? `；修订 ${j.revisions.length} 个已发布结果` : '；无已发布结果受影响';
    toast(`删除已登记：${rid} @ v${j.entry.version}${j.entry.duplicate ? '（重复，幂等）' : ''}${rev}`);
  } catch (e) { toast('删除未受理：' + e.message, true); }
  refresh();
}
async function startBackfill() {
  try {
    const j = await api('/api/backfill', {lo: +document.getElementById('bf-lo').value, hi: +document.getElementById('bf-hi').value});
    toast(`回补 ${j.id} 已发起`);
  } catch (e) { toast('发起失败：' + e.message, true); }
  refresh();
}
async function resumeRun(id) {
  try { await api('/api/runs/' + id + '/resume', {}); toast(`${id} 已恢复`); }
  catch (e) { toast('恢复失败：' + e.message, true); }
  refresh();
}
async function publishOne(id) {
  try {
    const j = await api('/api/publish', {result_id: id});
    toast(j.ok ? `已发布 v${j.version}（${j.note}）` : `发布被阻断：${j.reason}`, !j.ok);
  } catch (e) { toast('发布失败：' + e.message, true); }
  refresh();
}
async function publishAll() {
  try {
    const j = await api('/api/publish-all', {});
    const blocked = j.results.filter(r => r.blocked);
    toast(blocked.length ? `${blocked.length} 个结果被阻断（fail-closed）` : `已发布 ${j.results.length} 个结果`, blocked.length > 0);
  } catch (e) { toast(e.message, true); }
  refresh();
}
async function registryToggle(offline) {
  try { await api('/api/registry/' + (offline ? 'offline' : 'online'), {}); toast(offline ? '登记服务已离线（模拟故障）' : '登记服务已恢复'); }
  catch (e) { toast(e.message, true); }
  refresh();
}
async function genData() {
  try { const j = await api('/api/gen-data', {}); toast(`已封存 ${j.packages} 包 / ${j.records} 条`); }
  catch (e) { toast(e.message, true); }
  refresh();
}
async function verify() {
  try {
    const j = await api('/api/verify', {});
    document.getElementById('verify-result').innerHTML =
      `<div class="${j.ok ? 'ok' : 'bad'}"><b>${j.ok ? '✅ 封存完整' : '⛔ 发现篡改/损坏'}</b>（${j.verified_at}）</div>` +
      j.checks.map(c => `<div class="${c.ok ? 'ok' : 'bad'}">${c.ok ? '✓' : '✗'} ${esc(c.item)}：${esc(c.detail)}</div>`).join('');
  } catch (e) { toast(e.message, true); }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "BackfillConsole/1.0"

    def log_message(self, *args):
        pass

    def _json(self, code: int, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            return {}

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/state":
            self._json(200, pipeline.snapshot())
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._body()
        try:
            if path == "/api/gen-data":
                self._json(200, pipeline.generate_packages())
            elif path == "/api/deletions":
                rid = (body.get("record_id") or "").strip()
                if not rid:
                    return self._json(400, {"error": "缺少 record_id"})
                self._json(200, pipeline.register_deletion(rid))
            elif path == "/api/backfill":
                self._json(200, pipeline.start_backfill(int(body.get("lo", 0)), int(body.get("hi", 0))))
            elif path.startswith("/api/runs/") and path.endswith("/resume"):
                self._json(200, pipeline.resume_run(path.split("/")[3]))
            elif path == "/api/publish":
                self._json(200, pipeline.publish_pending(body.get("result_id", "")))
            elif path == "/api/publish-all":
                self._json(200, {"results": pipeline.publish_all()})
            elif path == "/api/registry/offline":
                self._json(200, pipeline.client().set_offline(True))
            elif path == "/api/registry/online":
                resp = pipeline.client().set_offline(False)
                revisions = pipeline.sweep_after_recovery()  # 恢复后补扫，作为恢复依据的一部分
                self._json(200, {**resp, "recovery_sweep": revisions})
            elif path == "/api/verify":
                self._json(200, pipeline.verify_all())
            else:
                self._json(404, {"error": "not_found"})
        except RegistryUnavailable as e:
            self._json(503, {"error": str(e)})
        except (RuntimeError, ValueError) as e:
            self._json(400, {"error": str(e)})


def serve(port: int):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    print(f"[web] 控制台 http://127.0.0.1:{port}")
    srv.serve_forever()
