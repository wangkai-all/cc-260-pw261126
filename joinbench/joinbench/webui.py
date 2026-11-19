"""单页监控台: 纯原生 JS 轮询 /api/status。展示工作进程、分片、积压、输出、方案对比与预算。"""
import json

PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>joinbench · 数据关联作业台</title>
<style>
  :root { --bg:#0f1420; --card:#182034; --line:#26304d; --fg:#dbe4f5; --dim:#8fa0c2;
          --ok:#3ecf8e; --run:#4aa3ff; --bad:#ff5d6c; --warn:#ffbe4a; --pend:#3a4767; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:13px/1.5 -apple-system,"Segoe UI",Roboto,"Noto Sans SC",sans-serif; }
  header { padding:12px 20px; border-bottom:1px solid var(--line); display:flex;
           align-items:baseline; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:16px; margin:0; }
  header .sub { color:var(--dim); }
  main { padding:16px 20px; display:grid; gap:16px; grid-template-columns: 1fr; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
  .card h2 { font-size:13px; margin:0 0 10px; color:var(--dim); text-transform:uppercase; letter-spacing:.06em; }
  table { border-collapse:collapse; width:100%; }
  th,td { text-align:left; padding:4px 8px; border-bottom:1px solid var(--line); white-space:nowrap; }
  th { color:var(--dim); font-weight:500; }
  .badge { display:inline-block; padding:1px 8px; border-radius:10px; font-size:11px; }
  .b-RUNNING { background:#12325c; color:var(--run); }
  .b-DONE,.b-alive { background:#0f3d2e; color:var(--ok); }
  .b-FAILED,.b-dead { background:#4a1620; color:var(--bad); }
  .b-CANCELLED { background:#3d2f10; color:var(--warn); }
  .b-PENDING { background:#232c46; color:var(--dim); }
  .grid { display:flex; flex-wrap:wrap; gap:3px; margin:6px 0; }
  .cell { width:16px; height:16px; border-radius:3px; background:var(--pend); position:relative; }
  .cell.RUNNING { background:var(--run); animation:pulse 1s infinite alternate; }
  .cell.DONE { background:var(--ok); }
  .cell.FAILED { background:var(--bad); }
  @keyframes pulse { from{opacity:.55} to{opacity:1} }
  .bar { height:8px; background:#232c46; border-radius:4px; overflow:hidden; min-width:80px; }
  .bar > i { display:block; height:100%; background:var(--run); }
  .bar.over > i { background:var(--bad); }
  .bars { display:flex; align-items:flex-end; gap:2px; height:56px; margin:6px 0; }
  .bars .col { flex:1; max-width:26px; display:flex; flex-direction:column-reverse; height:100%; }
  .bars .col i { display:block; width:100%; }
  .bars .L { background:var(--run); } .bars .S { background:var(--warn); }
  .muted { color:var(--dim); } .mono { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:12px; }
  .chosen { outline:2px solid var(--ok); }
  button { background:#223055; color:var(--fg); border:1px solid var(--line); border-radius:6px;
           padding:4px 10px; cursor:pointer; font-size:12px; }
  button:hover { background:#2c3d6b; }
  button.danger { background:#4a1620; }
  .row { display:flex; gap:18px; flex-wrap:wrap; align-items:flex-start; }
  .kv { margin-right:18px; } .kv b { color:var(--fg); } .kv span { color:var(--dim); }
  #log { max-height:220px; overflow:auto; font-family:ui-monospace,Menlo,Consolas,monospace;
         font-size:11.5px; white-space:pre-wrap; }
  .stage-head { display:flex; gap:10px; align-items:center; margin-top:8px; }
  .pill { font-size:11px; color:var(--dim); }
  details { margin-top:6px; } summary { cursor:pointer; color:var(--dim); }
  .out { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:11.5px; max-height:180px; overflow:auto;
         background:#0b101d; border-radius:6px; padding:8px; }
</style>
</head>
<body>
<header>
  <h1>joinbench · 数据关联作业台</h1>
  <span class="sub" id="cluster">连接中…</span>
  <span style="flex:1"></span>
  <span class="sub">每 1s 刷新</span>
</header>
<main>
  <div class="card"><h2>工作进程(独立进程 × 2, 预算硬约束)</h2><div id="workers"></div></div>
  <div class="card"><h2>作业 / 方案选择 / 分片 / 积压 / 输出</h2><div id="jobs"></div></div>
  <div class="card"><h2>故障注入与演示操作</h2>
    <div class="row" id="faults">
      <span class="pill">故障点:</span>
      <button onclick="armFault('after_shuffle_write')">下次洗牌写完未登记时崩溃</button>
      <button onclick="armFault('before_output_commit')">下次输出写完未提交时崩溃</button>
      <button onclick="clearFault()">清除故障配置</button>
      <span class="pill" id="faultState"></span>
    </div>
  </div>
  <div class="card"><h2>事件日志</h2><div id="log"></div></div>
</main>
<script>
const fmtB = n => n==null?'-':(n<1024?n+' B':n<1048576?(n/1024).toFixed(1)+' KiB':n<1073741824?(n/1048576).toFixed(1)+' MiB':(n/1073741824).toFixed(2)+' GiB');
const fmtN = n => n==null?'-':(n<1000?n:n<1e6?(n/1e3).toFixed(1)+'K':(n/1e6).toFixed(2)+'M');
const esc = s => String(s).replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));

function bar(used, limit) {
  const pct = limit>0 ? Math.min(100, 100*used/limit) : 0;
  const over = limit>0 && used>limit;
  return `<div class="bar ${over?'over':''}" title="${fmtB(used)} / ${fmtB(limit)}"><i style="width:${pct}%"></i></div>`;
}

function renderWorkers(ws, budgets) {
  if (!ws.length) return '<span class="muted">无工作进程</span>';
  let h = `<table><tr><th>ID</th><th>状态</th><th>当前任务</th><th>已完成</th>
    <th>内存峰值 / 预算</th><th>临时盘 / 预算</th><th>最后心跳</th></tr>`;
  for (const w of ws) {
    const ago = Math.max(0, Date.now()/1000 - w.last_hb).toFixed(0);
    h += `<tr><td class="mono">${esc(w.id)}</td>
      <td><span class="badge b-${w.state}">${w.state}</span></td>
      <td class="mono">${esc(w.current_task||'-')}</td><td>${w.done}</td>
      <td>${bar(w.mem_peak||0, w.mem_budget||budgets.mem)} <span class="muted">${fmtB(w.mem_peak)} / ${fmtB(w.mem_budget||budgets.mem)}</span></td>
      <td>${bar(w.tmp_used||0, w.tmp_budget||budgets.tmp)} <span class="muted">${fmtB(w.tmp_used)} / ${fmtB(w.tmp_budget||budgets.tmp)}</span></td>
      <td class="muted">${ago}s 前</td></tr>`;
  }
  return h + '</table>';
}

function planTable(plan) {
  if (!plan) return '';
  let h = `<table><tr><th>候选方案</th><th>预计读取</th><th>预计临时盘</th><th>单任务最大量(长尾)</th>
           <th>可行</th><th>评分</th></tr>`;
  for (const p of plan.plans) {
    const cls = p.name===plan.chosen ? 'chosen' : '';
    h += `<tr class="${cls}"><td><b>${p.name}</b><br><span class="muted" style="white-space:normal">${esc(p.desc)}</span></td>
      <td>${fmtB(p.est_read)}</td><td>${fmtB(p.est_tmp)}</td><td>${fmtB(p.est_max_task)}</td>
      <td>${p.feasible?'✓':'✗ <span class="muted">'+esc(p.why_not||'')+'</span>'}</td>
      <td class="mono">${p.score?fmtN(p.score):'-'}</td></tr>`;
  }
  h += `</table><div class="pill">选定: <b>${plan.chosen||'无'}</b> · 分区数 ${plan.partitions} · 热点键 ${plan.hot_keys.length} 个(占 L ${(100*plan.hot_share).toFixed(1)}%)${plan.error?' · <span style="color:var(--bad)">'+esc(plan.error)+'</span>':''}</div>`;
  return h;
}

function stageBlock(st) {
  const counts = {PENDING:0, RUNNING:0, DONE:0, FAILED:0};
  for (const t of st.tasks) counts[t.state] = (counts[t.state]||0)+1;
  let cells = st.tasks.map(t =>
    `<div class="cell ${t.state}" title="${t.id} ${t.state} 尝试${t.attempt} ${t.worker||''}"></div>`).join('');
  let spill = '';
  const scans = Math.max(0, ...st.tasks.map(t => (t.metrics&&t.metrics.l_scans)||0));
  if (scans > 1) spill = `<span class="pill" style="color:var(--warn)">内存预算生效: S 分块 ×${scans} 遍扫 L</span>`;
  return `<div class="stage-head"><span class="badge b-${st.state}">${st.kind}</span>
    <span class="pill">待 ${counts.PENDING||0} · 跑 ${counts.RUNNING||0} · 完 ${counts.DONE||0}${counts.FAILED?' · <span style="color:var(--bad)">败 '+counts.FAILED+'</span>':''}</span>${spill}</div>
    <div class="grid">${cells}</div>`;
}

function partBars(pb) {
  if (!pb || !pb.length) return '';
  const mx = Math.max(1, ...pb.map(p => p.S + p.L));
  const cols = pb.map(p => {
    const hL = 100*p.L/mx, hS = 100*p.S/mx;
    return `<div class="col" title="分区${p.p}: L=${fmtB(p.L)} S=${fmtB(p.S)}"><i class="L" style="height:${hL}%"></i><i class="S" style="height:${hS}%"></i></div>`;
  }).join('');
  return `<div class="pill">洗牌分区字节(蓝=L 橙=S), 最大 ${fmtB(mx)} —— 倾斜是否被摊开一目了然</div><div class="bars">${cols}</div>`;
}

function jobCard(j) {
  const a = j.actuals||{};
  let h = `<div class="card" style="margin-bottom:12px"><div class="row" style="justify-content:space-between">
    <div><b>${esc(j.name)}</b> <span class="mono muted">${j.id}</span> <span class="badge b-${j.state}">${j.state}</span></div>
    <div>${j.state==='RUNNING'?`<button class="danger" onclick="cancelJob('${j.id}')">取消作业(只清自己的临时目录)</button>`:''}</div></div>`;
  if (j.error) h += `<div style="color:var(--bad);margin:4px 0">${esc(j.error)}</div>`;
  h += `<div class="row" style="margin:6px 0">
    <span class="kv"><span>实际读取 </span><b>${fmtB(a.read)}</b></span>
    <span class="kv"><span>实际洗牌 </span><b>${fmtB(a.shuffle)}</b></span>
    <span class="kv"><span>实际输出 </span><b>${fmtB(a.out)}</b></span>
    <span class="kv"><span>内存峰值 </span><b>${fmtB(a.mem_peak)}</b><span> / 预算 ${fmtB(j.spec.mem_budget)}</span></span>
    <span class="kv"><span>临时盘峰值 </span><b>${fmtB(a.tmp_peak)}</b><span> / 预算 ${fmtB(j.spec.tmp_budget)}</span></span>
  </div>`;
  h += planTable(j.plan);
  for (const st of j.stages) h += stageBlock(st);
  h += partBars(j.partition_bytes);
  if (j.verify) {
    const v = j.verify;
    const m = v.match===true?'<span style="color:var(--ok)">PASS ✓ 与全量基准一致</span>'
           : v.match===false?'<span style="color:var(--bad)">FAIL ✗ 与基准不一致!</span>'
           : '<span class="muted">无基准</span>';
    h += `<div class="pill">输出 ${fmtN(v.rows)} 行 · 多重集哈希 <span class="mono">${v.hash.s1}/${v.hash.s2}</span> · 核对: ${m}
      <button onclick="preview('${j.id}')">查看输出样例</button></div><div class="out" id="out-${j.id}" style="display:none"></div>`;
  }
  return h + '</div>';
}

async function refresh() {
  try {
    const s = await (await fetch('/api/status')).json();
    const alive = s.workers.filter(w=>w.state==='alive').length;
    document.getElementById('cluster').textContent =
      `工作进程 ${alive}/${s.workers.length} 存活 · 默认预算 内存 ${fmtB(s.budgets.mem)} / 临时盘 ${fmtB(s.budgets.tmp)}`;
    document.getElementById('workers').innerHTML = renderWorkers(s.workers, s.budgets);
    document.getElementById('jobs').innerHTML = s.jobs.length
      ? s.jobs.slice().reverse().map(jobCard).join('') : '<span class="muted">暂无作业 —— 用 run_demo.py 提交</span>';
    document.getElementById('log').textContent = s.events.map(e=>e.msg).reverse().join('\n');
    document.getElementById('faultState').textContent =
      Object.keys(s.faults||{}).length ? '当前配置: '+JSON.stringify(s.faults) : '';
  } catch(e) { document.getElementById('cluster').textContent = '协调器不可达: '+e; }
}
async function cancelJob(jid){ await fetch(`/api/jobs/${jid}/cancel`, {method:'POST', body:'{}'}); refresh(); }
async function armFault(point){
  await fetch('/api/fault', {method:'POST', body: JSON.stringify({points:{[point]:'*'}})}); refresh(); }
async function clearFault(){ await fetch('/api/fault', {method:'POST', body:'{}'}); refresh(); }
async function preview(jid){
  const el = document.getElementById('out-'+jid);
  if (el.style.display==='block'){ el.style.display='none'; return; }
  const d = await (await fetch('/api/output/'+jid)).json();
  el.textContent = d.lines.join('\n') || '(空)';
  el.style.display='block';
}
refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>
"""
