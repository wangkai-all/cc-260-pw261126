"""工作进程内的任务执行引擎。

预算模型(诚实工程口径): 对"数据缓冲区的编码字节数"做逻辑记账 ——
洗牌缓冲、哈希表、输出缓冲都计入 mem; 落盘字节计入 tmp。
超限即抛 BudgetExceeded, 任务失败并如实上报, 绝不静默突破。

关联实现统一走"分块嵌套循环": S 侧按预算切块, 每块对 L 分区全量流式探测。
S 一块装得下就是普通哈希关联; 装不下就自动多扫几遍 L(指标 l_scans>1 可见),
内存永远有界 —— 这就是内存预算"真的生效"的兜底机制。
"""
import json
import os
import random
import time

from .util import read_jsonl, stable_key_hash, ensure_dir, fsync_dir


class BudgetExceeded(Exception):
    def __init__(self, kind, used, limit):
        super().__init__("%s budget exceeded: %d > %d" % (kind, used, limit))
        self.kind = kind
        self.used = used
        self.limit = limit


class TaskCancelled(Exception):
    pass


class Budget:
    """单任务预算记账。mem=逻辑缓冲字节, tmp=落盘字节。"""

    def __init__(self, mem_limit, tmp_limit):
        self.mem_limit = mem_limit
        self.tmp_limit = tmp_limit
        self.mem_used = 0
        self.tmp_used = 0
        self.mem_peak = 0

    def mem_alloc(self, n):
        self.mem_used += n
        if self.mem_used > self.mem_peak:
            self.mem_peak = self.mem_used
        if self.mem_used > self.mem_limit:
            raise BudgetExceeded("mem", self.mem_used, self.mem_limit)

    def mem_free(self, n):
        self.mem_used = max(0, self.mem_used - n)

    def tmp_alloc(self, n):
        self.tmp_used += n
        if self.tmp_used > self.tmp_limit:
            raise BudgetExceeded("tmp", self.tmp_used, self.tmp_limit)


class Faults:
    """故障注入点。配置来自协调器(随任务下发), 触发记录持久化在 worker 目录,
    保证"只炸一次": 进程被 supervisor 重启后不会在同一任务上反复炸。"""

    def __init__(self, state_path):
        self.state_path = state_path
        self.fired = set()
        if os.path.exists(state_path):
            try:
                self.fired = set(json.load(open(state_path)))
            except Exception:
                self.fired = set()
        self.cfg = {}

    def update(self, cfg):
        self.cfg = cfg or {}

    def _mark(self, tag):
        self.fired.add(tag)
        ensure_dir(os.path.dirname(self.state_path))
        with open(self.state_path, "w") as f:
            json.dump(sorted(self.fired), f)

    def point(self, name, task_id):
        """命名故障点: 配置 {"points": {"after_shuffle_write": "<task_id 或 *>"}}"""
        spec = (self.cfg.get("points") or {}).get(name)
        if not spec:
            return
        if spec not in ("*", task_id):
            return
        # "*" 每个工作进程只炸一次; 指定任务 id 则只在该任务上炸一次
        tag = name if spec == "*" else "%s@%s" % (name, task_id)
        if tag in self.fired:
            return
        self._mark(tag)
        # 硬崩溃: 不给任何清理机会, 模拟断电/段错误
        os._exit(42)

    def maybe_slow(self):
        ms = self.cfg.get("slow_ms") or 0
        if ms:
            time.sleep(ms / 1000.0)

    def maybe_crash_random(self):
        p = self.cfg.get("crash_pct") or 0
        if p and random.random() < p:
            os._exit(43)


class OutputWriter:
    """输出缓冲 writer: 缓冲计入内存预算, 定期落盘, 关闭时 fsync。"""

    def __init__(self, path, budget, flush_bytes):
        ensure_dir(os.path.dirname(path))
        self.path = path
        self.budget = budget
        self.flush_bytes = max(64 * 1024, flush_bytes)
        self.f = open(path, "wb")
        self.buf = []
        self.buf_bytes = 0
        self.rows = 0
        self.bytes_written = 0

    def emit(self, obj):
        raw = (json.dumps(obj, separators=(",", ":")) + "\n").encode()
        self.buf.append(raw)
        self.buf_bytes += len(raw)
        self.budget.mem_alloc(len(raw))
        self.rows += 1
        if self.buf_bytes >= self.flush_bytes:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        data = b"".join(self.buf)
        self.f.write(data)
        self.bytes_written += len(data)
        self.budget.mem_free(self.buf_bytes)
        self.buf = []
        self.buf_bytes = 0

    def close(self):
        self.flush()
        self.f.flush()
        os.fsync(self.f.fileno())
        self.f.close()
        fsync_dir(os.path.dirname(self.path))


class ShuffleWriter:
    """洗牌写: P 个分区缓冲, 总缓冲超水位就冲刷最大分区; 落盘字节计入 tmp 预算。"""

    def __init__(self, out_dir, num_partitions, table, budget, high_water):
        ensure_dir(out_dir)
        self.out_dir = out_dir
        self.P = num_partitions
        self.table = table
        self.budget = budget
        self.high_water = max(256 * 1024, high_water)
        self.buffers = [[] for _ in range(num_partitions)]
        self.buf_bytes = [0] * num_partitions
        self.total = 0
        self.bytes_written = 0

    def write(self, p, raw: bytes):
        n = len(raw) + 1
        self.buffers[p].append(raw)
        self.buf_bytes[p] += n
        self.total += n
        self.budget.mem_alloc(n)
        if self.total > self.high_water:
            self._flush_largest()

    def _flush_largest(self):
        p = max(range(self.P), key=lambda i: self.buf_bytes[i])
        if self.buf_bytes[p]:
            self._flush(p)

    def _flush(self, p):
        path = os.path.join(self.out_dir, "part-%05d.%s.jl" % (p, self.table))
        data = b"\n".join(self.buffers[p]) + b"\n"
        with open(path, "ab") as f:
            f.write(data)
        self.bytes_written += len(data)
        self.budget.tmp_alloc(len(data))
        self.budget.mem_free(self.buf_bytes[p])
        self.total -= self.buf_bytes[p]
        self.buffers[p] = []
        self.buf_bytes[p] = 0

    def close(self):
        for p in range(self.P):
            if self.buffers[p]:
                self._flush(p)
        # 落盘确认: 每个文件 fsync + 目录 fsync, 崩溃后能看到完整文件
        for name in sorted(os.listdir(self.out_dir)):
            if name.endswith(".%s.jl" % self.table):
                fd = os.open(os.path.join(self.out_dir, name), os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        fsync_dir(self.out_dir)

    def files_meta(self):
        out = []
        for name in sorted(os.listdir(self.out_dir)):
            if not name.endswith(".%s.jl" % self.table):
                continue
            p = int(name.split("-")[1].split(".")[0])
            path = os.path.join(self.out_dir, name)
            out.append({"path": path, "bytes": os.path.getsize(path),
                        "partition": p, "table": self.table})
        return out


def _heartbeat(hb, metrics, every_rows=20000):
    if metrics["rows_in"] % every_rows == 0:
        hb(metrics)


# ---------------- 任务: 统计 ----------------

def run_stats(spec, budget, hb, faults):
    from .stats import table_stats
    metrics = {"rows_in": 0, "rows_out": 0, "bytes_read": 0}

    def progress(rows, nbytes):
        metrics["rows_in"] = rows
        metrics["bytes_read"] = nbytes
        hb(metrics)

    stats = table_stats(spec["inputs"], progress=progress)
    metrics["rows_in"] = stats["rows"]
    metrics["bytes_read"] = stats["bytes"]
    hb(metrics)
    return {"stats": stats}, metrics


# ---------------- 任务: 洗牌写 ----------------

def run_shuffle(spec, budget, hb, faults):
    """扫一个表的一个分片, 按方案把行写进 P 个分区文件。

    倾斜感知(skew 方案):
      S 表热点键 -> 不洗牌(由广播小文件覆盖, 协调器已生成)
      L 表热点键 -> 加盐打散: (hash(k)+序号) % P, 均匀散布到所有分区
      冷键        -> 稳定哈希
    """
    P = spec["num_partitions"]
    table = spec["table"]
    hot = set(spec.get("hot_keys") or [])
    out_dir = spec["out_dir"]
    writer = ShuffleWriter(out_dir, P, table, budget, high_water=budget.mem_limit * 0.6)
    salt_ctr = {}
    metrics = {"rows_in": 0, "rows_out": 0, "bytes_read": 0, "hot_rows": 0, "skipped_hot_s": 0}
    for path in spec["inputs"]:
        for raw, obj in read_jsonl(path):
            metrics["rows_in"] += 1
            metrics["bytes_read"] += len(raw) + 1
            k = obj["k"]
            if k in hot:
                if table == "S":
                    metrics["skipped_hot_s"] += 1   # 热点维表行走广播文件
                    continue
                c = salt_ctr.get(k, 0)               # 热点事实行加盐打散
                salt_ctr[k] = c + 1
                p = (stable_key_hash(k) + c) % P
                metrics["hot_rows"] += 1
            else:
                p = stable_key_hash(k) % P
            writer.write(p, raw)
            metrics["rows_out"] += 1
            _heartbeat(hb, metrics)
            faults.maybe_crash_random()
    writer.close()
    metrics["bytes_written"] = writer.bytes_written
    files = writer.files_meta()
    # 故障点: 中间文件已写完并 fsync, 但还没向协调器登记 —— 就在这儿崩
    faults.point("after_shuffle_write", spec["task_id"])
    return {"files": files}, metrics


# ---------------- 关联内核: 分块嵌套循环 ----------------

def _s_chunks(s_files, chunk_limit):
    """把 S 侧文件切成不超过 chunk_limit 字节的块, 逐块 yield {k: [v,...]}。

    即使 S 侧为空也 yield 一个空块: 热点广播表仍需对 L 探测一遍。
    """
    chunk = {}
    used = 0
    rows = 0
    produced = False
    for path in s_files:
        for raw, obj in read_jsonl(path):
            n = len(raw) + 1
            if used + n > chunk_limit and chunk:
                produced = True
                yield chunk, used, rows
                chunk, used, rows = {}, 0, 0
            chunk.setdefault(obj["k"], []).append(obj["v"])
            used += n
            rows += 1
    if chunk:
        yield chunk, used, rows
    elif not produced:
        yield {}, 0, 0


def join_core(s_files, l_files, hot_map, out, budget, hb, metrics):
    """对一组 S/L 文件做内连接。S 超预算自动分块, L 重扫, 内存始终有界。

    注意: 热点广播表只在第一遍扫 L 时探测 —— 否则分块重扫会把热点行输出多次。
    """
    chunk_limit = max(64 << 10, int(budget.mem_limit * 0.5))
    l_scans = 0
    for chunk, chunk_bytes, s_rows in _s_chunks(s_files, chunk_limit):
        budget.mem_alloc(chunk_bytes)
        metrics["s_rows"] += s_rows
        l_scans += 1
        active_hot = hot_map if l_scans == 1 else None
        try:
            for path in l_files:
                for raw, obj in read_jsonl(path):
                    metrics["rows_in"] += 1
                    metrics["bytes_read"] += len(raw) + 1
                    k = obj["k"]
                    hv = active_hot.get(k) if active_hot else None
                    cv = chunk.get(k)
                    if hv is None and cv is None:
                        continue
                    for sv in (hv or ()):
                        out.emit({"k": k, "s": sv, "l": obj["v"]})
                        metrics["rows_out"] += 1
                    for sv in (cv or ()):
                        out.emit({"k": k, "s": sv, "l": obj["v"]})
                        metrics["rows_out"] += 1
                    _heartbeat(hb, metrics)
        finally:
            budget.mem_free(chunk_bytes)
    metrics["l_scans"] = l_scans
    metrics["spill"] = l_scans > 1


def _load_hot_map(path, budget):
    """装载热点广播小文件(有界)。超预算即失败 —— 预算硬约束。"""
    hot = {}
    if not path:
        return hot
    for raw, obj in read_jsonl(path):
        budget.mem_alloc(len(raw) + 1)
        hot.setdefault(obj["k"], []).append(obj["v"])
    return hot


# ---------------- 任务: 分区关联 ----------------

def run_join(spec, budget, hb, faults):
    metrics = {"rows_in": 0, "rows_out": 0, "bytes_read": 0, "s_rows": 0}
    hot_map = _load_hot_map(spec.get("broadcast_file"), budget)
    metrics["hot_keys"] = len(hot_map)
    out = OutputWriter(spec["output_tmp"], budget, flush_bytes=budget.mem_limit * 0.2)
    join_core(spec["s_files"], spec["l_files"], hot_map, out, budget, hb, metrics)
    out.close()
    metrics["bytes_written"] = out.bytes_written
    # 故障点: 输出临时文件已写完并 fsync, 但还没向协调器提交 —— 就在这儿崩
    faults.point("before_output_commit", spec["task_id"])
    return {"output_tmp": spec["output_tmp"], "rows": metrics["rows_out"],
            "bytes": out.bytes_written}, metrics


# ---------------- 任务: 广播关联 ----------------

def run_bjoin(spec, budget, hb, faults):
    """整份 S 装进内存(规划器已判定装得下), 流式扫 L 分片探测。"""
    metrics = {"rows_in": 0, "rows_out": 0, "bytes_read": 0, "s_rows": 0}
    build = {}
    for path in spec["s_files"]:
        for raw, obj in read_jsonl(path):
            budget.mem_alloc(len(raw) + 1)   # 装不下就抛 BudgetExceeded, 如实失败
            build.setdefault(obj["k"], []).append(obj["v"])
            metrics["s_rows"] += 1
    metrics["build_bytes"] = budget.mem_used
    out = OutputWriter(spec["output_tmp"], budget, flush_bytes=budget.mem_limit * 0.2)
    join_core([], spec["inputs"], build, out, budget, hb, metrics)
    out.close()
    metrics["bytes_written"] = out.bytes_written
    faults.point("before_output_commit", spec["task_id"])
    return {"output_tmp": spec["output_tmp"], "rows": metrics["rows_out"],
            "bytes": out.bytes_written}, metrics


RUNNERS = {
    "stats": run_stats,
    "shuffle": run_shuffle,
    "join": run_join,
    "bjoin": run_bjoin,
}
