"""协调器: 调度 + 恢复 + HTTP API + 页面。

调度: 作业 -> 阶段 -> 任务; 两个工作进程通过 claim 拉取任务, 心跳续租。
恢复: 租约超时重排队, 尝试号单调递增; 只有当前尝试能登记/提交。
提交协议(不重复输出的关键):
  1. 工作进程把中间文件写在自己的 attempt 目录, 先 REGISTER 再 COMPLETE;
  2. 协调器只把"已提交尝试"登记的文件交给下游 —— 写完没登记的孤儿文件无人引用, 直接回收;
  3. 输出先写临时文件, 协调器记日志后原子改名到最终目录, 改名幂等可重放。
清理: 取消/完成只删 tmp/<job>/, 路径守卫保证碰不到共享输入 data/。
"""
import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import planner
from .journal import Journal
from .util import (MultisetHash, atomic_rename, ensure_dir, fmt_bytes, is_under,
                   list_shards, read_jsonl, rm_tree_guarded)

LEASE_TIMEOUT = 6.0
WORKER_DEAD_AFTER = 9.0
MAX_TASK_ATTEMPTS = 4


def _t():
    return time.time()


class Coordinator:
    def __init__(self, root, mem_budget, tmp_budget):
        self.root = root
        self.tmp_root = ensure_dir(os.path.join(root, "tmp"))
        self.out_root = ensure_dir(os.path.join(root, "output"))
        self.state_dir = ensure_dir(os.path.join(root, "state"))
        self.def_mem = mem_budget
        self.def_tmp = tmp_budget
        self.lock = threading.RLock()
        self.journal = Journal(os.path.join(self.state_dir, "journal.jsonl"))
        self.workers = {}
        self.jobs = {}
        self.events = []
        self.fault_cfg = {}
        self.job_seq = 0
        self._replay()
        self._sweep_orphans()
        self.stop_flag = False
        threading.Thread(target=self._sweeper, daemon=True).start()

    # ---------------- 事件 ----------------
    def log(self, msg, *args):
        line = "[%s] %s" % (time.strftime("%H:%M:%S"), (msg % args if args else msg))
        with self.lock:
            self.events.append({"t": _t(), "msg": line})
            del self.events[:-400]
        print(line, flush=True)

    # ---------------- 日志恢复 ----------------
    def _replay(self):
        for rec in Journal.replay(self.journal.path):
            self._apply(rec)
        with self.lock:
            for job in self.jobs.values():
                if job["state"] in ("RUNNING",):
                    for st in job["stages"]:
                        for t in st["tasks"].values():
                            if t["state"] == "RUNNING":
                                t["state"] = "PENDING"   # 崩溃时租约作废, 重新排队
                                t["worker"] = None
                            # 崩溃发生在"已提交未改名"窗口: 重放改名(幂等)
                            if (t["state"] == "DONE" and t["kind"] in ("join", "bjoin")
                                    and not t["output_final"] and t["result"]
                                    and t["result"].get("output_tmp")
                                    and os.path.exists(t["result"]["output_tmp"])):
                                final = os.path.join(self.out_root, job["id"],
                                                     "part-%05d.jl" % t["index"])
                                atomic_rename(t["result"]["output_tmp"], final)
                                t["output_final"] = final
                                self.journal.append({"r": "renamed", "task": t["id"],
                                                     "final": final})
                    self._maybe_advance(job)
        n = len(self.jobs)
        if n:
            self.log("恢复: 重放日志, 重建 %d 个作业", n)

    def _apply(self, rec):
        # 重放时按记录类型重建内存状态(精简版, 覆盖关键路径)
        kind = rec.get("r")
        with self.lock:
            if kind == "job":
                self.job_seq = max(self.job_seq, int(rec["job"]["id"][1:]))
                self.jobs[rec["job"]["id"]] = rec["job"]
            elif kind == "plan":
                job = self.jobs.get(rec["job"])
                if job:
                    job["plan"] = rec["plan"]
            elif kind == "stage":
                job = self.jobs.get(rec["job"])
                if job and not any(s["id"] == rec["stage"]["id"] for s in job["stages"]):
                    job["stages"].append(rec["stage"])
            elif kind == "commit":
                t = self._find_task(rec["task"])
                if t:
                    t["state"] = "DONE"
                    t["committed_attempt"] = rec["attempt"]
                    t["result"] = rec.get("result")
                    t["metrics"] = rec.get("metrics") or {}
                    if rec.get("registered"):
                        t["registrations"][str(rec["attempt"])] = rec["registered"]
            elif kind == "register":
                t = self._find_task(rec["task"])
                if t:
                    t["registrations"][str(rec["attempt"])] = rec["files"]
            elif kind == "renamed":
                t = self._find_task(rec["task"])
                if t:
                    t["output_final"] = rec["final"]
            elif kind == "job_state":
                job = self.jobs.get(rec["job"])
                if job:
                    job["state"] = rec["state"]
                    job["error"] = rec.get("error")
            elif kind == "verify":
                job = self.jobs.get(rec["job"])
                if job:
                    job["verify"] = rec["verify"]

    def _sweep_orphans(self):
        """启动时回收: tmp 下不属于任何存活作业的目录(含崩溃遗留的未登记文件)。"""
        live = {j for j, job in self.jobs.items() if job["state"] == "RUNNING"}
        for name in os.listdir(self.tmp_root):
            d = os.path.join(self.tmp_root, name)
            if os.path.isdir(d) and name not in live:
                try:
                    rm_tree_guarded(d, self.tmp_root)
                    self.log("回收孤儿临时目录: %s", d)
                except Exception as e:
                    self.log("回收失败 %s: %s", d, e)

    def _sweeper(self):
        while not self.stop_flag:
            time.sleep(1.0)
            with self.lock:
                now = _t()
                for w in self.workers.values():
                    if w["state"] == "alive" and now - w["last_hb"] > WORKER_DEAD_AFTER:
                        w["state"] = "dead"
                        w["current_task"] = None
                        self.log("工作进程 %s 心跳丢失, 判定死亡", w["id"])
                for job in list(self.jobs.values()):
                    if job["state"] != "RUNNING":
                        continue
                    for st in job["stages"]:
                        for t in st["tasks"].values():
                            if t["state"] == "RUNNING" and now - t["last_hb"] > LEASE_TIMEOUT:
                                self.log("任务 %s(尝试 %d)租约超时, 重新排队", t["id"], t["attempt"])
                                t["state"] = "PENDING"
                                t["worker"] = None
                    self._maybe_advance(job)

    # ---------------- 作业生命周期 ----------------
    def submit_job(self, spec):
        with self.lock:
            self.job_seq += 1
            jid = "j%d" % self.job_seq
            mem = int(spec.get("mem_budget") or self.def_mem)
            tmp = int(spec.get("tmp_budget") or self.def_tmp)
            left, right = spec["left"], spec["right"]
            job = {
                "id": jid, "name": spec.get("name") or jid,
                "spec": {"left": left, "right": right, "mem_budget": mem, "tmp_budget": tmp,
                         "partitions": spec.get("partitions"),
                         "expected": spec.get("expected")},
                "state": "RUNNING", "created": _t(), "finished": None, "error": None,
                "plan": None, "stages": [], "verify": None,
                "actuals": {"read": 0, "shuffle": 0, "out": 0, "mem_peak": 0,
                            "tmp_peak": 0, "l_scans_max": 0},
                "partition_bytes": [],
            }
            self.jobs[jid] = job
            self.journal.append({"r": "job", "job": job})
            # 阶段 0: 流式统计(每表一个任务)
            tasks = []
            for i, (table, d) in enumerate((("L", left), ("S", right))):
                tasks.append(self._new_task(job, "", i, "stats",
                                            {"table": table, "inputs": list_shards(d)}))
            self._add_stage(job, "stats", tasks)
            self.log("作业 %s(%s)提交: L=%s S=%s 内存预算=%s 临时盘预算=%s",
                     jid, job["name"], left, right, fmt_bytes(mem), fmt_bytes(tmp))
            return jid

    def _new_task(self, job, stage_id, index, kind, spec):
        return {"id": "%s-t%d" % (stage_id, index), "job": job["id"], "stage": stage_id,
                "kind": kind, "index": index, "state": "PENDING", "attempt": 0,
                "worker": None, "leased_at": None, "last_hb": None,
                "spec": spec, "registrations": {}, "committed_attempt": None,
                "result": None, "metrics": {}, "progress": {}, "error": None,
                "output_final": None}

    def _add_stage(self, job, kind, tasks):
        sid = "%s-s%d" % (job["id"], len(job["stages"]))
        for t in tasks:
            t["stage"] = sid
            t["id"] = "%s-t%d" % (sid, t["index"])
        stage = {"id": sid, "kind": kind, "state": "PENDING",
                 "tasks": {t["id"]: t for t in tasks}, "order": [t["id"] for t in tasks]}
        job["stages"].append(stage)
        self.journal.append({"r": "stage", "job": job["id"], "stage": stage})
        return stage

    def _find_task(self, tid):
        for job in self.jobs.values():
            for st in job["stages"]:
                if tid in st["tasks"]:
                    return st["tasks"][tid]
        return None

    def _current_stage(self, job):
        for st in job["stages"]:
            if st["state"] != "DONE":
                return st
        return None

    def _maybe_advance(self, job):
        """阶段推进。只在锁内调用。幂等。"""
        if job["state"] != "RUNNING":
            return
        st = self._current_stage(job)
        if st is None:
            return
        if st["state"] == "PENDING":
            st["state"] = "RUNNING"
        if not all(t["state"] == "DONE" for t in st["tasks"].values()):
            return
        st["state"] = "DONE"
        self.log("作业 %s 阶段 %s(%s)完成", job["id"], st["id"], st["kind"])
        if st["kind"] == "stats":
            if job["plan"] is None:                    # 幂等: 恢复重放后不再重复建阶段
                self._build_plan(job, st)
        elif st["kind"] == "shuffle":
            if not any(s["kind"] == "join" for s in job["stages"]):
                self._build_join_stage(job, st)
        elif st["kind"] in ("join", "bjoin"):
            if job["verify"] is None:
                self._finish_job(job, st)

    def _build_plan(self, job, stats_stage):
        tasks = [stats_stage["tasks"][tid] for tid in stats_stage["order"]]
        stats = {t["spec"]["table"]: t["result"]["stats"] for t in tasks}
        spec = job["spec"]
        n_l_shards = len(list_shards(spec["left"]))
        n_workers = max(1, sum(1 for w in self.workers.values() if w["state"] == "alive")) or 2
        plan = planner.estimate_plans(stats["L"], stats["S"], spec["mem_budget"],
                                      spec["tmp_budget"], n_l_shards, n_workers,
                                      spec.get("partitions"))
        job["plan"] = plan
        self.journal.append({"r": "plan", "job": job["id"], "plan": plan})
        if not plan["chosen"]:
            self._set_job_state(job, "FAILED", plan["error"])
            return
        self.log("作业 %s 选定方案 %s: %s", job["id"], plan["chosen"],
                 next(p["desc"] for p in plan["plans"] if p["name"] == plan["chosen"]))
        left, right = spec["left"], spec["right"]
        P = plan["partitions"]
        hot = plan["hot_keys"] if plan["chosen"] == "skew" else []
        if plan["chosen"] == "broadcast":
            tasks = []
            for i, shard in enumerate(list_shards(left)):
                tasks.append(self._new_task(job, "", i, "bjoin",
                                            {"inputs": [shard], "s_files": list_shards(right)}))
            self._add_stage(job, "bjoin", tasks)
        else:
            if hot:
                self._ensure_hot_file(job, right, set(hot))
            tasks = []
            i = 0
            for table, d in (("L", left), ("S", right)):
                for shard in list_shards(d):
                    tasks.append(self._new_task(job, "", i, "shuffle",
                                                {"table": table, "inputs": [shard],
                                                 "num_partitions": P, "hot_keys": hot}))
                    i += 1
            self._add_stage(job, "shuffle", tasks)

    def _hot_file_path(self, job):
        """热点广播文件路径: 由作业 id 确定性推出, 协调器重启后依然找得到。"""
        return os.path.join(self.tmp_root, job["id"], "hot.jl")

    def _ensure_hot_file(self, job, s_dir, hot_keys):
        """从 S 表流式筛出热点键的维表行, 生成广播小文件(有界)。幂等: 已存在就直接用。"""
        path = self._hot_file_path(job)
        if os.path.exists(path):
            return path
        ensure_dir(os.path.dirname(path))
        n = 0
        with open(path, "wb") as f:
            for shard in list_shards(s_dir):
                for raw, obj in read_jsonl(shard):
                    if obj["k"] in hot_keys:
                        f.write(raw + b"\n")
                        n += 1
            f.flush()
            os.fsync(f.fileno())
        self.log("作业 %s 热点广播文件: %d 行 -> %s", job["id"], n, path)
        return path

    def _build_join_stage(self, job, shuffle_stage):
        """洗牌完成: 只用"已提交尝试"登记的文件组装关联任务 —— 未登记的孤儿文件被无视。"""
        P = job["plan"]["partitions"]
        s_files = [[] for _ in range(P)]
        l_files = [[] for _ in range(P)]
        part_bytes = [{"p": p, "S": 0, "L": 0} for p in range(P)]
        for tid in shuffle_stage["order"]:
            t = shuffle_stage["tasks"][tid]
            for f in t["registrations"].get(str(t["committed_attempt"]), []):
                (s_files if f["table"] == "S" else l_files)[f["partition"]].append(f["path"])
                part_bytes[f["partition"]][f["table"]] += f["bytes"]
        job["partition_bytes"] = part_bytes
        mx = max((p["S"] + p["L"]) for p in part_bytes)
        self.log("作业 %s 洗牌完成: %d 个分区, 最大分区 %s", job["id"], P, fmt_bytes(mx))
        broadcast = None
        if job["plan"]["chosen"] == "skew":
            # 路径确定 + 缺失即重建: 协调器重启后热点广播文件依然可用
            broadcast = self._ensure_hot_file(job, job["spec"]["right"],
                                              set(job["plan"]["hot_keys"]))
        tasks = []
        for p in range(P):
            tasks.append(self._new_task(job, "", p, "join",
                                        {"partition": p, "s_files": s_files[p],
                                         "l_files": l_files[p],
                                         "broadcast_file": broadcast}))
        self._add_stage(job, "join", tasks)

    def _finish_job(self, job, out_stage):
        """输出核对: 流式算多重集哈希, 与全量基准比对(若提供)。"""
        mh = MultisetHash()
        out_dir = os.path.join(self.out_root, job["id"])
        total = 0
        for name in sorted(os.listdir(out_dir)) if os.path.isdir(out_dir) else []:
            if not name.endswith(".jl"):
                continue
            for raw, obj in read_jsonl(os.path.join(out_dir, name)):
                mh.add_obj(obj)   # 规范化后哈希, 与基准口径一致(与键序/空白无关)
                total += 1
        digest = mh.digest()
        expected = job["spec"].get("expected")
        match = None
        if expected:
            match = (digest == expected)
        job["verify"] = {"rows": digest["rows"], "hash": digest, "expected": expected,
                         "match": match}
        self.journal.append({"r": "verify", "job": job["id"], "verify": job["verify"]})
        self._set_job_state(job, "DONE")
        self.log("作业 %s 完成: 输出 %d 行, 核对=%s", job["id"], total,
                 {True: "PASS", False: "FAIL", None: "无基准"}[match])
        self._cleanup_job_tmp(job)

    def _set_job_state(self, job, state, error=None):
        job["state"] = state
        job["error"] = error
        job["finished"] = _t() if state != "RUNNING" else None
        self.journal.append({"r": "job_state", "job": job["id"], "state": state, "error": error})
        if state in ("FAILED", "CANCELLED"):
            self._cleanup_job_tmp(job)

    def _cleanup_job_tmp(self, job):
        """清理作业临时目录。路径守卫: 只允许删 tmp/<jid>/, 共享输入 data/ 碰不到。"""
        d = os.path.join(self.tmp_root, job["id"])
        try:
            if rm_tree_guarded(d, self.tmp_root):
                self.log("作业 %s 临时目录已清理(输入数据不动): %s", job["id"], d)
        except Exception as e:
            self.log("清理 %s 被拒绝/失败: %s", d, e)

    def cancel_job(self, jid):
        with self.lock:
            job = self.jobs.get(jid)
            if not job or job["state"] != "RUNNING":
                return False
            self._set_job_state(job, "CANCELLED")
            self.log("作业 %s 已取消, 只清理自己的临时目录", jid)
            return True

    # ---------------- 工作进程交互 ----------------
    def register_worker(self, wid, mem, tmp):
        with self.lock:
            self.workers[wid] = {"id": wid, "mem_budget": mem, "tmp_budget": tmp,
                                 "state": "alive", "last_hb": _t(), "current_task": None,
                                 "done": 0, "mem_peak": 0, "tmp_used": 0}
            self.log("工作进程 %s 上线: 内存预算=%s 临时盘预算=%s", wid, fmt_bytes(mem), fmt_bytes(tmp))

    def _touch_worker(self, wid):
        w = self.workers.get(wid)
        if w:
            w["last_hb"] = _t()
            if w["state"] != "alive":
                w["state"] = "alive"
                self.log("工作进程 %s 重新上线", wid)
        return w

    def claim(self, wid):
        with self.lock:
            w = self._touch_worker(wid)
            if not w:
                return {"error": "unknown worker"}
            for job in sorted(self.jobs.values(), key=lambda j: j["created"]):
                if job["state"] != "RUNNING":
                    continue
                st = self._current_stage(job)
                if not st:
                    continue
                for tid in st["order"]:
                    t = st["tasks"][tid]
                    if t["state"] != "PENDING":
                        continue
                    t["attempt"] += 1
                    t["state"] = "RUNNING"
                    t["worker"] = wid
                    t["leased_at"] = t["last_hb"] = _t()
                    t["progress"] = {}
                    w["current_task"] = t["id"]
                    self._gc_attempt_dirs(t)
                    spec = self._materialize_spec(job, st, t)
                    self.journal.append({"r": "lease", "task": t["id"], "attempt": t["attempt"],
                                         "worker": wid})
                    self.log("任务 %s 第 %d 次尝试 -> %s", t["id"], t["attempt"], wid)
                    return {"task": spec, "fault_cfg": self.fault_cfg,
                            "mem_budget": job["spec"]["mem_budget"],
                            "tmp_budget": job["spec"]["tmp_budget"]}
            return {"task": None}

    def _materialize_spec(self, job, stage, t):
        """租约时生成确定路径: 每次尝试有独立目录, 崩溃不会污染下一次尝试。"""
        spec = dict(t["spec"])
        spec["kind"] = t["kind"]
        attempt_dir = os.path.join(self.tmp_root, job["id"], stage["id"], t["id"],
                                   "attempt-%d" % t["attempt"])
        spec["task_id"] = t["id"]
        spec["attempt"] = t["attempt"]
        spec["job"] = job["id"]
        if t["kind"] == "shuffle":
            spec["out_dir"] = attempt_dir
        elif t["kind"] in ("join", "bjoin"):
            spec["output_tmp"] = os.path.join(attempt_dir, "output.jl")
        return spec

    def _gc_attempt_dirs(self, t):
        """回收已被取代的尝试目录(包括'写完没登记就崩了'留下的孤儿文件)。"""
        base = os.path.join(self.tmp_root, t["job"], t["stage"], t["id"])
        if not os.path.isdir(base):
            return
        keep = {t["attempt"]}
        if t["committed_attempt"]:
            keep.add(t["committed_attempt"])
        for name in os.listdir(base):
            if not name.startswith("attempt-"):
                continue
            n = int(name.split("-")[1])
            if n not in keep and n < t["attempt"]:
                try:
                    rm_tree_guarded(os.path.join(base, name), self.tmp_root)
                except Exception:
                    pass

    def heartbeat(self, wid, tid, attempt, progress):
        with self.lock:
            self._touch_worker(wid)
            t = self._find_task(tid)
            if not t or t["state"] != "RUNNING" or t["attempt"] != attempt:
                return {"cancel": True, "stale": True}
            t["last_hb"] = _t()
            t["progress"] = progress or {}
            w = self.workers.get(wid)
            if w and progress:
                w["mem_peak"] = max(w.get("mem_peak", 0), progress.get("mem_peak", 0))
                w["tmp_used"] = max(w.get("tmp_used", 0), progress.get("tmp_used", 0))
            job = self.jobs.get(t["job"])
            return {"cancel": job["state"] != "RUNNING"}

    def register_files(self, wid, tid, attempt, files):
        with self.lock:
            self._touch_worker(wid)
            t = self._find_task(tid)
            if not t or t["state"] != "RUNNING" or t["attempt"] != attempt:
                return {"ok": False, "stale": True}
            t["registrations"][str(attempt)] = files
            self.journal.append({"r": "register", "task": tid, "attempt": attempt, "files": files})
            return {"ok": True}

    def complete(self, wid, tid, attempt, result, metrics):
        with self.lock:
            self._touch_worker(wid)
            t = self._find_task(tid)
            if not t or t["state"] != "RUNNING" or t["attempt"] != attempt:
                self.log("忽略过期提交: %s 尝试 %d(当前状态 %s/尝试 %s)", tid, attempt,
                         t["state"] if t else "?", t["attempt"] if t else "?")
                return {"ok": False, "stale": True}
            job = self.jobs[t["job"]]
            registered = t["registrations"].get(str(attempt))
            # 1) 先记日志(含结果), 再让输出可见 —— 崩溃恢复可重放, 不会重复
            self.journal.append({"r": "commit", "task": tid, "attempt": attempt,
                                 "result": result, "metrics": metrics,
                                 "registered": registered})
            t["state"] = "DONE"
            t["committed_attempt"] = attempt
            t["result"] = result
            t["metrics"] = metrics or {}
            w = self.workers.get(wid)
            if w:
                w["done"] += 1
                w["current_task"] = None
            self._aggregate(job, t, metrics or {})
            # 2) 输出原子改名到最终目录(幂等)
            if t["kind"] in ("join", "bjoin") and result and result.get("output_tmp"):
                final = os.path.join(self.out_root, job["id"], "part-%05d.jl" % t["index"])
                atomic_rename(result["output_tmp"], final)
                t["output_final"] = final
                self.journal.append({"r": "renamed", "task": tid, "final": final})
            self._gc_attempt_dirs(t)
            self.log("任务 %s 完成(尝试 %d): 读 %s 写 %s 内存峰值 %s",
                     tid, attempt, fmt_bytes((metrics or {}).get("bytes_read")),
                     fmt_bytes((metrics or {}).get("bytes_written")),
                     fmt_bytes((metrics or {}).get("mem_peak")))
            self._maybe_advance(job)
            return {"ok": True}

    def _aggregate(self, job, t, m):
        a = job["actuals"]
        a["read"] += m.get("bytes_read", 0)
        if t["kind"] == "shuffle":
            a["shuffle"] += m.get("bytes_written", 0)
        if t["kind"] in ("join", "bjoin"):
            a["out"] += m.get("bytes_written", 0)
        a["mem_peak"] = max(a["mem_peak"], m.get("mem_peak", 0))
        a["tmp_peak"] = max(a["tmp_peak"], m.get("tmp_used", 0))
        a["l_scans_max"] = max(a["l_scans_max"], m.get("l_scans", 0))

    def fail(self, wid, tid, attempt, error, error_kind=None):
        with self.lock:
            self._touch_worker(wid)
            t = self._find_task(tid)
            if not t or t["state"] != "RUNNING" or t["attempt"] != attempt:
                return {"ok": False, "stale": True}
            job = self.jobs[t["job"]]
            w = self.workers.get(wid)
            if w:
                w["current_task"] = None
            self.log("任务 %s 第 %d 次尝试失败: %s", tid, attempt, error)
            if error_kind in ("mem", "tmp"):
                # 预算超支: 重试无意义, 作业失败并如实报告 —— 预算真的在起作用
                self._set_job_state(job, "FAILED",
                                    "预算强制生效: 任务 %s 超出 %s 预算 (%s)" % (tid, error_kind, error))
                return {"ok": True}
            t["state"] = "PENDING"
            t["worker"] = None
            t["error"] = error
            if t["attempt"] >= MAX_TASK_ATTEMPTS:
                self._set_job_state(job, "FAILED", "任务 %s 超过最大重试次数: %s" % (tid, error))
            return {"ok": True}

    # ---------------- 快照 ----------------
    def snapshot(self):
        with self.lock:
            jobs = []
            for job in sorted(self.jobs.values(), key=lambda j: j["created"]):
                stages = []
                for st in job["stages"]:
                    tasks = []
                    for tid in st["order"]:
                        t = st["tasks"][tid]
                        tasks.append({"id": t["id"], "kind": t["kind"], "state": t["state"],
                                      "attempt": t["attempt"], "worker": t["worker"],
                                      "index": t["index"], "progress": t["progress"],
                                      "metrics": t["metrics"], "error": t["error"]})
                    stages.append({"id": st["id"], "kind": st["kind"], "state": st["state"],
                                   "tasks": tasks})
                jobs.append({"id": job["id"], "name": job["name"], "state": job["state"],
                             "error": job["error"], "plan": job["plan"], "stages": stages,
                             "verify": job["verify"], "actuals": job["actuals"],
                             "spec": job["spec"], "partition_bytes": job["partition_bytes"],
                             "created": job["created"], "finished": job["finished"]})
            return {"workers": list(self.workers.values()), "jobs": jobs,
                    "events": self.events[-120:], "faults": self.fault_cfg,
                    "budgets": {"mem": self.def_mem, "tmp": self.def_tmp}}

    def read_output(self, jid, limit=20):
        job = self.jobs.get(jid)
        if not job:
            return {"lines": []}
        out_dir = os.path.join(self.out_root, jid)
        lines = []
        if os.path.isdir(out_dir):
            for name in sorted(os.listdir(out_dir)):
                if len(lines) >= limit:
                    break
                if not name.endswith(".jl"):
                    continue
                with open(os.path.join(out_dir, name), "rb") as f:
                    for line in f:
                        lines.append(line.decode().rstrip())
                        if len(lines) >= limit:
                            break
        return {"lines": lines, "verify": job["verify"]}


# ---------------- HTTP 层 ----------------

class Handler(BaseHTTPRequestHandler):
    coord = None

    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            from .webui import PAGE
            data = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/api/status":
            self._json(self.coord.snapshot())
        elif self.path.startswith("/api/output/"):
            jid = self.path.split("/")[-1]
            self._json(self.coord.read_output(jid))
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        try:
            body = self._body()
        except Exception:
            self._json({"error": "bad json"}, 400)
            return
        p = self.path
        c = self.coord
        if p == "/api/jobs":
            jid = c.submit_job(body)
            self._json({"job": jid})
        elif p.startswith("/api/jobs/") and p.endswith("/cancel"):
            jid = p.split("/")[3]
            self._json({"ok": c.cancel_job(jid)})
        elif p == "/api/fault":
            c.fault_cfg = body
            c.log("故障注入配置更新: %s", json.dumps(body))
            self._json({"ok": True})
        elif p == "/api/worker/register":
            c.register_worker(body["id"], body.get("mem_budget", 0), body.get("tmp_budget", 0))
            self._json({"ok": True})
        elif p == "/api/task/claim":
            self._json(c.claim(body["worker_id"]))
        elif p == "/api/task/heartbeat":
            self._json(c.heartbeat(body["worker_id"], body["task_id"], body["attempt"],
                                   body.get("progress")))
        elif p == "/api/task/register":
            self._json(c.register_files(body["worker_id"], body["task_id"], body["attempt"],
                                        body.get("files") or []))
        elif p == "/api/task/complete":
            self._json(c.complete(body["worker_id"], body["task_id"], body["attempt"],
                                  body.get("result"), body.get("metrics")))
        elif p == "/api/task/fail":
            self._json(c.fail(body["worker_id"], body["task_id"], body["attempt"],
                              body.get("error", "?"), body.get("error_kind")))
        else:
            self._json({"error": "not found"}, 404)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--root", required=True, help="运行目录(state/tmp/output 都在其下)")
    ap.add_argument("--mem-budget", type=int, default=16 << 20)
    ap.add_argument("--tmp-budget", type=int, default=512 << 20)
    args = ap.parse_args()
    coord = Coordinator(args.root, args.mem_budget, args.tmp_budget)
    Handler.coord = coord
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    coord.log("协调器就绪: http://127.0.0.1:%d  root=%s", args.port, args.root)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
