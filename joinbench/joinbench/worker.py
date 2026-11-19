"""独立工作进程: 从协调器拉任务, 在预算内执行, 上报心跳/登记/提交。

崩溃即进程退出(os._exit 模拟硬崩溃, 或真的被杀); 由外部 supervisor 重启。
重启后重新注册、重新拉任务 —— 协调器靠租约+尝试号保证不重复输出。
"""
import argparse
import json
import os
import shutil
import sys
import time
import urllib.request

from .engine import RUNNERS, Budget, BudgetExceeded, Faults, TaskCancelled


def post(base, path, payload, timeout=5.0):
    req = urllib.request.Request(base + path, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


class Worker:
    def __init__(self, wid, base, state_dir, mem_budget=0, tmp_budget=0):
        self.id = wid
        self.base = base
        self.state_dir = state_dir
        self.mem_budget = mem_budget
        self.tmp_budget = tmp_budget
        os.makedirs(state_dir, exist_ok=True)
        self.faults = Faults(os.path.join(state_dir, "fired.json"))

    def log(self, msg, *a):
        print("[%s %s] %s" % (time.strftime("%H:%M:%S"), self.id, msg % a if a else msg),
              flush=True)

    def cleanup_attempt(self, spec):
        """放弃一次尝试: 删掉自己的 attempt 目录(只删 attempt-* 目录, 不碰别的)。"""
        d = spec.get("out_dir") or os.path.dirname(spec.get("output_tmp") or "")
        if d and "attempt-" in os.path.basename(d) and os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)

    def run_task(self, spec, mem_budget, tmp_budget):
        budget = Budget(mem_budget, tmp_budget)
        tid = spec["task_id"]
        attempt = spec["attempt"]
        cancelled = {"flag": False}
        last_hb = {"t": 0.0}

        def hb(metrics):
            now = time.time()
            if now - last_hb["t"] < 0.5:
                return
            last_hb["t"] = now
            prog = dict(metrics or {})
            prog["mem_peak"] = budget.mem_peak
            prog["tmp_used"] = budget.tmp_used
            try:
                r = post(self.base, "/api/task/heartbeat",
                         {"worker_id": self.id, "task_id": tid, "attempt": attempt,
                          "progress": prog})
                if r.get("cancel"):
                    cancelled["flag"] = True
                    raise TaskCancelled()
            except TaskCancelled:
                raise
            except Exception:
                pass  # 协调器暂时不可达: 继续干, 提交时会见分晓

        self.faults.maybe_slow()
        runner = RUNNERS[spec["kind"]]
        result, metrics = runner(spec, budget, hb, self.faults)
        metrics = dict(metrics or {})
        metrics["mem_peak"] = budget.mem_peak
        metrics["tmp_used"] = budget.tmp_used
        if spec["kind"] == "shuffle":
            r = post(self.base, "/api/task/register",
                     {"worker_id": self.id, "task_id": tid, "attempt": attempt,
                      "files": result["files"]})
            if not r.get("ok"):
                raise TaskCancelled("stale register")
        r = post(self.base, "/api/task/complete",
                 {"worker_id": self.id, "task_id": tid, "attempt": attempt,
                  "result": result, "metrics": metrics})
        if not r.get("ok"):
            raise TaskCancelled("stale complete")
        return metrics

    def _register(self):
        post(self.base, "/api/worker/register",
             {"id": self.id, "mem_budget": self.mem_budget,
              "tmp_budget": self.tmp_budget})

    def loop(self):
        self.log("启动, 协调器=%s", self.base)
        while True:
            try:
                self._register()
                break
            except Exception:
                time.sleep(0.5)
        while True:
            try:
                r = post(self.base, "/api/task/claim", {"worker_id": self.id})
            except Exception:
                time.sleep(1.0)
                continue
            if r.get("error") == "unknown worker":
                # 协调器可能重启过, 内存里的注册表丢了 —— 重新注册
                try:
                    self._register()
                except Exception:
                    pass
                continue
            spec = r.get("task")
            if not spec:
                time.sleep(0.4)
                continue
            self.faults.update(r.get("fault_cfg"))
            tid = spec["task_id"]
            self.log("领取任务 %s(%s) 尝试 %d", tid, spec["kind"], spec["attempt"])
            try:
                m = self.run_task(spec, r["mem_budget"], r["tmp_budget"])
                self.log("任务 %s 完成: 读 %d 行, 内存峰值 %dB, 临时盘 %dB",
                         tid, m.get("rows_in", 0), m.get("mem_peak", 0), m.get("tmp_used", 0))
            except TaskCancelled as e:
                self.log("任务 %s 被取消/过期(%s), 清理本次尝试目录", tid, e)
                self.cleanup_attempt(spec)
            except BudgetExceeded as e:
                self.log("任务 %s 超出 %s 预算: %s", tid, e.kind, e)
                try:
                    post(self.base, "/api/task/fail",
                         {"worker_id": self.id, "task_id": tid, "attempt": spec["attempt"],
                          "error": str(e), "error_kind": e.kind})
                except Exception:
                    pass
                self.cleanup_attempt(spec)
            except Exception as e:
                self.log("任务 %s 异常: %r", tid, e)
                try:
                    post(self.base, "/api/task/fail",
                         {"worker_id": self.id, "task_id": tid, "attempt": spec["attempt"],
                          "error": repr(e)})
                except Exception:
                    pass
                self.cleanup_attempt(spec)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--coordinator", required=True)
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--mem-budget", type=int, default=0, help="仅用于页面展示")
    ap.add_argument("--tmp-budget", type=int, default=0)
    args = ap.parse_args()
    Worker(args.id, args.coordinator, args.state_dir,
           args.mem_budget, args.tmp_budget).loop()


if __name__ == "__main__":
    main()
