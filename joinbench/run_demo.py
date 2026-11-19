#!/usr/bin/env python3
"""端到端演示: 起 1 个协调器 + 2 个独立工作进程, 跑全部场景并核对结果。

场景:
  small    小数据 + 全量基准核对(预期选广播)
  skewbig  倾斜大数据(预期选热点单独处理), 观察分片均衡与预算
  uniform  均匀大数据(预期选分区关联)
  fault    洗牌写完未登记崩溃 + 输出写完未提交崩溃, 恢复后结果仍与基准一致(不重复)
  cancel   两作业共享输入, 取消其一: 只清自己的临时目录, 另一个照常完成
  budget   临时盘预算超支 -> 作业如实失败; 内存预算收紧 -> 分块重扫兜底成功
  serve    只起集群并提交演示作业, 保持运行供浏览器访问

用法: python3 run_demo.py [--scenario all|small|...] [--root DIR] [--port 8788]
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from joinbench import datagen                       # noqa: E402
from joinbench.baseline import baseline_join        # noqa: E402
from joinbench.util import fmt_bytes                # noqa: E402

MEM = 8 << 20          # 工作进程内存预算 8 MiB
TMP = 256 << 20        # 临时盘预算 256 MiB


def post(base, path, payload=None, timeout=10.0):
    req = urllib.request.Request(base + path, data=json.dumps(payload or {}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get(base, path, timeout=10.0):
    with urllib.request.urlopen(base + path, timeout=timeout) as r:
        return json.loads(r.read())


class Proc:
    """带重启监督的子进程。"""

    def __init__(self, name, cmd, env=None):
        self.name, self.cmd, self.env = name, cmd, env
        self.p = None
        self.restarts = 0
        self.stopped = False
        self._devnull = open(os.devnull, "w") if QUIET else None

    def start(self):
        self.p = subprocess.Popen(self.cmd, env=self.env,
                                  stdout=self._devnull, stderr=subprocess.STDOUT)
        return self

    def poll_restart(self):
        if self.stopped:
            return
        rc = self.p.poll()
        if rc is not None:
            self.restarts += 1
            print("  [supervisor] %s 退出(rc=%s), 重启(#%d)" % (self.name, rc, self.restarts),
                  flush=True)
            time.sleep(0.3)
            self.start()

    def stop(self):
        self.stopped = True
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(5)
            except subprocess.TimeoutExpired:
                self.p.kill()


QUIET = False


class Cluster:
    def __init__(self, root, port):
        self.root = root
        self.port = port
        self.base = "http://127.0.0.1:%d" % port
        self.procs = []
        env = dict(os.environ)
        env["PYTHONPATH"] = HERE + os.pathsep + env.get("PYTHONPATH", "")
        coord = [sys.executable, "-m", "joinbench.coordinator",
                 "--port", str(port), "--root", root,
                 "--mem-budget", str(MEM), "--tmp-budget", str(TMP)]
        self.procs.append(Proc("coordinator", coord, env))
        for i in (1, 2):
            cmd = [sys.executable, "-m", "joinbench.worker",
                   "--id", "w%d" % i, "--coordinator", self.base,
                   "--state-dir", os.path.join(root, "worker-w%d" % i),
                   "--mem-budget", str(MEM), "--tmp-budget", str(TMP)]
            self.procs.append(Proc("w%d" % i, cmd, env))

    def __enter__(self):
        for p in self.procs:
            p.start()
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                s = get(self.base, "/api/status")
                if len([w for w in s["workers"] if w["state"] == "alive"]) >= 2:
                    return self
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError("集群启动超时")

    def supervise(self):
        for p in self.procs:
            p.poll_restart()

    def __exit__(self, *a):
        for p in self.procs:
            p.stop()


def wait_job(base, jid, timeout=300, supervise=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if supervise:
            supervise()
        s = get(base, "/api/status")
        for j in s["jobs"]:
            if j["id"] == jid and j["state"] != "RUNNING":
                return j
        time.sleep(0.5)
    raise TimeoutError("作业 %s 超时" % jid)


def submit(base, name, ds, mem=MEM, tmp=TMP, partitions=None, expected=None):
    spec = {"name": name, "left": os.path.join(ds, "L"), "right": os.path.join(ds, "S"),
            "mem_budget": mem, "tmp_budget": tmp}
    if partitions:
        spec["partitions"] = partitions
    if expected:
        spec["expected"] = expected
    return post(base, "/api/jobs", spec)["job"]


def expect(cond, msg):
    print(("  ✓ " if cond else "  ✗ ") + msg, flush=True)
    return bool(cond)


RESULTS = []


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(("==> [%s] %s %s" % ("PASS" if ok else "FAIL", name, detail)), flush=True)


def job_by_id(base, jid):
    return next(j for j in get(base, "/api/status")["jobs"] if j["id"] == jid)


# ---------------- 场景 ----------------

def sc_small(cl, data):
    base = cl.base
    print("\n--- 场景 small: 小数据 + 全量基准核对 ---", flush=True)
    ds = os.path.join(data, "small")
    rows, digest = baseline_join(os.path.join(ds, "L"), os.path.join(ds, "S"))
    print("  全量基准: %d 行, hash=%s/%s" % (rows, digest["s1"], digest["s2"]))
    jid = submit(base, "small-广播核对", ds, expected=digest)
    j = wait_job(base, jid, supervise=cl.supervise)
    ok = expect(j["state"] == "DONE", "作业完成") & \
        expect(j["plan"]["chosen"] == "broadcast", "S 很小 -> 选定广播小表") & \
        expect(j["verify"]["match"] is True, "分布式输出 == 全量基准(多重集哈希一致)") & \
        expect(j["actuals"]["shuffle"] == 0, "广播方案零洗牌零临时盘")
    record("small", ok, "行数 %d" % rows)


def sc_skewbig(cl, data):
    base = cl.base
    print("\n--- 场景 skewbig: 倾斜大数据, 热点键不拖死全局 ---", flush=True)
    ds = os.path.join(data, "skewbig")
    jid = submit(base, "skewbig-倾斜", ds)
    j = wait_job(base, jid, supervise=cl.supervise)
    plan = j["plan"]
    pb = j["partition_bytes"] or []
    mx = max((p["S"] + p["L"]) for p in pb) if pb else 0
    avg = (sum(p["S"] + p["L"] for p in pb) / len(pb)) if pb else 1
    naive_est = next(p for p in plan["plans"] if p["name"] == "partitioned")
    ok = expect(j["state"] == "DONE", "作业完成") & \
        expect(plan["chosen"] == "skew", "热点键占 L 25%% -> 选定热点单独处理") & \
        expect(mx < avg * 2.5, "分片均衡: 最大分区 %s < 2.5×均值 %s(热点被加盐摊开)" %
               (fmt_bytes(mx), fmt_bytes(avg))) & \
        expect(j["actuals"]["mem_peak"] <= j["spec"]["mem_budget"],
               "内存峰值 %s ≤ 预算 %s" % (fmt_bytes(j["actuals"]["mem_peak"]),
                                          fmt_bytes(j["spec"]["mem_budget"]))) & \
        expect(j["actuals"]["tmp_peak"] <= j["spec"]["tmp_budget"],
               "临时盘峰值 %s ≤ 预算 %s" % (fmt_bytes(j["actuals"]["tmp_peak"]),
                                            fmt_bytes(j["spec"]["tmp_budget"])))
    print("  对照: 若普通分区关联, 热点键将使单任务处理 ~%s(估计), 实际最大分区只有 %s"
          % (fmt_bytes(naive_est["est_max_task"]), fmt_bytes(mx)))
    record("skewbig", ok, "最大/均值分区 = %.2f" % (mx / max(1, avg)))


def sc_uniform(cl, data):
    base = cl.base
    print("\n--- 场景 uniform: 均匀大数据 ---", flush=True)
    ds = os.path.join(data, "uniform")
    jid = submit(base, "uniform-均匀", ds)
    j = wait_job(base, jid, supervise=cl.supervise)
    ok = expect(j["state"] == "DONE", "作业完成") & \
        expect(j["plan"]["chosen"] == "partitioned", "无热点且 S 超内存 -> 选定分区关联")
    record("uniform", ok)


def sc_fault(cl, data):
    base = cl.base
    print("\n--- 场景 fault: 崩溃恢复不重复输出 ---", flush=True)
    ds = os.path.join(data, "faulty")
    rows, digest = baseline_join(os.path.join(ds, "L"), os.path.join(ds, "S"))
    print("  全量基准: %d 行" % rows)
    # 小内存预算迫使走洗牌; 两个故障点各炸一次:
    # 1) 洗牌文件写完、登记之前崩溃  2) 输出写完、提交之前崩溃
    post(base, "/api/fault", {"points": {"after_shuffle_write": "*",
                                         "before_output_commit": "*"}})
    jid = submit(base, "fault-崩溃恢复", ds, mem=2 << 20, expected=digest)
    j = wait_job(base, jid, timeout=600, supervise=cl.supervise)
    post(base, "/api/fault", {})
    attempts = sum(t["attempt"] for st in j["stages"] for t in st["tasks"])
    ntasks = sum(len(st["tasks"]) for st in j["stages"])
    ok = expect(j["state"] == "DONE", "注入崩溃后作业仍完成(尝试 %d 次 / 任务 %d 个)"
                % (attempts, ntasks)) & \
        expect(attempts > ntasks, "确实发生了崩溃重试") & \
        expect(j["verify"]["match"] is True,
               "恢复后输出与基准一致: 没有重复、没有丢失")
    record("fault", ok, "尝试 %d/%d" % (attempts, ntasks))


def sc_cancel(cl, data):
    base = cl.base
    print("\n--- 场景 cancel: 取消清理不碰共享输入 ---", flush=True)
    ds = os.path.join(data, "skewbig")
    before = sorted(os.listdir(ds))
    jA = submit(base, "cancel-将被取消", ds)
    jB = submit(base, "cancel-共享同输入", ds)
    # 等 A 进入洗牌(产生了自己的临时文件)再取消
    deadline = time.time() + 120
    while time.time() < deadline:
        a = job_by_id(base, jA)
        if any(st["kind"] == "shuffle" and
               any(t["state"] in ("RUNNING", "DONE") for t in st["tasks"])
               for st in a["stages"]):
            break
        time.sleep(0.3)
    tmpA = os.path.join(cl.root, "tmp", jA)
    tmpB = os.path.join(cl.root, "tmp", jB)
    had_tmp = os.path.isdir(tmpA)
    post(base, "/api/jobs/%s/cancel" % jA)
    time.sleep(2.0)
    a = job_by_id(base, jA)
    gone = not os.path.exists(tmpA)
    b_alive = os.path.isdir(tmpB) or job_by_id(base, jB)["state"] == "DONE"
    jB_done = wait_job(base, jB, supervise=cl.supervise)
    after = sorted(os.listdir(ds))
    ok = expect(a["state"] == "CANCELLED", "作业 A 已取消") & \
        expect(had_tmp and gone, "A 的临时目录已清理(取消前存在: %s)" % had_tmp) & \
        expect(b_alive, "B 的临时目录未被误伤") & \
        expect(jB_done["state"] == "DONE", "共享同一份输入的 B 照常完成") & \
        expect(before == after, "共享输入 data/ 一个字节都没动")
    record("cancel", ok)


def sc_budget(cl, data):
    base = cl.base
    print("\n--- 场景 budget: 预算是硬约束 ---", flush=True)
    ds = os.path.join(data, "uniform")
    # 1) 临时盘预算远小于洗牌量 -> 必须失败且如实报告
    jid = submit(base, "budget-临时盘超限", ds, tmp=4 << 20)
    j = wait_job(base, jid, supervise=cl.supervise)
    ok = expect(j["state"] == "FAILED" and "预算" in (j["error"] or ""),
                "临时盘预算 4MiB << 洗牌量 -> 作业失败: %s" % (j["error"] or "")[:60])
    # 2) 内存预算收紧 + 强制 1 个分区 -> S 装不下, 分块重扫兜底, 结果仍正确
    ds2 = os.path.join(data, "faulty")
    rows, digest = baseline_join(os.path.join(ds2, "L"), os.path.join(ds2, "S"))
    jid2 = submit(base, "budget-内存兜底", ds2, mem=2 << 20, partitions=1, expected=digest)
    j2 = wait_job(base, jid2, timeout=600, supervise=cl.supervise)
    ok2 = expect(j2["state"] == "DONE", "内存 2MiB + 单分区 -> 仍完成") & \
        expect(j2["actuals"]["l_scans_max"] > 1,
               "S 被分块, L 被重扫 %d 遍(预算生效的直接证据)" % j2["actuals"]["l_scans_max"]) & \
        expect(j2["verify"]["match"] is True, "分块重扫结果与基准一致") & \
        expect(j2["actuals"]["mem_peak"] <= j2["spec"]["mem_budget"],
               "内存峰值 %s ≤ 预算 %s" % (fmt_bytes(j2["actuals"]["mem_peak"]),
                                          fmt_bytes(j2["spec"]["mem_budget"])))
    record("budget", ok and ok2)


SCENARIOS = {}


def scenario(name):
    def deco(f):
        SCENARIOS[name] = f
        return f
    return deco


scenario("small")(sc_small)
scenario("skewbig")(sc_skewbig)
scenario("uniform")(sc_uniform)
scenario("fault")(sc_fault)
scenario("cancel")(sc_cancel)
scenario("budget")(sc_budget)

ORDER = ["small", "skewbig", "uniform", "fault", "cancel", "budget"]


def main():
    global QUIET
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default="all",
                    choices=["all", "serve"] + ORDER)
    ap.add_argument("--root", default=os.path.join(HERE, "run"))
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--quiet", action="store_true", help="子进程输出丢到 /dev/null")
    args = ap.parse_args()
    QUIET = args.quiet
    data = os.path.join(args.root, "data")

    if args.scenario == "all":
        shutil.rmtree(args.root, ignore_errors=True)
    os.makedirs(data, exist_ok=True)
    print("生成合成数据(规模/倾斜可调, 见 joinbench/datagen.py)…", flush=True)
    for name in ("small", "skewbig", "uniform", "faulty"):
        if not os.path.isdir(os.path.join(data, name)):
            datagen.gen_dataset(data, name, **datagen.PRESETS[name])
            print("  %s: %s" % (name, datagen.PRESETS[name]))

    with Cluster(args.root, args.port) as cl:
        print("集群就绪: 协调器 + 2 工作进程, 页面 http://127.0.0.1:%d" % args.port, flush=True)
        if args.scenario == "serve":
            ds = os.path.join(data, "skewbig")
            submit(cl.base, "serve-演示", ds)
            print("已提交演示作业, Ctrl-C 退出。", flush=True)
            try:
                while True:
                    time.sleep(1)
                    cl.supervise()
            except KeyboardInterrupt:
                pass
            return
        todo = ORDER if args.scenario == "all" else [args.scenario]
        for name in todo:
            cl.supervise()
            SCENARIOS[name](cl, data)

    print("\n================ 汇总 ================", flush=True)
    allok = True
    for name, ok, detail in RESULTS:
        allok &= ok
        print("  [%s] %-8s %s" % ("PASS" if ok else "FAIL", name, detail))
    print("页面与日志目录: %s (协调器已停止)" % args.root)
    sys.exit(0 if allok else 1)


if __name__ == "__main__":
    main()
