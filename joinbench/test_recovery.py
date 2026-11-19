#!/usr/bin/env python3
"""协调器崩溃恢复实测: 作业跑到一半 kill -9 协调器, 重启后作业应继续完成,
输出与全量基准一致(不重复、不丢失)。"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from joinbench import datagen
from joinbench.baseline import baseline_join

ROOT = os.path.join(HERE, "run-recovery")
PORT = 8791
BASE = "http://127.0.0.1:%d" % PORT
ENV = dict(os.environ, PYTHONPATH=HERE)


def post(path, payload=None):
    req = urllib.request.Request(BASE + path, data=json.dumps(payload or {}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.loads(r.read())


def start_coord():
    return subprocess.Popen([sys.executable, "-m", "joinbench.coordinator",
                             "--port", str(PORT), "--root", ROOT,
                             "--mem-budget", str(8 << 20),
                             "--tmp-budget", str(256 << 20)], env=ENV)


def start_worker(wid):
    return subprocess.Popen([sys.executable, "-m", "joinbench.worker",
                             "--id", wid, "--coordinator", BASE,
                             "--state-dir", os.path.join(ROOT, "worker-" + wid)], env=ENV)


def job_state(jid):
    for j in get("/api/status")["jobs"]:
        if j["id"] == jid:
                return j
    return None


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT)
    ds = datagen.gen_dataset(os.path.join(ROOT, "data"), "rec", s_rows=60_000,
                             l_rows=300_000, skew=1.0, hot_share=0.2, shards=6, seed=11)
    _rows, digest = baseline_join(os.path.join(ds, "L"), os.path.join(ds, "S"))
    print("基准:", digest)

    coord = start_coord()
    workers = [start_worker("w1"), start_worker("w2")]
    time.sleep(2.5)
    jid = post("/api/jobs", {"name": "recovery", "left": os.path.join(ds, "L"),
                             "right": os.path.join(ds, "S"),
                             "mem_budget": 2 << 20,   # 小内存迫使走洗牌, 拉长作业窗口
                             "expected": digest})["job"]
    # 等作业真正进入洗牌阶段(有任务处于 RUNNING)再杀协调器
    deadline = time.time() + 60
    while time.time() < deadline:
        j = job_state(jid)
        if j and any(st["kind"] == "shuffle" and
                     any(t["state"] == "RUNNING" for t in st["tasks"])
                     for st in j["stages"]):
            break
        time.sleep(0.3)
    print("kill -9 协调器(洗牌进行中)…")
    coord.kill()
    coord.wait()
    time.sleep(1.0)
    coord = start_coord()
    print("协调器已重启, 等待作业完成 …")
    deadline = time.time() + 180
    j = None
    while time.time() < deadline:
        try:
            j = job_state(jid)
            if j and j["state"] != "RUNNING":
                break
        except Exception:
            pass
        time.sleep(1.0)
    ok = (j and j["state"] == "DONE" and j["verify"] and j["verify"]["match"] is True)
    print("作业状态:", j["state"] if j else "?",
          "核对:", j["verify"]["match"] if j and j["verify"] else "?")
    print("RECOVERY-TEST:", "PASS" if ok else "FAIL")
    coord.terminate()
    for w in workers:
        w.terminate()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
