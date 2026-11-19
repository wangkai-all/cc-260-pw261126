"""核心正确性测试: 倾斜关联 == 朴素全量关联; 溢写兜底; 崩溃不重复; 清理守卫。

运行: python3 -m unittest discover -s tests -v
"""
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from joinbench import datagen
from joinbench.baseline import baseline_join
from joinbench.engine import RUNNERS, Budget, Faults
from joinbench.stats import table_stats
from joinbench import planner
from joinbench.util import (MultisetHash, list_shards, read_jsonl, rm_tree_guarded)

MEM = 4 << 20
TMP = 64 << 20


def h0(_m):
    return None


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="jb-test-")
        self.faults = Faults(os.path.join(self.dir, "fired.json"))
        self.ds = datagen.gen_dataset(self.dir, "t", s_rows=500, l_rows=8000,
                                      skew=1.2, hot_share=0.3, shards=3, seed=3)
        self.L = list_shards(os.path.join(self.ds, "L"))
        self.S = list_shards(os.path.join(self.ds, "S"))
        self.exp_rows, self.exp_digest = baseline_join(os.path.join(self.ds, "L"),
                                                       os.path.join(self.ds, "S"))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def hash_dir(self, d):
        mh = MultisetHash()
        for name in sorted(os.listdir(d)):
            for _raw, obj in read_jsonl(os.path.join(d, name)):
                mh.add_obj(obj)
        return mh.digest()

    def plan(self, mem=MEM, partitions=None):
        sl = table_stats(self.L)
        ss = table_stats(self.S)
        return planner.estimate_plans(sl, ss, mem, TMP, len(self.L), 2,
                                      partitions_override=partitions), sl, ss


class TestPlans(Base):
    def test_skew_plan_matches_baseline(self):
        """完整走一遍 skew 方案(洗牌+热点广播+加盐), 输出必须等于全量基准。

        规划用小内存预算让广播不可行, 从而走到 skew 方案; 执行用正常预算。
        """
        plan, _sl, _ss = self.plan(mem=16 << 10)
        self.assertEqual(plan["chosen"], "skew")
        hot = plan["hot_keys"]
        P = plan["partitions"]
        tmp = os.path.join(self.dir, "tmp")
        # 热点广播文件
        hot_file = os.path.join(tmp, "hot.jl")
        os.makedirs(tmp, exist_ok=True)
        with open(hot_file, "wb") as f:
            for p in self.S:
                for raw, obj in read_jsonl(p):
                    if obj["k"] in set(hot):
                        f.write(raw + b"\n")
        # 洗牌(模拟: 某个任务第一次"写完没登记就崩", 第二次尝试才被采用)
        manifest = {}
        i = 0
        for table, shards in (("L", self.L), ("S", self.S)):
            for shard in shards:
                tid = "t%d" % i
                i += 1
                spec = {"task_id": tid, "table": table, "inputs": [shard],
                        "num_partitions": P, "hot_keys": hot}
                if i == 2:
                    # 尝试 1: 写完文件, 不登记(崩溃) —— 孤儿文件留在磁盘上
                    spec1 = dict(spec, out_dir=os.path.join(tmp, tid, "attempt-1"))
                    RUNNERS["shuffle"](spec1, Budget(MEM, TMP), h0, self.faults)
                # 尝试 2: 重新执行并登记
                spec2 = dict(spec, out_dir=os.path.join(tmp, tid, "attempt-2"))
                res, _m = RUNNERS["shuffle"](spec2, Budget(MEM, TMP), h0, self.faults)
                manifest[tid] = res["files"]
        # 关联: 只读"已登记"的文件
        out = os.path.join(self.dir, "out")
        os.makedirs(out, exist_ok=True)
        s_files = [[] for _ in range(P)]
        l_files = [[] for _ in range(P)]
        for files in manifest.values():
            for f in files:
                (s_files if f["table"] == "S" else l_files)[f["partition"]].append(f["path"])
        for p in range(P):
            spec = {"task_id": "j%d" % p, "partition": p, "s_files": s_files[p],
                    "l_files": l_files[p], "broadcast_file": hot_file,
                    "output_tmp": os.path.join(out, "part-%05d.jl" % p)}
            RUNNERS["join"](spec, Budget(MEM, TMP), h0, self.faults)
        got = self.hash_dir(out)
        self.assertEqual(got, self.exp_digest,
                         "倾斜关联输出 != 全量基准(有重复或丢失)")
        self.assertEqual(got["rows"], self.exp_rows)

    def test_broadcast_join_matches_baseline(self):
        out = os.path.join(self.dir, "out")
        os.makedirs(out, exist_ok=True)
        for i, shard in enumerate(self.L):
            spec = {"task_id": "b%d" % i, "inputs": [shard], "s_files": self.S,
                    "output_tmp": os.path.join(out, "part-%05d.jl" % i)}
            RUNNERS["bjoin"](spec, Budget(MEM, TMP), h0, self.faults)
        self.assertEqual(self.hash_dir(out), self.exp_digest)

    def test_spill_join_matches_baseline(self):
        """内存预算压到 S 一块装不下: 分块重扫, 结果仍等于基准。

        单独生成 S 较大的数据集, 强制 1 个分区, 使 S 分区(>1MiB)远超内存块。
        """
        ds = datagen.gen_dataset(self.dir, "spill", s_rows=20_000, l_rows=20_000,
                                 skew=0.5, hot_share=0.0, shards=2, seed=5)
        L = list_shards(os.path.join(ds, "L"))
        S = list_shards(os.path.join(ds, "S"))
        _rows, digest = baseline_join(os.path.join(ds, "L"), os.path.join(ds, "S"))
        P = 1
        tmp = os.path.join(self.dir, "tmp2")
        manifest = {}
        i = 0
        for table, shards in (("L", L), ("S", S)):
            for shard in shards:
                spec = {"task_id": "t%d" % i, "table": table, "inputs": [shard],
                        "num_partitions": P, "hot_keys": [],
                        "out_dir": os.path.join(tmp, "t%d" % i, "attempt-1")}
                i += 1
                res, _m = RUNNERS["shuffle"](spec, Budget(MEM, TMP), h0, self.faults)
                manifest[spec["task_id"]] = res["files"]
        out = os.path.join(self.dir, "out2")
        os.makedirs(out, exist_ok=True)
        tiny = 256 * 1024  # 256KiB << S 分区 ~1.3MiB
        s_files = [f["path"] for files in manifest.values() for f in files if f["table"] == "S"]
        l_files = [f["path"] for files in manifest.values() for f in files if f["table"] == "L"]
        spec = {"task_id": "j0", "partition": 0, "s_files": s_files,
                "l_files": l_files, "broadcast_file": None,
                "output_tmp": os.path.join(out, "part-00000.jl")}
        _res, m = RUNNERS["join"](spec, Budget(tiny, TMP), h0, self.faults)
        self.assertTrue(m["l_scans"] > 1, "小预算应触发分块重扫, 实际 l_scans=%s" % m["l_scans"])
        mh = MultisetHash()
        for _raw, obj in read_jsonl(os.path.join(out, "part-00000.jl")):
            mh.add_obj(obj)
        self.assertEqual(mh.digest(), digest)

    def test_skew_plus_spill_matches_baseline(self):
        """回归: 热点广播表 + S 分块重扫同时发生时, 热点行不得重复输出。"""
        ds = datagen.gen_dataset(self.dir, "skewspill", s_rows=20_000, l_rows=40_000,
                                 skew=1.0, hot_share=0.3, shards=2, seed=9)
        L = list_shards(os.path.join(ds, "L"))
        S = list_shards(os.path.join(ds, "S"))
        _rows, digest = baseline_join(os.path.join(ds, "L"), os.path.join(ds, "S"))
        sl, ss = table_stats(L), table_stats(S)
        plan = planner.estimate_plans(sl, ss, 16 << 10, TMP, len(L), 2,
                                      partitions_override=1)
        hot = plan["hot_keys"]
        self.assertTrue(hot, "应检测到热点键")
        P = 1
        tmp = os.path.join(self.dir, "tmp3")
        os.makedirs(tmp, exist_ok=True)
        hot_file = os.path.join(tmp, "hot.jl")
        with open(hot_file, "wb") as f:
            for p in S:
                for raw, obj in read_jsonl(p):
                    if obj["k"] in set(hot):
                        f.write(raw + b"\n")
        manifest = {}
        i = 0
        for table, shards in (("L", L), ("S", S)):
            for shard in shards:
                spec = {"task_id": "t%d" % i, "table": table, "inputs": [shard],
                        "num_partitions": P, "hot_keys": hot,
                        "out_dir": os.path.join(tmp, "t%d" % i, "attempt-1")}
                i += 1
                res, _m = RUNNERS["shuffle"](spec, Budget(MEM, TMP), h0, self.faults)
                manifest[spec["task_id"]] = res["files"]
        out = os.path.join(self.dir, "out3")
        os.makedirs(out, exist_ok=True)
        tiny = 256 * 1024  # S 冷分区 ~1MiB -> 多块; 热点广播表同时在用
        s_files = [f["path"] for files in manifest.values() for f in files if f["table"] == "S"]
        l_files = [f["path"] for files in manifest.values() for f in files if f["table"] == "L"]
        spec = {"task_id": "j0", "partition": 0, "s_files": s_files,
                "l_files": l_files, "broadcast_file": hot_file,
                "output_tmp": os.path.join(out, "part-00000.jl")}
        _res, m = RUNNERS["join"](spec, Budget(tiny, TMP), h0, self.faults)
        self.assertTrue(m["l_scans"] > 1, "应触发分块重扫")
        mh = MultisetHash()
        for _raw, obj in read_jsonl(os.path.join(out, "part-00000.jl")):
            mh.add_obj(obj)
        self.assertEqual(mh.digest(), digest)

    def test_budget_enforced(self):
        """临时盘预算小于洗牌量: 必须抛 BudgetExceeded, 而不是静默超支。"""
        from joinbench.engine import BudgetExceeded
        spec = {"task_id": "t0", "table": "L", "inputs": self.L,
                "num_partitions": 4, "hot_keys": [],
                "out_dir": os.path.join(self.dir, "tmp", "t0", "attempt-1")}
        with self.assertRaises(BudgetExceeded) as cm:
            RUNNERS["shuffle"](spec, Budget(MEM, 1024), h0, self.faults)
        self.assertEqual(cm.exception.kind, "tmp")


class TestGuards(unittest.TestCase):
    def test_cleanup_guard(self):
        d = tempfile.mkdtemp(prefix="jb-guard-")
        try:
            tmp_root = os.path.join(d, "tmp")
            data = os.path.join(d, "data")
            os.makedirs(os.path.join(tmp_root, "j1"))
            os.makedirs(data)
            # 允许: 删 tmp 根下的作业目录
            self.assertTrue(rm_tree_guarded(os.path.join(tmp_root, "j1"), tmp_root))
            # 拒绝: 删共享输入、tmp 根本身、根目录
            for bad in (data, tmp_root, d, "/"):
                with self.assertRaises(PermissionError):
                    rm_tree_guarded(bad, tmp_root)
            self.assertTrue(os.path.isdir(data))
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_multiset_hash(self):
        a, b = MultisetHash(), MultisetHash()
        rows = [{"k": "k1", "s": "x", "l": "y"}, {"k": "k2", "s": "p", "l": "q"}]
        for r in rows:
            a.add_obj(r)
        for r in reversed(rows):           # 顺序无关
            b.add_obj(r)
        self.assertEqual(a.digest(), b.digest())
        b.add_obj(rows[0])                  # 多一行必须被发现
        self.assertNotEqual(a.digest(), b.digest())


if __name__ == "__main__":
    unittest.main()
