"""合成数据生成: 规模、倾斜度、热点键份额、分片数均可调。确定性(种子固定)。

表 S (维表, 关联键唯一):  {"k": "k00000001", "v": "<payload>"}   x s_rows
表 L (事实表, 键有倾斜):   {"k": <按 zipf/热点分布抽样>, "v": "<payload>"} x l_rows

倾斜模型: 单个热点键占 hot_share 份额(默认 0), 其余按 zipf(skew) 分布。
行按 round-robin 写入各分片, 热点行天然散布在所有分片里(不会只有某个分片热)。
"""
import argparse
import json
import os
import random

from .util import ensure_dir, atomic_write_bytes


def _payload(rng, n):
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(n))


def gen_dataset(root, name, s_rows=2000, l_rows=20000, skew=1.1, hot_share=0.0,
                shards=8, payload=48, seed=7):
    rng = random.Random(seed)
    d = ensure_dir(os.path.join(root, name))
    s_dir = ensure_dir(os.path.join(d, "S"))
    l_dir = ensure_dir(os.path.join(d, "L"))

    keys = ["k%08d" % i for i in range(s_rows)]
    hot_key = keys[0] if hot_share > 0 else None

    # S: 键唯一
    s_files = [open(os.path.join(s_dir, "part-%05d.jl" % i), "wb") for i in range(shards)]
    for i, k in enumerate(keys):
        row = {"k": k, "v": _payload(rng, payload)}
        s_files[i % shards].write((json.dumps(row) + "\n").encode())
    for f in s_files:
        f.close()

    # L: zipf 权重 + 可选热点键。CDF 预计算一次, 逐行二分抽样(O(log n))。
    import bisect
    weights = [1.0 / ((i + 1) ** skew) for i in range(s_rows)]
    if hot_key is not None:
        rest = 1.0 - hot_share
        weights = [w * rest for w in weights]
        weights[0] += hot_share
    cdf = []
    acc = 0.0
    for w in weights:
        acc += w
        cdf.append(acc)
    total = cdf[-1]
    l_files = [open(os.path.join(l_dir, "part-%05d.jl" % i), "wb") for i in range(shards)]
    for i in range(l_rows):
        k = keys[bisect.bisect_left(cdf, rng.random() * total)]
        row = {"k": k, "v": _payload(rng, payload)}
        l_files[i % shards].write((json.dumps(row) + "\n").encode())
    for f in l_files:
        f.close()

    # 真实分布(供页面"估计 vs 实际"对照; 调度不依赖它)
    truth = {"name": name, "s_rows": s_rows, "l_rows": l_rows, "skew": skew,
             "hot_share": hot_share, "hot_key": hot_key, "shards": shards, "seed": seed}
    atomic_write_bytes(os.path.join(d, "truth.json"),
                       json.dumps(truth, indent=1).encode())
    return d


PRESETS = {
    # 小数据: 可全量基准核对; S 很小 -> 预期选广播
    "small": dict(s_rows=2_000, l_rows=30_000, skew=1.2, hot_share=0.0, shards=4),
    # 倾斜大数据: S 超过内存预算 -> 广播不可行; 单热点键 25% -> 预期选热点单独处理
    "skewbig": dict(s_rows=150_000, l_rows=600_000, skew=1.0, hot_share=0.25, shards=8),
    # 均匀大数据: 无热点 -> 预期选分区关联
    "uniform": dict(s_rows=150_000, l_rows=400_000, skew=0.3, hot_share=0.0, shards=8),
    # 故障演练用中等数据
    "faulty": dict(s_rows=40_000, l_rows=200_000, skew=1.0, hot_share=0.15, shards=6),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--preset", choices=sorted(PRESETS))
    ap.add_argument("--name")
    ap.add_argument("--s-rows", type=int)
    ap.add_argument("--l-rows", type=int)
    ap.add_argument("--skew", type=float)
    ap.add_argument("--hot-share", type=float)
    ap.add_argument("--shards", type=int)
    args = ap.parse_args()
    if args.preset:
        cfg = dict(PRESETS[args.preset])
        name = args.name or args.preset
    else:
        cfg = {}
        name = args.name or "custom"
    for k, arg in (("s_rows", args.s_rows), ("l_rows", args.l_rows), ("skew", args.skew),
                   ("hot_share", args.hot_share), ("shards", args.shards)):
        if arg is not None:
            cfg[k] = arg
    d = gen_dataset(args.out, name, **cfg)
    print("generated:", d, json.dumps(cfg))


if __name__ == "__main__":
    main()
