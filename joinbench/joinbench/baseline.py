"""全量基准: 小数据可以整表装进内存做真值关联, 输出多重集哈希供分布式结果核对。

仅用于小规模核对(这正是它的用途边界); 大数据看预算指标与抽查。
"""
import argparse
import json

from .util import MultisetHash, list_shards, read_jsonl


def baseline_join(left_dir, right_dir):
    """返回 (rows, digest)。S 装内存建哈希, L 流式探测。"""
    build = {}
    for p in list_shards(right_dir):
        for _raw, obj in read_jsonl(p):
            build.setdefault(obj["k"], []).append(obj["v"])
    mh = MultisetHash()
    for p in list_shards(left_dir):
        for _raw, obj in read_jsonl(p):
            vs = build.get(obj["k"])
            if vs:
                for sv in vs:
                    mh.add_obj({"k": obj["k"], "s": sv, "l": obj["v"]})
    return mh.rows, mh.digest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--right", required=True)
    args = ap.parse_args()
    rows, digest = baseline_join(args.left, args.right)
    print(json.dumps({"rows": rows, "hash": digest}))


if __name__ == "__main__":
    main()
