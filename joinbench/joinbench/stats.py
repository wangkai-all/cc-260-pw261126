"""流式表统计: 行数、字节数、近似重 hitter(Space-Saving 素描)。

内存有界: 素描容量固定(默认 5 万项), 不随表大小增长 —— 统计也不整表读入。
"""
import heapq

from .util import read_jsonl


class SpaceSaving:
    """Metwally 等人的 Space-Saving 算法: 容量 C 的 Top-K 频度素描。

    命中直接 +1; 未命中且已满时, 淘汰最小计数项并把计数置为 min+1。
    用懒惰堆维护最小值: O(log C) 每次操作; 堆过大时按 counts 重建。
    """

    def __init__(self, capacity=50_000):
        self.capacity = capacity
        self.counts = {}          # key -> (count, err)
        self._heap = []           # (count, key), 允许过期项

    def add(self, key, n=1):
        c = self.counts
        ent = c.get(key)
        if ent is not None:
            cnt = ent[0] + n
            c[key] = (cnt, ent[1])
            heapq.heappush(self._heap, (cnt, key))
        elif len(c) < self.capacity:
            c[key] = (n, 0)
            heapq.heappush(self._heap, (n, key))
        else:
            # 淘汰真实最小项(跳过过期堆项)
            while True:
                cnt, k = heapq.heappop(self._heap)
                cur = c.get(k)
                if cur is not None and cur[0] == cnt:
                    break
            del c[k]
            c[key] = (cnt + n, cnt)
            heapq.heappush(self._heap, (cnt + n, key))
        if len(self._heap) > 4 * len(c) + 1024:      # 堆膨胀控制
            self._heap = [(v[0], k) for k, v in c.items()]
            heapq.heapify(self._heap)

    def top(self, k):
        return sorted(self.counts.items(), key=lambda kv: -kv[1][0])[:k]


def table_stats(paths, sketch_capacity=50_000, top_k=64, progress=None):
    """流式扫一个表的所有分片, 返回统计 dict。内存占用 = 素描容量, 与表大小无关。"""
    rows = 0
    nbytes = 0
    sketch = SpaceSaving(sketch_capacity)
    for p in paths:
        for raw, obj in read_jsonl(p):
            rows += 1
            nbytes += len(raw) + 1
            sketch.add(obj["k"])
            if progress and rows % 50000 == 0:
                progress(rows, nbytes)
    top = [[k, c] for k, (c, _e) in sketch.top(top_k)]
    return {
        "rows": rows,
        "bytes": nbytes,
        "distinct_lower": len(sketch.counts),
        "sketch_capacity": sketch_capacity,
        "top": top,  # [[key, approx_count], ...] 按频度降序
    }
