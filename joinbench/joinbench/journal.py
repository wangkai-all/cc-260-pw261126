"""协调器日志: append-only JSONL, 每条 fsync。崩溃恢复时重放。

关键不变式: 先写日志, 后做可见副作用(如输出改名)。
恢复时按日志重放副作用, 改名幂等, 不会重复输出。
"""
import json
import os

from .util import ensure_dir


class Journal:
    def __init__(self, path):
        self.path = path
        ensure_dir(os.path.dirname(path))
        self.f = open(path, "ab")

    def append(self, rec):
        rec = dict(rec)
        data = (json.dumps(rec, separators=(",", ":")) + "\n").encode()
        self.f.write(data)
        self.f.flush()
        os.fsync(self.f.fileno())

    def close(self):
        try:
            self.f.close()
        except Exception:
            pass

    @staticmethod
    def replay(path):
        if not os.path.exists(path):
            return []
        out = []
        with open(path, "rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    break  # 最后一条写了一半(崩溃): 丢弃, 其副作用会被重放逻辑安全重做
        return out
