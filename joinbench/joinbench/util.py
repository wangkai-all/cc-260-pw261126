"""基础工具: 原子写、fsync、路径守卫、多重集哈希、JSONL 流式读取。"""
import hashlib
import json
import os
import shutil
import time


def now():
    return time.time()


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def fsync_dir(path):
    """fsync 目录本身, 保证目录项(新建/改名)落盘。"""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path, data: bytes):
    """写临时文件 + fsync + 原子改名, 崩溃不会留下半个文件。"""
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path))


def atomic_rename(src, dst):
    """提交输出: 同分区原子改名。重复执行幂等(恢复时可能重放)。"""
    ensure_dir(os.path.dirname(dst))
    os.replace(src, dst)
    fsync_dir(os.path.dirname(dst))


def is_under(path, root):
    """路径守卫: 清理只允许发生在指定根目录之内。"""
    rp = os.path.realpath(path)
    rr = os.path.realpath(root)
    return rp == rr or rp.startswith(rr.rstrip(os.sep) + os.sep)


def rm_tree_guarded(path, must_be_under):
    """删除目录树, 但必须位于 must_be_under 之内 —— 防止取消任务时误删共享输入。"""
    if not os.path.exists(path):
        return False
    if not is_under(path, must_be_under) or os.path.realpath(path) == os.path.realpath(must_be_under):
        raise PermissionError("refuse to delete %s: not strictly under %s" % (path, must_be_under))
    shutil.rmtree(path)
    return True


def read_jsonl(path):
    """流式读 JSONL, 逐行 yield (raw_bytes, obj)。绝不整表读入。"""
    with open(path, "rb") as f:
        for line in f:
            line = line.rstrip(b"\n")
            if line:
                yield line, json.loads(line)


def count_lines_bytes(path):
    rows = 0
    b = 0
    with open(path, "rb") as f:
        for line in f:
            if line.strip():
                rows += 1
                b += len(line)
    return rows, b


def list_shards(d):
    """列出一个表目录下的所有分片文件(排序, 确定性)。"""
    out = []
    for name in sorted(os.listdir(d)):
        p = os.path.join(d, name)
        if os.path.isfile(p) and name.endswith(".jl"):
            out.append(p)
    return out


def stable_key_hash(key: str) -> int:
    """跨进程稳定的键哈希。绝不能用水 Python 内置 hash()(每进程随机盐)。"""
    return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest()[:8], "big")


class MultisetHash:
    """顺序无关的多重集哈希: 用于结果核对(分布式输出 vs 全量基准)。

    digest = (行数, 每行 sha256 前 8 字节之和 mod 2^64, 每行哈希平方和 mod 2^64)
    两个分量降低偶然碰撞; 内存 O(1), 可流式计算。
    """

    MOD = 1 << 64

    def __init__(self):
        self.rows = 0
        self.s1 = 0
        self.s2 = 0

    def add_obj(self, obj):
        canon = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.add_bytes(canon)

    def add_bytes(self, b: bytes):
        h = int.from_bytes(hashlib.sha256(b).digest()[:8], "big")
        self.rows += 1
        self.s1 = (self.s1 + h) % self.MOD
        self.s2 = (self.s2 + h * h) % self.MOD

    def digest(self):
        return {"rows": self.rows, "s1": "%016x" % self.s1, "s2": "%016x" % self.s2}


def fmt_bytes(n):
    if n is None:
        return "-"
    n = float(n)
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or u == "TiB":
            return ("%.1f %s" % (n, u)) if u != "B" else ("%d B" % n)
        n /= 1024.0
    return "%.1f TiB" % n


def fmt_num(n):
    if n is None:
        return "-"
    n = float(n)
    for u in ("", "K", "M", "B"):
        if abs(n) < 1000 or u == "B":
            return ("%.1f%s" % (n, u)) if u else ("%d" % n)
        n /= 1000.0
    return "%.1fB" % n
