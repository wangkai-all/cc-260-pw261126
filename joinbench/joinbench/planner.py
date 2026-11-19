"""执行方案选择: 在 广播小表 / 分区关联 / 热点单独处理 之间做基于代价的选择。

每种候选方案估算:
  est_read       总读取字节(输入 + 中间层复读 + 广播复制)
  est_tmp        临时盘写入字节
  est_max_task   单个任务预计处理的最大字节(长尾/拖尾风险 = 估计完成时间下界)
  feasible       在内存/临时盘预算下是否可行 + 原因

评分 = est_read + est_tmp + STRAGGLER_WEIGHT * est_max_task
(读盘和写临时盘都花真实 IO; 单任务过大意味着两个工作进程里有一个被拖死,
 所以给长尾一个权重。热点键会把分区关联的 est_max_task 顶上去, 从而自动落选。)
"""
import math

STRAGGLER_WEIGHT = 4.0
MEM_SAFETY = 0.7        # 单任务内存占用上限 = 预算 * 0.7, 留缓冲
HOT_MIN_SHARE = 0.01    # 单键占 L 超过 1% 才算热点
HOT_MIN_RATIO = 20.0    # 或超过平均频度 20 倍
HOT_MAX_KEYS = 4096     # 热点集合上限(广播热点维表行的内存上限由其字节数约束)
HOT_S_MEM_FRAC = 0.25   # 热点维表行总字节不得超过内存预算的 25%


def pick_hot_keys(stats_l, stats_s, mem_budget):
    """从 L 的重 hitter 素描里挑热点键, 并估算其 L 侧/S 侧字节。

    返回 (hot: {key: l_count}, hot_l_share, hot_s_bytes)
    热点集合受预算约束: 对应 S 行总字节 <= HOT_S_MEM_FRAC * mem_budget。
    """
    l_rows = max(1, stats_l["rows"])
    avg = l_rows / max(1, stats_l["distinct_lower"])
    s_top = {k: c for k, c in stats_s["top"]}
    s_avg_row = stats_s["bytes"] / max(1, stats_s["rows"])
    l_avg_row = stats_l["bytes"] / max(1, stats_l["rows"])

    hot = {}
    hot_l_rows = 0
    hot_s_bytes = 0.0
    cap = HOT_S_MEM_FRAC * mem_budget
    for k, c in stats_l["top"]:
        share = c / l_rows
        if share < HOT_MIN_SHARE and c < HOT_MIN_RATIO * avg:
            continue
        s_rows_k = s_top.get(k, 1)          # 维表通常每键一行; 素描里没有就按 1 估
        s_bytes_k = s_rows_k * s_avg_row
        if hot and (hot_s_bytes + s_bytes_k) > cap:
            break                            # 热点广播预算用完, 停止扩张
        if len(hot) >= HOT_MAX_KEYS:
            break
        hot[k] = c
        hot_l_rows += c
        hot_s_bytes += s_bytes_k
    return hot, hot_l_rows / l_rows, hot_s_bytes, l_avg_row


def _spill_scans(side_bytes, mem_budget):
    """S 侧一块装不下时, 执行器会分块重扫 L(内存永远有界)。返回预计扫描遍数。"""
    chunk = max(1 << 20, int(0.5 * mem_budget))
    return max(1, math.ceil(side_bytes / chunk))


def estimate_plans(stats_l, stats_s, mem_budget, tmp_budget, n_l_shards, n_workers,
                   partitions_override=None):
    L, S = stats_l["bytes"], stats_s["bytes"]
    hot, hot_share, hot_s_bytes, l_avg_row = pick_hot_keys(stats_l, stats_s, mem_budget)
    hot_l_bytes = L * hot_share

    # 分区数: 让冷数据 S 侧单分区 comfortably 放进内存
    p_auto = max(2 * n_workers, math.ceil(S / (MEM_SAFETY * 0.7 * mem_budget)))
    P = partitions_override or min(512, max(8, p_auto))

    plans = []

    # ---- 方案 1: 广播小表 ----
    # 每个关联任务把整份 S 装进内存, 流式扫自己负责的 L 分片。无洗牌、无临时盘。
    b_feasible = S <= MEM_SAFETY * mem_budget
    plans.append({
        "name": "broadcast",
        "desc": "广播小表: S 装入每个任务内存, L 流式探测, 零洗牌零临时盘",
        "feasible": b_feasible,
        "why_not": "" if b_feasible else "S=%dB 超过单任务内存上限 %dB" % (S, MEM_SAFETY * mem_budget),
        "est_read": L + S * n_l_shards,      # 每个 L 分片任务都要读一遍 S
        "est_tmp": 0,
        "est_max_task": (L / n_l_shards) + S,
        "partitions": 0,
    })

    # ---- 方案 2: 分区关联(shuffle hash join) ----
    # 双表都按键哈希进 P 个分区落盘, 再逐分区关联。热点键的全部 L 行挤进同一分区。
    # S 分区超内存时不判死: 执行器分块重扫 L(读量上升, 内存仍有界)。
    worst_part_l = max(hot_l_bytes, L / P)   # 热点键集中到一个分区
    scans = _spill_scans(S / P, mem_budget)
    spill_note = "" if scans == 1 else "; S 分区超内存块, 分 %d 块重扫 L(预算兜底)" % scans
    plans.append({
        "name": "partitioned",
        "desc": "分区关联: 双表按键哈希洗牌成 %d 个分区, 逐分区建哈希关联%s" % (P, spill_note),
        "feasible": (L + S) <= tmp_budget,
        "why_not": "" if (L + S) <= tmp_budget else "洗牌临时盘 %dB 超预算 %dB" % (L + S, tmp_budget),
        "est_read": 2 * (L + S) + L * (scans - 1),
        "est_tmp": L + S,
        "est_max_task": worst_part_l * scans + S / P,
        "partitions": P,
    })

    # ---- 方案 3: 热点单独处理(倾斜感知混合) ----
    # 热点键的 S 行广播(有界小表), 热点键的 L 行加盐打散到全部 P 个分区;
    # 冷键正常哈希洗牌。没有任何一个任务会被热点键拖住。
    has_hot = len(hot) > 0
    cold_l, cold_s = L - hot_l_bytes, S - hot_s_bytes
    scans_sk = _spill_scans(cold_s / P + hot_s_bytes, mem_budget)
    plans.append({
        "name": "skew",
        "desc": "热点单独处理: %d 个热点键(占 L %.1f%%)的维表行广播, 热点事实行加盐打散, 冷键哈希洗牌" % (
            len(hot), hot_share * 100),
        "feasible": has_hot and (L + cold_s) <= tmp_budget,
        "why_not": "" if has_hot else "未检测到热点键, 退化为分区关联",
        "est_read": 2 * (L + cold_s) + hot_s_bytes * P + L * (scans_sk - 1),
        "est_tmp": L + cold_s,
        "est_max_task": (hot_l_bytes + cold_l) / P * scans_sk + cold_s / P + hot_s_bytes,
        "partitions": P,
    })

    for p in plans:
        p["score"] = (p["est_read"] + p["est_tmp"] + STRAGGLER_WEIGHT * p["est_max_task"]) \
            if p["feasible"] else None

    feasible = [p for p in plans if p["feasible"]]
    if not feasible:
        return {"chosen": None, "plans": plans, "hot_keys": sorted(hot),
                "hot_share": hot_share, "partitions": P,
                "error": "所有方案都超出预算: 请提高内存/临时盘预算或增大数据分片"}
    # 无热点时 skew 与 partitioned 等价, 直接排除 skew 避免噪声
    if not has_hot:
        feasible = [p for p in feasible if p["name"] != "skew"]
    chosen = min(feasible, key=lambda p: p["score"])
    return {"chosen": chosen["name"], "plans": plans, "hot_keys": sorted(hot),
            "hot_share": hot_share, "hot_s_bytes": hot_s_bytes, "partitions": P}
