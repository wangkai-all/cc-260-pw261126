# joinbench · 数据关联作业台

两个独立工作进程在**限定内存和临时盘预算**下关联倾斜数据; 协调器从零实现调度、
磁盘中间层、恢复与页面。纯 Python 标准库, 无第三方依赖。

## 一分钟跑起来

```bash
cd joinbench
python3 run_demo.py --scenario all        # 全部场景(约 3-5 分钟)
python3 run_demo.py --scenario serve      # 起集群+演示作业, 浏览器开 http://127.0.0.1:8788
python3 -m unittest discover -s tests -v  # 单元测试
python3 test_recovery.py                  # 协调器 kill -9 后再启动: 日志重放, 作业继续且核对一致
```

## 组成

```
joinbench/
  datagen.py      合成数据: 行数/倾斜度(zipf)/热点键份额/分片数 全部可调
  stats.py        流式统计: Space-Saving 素描找热点键, 内存有界, 不整表读入
  planner.py      方案选择: 广播小表 / 分区关联 / 热点单独处理, 基于代价(真实读量/临时盘/长尾)
  engine.py       执行器: 洗牌写、分区关联(预算不够自动分块重扫 L)、广播关联
  coordinator.py  调度器: 租约、尝试号、登记/提交协议、日志恢复、HTTP API
  worker.py       独立工作进程: 拉任务、预算记账、心跳、故障注入点
  baseline.py     小数据全量基准(真值), 输出多重集哈希供核对
  webui.py        单页监控: 工作进程/方案对比/分片/积压/输出/事件
run_demo.py       端到端演示: 6 个场景自动跑并核对
tests/            单元测试(含"崩溃不重复输出"回归)
```

## 三种执行方案怎么选

规划器用统计阶段的**真实字节数**估算每个候选方案: 预计读取量、临时盘写入、
单任务最大处理量(长尾), 在预算内选评分最低者(评分 = 读 + 临时盘 + 4×长尾):

| 数据形态 | 选择 | 原因 |
|---|---|---|
| S 装得进单任务内存 | **广播小表** | 零洗牌零临时盘, 读量 = L + S×分片数 |
| S 超内存、键分布均匀 | **分区关联** | 双表哈希洗牌, 逐分区建哈希; S 分区仍超内存则分块重扫 L 兜底 |
| 存在热点键 | **热点单独处理** | 热点维表行广播(有界), 热点事实行**加盐打散**到全部分区, 冷键正常洗牌 |

演示实测(skewbig, 热点键占 L 25%): 普通分区关联单任务要处理 ~18.6MiB,
热点方案把最大分区压到 6.9MiB(最大/均值 = 1.02), 没有任务被热点拖死。

## 预算真的生效

- **内存**: 对洗牌缓冲/哈希表/输出缓冲做编码字节记账, 超限即失败;
  关联侧有兜底 —— S 分块、L 重扫(页面可见 `l_scans>1`), 内存永远有界。
- **临时盘**: 落盘字节记账, 超限任务失败、作业如实报告"预算强制生效"。
- 页面实时显示每个工作进程和每个作业的 峰值/预算 仪表。

## 崩溃恢复: 不重复输出

提交协议(关键路径都有故障注入点, `run_demo.py --scenario fault` 实测):

1. 中间文件写在 `tmp/<job>/<stage>/<task>/attempt-N/`, **先登记(REGISTER)再提交(COMPLETE)**;
2. 写完没登记就崩 → 孤儿文件无人引用, 被回收; 任务以新尝试号重跑;
3. 关联输出先写临时文件, 协调器**先记日志再原子改名**到 `output/<job>/`,
   改名幂等可重放; 只有当前尝试能提交(过期尝试被拒绝并自清);
4. 结果核对: 输出多重集哈希 == 全量基准哈希(小数据), 证明不重复不丢失。

## 取消清理: 不碰共享输入

取消/完成只删 `tmp/<job>/`, 删除前有路径守卫(`rm_tree_guarded`)拒绝越界;
`data/` 输入只读, 多个作业可安全共享同一份输入(场景 cancel 实测)。

## 手动操作

```bash
# 生成自定义规模数据
python3 -m joinbench.datagen --out run/data --name big --s-rows 1000000 --l-rows 5000000 --skew 1.2 --hot-share 0.3

# 起集群
python3 -m joinbench.coordinator --port 8788 --root run --mem-budget $((8<<20)) --tmp-budget $((256<<20)) &
python3 -m joinbench.worker --id w1 --coordinator http://127.0.0.1:8788 --state-dir run/worker-w1 &
python3 -m joinbench.worker --id w2 --coordinator http://127.0.0.1:8788 --state-dir run/worker-w2 &

# 提交作业
curl -X POST http://127.0.0.1:8788/api/jobs -d '{"name":"x","left":"run/data/big/L","right":"run/data/big/S"}'

# 注入故障: 下次洗牌写完未登记时崩溃
curl -X POST http://127.0.0.1:8788/api/fault -d '{"points":{"after_shuffle_write":"*"}}'
```

## 已知边界

- 预算记账是"数据缓冲区编码字节"口径(不含 Python 对象开销), 工程上等价于
  流式系统的 buffer 池配额;
- 洗牌文件写本地盘, 关联任务经共享文件系统读取(单机演示); 多机部署时这里
  应换成洗牌服务;
- 维表 S 若单键行数极多(交叉积爆炸)不在演示范围内。
