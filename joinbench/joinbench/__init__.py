"""joinbench - 数据关联作业台：限定内存/临时盘预算下的倾斜数据分布式关联。

模块:
  util        原子写、路径守卫、多重集哈希等基础工具
  datagen     规模/倾斜可调的合成数据生成
  stats       流式表统计(有界内存, Space-Saving 重 hitter 素描)
  planner     执行方案选择: 广播小表 / 分区关联 / 热点单独处理
  engine      工作进程内的任务执行: 洗牌写、分区关联(可溢写降级)、广播关联
  coordinator 调度器 + 日志恢复 + HTTP API + 页面
  worker      独立工作进程
  baseline    小数据全量基准(用于结果核对)
"""

__version__ = "0.1.0"
