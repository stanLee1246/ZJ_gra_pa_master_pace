# 第一版实现与研究口径

## 数据与任务

- 预测单位：平台、匿名用户、匿名商品、锚点。
- 候选：过去48小时内有非目标动作的用户—商品对。
- 特征：`[t-48h,t)`，标签：`[t,t+24h)`；时间边界包含左端、不包含右端。
- 目标：淘宝购买记录／京东下单记录；不是付款、收益或营销增量。
- 锚点按Asia/Shanghai配置，内部用UTC秒进行比较。
- 非候选未来购买只进入覆盖统计，不补入候选。
- 同一用户已购商品允许再次成为候选，历史购买另列特征。

## 阶段与产物

| 阶段 | 实现 | 产物 |
| --- | --- | --- |
| inspect | 来源、SHA-256、重复文件与演示侧边信息 | manifest.json |
| clean | 显式CSV模式、语义适配、隔离、未知类目、重复报告 | clean/quarantine/overview/duplicate_groups/quality.json |
| sessions | 先用户会话，再同商品时间组；组内动作无序 | sessions |
| mine | 只用训练历史的PrefixSpan和转移矩阵 | patterns/transitions/path_examples/pattern_features.json |
| features | 按锚点生成候选、未来标签、用户商品交互聚合与路径标记 | features/sample_summary/candidate_coverage/funnels |
| profiles | 随features阶段执行；训练历史拟合标准化与K-means | profiles/models/user_clusters |
| train | 常数、规则、逻辑回归、基础GBT、路径增强GBT | models/predictions/training.json/feature_importance.json |
| evaluate | 验证选阈值；测试AP、ROC、Brier、F1、Top-K、分组 | metrics/topk/cohorts/calibration/thresholds.json |
| reports | 汇总运行与评价，保留数据性质 | report.md |

所有Parquet产物默认拒绝覆盖。运行清单记录阶段状态、错误和耗时；未成功的阶段不标记完成。相同配置可跳过已完成阶段；阶段内有半成品时采用新run-id重新运行，不自动删除用户数据。

## 序列处理

同一秒内`browse`和`cart_add`保存为同一项集，不能匹配严格有序的`[[browse],[cart_add]]`。用户会话按连续活动间隔大于30分钟划分，同商品子序列保留原时间。缺乏稳定事件ID的相同行为只报告，不直接删掉。

PrefixSpan支持度分母是训练期用户—商品会话数量；后续预测前缀匹配在48小时候选历史上执行，两个粒度分别记录。模式字典只用训练历史筛选，未来标签不参与模式选择。

## 模型与评价

第一版采用固定参数，检查可复现的基线与路径消融。没有进行系统搜索、负采样、校准或显著性分析。规则分数为启发式排序；LR和GBT输出未经校准的估计概率。

AP按预测分数并列值分组后精确计算，与梯形积分PR-AUC区分。ROC按相同阈值组积分；某日期仅有单类别时ROC为未定义。阈值从验证集选F1最优值，测试集不参与阈值选择。按日期和ALL分别展示，Top-K按候选对计数，并用固定哈希解决同分顺序。

## 大规模处理与已知边界

- 清洗、窗口、关联、序列和模型通过Spark执行；展示使用DuckDB查询Parquet。
- 小型模式元数据与报告汇总允许有界collect；不把全量原始日志转pandas。
- 画像页最多读取20,000条，候选页100条，其余展示表有界；大数据需要更精细的查询过滤和分页。
- 全局精确排名需要排序；一亿原始事件的全量处理成本尚未验证。
- 单机Spark实测不能当作集群扩展实验。第一版未完成不同规模、存储和并行度的性能对照。
- 未完成计划书全部研究实验，特别是京东多表接入、拼多多真实接入、置信区间、完整清洗敏感性与窗口搜索。
- 公开／授权来源声明需要原始字典和取得证明配合，程序无法仅凭字段证明来源真实。
