# 电商用户行为路径挖掘与购买转化预测

面向数据科学与大数据专业的研究项目。第一版已实现数据接入、清洗、会话与PrefixSpan、行为分群、时间窗口特征、购买预测、精确评价和交互展示。

淘宝是主研究平台；京东实现基础行为适配，真实数据取得后单独配置运行；拼多多没有经核验日志，保持未接入状态。人工演示数据与公开／授权数据的状态贯穿来源、报告和界面。

## 从零运行

需要Python 3.11—3.13和Java 17或21。当前云环境实际验证组合是Python 3.12、Java 21、Spark 4.0.1，与计划书最初建议的Spark 3.5.x不同。

```bash
bash scripts/setup.sh
source .venv/bin/activate
# 首次生成人工流程验证数据；已有文件时不会覆盖。
ecommerce-research demo --users 120
ecommerce-research run --config configs/demo.yaml --run-id demo-v1
streamlit run app/main.py
```

本次云环境使用的独立虚拟环境位于`/workspace/.venvs/ecommerce`，可以直接激活后使用相同命令。安装依赖已经在该环境完成。

重复执行同一配置和run-id会跳过已完成阶段。修改配置应使用新run-id。阶段内失败留下的半成品不自动覆盖或删除；检查错误后用新run-id重跑。

## 真实数据与研究口径

先阅读[数据取得说明](docs/DATA_ACCESS.md)，取得原始数据和字典，核实完整覆盖、许可和版本。`configs/taobao.yaml`用于真实淘宝日志；京东配置的日期有意留空，完成验收后填写。原始数据不在仓库内。

主任务为过去48小时交互候选中，预测同一用户对同一商品未来24小时的目标事件。特征区间`[t-48h,t)`、标签`[t,t+24h)`。候选不补入未来商品，同秒动作组内无序，疑似重复不盲目删除。购买／下单不等于支付成功或营销收益。

运行产物在忽略目录`artifacts/<run-id>/`，包括来源、质量报告、Parquet、模式、模型、预测、指标与Markdown运行报告。展示端读取这些真实产物，不使用预写模型分数。

```bash
# 单阶段调试：inspect、clean、sessions、mine、features、train、evaluate、reports
python scripts/run_pipeline.py --config configs/demo.yaml --run-id another-run --stage inspect
# 关键逻辑检查，含Spark边界、清洗与AP对照
pytest -q
# 核验一个已经完成的实际运行，以及七个界面视图
python scripts/verify_run.py --run artifacts/demo-v1 --app
```

模型包括常数、规则、逻辑回归、基础GBT和路径增强GBT。阈值只由验证集选择，报告自然分布测试的逐日／合并AP、ROC-AUC、F1、Brier和Top-K。AP按并列分数组精确计算，并与scikit-learn核对。

## 代码与文档

- `src/ecommerce_research/`：分阶段Spark研究流水线与CLI。
- `configs/`：演示、淘宝、京东配置及禁用的拼多多模板。
- `app/main.py`：Streamlit研究工作台。
- `tests/`：时点边界、身份隔离、同秒顺序、来源状态、清洗与评价检查。
- [架构、已实现范围与限制](docs/ARCHITECTURE.md)。
- [数据取得与平台适配](docs/DATA_ACCESS.md)。
- [当前实例的实际验证记录](docs/VERIFICATION.md)。

完整的一亿条处理、真实平台模型、京东多表历史关联、清洗敏感性、统计区间和集群性能仍需实际数据与后续实验。人工演示只验证软件流程，不作为毕业论文业务结论。

## 研究资料

以下保留2026年10月22日的调研与完整计划，计划中的项目状态是编制时状态；当前实现范围以本README和架构说明为准。

- [GitHub 项目采集表](GitHub项目采集表.csv)：10 个项目的链接、类型与核查证据。
- [调研与毕业设计选题建议](GitHub调研与毕业设计选题建议.md)：三个候选方向及功能、技术路线和实验要求。

- [电商用户行为研究详细计划书](电商用户行为路径挖掘与购买转化预测_详细计划书.md)：25节、4个附录，涵盖技术实现、数据清洗、路径挖掘、预测实验与实施进度。

当前研究方向：**电商用户行为路径挖掘与购买转化预测**。淘宝作为主实验，京东取得原始数据后独立对照，拼多多按真实日志可获得条件扩展。

原始研究资料中的其他选题用于记录前期调研，不代表本项目正在实现其他方向。
