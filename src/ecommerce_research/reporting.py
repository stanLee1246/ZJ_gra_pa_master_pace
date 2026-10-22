from __future__ import annotations

from datetime import datetime, timezone

from .storage import read_json, read_table


def reports_stage(spark, cfg, root):
    quality = read_json(root / "quality.json")
    metrics = [r.asDict() for r in read_table(spark, root, "metrics").filter("snapshot_date = 'ALL'").orderBy("model").collect()]
    kind = cfg["data"]["kind"]
    lines = ["# 用户行为研究运行报告", "", f"- 平台：{cfg['data']['platform']}",
             f"- 数据性质：{kind}", f"- 数据版本：{cfg['data']['version']}",
             f"- 生成时间：{datetime.now(timezone.utc).isoformat()}",
             f"- 保留事件：{quality['retained_rows']}；隔离事件：{quality['rejected_rows']}", ""]
    if kind == "synthetic":
        lines += ["> 本报告来自人工生成数据，仅证明软件流程可运行，不构成真实电商平台研究结论。", ""]
    lines += ["验证集选择阈值，测试集保持自然分布；AP采用按分数并列值分组的精确定义。", "",
              "| 模型 | 测试候选 | 正例率 | AP | ROC-AUC | Brier | F1 |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in metrics:
        values = [row["model"], str(row["candidates"])]
        values += [f"{row[key]:.6f}" if row[key] is not None else "未定义" for key in
                   ("positive_rate", "average_precision", "roc_auc", "brier_score", "f1")]
        lines.append("| " + " | ".join(values) + " |")
    lines += ["", "## 解释边界", "", "- 候选仅来自历史48小时交互，标签为同用户同商品未来24小时目标记录。",
              "- 淘宝购买记录与京东下单语义分别保留，未接入平台不生成实验结果。",
              "- 不确定的重复行为保留并报告；同秒动作组内无序。",
              "- 窗口完整性依赖来源声明和文件完整性，按日有事件不能证明日志完整。",
              "- 第一期未开展超参数搜索、概率校准、因果干预或真实营销实验。",
              "- 原始事件、预测、质量问题与模型产物仅保存在本地忽略目录。", ""]
    (root / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return {"report": str(root / "report.md")}
