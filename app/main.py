from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT = Path(__file__).resolve().parents[1]
ARTIFACTS = Path(os.environ.get("ECOMMERCE_ARTIFACTS", str(PROJECT / "artifacts"))).resolve()

st.set_page_config(page_title="电商行为研究工作台", page_icon="📊", layout="wide")
st.markdown("""<style>
    .block-container {padding-top: 2rem; max-width: 1480px;}
    [data-testid="stMetric"] {border: 1px solid #e2e8f0; border-radius: 12px; padding: 18px;}
    h1 {letter-spacing: -0.04em;} .stCaption {color: #64748b;}
</style>""", unsafe_allow_html=True)
st.title("电商行为研究工作台")
st.caption("数据质量 · 行为路径 · 购买意向 · 可复查实验")


def json_file(root, name):
    p = root / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def table(root, name, limit=5000, where="", params=None):
    # Table names and SQL fragments are internal constants; user values are bound parameters.
    pattern = str(root / name / "*.parquet")
    if not list((root / name).glob("*.parquet")):
        return pd.DataFrame()
    with duckdb.connect() as connection:
        connection.execute("SET threads=2")
        return connection.execute(f"SELECT * FROM read_parquet(?) {where} LIMIT {int(limit)}",
                                  [pattern, *(params or [])]).df()


def show_table(root, name, **kwargs):
    frame = table(root, name, **kwargs)
    if frame.empty:
        st.info("该阶段尚未生成可用结果。")
    else:
        st.dataframe(frame, width="stretch", hide_index=True)
    return frame


runs = []
for p in ARTIFACTS.glob("*/run.json"):
    try:
        meta = json.loads(p.read_text(encoding="utf-8"))
        runs.append((p.parent, meta))
    except (OSError, ValueError):
        continue
platform = st.sidebar.selectbox("平台", ["淘宝", "京东", "拼多多"])
platform_key = {"淘宝": "taobao", "京东": "jd", "拼多多": "pdd"}[platform]
available = [(p, m) for p, m in runs if m["platform"] == platform_key]
if not available:
    st.info(f"{platform}尚未接入可用研究结果。真实行为日志、来源和事件字典验收后才能开展该平台实验。")
    if platform == "淘宝":
        st.code("ecommerce-research demo\necommerce-research run --config configs/demo.yaml --run-id demo-v1")
    st.stop()
available.sort(key=lambda pair: (pair[1]["status"] == "complete", pair[1]["created_at"]), reverse=True)
labels = [f"{m['run_id']} · {m['kind']} · {m['status']}" for _, m in available]
chosen = st.sidebar.selectbox("实验运行", range(len(available)), format_func=lambda i: labels[i])
root, metadata = available[chosen]
if metadata["kind"] == "synthetic":
    st.warning("人工演示数据：以下结果用于验证代码与操作流程，不能作为真实平台规律或毕业论文实验结论。")
else:
    st.caption("公开／授权数据声明来自运行配置；请同时核对来源清单和原始说明。")
st.caption(f"数据版本：{metadata['config']['data']['version']}　|　运行：{metadata['run_id']}　|　状态：{metadata['status']}")
page = st.sidebar.radio("研究视图", ["研究总览", "数据质量", "路径挖掘", "用户画像", "购买意向", "实验评价", "运行与来源"])

if page == "研究总览":
    quality = json_file(root, "quality.json")
    if quality:
        c1, c2, c3 = st.columns(3)
        c1.metric("保留事件", f"{quality['retained_rows']:,}")
        c2.metric("隔离事件", f"{quality['rejected_rows']:,}")
        c3.metric("疑似重复组", f"{quality['uncertain_duplicate_groups']:,}")
    frame = table(root, "overview")
    if not frame.empty:
        st.plotly_chart(px.line(frame, x="event_date", y="events", color="action", markers=True,
                               labels={"event_date": "业务日期", "events": "事件数", "action": "行为"}))
    st.subheader("候选与标签样本")
    show_table(root, "sample_summary")
    st.caption("候选单位为用户—商品—时点；未来24小时标签不表示支付成功或营销增量。")
    st.subheader("未来目标事件的候选覆盖")
    show_table(root, "candidate_coverage")
elif page == "数据质量":
    quality = json_file(root, "quality.json")
    if quality:
        st.subheader("处置与对账")
        st.dataframe(pd.DataFrame([{"处置": k, "事件数": v} for k, v in quality["by_reason"].items()]), hide_index=True)
        st.caption("不确定重复行为保留；有事件的日期不构成日志完整性证明。")
        st.json(quality)
    st.subheader("隔离记录（最多200条）")
    show_table(root, "quarantine", limit=200)
    st.subheader("疑似重复组")
    show_table(root, "duplicate_groups", limit=200)
elif page == "路径挖掘":
    st.subheader("历史有序行为 → 未来24小时目标事件")
    frame = show_table(root, "funnels")
    if not frame.empty:
        st.plotly_chart(px.bar(frame, x="branch", y=["candidates", "positives"], barmode="group"))
    st.caption("三个分支分别统计，不是要求收藏、加购依次发生的统一线性漏斗。")
    st.subheader("训练期状态转移")
    frame = table(root, "transitions")
    if not frame.empty:
        pivot = frame.pivot(index="from_state", columns="to_state", values="probability").fillna(0)
        st.plotly_chart(px.imshow(pivot, text_auto=".2f", color_continuous_scale="Blues"))
    st.subheader("PrefixSpan训练期模式")
    show_table(root, "patterns", where="ORDER BY freq DESC", limit=100)
    st.caption("同秒动作以无序项集保存。支持度分母为训练期用户—商品会话数；转移和提升是关联。")
    st.subheader("冻结的预测路径特征")
    st.json(json_file(root, "pattern_features.json") or {})
elif page == "用户画像":
    frame = table(root, "profiles", limit=20000)
    if not frame.empty:
        dates = sorted(frame["snapshot_date"].unique())
        day = st.selectbox("画像时点", dates)
        selected = frame[frame["snapshot_date"] == day]
        aggregate = selected.groupby("cluster").agg(users=("user_id", "count"),
                     mean_events=("user_events", "mean"), mean_cart=("user_cart", "mean"),
                     mean_items=("user_items", "mean")).reset_index()
        st.dataframe(aggregate, hide_index=True)
        st.plotly_chart(px.scatter(selected, x="user_events", y="user_cart", color=selected["cluster"].astype(str),
                                   hover_data=["user_id", "user_items"]))
        user = st.text_input("匿名用户ID（精确匹配）")
        if user:
            st.dataframe(selected[selected["user_id"] == user], hide_index=True)
        st.caption("页面最多读取20,000条画像；大型数据请按日期和ID精确查询。群体不代表收入、职业或真实价值。")
elif page == "购买意向":
    models = [x["model"] for x in (json_file(root, "training.json") or {}).get("models", [])]
    if not models:
        st.info("尚未完成模型训练。")
    else:
        model = st.selectbox("模型", models, index=len(models) - 1)
        user = st.text_input("筛选匿名用户ID（可留空）")
        where, values = "WHERE model = ? AND split = 'test'", [model]
        if user:
            where += " AND user_id = ?"
            values.append(user)
        frame = table(root, "predictions", limit=100, where=where + " ORDER BY score DESC", params=values)
        st.dataframe(frame, hide_index=True, width="stretch")
        if not frame.empty:
            st.download_button("导出当前候选CSV", frame.to_csv(index=False).encode("utf-8-sig"), "candidate_scores.csv", "text/csv")
        st.caption("展示完整未来窗口的历史回测结果；仅在历史交互候选中排序。规则分数未校准，预测不执行营销触达。")
elif page == "实验评价":
    st.subheader("相同测试范围的模型对照")
    frame = table(root, "metrics")
    if not frame.empty:
        aggregate = frame[frame["snapshot_date"] == "ALL"]
        st.plotly_chart(px.bar(aggregate, x="model", y="average_precision", color="model"))
        st.dataframe(frame, hide_index=True, width="stretch")
        st.download_button("导出评价CSV", frame.to_csv(index=False).encode("utf-8-sig"), "metrics.csv", "text/csv")
    st.caption("AP是按分数并列值分组的平均精确率；阈值仅由验证集选择。ALL为合并测试范围。")
    st.subheader("Top-K与分组误差")
    show_table(root, "topk")
    show_table(root, "cohorts")
    st.subheader("特征解释")
    importance = json_file(root, "feature_importance.json")
    if importance:
        st.dataframe(pd.DataFrame(importance), hide_index=True)
elif page == "运行与来源":
    st.subheader("数据来源与完整性")
    st.json(json_file(root, "manifest.json") or [])
    st.subheader("阶段状态与耗时")
    st.dataframe(pd.DataFrame([{"阶段": k, **v} for k, v in metadata["stages"].items()]).drop(columns=["result"], errors="ignore"), hide_index=True)
    st.json(metadata["config"])
    report = root / "report.md"
    if report.exists():
        st.download_button("下载研究运行报告", report.read_bytes(), "research_report.md", "text/markdown")
