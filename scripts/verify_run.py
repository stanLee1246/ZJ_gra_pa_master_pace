"""Functional verification of an actual completed run, optionally including the UI."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb
from sklearn.metrics import average_precision_score


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--app", action="store_true")
    args = parser.parse_args()
    root = args.run.resolve()
    metadata = json.loads((root / "run.json").read_text())
    assert metadata["status"] == "complete", "该运行尚未完整完成。"
    quality = json.loads((root / "quality.json").read_text())
    assert quality["input_rows"] == quality["retained_rows"] + quality["rejected_rows"]
    connection = duckdb.connect()
    connection.execute("SET threads=2")
    features = str(root / "features" / "*.parquet")
    assert connection.execute("SELECT count(*) FROM read_parquet(?) WHERE feature_max_event_ts >= anchor_ts", [features]).fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM (SELECT platform,user_id,item_id,anchor_ts,count(*) n FROM read_parquet(?) GROUP BY ALL HAVING n != 1)", [features]).fetchone()[0] == 0
    metrics = connection.execute("SELECT * FROM read_parquet(?) WHERE snapshot_date='ALL'", [str(root / "metrics" / "*.parquet")]).df()
    assert set(metrics["model"]) == {"constant", "rule", "logistic", "gbt_base", "gbt_paths"}
    assert metrics["candidates"].nunique() == 1
    assert metrics["average_precision"].between(0, 1).all()
    # Bound local cross-check to a genuinely small completed run, not a large public dataset.
    n = connection.execute("SELECT count(*) FROM read_parquet(?) WHERE split='test'", [str(root / "predictions" / "*.parquet")]).fetchone()[0]
    if n <= 100_000:
        predictions = connection.execute("SELECT model,label,score FROM read_parquet(?) WHERE split='test'", [str(root / "predictions" / "*.parquet")]).df()
        for name, frame in predictions.groupby("model"):
            actual = average_precision_score(frame["label"], frame["score"])
            reported = metrics.loc[metrics["model"] == name, "average_precision"].iloc[0]
            assert abs(actual - reported) < 1e-10, (name, actual, reported)
    assert (root / "report.md").exists()
    if metadata["kind"] == "synthetic":
        assert "人工生成数据" in (root / "report.md").read_text()
    print(f"Verified run {metadata['run_id']}: row reconciliation, historical boundaries, candidate uniqueness, five models and AP.")
    connection.close()
    if args.app:
        from streamlit.testing.v1 import AppTest
        os.environ["ECOMMERCE_ARTIFACTS"] = str(root.parent)
        app_path = Path(__file__).resolve().parents[1] / "app" / "main.py"
        app = AppTest.from_file(str(app_path)).run(timeout=60)
        assert not app.exception, [e.message for e in app.exception]
        # The requested run may not be the latest one.
        selected = [i for i, option in enumerate(app.sidebar.selectbox[1].options) if option.startswith(metadata["run_id"] + " ·")]
        assert selected
        app.sidebar.selectbox[1].set_value(selected[0]).run(timeout=60)
        for view in ["研究总览", "数据质量", "路径挖掘", "用户画像", "购买意向", "实验评价", "运行与来源"]:
            app.sidebar.radio[0].set_value(view).run(timeout=60)
            assert not app.exception, (view, [e.message for e in app.exception])
            if metadata["kind"] == "synthetic":
                assert any("人工演示" in warning.value for warning in app.warning)
            print(f"Verified view: {view}")
        for platform in ["京东", "拼多多"]:
            app.sidebar.selectbox[0].set_value(platform).run(timeout=60)
            assert not app.exception
            assert any("尚未接入" in note.value for note in app.info)
            assert not app.metric
        print("Verified unavailable platforms remain explicitly unavailable.")


if __name__ == "__main__":
    main()
