import csv

import pytest
from sklearn.metrics import average_precision_score, roc_auc_score

from ecommerce_research.evaluation import grouped_metrics
from ecommerce_research.quality import clean_events, inspect_sources

pytestmark = pytest.mark.spark


def test_cleaning_preserves_uncertain_duplicates_and_reconciles(spark, tmp_path, cfg):
    from ecommerce_research.config import epoch

    raw = tmp_path / "events.csv"
    rows = [["u", "i", "c", "pv", str(epoch(f"2017-11-{day:02d}T12:00:00+08:00"))] for day in range(25, 31)]
    rows += [["u", "i", "c", "pv", str(epoch(f"2017-12-{day:02d}T12:00:00+08:00"))] for day in range(1, 4)]
    rows += [rows[0][:], ["", "i", "c", "pv", "1511539200"], ["u", "i", "c", "pv", "bad"],
             ["u", "i", "c", "unknown", "1511539200"], ["u", "i", "", "pv", "1511539201"],
             ["u", "i", "c", "pv", "1511539200", "unexpected-column"]]
    with raw.open("w", newline="") as f:
        csv.writer(f).writerows(rows)
    cfg["data"]["input_glob"] = str(raw)
    root = tmp_path / "run"
    root.mkdir()
    inspect_sources(cfg, root)
    summary = clean_events(spark, cfg, root)
    assert summary["input_rows"] == len(rows)
    assert summary["retained_rows"] + summary["rejected_rows"] == len(rows)
    assert summary["uncertain_duplicate_excess_rows"] == 1
    assert summary["unknown_category_rows"] == 1
    assert summary["by_reason"]["MALFORMED_ROW"] == 1
    assert summary["by_reason"]["MISSING_USER"] == 1
    assert not summary["missing_days"]


def test_exact_metrics_match_sklearn_with_tied_scores(spark):
    labels = [1, 0, 1, 0, 0, 1]
    scores = [0.9, 0.9, 0.6, 0.4, 0.4, 0.1]
    frame = spark.createDataFrame([("m", "ALL", int(y), float(s)) for y, s in zip(labels, scores)],
                                  "model string, snapshot_date string, label int, score double")
    thresholds = spark.createDataFrame([("m", 0.6)], "model string, threshold double")
    result = grouped_metrics(frame, thresholds).first()
    assert result["average_precision"] == pytest.approx(average_precision_score(labels, scores))
    assert result["roc_auc"] == pytest.approx(roc_auc_score(labels, scores))
    assert result["candidates"] == 6


def test_jd_adapter_keeps_order_distinct_and_business_timezone(spark, tmp_path, cfg):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    raw = tmp_path / "JData_Action_fixture.csv"
    base = datetime(2017, 11, 25, 12, tzinfo=ZoneInfo("Asia/Shanghai"))
    with raw.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "sku_id", "time", "type", "cate"])
        for day in range(9):
            writer.writerow(["same-id", "sku", (base + timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S"), "1", "c"])
        writer.writerow(["same-id", "sku", base.strftime("%Y-%m-%d %H:%M:%S"), "4", "c"])
        writer.writerow(["same-id", "sku", base.strftime("%Y-%m-%d %H:%M:%S"), "3", "c"])
    cfg["data"].update(platform="jd", input_glob=str(raw), source_timezone="Asia/Shanghai")
    root = tmp_path / "run"
    root.mkdir()
    inspect_sources(cfg, root)
    summary = clean_events(spark, cfg, root)
    events = spark.read.parquet(str(root / "clean"))
    assert summary["retained_rows"] == 11
    assert {r["action"] for r in events.select("action").distinct().collect()} == {"browse", "order", "cart_remove"}
    assert events.filter("action = 'order'").first()["event_ts"] == int(base.timestamp())
    assert {r["platform"] for r in events.select("platform").distinct().collect()} == {"jd"}
