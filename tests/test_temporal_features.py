import pytest

from ecommerce_research.config import epoch
from ecommerce_research.features import build_candidate_features

pytestmark = pytest.mark.spark


def test_candidate_labels_are_item_specific_and_half_open(spark, cfg):
    t = epoch("2017-11-30T00:00:00+08:00")
    rows = [
        ("u1", "i1", "browse", t - 48 * 3600),
        ("u1", "i1", "cart_add", t - 3600),
        ("u1", "i1", "purchase", t),
        ("u1", "i2", "browse", t - 100),
        ("u1", "i2", "purchase", t + 24 * 3600),
        ("u2", "i3", "browse", t),
        ("u2", "i3", "purchase", t + 100),
        ("u3", "i4", "browse", t - 100),
        ("u3", "i4", "purchase", t - 50),
        ("u3", "i4", "purchase", t + 100),
        ("u4", "i5", "browse", t - 100),
        ("u4", "i5", "cart_add", t - 100),
    ]
    events = spark.createDataFrame([( "taobao", u, i, "c", a, ts, False) for u, i, a, ts in rows],
                 "platform string, user_id string, item_id string, category_id string, action string, event_ts long, unknown_category boolean")
    anchors = spark.createDataFrame([(t, "2017-11-30", "test")], "anchor_ts long, snapshot_date string, split string")
    frame, _, _ = build_candidate_features(events, anchors, cfg)
    result = {(r["user_id"], r["item_id"]): r.asDict() for r in frame.collect()}
    assert set(result) == {("u1", "i1"), ("u1", "i2"), ("u3", "i4"), ("u4", "i5")}
    assert result["u1", "i1"]["label"] == 1
    assert result["u1", "i2"]["label"] == 0
    assert result["u3", "i4"]["pair_prior_target"] == 1
    assert result["u3", "i4"]["label"] == 1
    assert result["u4", "i5"]["browse_then_cart"] == 0
    assert all(row["feature_max_event_ts"] < t for row in result.values())
    assert result["u1", "i1"]["pair_browse"] == 1  # left endpoint is included


def test_platform_keys_do_not_merge_identical_user_and_item(spark, cfg):
    t = epoch("2017-11-30T00:00:00+08:00")
    events = spark.createDataFrame([
        ("taobao", "same", "same", "c", "browse", t - 100, False),
        ("jd", "same", "same", "c", "browse", t - 100, False),
        ("taobao", "same", "same", "c", "purchase", t + 100, False),
    ], "platform string, user_id string, item_id string, category_id string, action string, event_ts long, unknown_category boolean")
    anchors = spark.createDataFrame([(t, "2017-11-30", "test")], "anchor_ts long, snapshot_date string, split string")
    frame, _, _ = build_candidate_features(events, anchors, cfg)
    labels = {r["platform"]: r["label"] for r in frame.select("platform", "label").collect()}
    assert labels == {"taobao": 1, "jd": 0}
