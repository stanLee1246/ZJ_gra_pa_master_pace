from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F, types as T

from .config import CANDIDATE_ACTIONS, TARGETS
from .mining import pattern_matches
from .storage import read_json, read_table, save_table, write_json

KEYS = ["platform", "user_id", "item_id", "anchor_ts"]
BASE_FEATURES = ["pair_browse", "pair_cart", "pair_favorite", "pair_click", "pair_remove",
                 "pair_prior_target", "pair_recency_hours", "pair_span_hours", "user_events",
                 "user_items", "user_categories", "user_cart", "item_users", "item_events",
                 "unknown_category_flag"]
PATH_FEATURES = ["cart_then_revisit", "browse_then_cart", "browse_then_favorite", "sequence_length"]


def anchor_frame(spark, cfg):
    schema = "anchor_ts long, snapshot_date string, split string"
    return spark.createDataFrame([(a["anchor_ts"], a["snapshot_date"], a["split"]) for a in cfg["_anchors"]], schema)


def build_candidate_features(events, anchors, cfg):
    """Every aggregate is constrained by the prediction timestamp, before labels join."""
    lookback = cfg["task"]["lookback_hours"] * 3600
    horizon = cfg["task"]["horizon_hours"] * 3600
    target = TARGETS[cfg["data"]["platform"]]
    crossed = events.crossJoin(F.broadcast(anchors))
    history = crossed.filter((F.col("event_ts") >= F.col("anchor_ts") - lookback) &
                             (F.col("event_ts") < F.col("anchor_ts")))
    candidates = history.filter(F.col("action").isin(CANDIDATE_ACTIONS)).select(*KEYS).distinct()
    pair = history.groupBy(*KEYS).agg(
        *[F.sum(F.when(F.col("action") == action, 1).otherwise(0)).alias(name)
          for action, name in [("browse", "pair_browse"), ("cart_add", "pair_cart"),
                               ("favorite", "pair_favorite"), ("click", "pair_click"),
                               ("cart_remove", "pair_remove"), (target, "pair_prior_target")]],
        F.min("event_ts").alias("pair_first_ts"), F.max("event_ts").alias("feature_max_event_ts"),
        F.min(F.when(F.col("action") == "cart_add", F.col("event_ts"))).alias("first_cart_ts"),
        F.max(F.when(F.col("action") == "cart_add", F.col("event_ts"))).alias("last_cart_ts"),
        F.min(F.when(F.col("action") == "browse", F.col("event_ts"))).alias("first_browse_ts"),
        F.max(F.when(F.col("action") == "browse", F.col("event_ts"))).alias("last_browse_ts"),
        F.max(F.when(F.col("action") == "favorite", F.col("event_ts"))).alias("last_favorite_ts"),
        F.max(F.col("unknown_category").cast("int")).alias("unknown_category_flag"))
    pair = pair.withColumn("pair_recency_hours", (F.col("anchor_ts") - F.col("feature_max_event_ts")) / 3600)
    pair = pair.withColumn("pair_span_hours", (F.col("feature_max_event_ts") - F.col("pair_first_ts")) / 3600)
    for name, left, right in [("cart_then_revisit", "first_cart_ts", "last_browse_ts"),
                               ("browse_then_cart", "first_browse_ts", "last_cart_ts"),
                               ("browse_then_favorite", "first_browse_ts", "last_favorite_ts")]:
        pair = pair.withColumn(name, F.coalesce((F.col(left) < F.col(right)).cast("int"), F.lit(0)))
    users = history.groupBy("platform", "user_id", "anchor_ts").agg(
        F.count("*").alias("user_events"), F.countDistinct("item_id").alias("user_items"),
        F.countDistinct("category_id").alias("user_categories"),
        F.sum(F.when(F.col("action") == "cart_add", 1).otherwise(0)).alias("user_cart"))
    items = history.groupBy("platform", "item_id", "anchor_ts").agg(
        F.countDistinct("user_id").alias("item_users"), F.count("*").alias("item_events"))
    groups = history.groupBy(*KEYS, "event_ts").agg(F.sort_array(F.collect_set("action")).alias("actions"))
    sequences = groups.groupBy(*KEYS).agg(F.sort_array(F.collect_list(F.struct("event_ts", "actions"))).alias("time_groups"))
    sequences = sequences.withColumn("sequence", F.transform("time_groups", lambda x: x["actions"]))
    sequences = sequences.withColumn("sequence_length", F.size("sequence")).drop("time_groups")
    features = candidates.join(pair, KEYS).join(users, ["platform", "user_id", "anchor_ts"]) \
        .join(items, ["platform", "item_id", "anchor_ts"]).join(sequences, KEYS)
    positives = crossed.filter((F.col("event_ts") >= F.col("anchor_ts")) &
                               (F.col("event_ts") < F.col("anchor_ts") + horizon) &
                               (F.col("action") == target)).select(*KEYS).distinct().withColumn("label", F.lit(1))
    labeled = features.join(positives, KEYS, "left").fillna({"label": 0}).join(anchors, "anchor_ts")
    return labeled, users, positives


def features_stage(spark, cfg, root):
    quality = read_json(root / "quality.json")
    if quality["missing_days"]:
        raise ValueError("覆盖日期不完整，不生成确定负标签。")
    events = read_table(spark, root, "clean")
    anchors = anchor_frame(spark, cfg)
    frame, users, positives = build_candidate_features(events, anchors, cfg)
    patterns = read_json(root / "pattern_features.json")["patterns"]
    for pattern in patterns:
        sequence = pattern["sequence"]
        matcher = F.udf(lambda seq, p=sequence: int(pattern_matches(seq, p)), T.IntegerType())
        frame = frame.withColumn(pattern["column"], matcher("sequence"))
    frame = frame.cache()
    if frame.filter(F.col("feature_max_event_ts") >= F.col("anchor_ts")).limit(1).count():
        raise ValueError("检测到特征引用预测时点之后的事件。")
    duplicates = frame.groupBy(*KEYS).count().filter("count != 1").limit(1).count()
    if duplicates:
        raise ValueError("候选特征键不唯一，可能发生关联膨胀。")
    save_table(frame.drop("sequence"), root / "features")
    summary = frame.groupBy("split", "snapshot_date").agg(
        F.count("*").alias("candidates"), F.sum("label").alias("positives"),
        F.countDistinct("user_id").alias("users"), F.countDistinct("item_id").alias("items"))
    save_table(summary, root / "sample_summary")
    future_counts = positives.groupBy("anchor_ts").count().withColumnRenamed("count", "future_positive_pairs")
    covered = frame.groupBy("anchor_ts").agg(F.sum("label").alias("covered_positive_pairs"))
    coverage = anchors.join(future_counts, "anchor_ts", "left").join(covered, "anchor_ts", "left").fillna(0)
    coverage = coverage.withColumn("candidate_coverage", F.when(F.col("future_positive_pairs") > 0,
                                  F.col("covered_positive_pairs") / F.col("future_positive_pairs")))
    save_table(coverage, root / "candidate_coverage")
    # Ordered historical stages, followed by a complete future target window.
    funnels = []
    for branch, prefix in [("浏览→目标事件", F.col("pair_browse") > 0),
                           ("浏览→加购→目标事件", F.col("browse_then_cart") == 1),
                           ("浏览→收藏→目标事件", F.col("browse_then_favorite") == 1)]:
        row = frame.filter(prefix).agg(F.count("*").alias("candidates"), F.sum("label").alias("positives")).first()
        funnels.append((branch, row["candidates"], row["positives"] or 0))
    save_table(spark.createDataFrame(funnels, "branch string, candidates long, positives long"), root / "funnels")
    # Each profile is a historical user snapshot. Fit clustering on training snapshots only.
    profiles = users.join(anchors, "anchor_ts")
    profile_columns = ["user_events", "user_items", "user_categories", "user_cart"]
    for col in profile_columns:
        profiles = profiles.withColumn("log_" + col, F.log1p(col))
    k = min(cfg["mining"]["clusters"], profiles.filter("split = 'train'").count())
    if k < 2:
        raise ValueError("训练画像不足以形成至少两个群体。")
    cluster_pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=["log_" + c for c in profile_columns], outputCol="profile_vector"),
        StandardScaler(inputCol="profile_vector", outputCol="scaled_profile", withMean=True, withStd=True),
        KMeans(k=k, seed=cfg["project"]["seed"], maxIter=20, featuresCol="scaled_profile", predictionCol="cluster")])
    cluster_model = cluster_pipeline.fit(profiles.filter("split = 'train'"))
    clustered = cluster_model.transform(profiles)
    cluster_model.write().save(str(root / "models" / "user_clusters"))
    save_table(clustered.select("platform", "user_id", "anchor_ts", "snapshot_date", "split", "cluster", *profile_columns), root / "profiles")
    silhouette = ClusteringEvaluator(featuresCol="scaled_profile", predictionCol="cluster").evaluate(
        clustered.filter("split = 'train'"))
    write_json(root / "feature_dictionary.json", {"base": BASE_FEATURES,
               "path": PATH_FEATURES + [p["column"] for p in patterns],
               "max_history_exclusive": "anchor_ts", "cluster_silhouette_training": silhouette,
               "cluster_k": k, "note": "群体仅代表历史行为，不代表职业、收入或真实价值。"})
    frame.unpersist()
    return {"anchors": len(cfg["_anchors"]), "path_patterns": len(patterns), "clusters": k}
