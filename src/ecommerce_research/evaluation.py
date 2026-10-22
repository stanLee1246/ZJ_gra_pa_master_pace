from __future__ import annotations

from pyspark.sql import Window, functions as F

from .storage import read_table, save_table, write_json


def ranking_curve(predictions, keys):
    """Exact score-tie-aware AP and trapezoidal ROC, without collecting predictions."""
    grouped = predictions.groupBy(*keys, "score").agg(F.count("*").alias("n"), F.sum("label").alias("p"))
    whole = Window.partitionBy(*keys)
    order = Window.partitionBy(*keys).orderBy(F.desc("score"))
    cumulative = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    curve = (grouped.withColumn("total_p", F.sum("p").over(whole))
             .withColumn("total_n", F.sum("n").over(whole))
             .withColumn("cum_p", F.sum("p").over(cumulative))
             .withColumn("cum_n", F.sum("n").over(cumulative)))
    curve = curve.withColumn("precision", F.col("cum_p") / F.col("cum_n"))
    curve = curve.withColumn("tpr", F.when(F.col("total_p") > 0, F.col("cum_p") / F.col("total_p")))
    curve = curve.withColumn("fpr", F.when(F.col("total_n") > F.col("total_p"),
                  (F.col("cum_n") - F.col("cum_p")) / (F.col("total_n") - F.col("total_p"))))
    curve = curve.withColumn("previous_tpr", F.lag("tpr", 1, 0.0).over(order))
    curve = curve.withColumn("previous_fpr", F.lag("fpr", 1, 0.0).over(order))
    return curve


def validation_thresholds(predictions):
    curve = ranking_curve(predictions.filter("split = 'validation'"), ["model"])
    curve = curve.withColumn("f1", 2 * F.col("cum_p") / (F.col("cum_n") + F.col("total_p")))
    order = Window.partitionBy("model").orderBy(F.desc("f1"), F.desc("score"))
    return curve.withColumn("rank", F.row_number().over(order)).filter("rank = 1").select(
        "model", F.col("score").alias("threshold"), F.col("f1").alias("validation_f1"))


def grouped_metrics(predictions, thresholds):
    keys = ["model", "snapshot_date"]
    curve = ranking_curve(predictions, keys)
    ranking = curve.groupBy(*keys).agg(
        F.sum(F.when(F.col("total_p") > 0, F.col("p") / F.col("total_p") * F.col("precision"))).alias("average_precision"),
        F.sum((F.col("fpr") - F.col("previous_fpr")) * (F.col("tpr") + F.col("previous_tpr")) / 2).alias("roc_auc"))
    scored = predictions.join(thresholds, "model")
    group = scored.groupBy(*keys).agg(
        F.count("*").alias("candidates"), F.sum("label").alias("positives"),
        F.avg("label").alias("positive_rate"), F.avg(F.pow(F.col("score") - F.col("label"), 2)).alias("brier_score"),
        F.sum(F.when(F.col("score") >= F.col("threshold"), 1).otherwise(0)).alias("predicted_positives"),
        F.sum(F.when((F.col("score") >= F.col("threshold")) & (F.col("label") == 1), 1).otherwise(0)).alias("true_positives"),
        F.first("threshold").alias("threshold"))
    group = group.withColumn("precision", F.when(F.col("predicted_positives") > 0,
                            F.col("true_positives") / F.col("predicted_positives")).otherwise(0.0))
    group = group.withColumn("recall", F.when(F.col("positives") > 0,
                            F.col("true_positives") / F.col("positives")))
    group = group.withColumn("f1", F.when(F.col("predicted_positives") + F.col("positives") > 0,
                            2 * F.col("true_positives") / (F.col("predicted_positives") + F.col("positives"))))
    result = group.join(ranking, keys)
    return result.withColumn("roc_auc", F.when((F.col("positives") > 0) &
                            (F.col("positives") < F.col("candidates")), F.col("roc_auc")))


def evaluate_stage(spark, cfg, root):
    predictions = read_table(spark, root, "predictions").cache()
    thresholds = validation_thresholds(predictions).cache()
    write_json(root / "thresholds.json", [r.asDict() for r in thresholds.collect()])
    test = predictions.filter("split = 'test'")
    expanded = test.unionByName(test.withColumn("snapshot_date", F.lit("ALL")))
    metrics = grouped_metrics(expanded, thresholds)
    save_table(metrics, root / "metrics")
    order = Window.partitionBy("model", "snapshot_date").orderBy(F.desc("score"),
             F.xxhash64("platform", "user_id", "item_id", "anchor_ts", F.lit(cfg["project"]["seed"])))
    ranked = expanded.withColumn("rank", F.row_number().over(order))
    sizes = expanded.groupBy("model", "snapshot_date").agg(F.count("*").alias("total"), F.sum("label").alias("positive"))
    ranked = ranked.join(sizes, ["model", "snapshot_date"])
    topk = None
    for fraction in (0.01, 0.05, 0.10):
        part = ranked.filter(F.col("rank") <= F.ceil(F.col("total") * fraction)).groupBy("model", "snapshot_date").agg(
            F.count("*").alias("k"), F.sum("label").alias("hits"), F.first("total").alias("total"),
            F.first("positive").alias("positive")).withColumn("fraction", F.lit(fraction))
        part = part.withColumn("precision_at_k", F.col("hits") / F.col("k"))
        part = part.withColumn("recall_at_k", F.when(F.col("positive") > 0, F.col("hits") / F.col("positive")))
        part = part.withColumn("lift_at_k", F.when(F.col("positive") > 0,
                              F.col("precision_at_k") / (F.col("positive") / F.col("total"))))
        topk = part if topk is None else topk.unionByName(part)
    save_table(topk, root / "topk")
    calibrated = test.withColumn("bin", F.least(F.lit(9), F.floor(F.col("score") * 10)))
    save_table(calibrated.groupBy("model", "bin").agg(F.count("*").alias("count"),
               F.avg("score").alias("mean_score"), F.avg("label").alias("observed_rate")), root / "calibration")
    strata = test.withColumn("cohort", F.when(F.col("pair_cart") > 0, "历史加购").otherwise("无历史加购"))
    save_table(strata.groupBy("model", "cohort").agg(F.count("*").alias("candidates"),
               F.avg("label").alias("positive_rate"), F.avg(F.pow(F.col("score") - F.col("label"), 2)).alias("brier_score")), root / "cohorts")
    thresholds.unpersist()
    predictions.unpersist()
    return {"primary_metric": "exact-score-tie-aware-average-precision", "threshold_source": "validation-only"}
