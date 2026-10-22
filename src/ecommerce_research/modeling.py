from __future__ import annotations

import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from .storage import read_json, read_table, save_table, write_json


def train_stage(spark, cfg, root):
    frame = read_table(spark, root, "features").cache()
    train = frame.filter("split = 'train'")
    if train.select("label").distinct().count() != 2:
        raise ValueError("训练数据必须同时有正负样本，不能伪造标签继续训练。")
    if frame.filter("split = 'validation'").select("label").distinct().count() != 2:
        raise ValueError("验证集缺少正例或负例，无法可靠选择阈值。")
    evaluation = frame.filter("split IN ('validation', 'test')")
    dictionary = read_json(root / "feature_dictionary.json")
    base, path = dictionary["base"], dictionary["path"]
    retained = ["platform", "user_id", "item_id", "anchor_ts", "snapshot_date", "split", "label",
                "feature_max_event_ts", "pair_browse", "pair_cart", "pair_favorite", "cart_then_revisit"]
    prevalence = train.agg(F.avg("label")).first()[0]
    all_predictions = None
    timing, importance = [], []
    definitions = [
        ("constant", [], None), ("rule", [], None),
        ("logistic", base, LogisticRegression(featuresCol="scaled_features", labelCol="label", regParam=0.1,
                                              maxIter=cfg["model"]["lr_iterations"])),
        ("gbt_base", base, GBTClassifier(featuresCol="scaled_features", labelCol="label",
                   maxIter=cfg["model"]["gbt_iterations"], maxDepth=cfg["model"]["gbt_depth"],
                   seed=cfg["project"]["seed"], stepSize=0.1)),
        ("gbt_paths", base + path, GBTClassifier(featuresCol="scaled_features", labelCol="label",
                   maxIter=cfg["model"]["gbt_iterations"], maxDepth=cfg["model"]["gbt_depth"],
                   seed=cfg["project"]["seed"], stepSize=0.1)),
    ]
    for name, features, classifier in definitions:
        print(f"[train] {name}", flush=True)
        started = time.perf_counter()
        if name == "constant":
            scored = evaluation.withColumn("score", F.lit(prevalence))
        elif name == "rule":
            scored = evaluation.withColumn("score", F.lit(0.05) +
                     F.when(F.col("pair_cart") > 0, 0.45).otherwise(0.0) +
                     F.when(F.col("pair_favorite") > 0, 0.15).otherwise(0.0) +
                     F.col("cart_then_revisit") * 0.2)
        else:
            pipeline = Pipeline(stages=[VectorAssembler(inputCols=features, outputCol="raw_features"),
                    StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True), classifier])
            fitted = pipeline.fit(train)
            fitted.write().save(str(root / "models" / name))
            final = fitted.stages[-1]
            values = final.coefficients.toArray() if name == "logistic" else final.featureImportances.toArray()
            importance += [{"model": name, "feature": key, "value": float(value),
                            "method": "scaled_coefficient" if name == "logistic" else "tree_importance"}
                           for key, value in zip(features, values)]
            scored = fitted.transform(evaluation).withColumn("score", vector_to_array("probability")[1])
        scored = scored.select(*retained, "score").withColumn("model", F.lit(name))
        save_table(scored, root / "prediction_parts" / name)
        materialized = spark.read.parquet(str(root / "prediction_parts" / name))
        all_predictions = materialized if all_predictions is None else all_predictions.unionByName(materialized)
        timing.append({"model": name, "fit_and_score_seconds": time.perf_counter() - started,
                       "features": features, "hyperparameters_selected_by_test": False})
    save_table(all_predictions, root / "predictions")
    write_json(root / "training.json", {"models": timing, "constant_training_prevalence": prevalence,
               "seed": cfg["project"]["seed"], "parameter_search": "fixed-first-version-no-test-tuning",
               "training_negative_sampling": "none", "kind": cfg["data"]["kind"]})
    write_json(root / "feature_importance.json", importance)
    frame.unpersist()
    return {"models": [x[0] for x in definitions]}
