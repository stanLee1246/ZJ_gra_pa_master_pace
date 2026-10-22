from __future__ import annotations

from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import Window, functions as F

from .config import TARGETS
from .storage import read_table, save_table, write_json


def pattern_matches(sequence, pattern) -> bool:
    """Ordered itemsets; simultaneous actions never imply an internal order."""
    pos = 0
    for itemset in sequence or []:
        if pos < len(pattern) and set(pattern[pos]).issubset(set(itemset)):
            pos += 1
    return pos == len(pattern)


def build_paths(events, gap_minutes: int):
    keys = ["platform", "user_id"]
    instants = events.select(*keys, "event_ts").distinct()
    w = Window.partitionBy(*keys).orderBy("event_ts")
    instants = instants.withColumn("previous_ts", F.lag("event_ts").over(w))
    instants = instants.withColumn("new_session", F.when(
        F.col("previous_ts").isNull() | (F.col("event_ts") - F.col("previous_ts") > gap_minutes * 60), 1).otherwise(0))
    instants = instants.withColumn("session_id", F.sum("new_session").over(w)).drop("new_session", "previous_ts")
    grouped = (events.join(instants, keys + ["event_ts"])
               .groupBy(*keys, "session_id", "item_id", "event_ts")
               .agg(F.sort_array(F.collect_set("action")).alias("actions"), F.count("*").alias("raw_count")))
    paths = grouped.groupBy(*keys, "session_id", "item_id").agg(
        F.sort_array(F.collect_list(F.struct("event_ts", "actions"))).alias("groups"),
        F.sum("raw_count").alias("raw_count"), F.min("event_ts").alias("start_ts"),
        F.max("event_ts").alias("end_ts"))
    paths = paths.withColumn("sequence", F.transform("groups", lambda x: x["actions"]))
    return paths.withColumn("states", F.transform("sequence", lambda x: F.array_join(x, "+")))


def sessions_stage(spark, cfg, root):
    paths = build_paths(read_table(spark, root, "clean"), cfg["task"]["session_gap_minutes"])
    save_table(paths, root / "sessions")
    return {"path_granularity": "user-item-session", "same_second_policy": "unordered-itemset"}


def mining_stage(spark, cfg, root):
    cutoff = max(a["anchor_ts"] for a in cfg["_anchors"] if a["split"] == "train")
    historical = read_table(spark, root, "clean").filter(F.col("event_ts") < cutoff)
    paths = build_paths(historical, cfg["task"]["session_gap_minutes"]).cache()
    n = paths.count()
    if not n:
        paths.unpersist()
        raise ValueError("训练历史没有可挖掘路径。")
    raw_patterns = PrefixSpan(minSupport=cfg["mining"]["min_support"],
                             maxPatternLength=cfg["mining"]["max_pattern_length"],
                             maxLocalProjDBSize=2_000_000).findFrequentSequentialPatterns(paths.select("sequence"))
    patterns = raw_patterns.withColumn("support", F.col("freq") / F.lit(n))
    save_table(patterns, root / "patterns")
    candidates = patterns.filter(F.size("sequence") >= 2).orderBy(F.desc("freq"), F.to_json("sequence"))
    target = TARGETS[cfg["data"]["platform"]]
    selected = []
    # Bounded collection: only small pattern metadata is returned to the driver.
    for row in candidates.limit(cfg["mining"]["feature_patterns"] * 20).collect():
        pattern = row["sequence"]
        if all(target not in itemset for itemset in pattern):
            selected.append({"column": f"pattern_{len(selected):02d}", "sequence": pattern,
                             "training_support": row["support"], "training_sessions": row["freq"]})
        if len(selected) == cfg["mining"]["feature_patterns"]:
            break
    write_json(root / "pattern_features.json", {"fit_event_time_exclusive": cutoff,
               "denominator": "training-user-item-sessions", "sessions": n, "patterns": selected})
    expanded = paths.select("platform", "states", F.posexplode("states").alias("position", "from_state"))
    edges = expanded.withColumn("to_state", F.get("states", F.col("position") + 1)).filter("to_state IS NOT NULL")
    transitions = edges.groupBy("platform", "from_state", "to_state").count()
    totals = Window.partitionBy("platform", "from_state")
    transitions = transitions.withColumn("probability", F.col("count") / F.sum("count").over(totals))
    save_table(transitions, root / "transitions")
    save_table(paths.orderBy(F.desc("raw_count")).limit(200), root / "path_examples")
    paths.unpersist()
    return {"training_path_sessions": n, "feature_patterns": len(selected), "cutoff_exclusive": cutoff}
