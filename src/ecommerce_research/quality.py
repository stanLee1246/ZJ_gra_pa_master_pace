from __future__ import annotations

import csv
import glob
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import functions as F, types as T

from .config import ACTION_MAPS, epoch, resolve_path
from .storage import read_json, save_table, sha256_file, write_json


def inspect_sources(cfg: dict, root: Path) -> list[dict]:
    files = sorted(Path(p).resolve() for p in glob.glob(str(resolve_path(cfg, cfg["data"]["input_glob"]))))
    if not files:
        raise FileNotFoundError("未找到原始CSV。请依据docs/DATA_ACCESS.md准备数据，或先运行demo命令。")
    manifest, seen = [], set()
    for path in files:
        digest = sha256_file(path)
        if digest in seen:
            raise ValueError("输入包含内容完全相同的文件，拒绝重复导入。")
        seen.add(digest)
        sidecar = path.with_suffix(".source.json")
        source = read_json(sidecar) if sidecar.exists() else {}
        if source.get("kind") == "synthetic" and cfg["data"]["kind"] != "synthetic":
            raise ValueError("演示文件不能声明为公开或真实授权数据。")
        if source.get("coverage_start") and (source["coverage_start"] != cfg["data"]["coverage_start"]
                                             or source["coverage_end"] != cfg["data"]["coverage_end"]):
            # Compare instants, allowing equivalent ISO representations.
            if epoch(source["coverage_start"]) != epoch(cfg["data"]["coverage_start"]) or epoch(source["coverage_end"]) != epoch(cfg["data"]["coverage_end"]):
                raise ValueError("配置覆盖范围与源文件说明不符。")
        manifest.append({"file": str(path), "sha256": digest, "bytes": path.stat().st_size,
                         "platform": cfg["data"]["platform"], "kind": cfg["data"]["kind"],
                         "dataset_version": cfg["data"]["version"],
                         "coverage_declaration": "source-sidecar" if source else "operator-config-unverified",
                         "source_metadata": source})
    write_json(root / "manifest.json", manifest)
    return manifest


def _read_source(spark, cfg: dict, path: str):
    platform = cfg["data"]["platform"]
    if platform == "taobao":
        columns = ["user_id", "item_id", "category_id", "raw_action", "raw_timestamp"]
    else:
        with open(path, encoding=cfg["data"].get("encoding", "utf-8"), newline="") as f:
            columns = next(csv.reader(f))
        if len(columns) != len(set(columns)):
            raise ValueError("京东表头存在重复列。")
        required = {"user_id", "sku_id", "time", "type"}
        if not required.issubset(columns):
            raise ValueError(f"京东表缺少必要列：{sorted(required - set(columns))}")
    schema = T.StructType([T.StructField(c, T.StringType()) for c in columns + ["_corrupt_record"]])
    raw = (spark.read.schema(schema).option("header", str(platform == "jd").lower())
           .option("encoding", cfg["data"].get("encoding", "utf-8"))
           .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").csv(path))
    if platform == "jd":
        raw = raw.select(F.col("user_id"), F.col("sku_id").alias("item_id"),
                         (F.col("cate") if "cate" in columns else F.lit(None)).alias("category_id"),
                         F.col("type").alias("raw_action"), F.col("time").alias("raw_timestamp"),
                         F.col("_corrupt_record"))
    return raw.withColumn("source_file", F.lit(path))


def clean_events(spark, cfg: dict, root: Path) -> dict:
    manifest = read_json(root / "manifest.json")
    for entry in manifest:
        if sha256_file(Path(entry["file"])) != entry["sha256"]:
            raise ValueError("来源文件在inspect后发生变化，请使用新run-id重新验收。")
    raw = None
    for entry in manifest:
        part = _read_source(spark, cfg, entry["file"])
        raw = part if raw is None else raw.unionByName(part)
    platform = cfg["data"]["platform"]
    mapping = F.create_map(*[F.lit(x) for pair in ACTION_MAPS[platform].items() for x in pair])
    frame = raw.withColumn("platform", F.lit(platform))
    for column in ("user_id", "item_id", "category_id", "raw_action", "raw_timestamp"):
        frame = frame.withColumn(column, F.trim(F.col(column)))
    frame = frame.withColumn("action", mapping[F.col("raw_action")])
    if platform == "taobao":
        frame = frame.withColumn("event_ts", F.when(F.col("raw_timestamp").rlike(r"^\d{10}$"),
                                                   F.col("raw_timestamp").try_cast("long")))
    else:
        parsed = F.try_to_timestamp("raw_timestamp", F.lit("yyyy-MM-dd HH:mm:ss"))
        frame = frame.withColumn("event_ts", F.to_utc_timestamp(parsed, cfg["data"].get("source_timezone", "Asia/Shanghai")).cast("long"))
    start, end = epoch(cfg["data"]["coverage_start"]), epoch(cfg["data"]["coverage_end"])
    frame = frame.withColumn("reject_reason",
        F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_ROW")
        .when(F.col("user_id").isNull() | (F.col("user_id") == ""), "MISSING_USER")
        .when(F.col("item_id").isNull() | (F.col("item_id") == ""), "MISSING_ITEM")
        .when(F.col("event_ts").isNull(), "INVALID_TIME")
        .when(F.col("action").isNull(), "UNKNOWN_ACTION")
        .when((F.col("event_ts") < start) | (F.col("event_ts") >= end), "OUT_OF_SCOPE"))
    frame = frame.withColumn("unknown_category", F.col("category_id").isNull() | (F.col("category_id") == ""))
    frame = frame.withColumn("category_id", F.when(F.col("unknown_category"), "UNKNOWN").otherwise(F.col("category_id")))
    frame = frame.withColumn("event_time", F.timestamp_seconds("event_ts"))
    frame = frame.withColumn("event_date", F.to_date(F.from_utc_timestamp("event_time", cfg["data"]["business_timezone"])))
    frame = frame.cache()
    counts = {r["reject_reason"] or "ACCEPTED": r["count"] for r in frame.groupBy("reject_reason").count().collect()}
    accepted = frame.filter("reject_reason IS NULL").drop("reject_reason", "_corrupt_record")
    rejected = frame.filter("reject_reason IS NOT NULL")
    if not counts.get("ACCEPTED"):
        frame.unpersist()
        raise ValueError("没有有效事件，停止后续任务。")
    observed = {str(r["event_date"]) for r in accepted.select("event_date").distinct().collect()}
    first = datetime.fromisoformat(cfg["data"]["coverage_start"]).date()
    last = datetime.fromisoformat(cfg["data"]["coverage_end"]).date()
    required_days = {(first + timedelta(days=i)).isoformat() for i in range((last - first).days)}
    missing = sorted(required_days - observed)
    duplicates = accepted.groupBy("platform", "user_id", "item_id", "action", "event_ts").count().filter("count > 1")
    duplicate_summary = duplicates.agg(F.count("*").alias("groups"), F.sum(F.col("count") - 1).alias("excess")).first()
    summary = {"input_rows": sum(counts.values()), "retained_rows": counts.get("ACCEPTED", 0),
               "rejected_rows": sum(v for k, v in counts.items() if k != "ACCEPTED"),
               "by_reason": counts, "unknown_category_rows": accepted.filter("unknown_category").count(),
               "uncertain_duplicate_groups": duplicate_summary["groups"],
               "uncertain_duplicate_excess_rows": duplicate_summary["excess"] or 0,
               "duplicate_policy": "preserve_and_report", "missing_days": missing,
               "coverage_status": "declared-and-day-presence-checked-not-proof-of-complete-logging",
               "kind": cfg["data"]["kind"]}
    save_table(accepted, root / "clean")
    save_table(rejected, root / "quarantine")
    save_table(duplicates, root / "duplicate_groups")
    save_table(accepted.groupBy("platform", "event_date", "action").agg(
        F.count("*").alias("events"), F.countDistinct("user_id").alias("users")), root / "overview")
    write_json(root / "quality.json", summary)
    frame.unpersist()
    if missing:
        raise ValueError(f"数据缺少完整日期 {missing}；产物已保留，但不允许运行标签构建。")
    return summary
