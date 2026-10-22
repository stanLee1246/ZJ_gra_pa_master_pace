from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(8 * 1024 * 1024):
            h.update(block)
    return h.hexdigest()


def save_table(df, path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"产物已存在，请使用新的run-id或从未完成阶段继续：{path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("errorifexists").parquet(str(path))


def read_table(spark, root: Path, name: str):
    path = root / name
    if not (path / "_SUCCESS").exists():
        raise FileNotFoundError(f"缺少完整产物 {name}，请先执行前置阶段。")
    return spark.read.parquet(str(path))


def spark_session(cfg):
    from pyspark.sql import SparkSession

    settings = cfg["spark"]
    # Set before starting Java, rather than changing heap size after JVM startup.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (SparkSession.builder.master(settings["master"])
               .appName("ecommerce-behavior-research")
               .config("spark.driver.memory", settings["driver_memory"])
               .config("spark.sql.shuffle.partitions", settings["shuffle_partitions"])
               .config("spark.default.parallelism", settings["shuffle_partitions"])
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.csv.parser.columnPruning.enabled", "false")
               .config("spark.ui.enabled", "false")
               .config("spark.ui.showConsoleProgress", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    return session
