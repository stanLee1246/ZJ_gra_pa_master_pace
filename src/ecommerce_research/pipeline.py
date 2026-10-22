from __future__ import annotations

import platform
import hashlib
import importlib.metadata
import subprocess
import time
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

from .config import config_hash, public_config, run_directory
from .storage import read_json, spark_session, write_json

STAGES = ["inspect", "clean", "sessions", "mine", "features", "train", "evaluate", "reports"]


def execution_identity(cfg):
    digest = hashlib.sha256()
    for source in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(source.name.encode())
        digest.update(source.read_bytes())
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cfg["_project_root"], capture_output=True, text=True)
    return {"package_source_sha256": digest.hexdigest(), "git_head": git.stdout.strip() if git.returncode == 0 else None,
            "dependencies": {name: importlib.metadata.version(name) for name in ("pyspark", "duckdb", "streamlit", "scikit-learn")},
            "note": "源码哈希包含未提交改动；Git HEAD本身不能代表工作区全部内容。"}


def run_pipeline(cfg, run_id, stage="all"):
    from .evaluation import evaluate_stage
    from .features import features_stage
    from .mining import mining_stage, sessions_stage
    from .modeling import train_stage
    from .quality import clean_events, inspect_sources
    from .reporting import reports_stage

    root = run_directory(cfg, run_id)
    identity = execution_identity(cfg)
    root.mkdir(parents=True, exist_ok=True)
    metadata_path = root / "run.json"
    if metadata_path.exists():
        metadata = read_json(metadata_path)
        if metadata["config_hash"] != config_hash(cfg):
            raise ValueError("该run-id绑定了另一份配置。请使用新的run-id。")
    else:
        metadata = {"run_id": run_id, "config_hash": config_hash(cfg), "config": public_config(cfg),
                    "platform": cfg["data"]["platform"], "kind": cfg["data"]["kind"],
                    "python": platform.python_version(), "created_at": datetime.now(timezone.utc).isoformat(),
                    "status": "incomplete", "stages": {}}
        write_json(metadata_path, metadata)
    selected = STAGES if stage == "all" else [stage]
    session = None
    actions = {"clean": clean_events, "sessions": sessions_stage, "mine": mining_stage,
               "features": features_stage, "train": train_stage,
               "evaluate": evaluate_stage, "reports": reports_stage}
    try:
        for name in selected:
            if metadata["stages"].get(name, {}).get("status") == "complete":
                print(f"[skip] {name} 已完成", flush=True)
                continue
            print(f"[stage] {name}", flush=True)
            started = time.perf_counter()
            metadata["stages"][name] = {"status": "running", "execution": identity}
            write_json(metadata_path, metadata)
            try:
                if name == "inspect":
                    result = {"files": len(inspect_sources(cfg, root))}
                else:
                    if session is None:
                        session = spark_session(cfg)
                        metadata["spark_version"] = session.version
                    result = actions[name](session, cfg, root)
                metadata["stages"][name] = {"status": "complete", "seconds": time.perf_counter() - started,
                                            "result": result, "execution": identity}
                write_json(metadata_path, metadata)
            except Exception as exc:
                metadata["status"] = "failed"
                metadata["stages"][name] = {"status": "failed", "seconds": time.perf_counter() - started,
                                            "error": f"{type(exc).__name__}: {exc}", "execution": identity}
                write_json(metadata_path, metadata)
                raise
        metadata["status"] = "complete" if all(metadata["stages"].get(s, {}).get("status") == "complete"
                                                for s in STAGES) else "incomplete"
        write_json(metadata_path, metadata)
    finally:
        if session is not None:
            with suppress(Exception):
                session.stop()
    return root
