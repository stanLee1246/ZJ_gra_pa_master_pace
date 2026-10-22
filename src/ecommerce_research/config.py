from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

ACTION_MAPS = {
    "taobao": {"pv": "browse", "fav": "favorite", "cart": "cart_add", "buy": "purchase"},
    "jd": {"1": "browse", "2": "cart_add", "3": "cart_remove", "4": "order",
           "5": "favorite", "6": "click"},
}
TARGETS = {"taobao": "purchase", "jd": "order"}
CANDIDATE_ACTIONS = ["browse", "favorite", "cart_add", "cart_remove", "click"]


def epoch(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("时间必须包含时区偏移。")
    return int(dt.timestamp())


def load_config(path: str | Path) -> dict:
    path = Path(path).resolve()
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict) or not isinstance(cfg.get("data"), dict):
        raise ValueError("配置缺少data。")
    data = cfg["data"]
    if data.get("platform") not in ACTION_MAPS or data.get("enabled") is False:
        raise ValueError("该平台尚未接入经核验的行为数据和事件字典。")
    if data.get("kind") not in {"synthetic", "public", "authorized"}:
        raise ValueError("data.kind必须明确为synthetic/public/authorized。")
    if not data.get("coverage_start") or not data.get("coverage_end"):
        raise ValueError("必须依据原始字典配置完整的coverage_start/coverage_end。")
    start, end = epoch(data["coverage_start"]), epoch(data["coverage_end"])
    if start >= end:
        raise ValueError("数据覆盖起点必须早于终点。")
    tz = ZoneInfo(data["business_timezone"])
    task = cfg["task"]
    if task["lookback_hours"] <= 0 or task["horizon_hours"] <= 0:
        raise ValueError("时间窗口必须为正数。")
    anchors = []
    for split in ("train", "validation", "test"):
        dates = task.get(f"{split}_dates", [])
        if not dates:
            raise ValueError(f"必须配置{split}_dates，不能使用占位时间。")
        for day in dates:
            dt = datetime.combine(date.fromisoformat(str(day)), time.min, tzinfo=tz)
            t = int(dt.timestamp())
            if t - task["lookback_hours"] * 3600 < start or t + task["horizon_hours"] * 3600 > end:
                raise ValueError(f"{day}的观察或标签窗口超出完整数据覆盖。")
            anchors.append({"anchor_ts": t, "snapshot_date": str(day), "split": split})
    ts = [a["anchor_ts"] for a in anchors]
    if len(ts) != len(set(ts)):
        raise ValueError("锚点不能重复或同时属于不同切分。")
    for left, right in (("train", "validation"), ("validation", "test")):
        left_end = max(a["anchor_ts"] for a in anchors if a["split"] == left) + task["horizon_hours"] * 3600
        right_start = min(a["anchor_ts"] for a in anchors if a["split"] == right)
        if left_end > right_start:
            raise ValueError(f"{left}标签尚未完整时已经进入{right}预测，存在时间泄漏。")
    base = path.parent.parent
    cfg["_project_root"] = str(base)
    cfg["_config_file"] = str(path)
    cfg["_anchors"] = anchors
    return cfg


def config_hash(cfg: dict) -> str:
    value = {k: v for k, v in cfg.items() if not k.startswith("_")}
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def resolve_path(cfg: dict, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else Path(cfg["_project_root"]) / p


def run_directory(cfg: dict, run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", run_id):
        raise ValueError("run-id仅支持字母、数字、短横线、下划线，最长80字符。")
    return resolve_path(cfg, cfg["output_root"]) / run_id


def public_config(cfg: dict) -> dict:
    return copy.deepcopy({k: v for k, v in cfg.items() if not k.startswith("_")})
