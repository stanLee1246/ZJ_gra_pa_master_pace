"""Synthetic fixtures only: not public platform observations or thesis results."""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .storage import write_json


def generate_demo(output: Path, users: int = 180, seed: int = 20261022) -> dict:
    if users < 30:
        raise ValueError("演示至少需要30个用户以覆盖分群及时间切分。")
    if output.exists():
        raise FileExistsError(f"不覆盖已有数据：{output}")
    rng = random.Random(seed)
    base = datetime(2017, 11, 25, tzinfo=ZoneInfo("Asia/Shanghai"))
    rows = []
    for day in range(9):
        for u in range(users):
            for j in range(2):
                item = str((u * 3 + j + (day // 3) * 7) % 90)
                cat = str(int(item) % 9)
                when = base + timedelta(days=day, hours=rng.randrange(8, 20), minutes=rng.randrange(60))
                rows.append([str(u), item, cat, "pv", str(int(when.timestamp()))])
                cart = rng.random() < 0.38
                favorite = rng.random() < 0.23
                if favorite:
                    rows.append([str(u), item, cat, "fav", str(int(when.timestamp()) + 120)])
                if cart:
                    rows.append([str(u), item, cat, "cart", str(int(when.timestamp()) + 240)])
                    rows.append([str(u), item, cat, "pv", str(int(when.timestamp()) + 300)])
                if rng.random() < (0.42 if cart else 0.10 if favorite else 0.025):
                    purchase = when + timedelta(hours=rng.randrange(6, 24))
                    if purchase < base + timedelta(days=9):
                        rows.append([str(u), item, cat, "buy", str(int(purchase.timestamp()))])
    # Deliberate quality probes, explicitly preserved in the source sidecar.
    rows += [rows[0][:], ["", "1", "1", "pv", str(int(base.timestamp()))],
             ["Q1", "1", "1", "pv", "not-a-time"],
             ["Q2", "1", "1", "unknown", str(int(base.timestamp()))],
             ["Q3", "1", "1", "pv", str(int(base.timestamp()) - 1)],
             ["Q4", "1", "", "pv", str(int(base.timestamp()) + 60)]]
    rng.shuffle(rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)
    metadata = {"kind": "synthetic", "version": "demo-v1", "seed": seed, "users_requested": users,
                "rows": len(rows), "warning": "人为生成的流程验证数据，不能作为真实业务实验结论。",
                "coverage_start": base.isoformat(), "coverage_end": (base + timedelta(days=9)).isoformat()}
    write_json(output.with_suffix(".source.json"), metadata)
    return metadata
