from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_config
from .demo import generate_demo
from .pipeline import STAGES, run_pipeline


def main(argv=None):
    parser = argparse.ArgumentParser(description="电商行为路径与购买转化研究")
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser("demo", help="生成人工流程验证数据，不是真实业务数据")
    demo.add_argument("--output", type=Path, default=Path("data/demo/UserBehavior.csv"))
    demo.add_argument("--users", type=int, default=180)
    demo.add_argument("--seed", type=int, default=20261022)
    run = sub.add_parser("run", help="执行某个阶段或完整研究流程")
    run.add_argument("--config", default="configs/demo.yaml")
    run.add_argument("--run-id", required=True)
    run.add_argument("--stage", choices=["all"] + STAGES, default="all")
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            result = generate_demo(args.output, args.users, args.seed)
            print(f"已生成 {result['rows']} 条人工事件。仅用于流程验证：{args.output}")
        else:
            root = run_pipeline(load_config(args.config), args.run_id, args.stage)
            print(f"阶段结束；请核对run.json中的实际完成范围：{root}")
        return 0
    except (ValueError, FileNotFoundError, FileExistsError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
