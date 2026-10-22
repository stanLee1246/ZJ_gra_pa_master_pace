"""Compatibility wrapper for the command interface proposed in the plan."""
from ecommerce_research.cli import main

if __name__ == "__main__":
    import sys
    raise SystemExit(main(["run", *sys.argv[1:]]))
