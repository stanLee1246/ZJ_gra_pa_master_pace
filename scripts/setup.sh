#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps -e .
.venv/bin/python -c 'import sys,pyspark; print("Python",sys.version.split()[0],"Spark",pyspark.__version__)'
java -version
