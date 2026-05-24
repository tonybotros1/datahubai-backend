#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8000}"
"$(dirname "$0")/.venv/bin/python" -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
