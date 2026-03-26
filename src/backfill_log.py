#!/usr/bin/env python3
"""
One-time backfill: populate data/convergence_log.jsonl from all existing
output/analysis_*.json files in chronological order.

Usage: python src/backfill_log.py
"""

import json
import sys
from pathlib import Path

# Allow imports from this directory
sys.path.insert(0, str(Path(__file__).parent))
from analyzer import append_convergence_log, BASE_DIR

log_path = BASE_DIR / "data" / "convergence_log.jsonl"
output_dir = BASE_DIR / "output"

files = sorted(output_dir.glob("analysis_*.json"))
if not files:
    print("No analysis files found in output/")
    sys.exit(0)

# Clear existing log — backfill is a full rebuild
if log_path.exists():
    log_path.unlink()
    print(f"Cleared existing {log_path.name}")

print(f"Backfilling from {len(files)} analysis file(s)...")

ok = 0
for f in files:
    try:
        analysis = json.loads(f.read_text())
        append_convergence_log(analysis, log_path)
        print(f"  ✓ {f.name}")
        ok += 1
    except Exception as e:
        print(f"  ✗ {f.name}: {e}")

total = len(log_path.read_text().strip().splitlines()) if log_path.exists() else 0
print(f"\nDone. {ok}/{len(files)} files processed — {total} records in convergence_log.jsonl")
