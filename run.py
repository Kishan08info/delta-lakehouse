"""Run the full Delta Lake lakehouse demo end to end.

Usage:
    python run.py            regenerate data + run pipeline
    python run.py --fresh    wipe warehouse/ + outputs/ first, then run
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", action="store_true")
    args = ap.parse_args()

    if args.fresh:
        for d in [ROOT / "warehouse", ROOT / "outputs"]:
            if d.exists():
                shutil.rmtree(d)
        print("wiped warehouse/ and outputs/")

    for script in ["generate.py", "pipeline.py", "analysis.py"]:
        print(f"\n=== {script} ===")
        r = subprocess.run([sys.executable, str(ROOT / script)], check=False)
        if r.returncode != 0:
            print(f"{script} FAILED (exit {r.returncode})")
            sys.exit(r.returncode)
    print("\nDone. See outputs/summary.json")


if __name__ == "__main__":
    main()
