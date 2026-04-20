"""
run_all.py
----------
Master script. Runs the full pipeline:
  1. gimbal_download  -> downloads .xlsx from Gimbal
  2. gimbal_transform -> transforms .xlsx into two CSVs
  3. gimbal_upload    -> uploads to HHAeXchange, saves failure CSVs
"""

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root.parent))
sys.path.insert(0, str(repo_root / "UpdateCaregivers"))

from gimbal_download import download_gimbal_report
from gimbal_transform import transform
from gimbal_upload import run as upload_all


def main():
    print("=" * 50)
    print("STEP 1: Download from Gimbal")
    print("=" * 50)
    xlsx_path = download_gimbal_report()

    print("\n" + "=" * 50)
    print("STEP 2: Transform data")
    print("=" * 50)
    csv_paths = transform(xlsx_path)

    print("\n" + "=" * 50)
    print("STEP 3: Upload to HHAeXchange")
    print("=" * 50)
    upload_all(csv_paths)

    print("\n" + "=" * 50)
    print("ALL DONE")
    print("=" * 50)


if __name__ == "__main__":
    main()