"""
run_all.py
----------
Master script. Runs the full pipeline:
  1. gimbal_download  -> downloads .xlsx from Gimbal
  2. gimbal_transform -> transforms .xlsx into two CSVs
  3. gimbal_upload    -> uploads to HHAeXchange, saves failure CSVs
  4. gimbal_notify    -> emails hold report for successfully updated caregivers
"""

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root.parent))
sys.path.insert(0, str(repo_root / "UpdateCaregivers"))

from gimbal_download import download_gimbal_report, GIMBAL_EMAIL_PA, GIMBAL_PASSWORD_PA, PROJECT_NAME_PA
from gimbal_transform import transform, transform_pa
from gimbal_upload import run as upload_all
from gimbal_notify import run as notify


def main():
    print("=" * 50)
    print("STEP 1: Download from Gimbal")
    print("=" * 50)
    xlsx_path = download_gimbal_report()

    print("\nDownloading from Gimbal PA account...")
    xlsx_path_pa = download_gimbal_report(
        email=GIMBAL_EMAIL_PA,
        password=GIMBAL_PASSWORD_PA,
        project_name=PROJECT_NAME_PA,
    )

    print("\n" + "=" * 50)
    print("STEP 2: Transform data")
    print("=" * 50)
    csv_paths = transform(xlsx_path)
    csv_paths.update(transform_pa(xlsx_path_pa))

    print("\n" + "=" * 50)
    print("STEP 3: Upload to HHAeXchange")
    print("=" * 50)
    all_results = upload_all(csv_paths)

    print("\n" + "=" * 50)
    print("STEP 4: Send hold notification email")
    print("=" * 50)
    notify(all_results)

    print("\n" + "=" * 50)
    print("ALL DONE")
    print("=" * 50)


if __name__ == "__main__":
    main()