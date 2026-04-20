"""
gimbal_upload.py
----------------
Reads the two CSVs produced by gimbal_transform.py and uploads them
to HHAeXchange using the existing post_medicals functions.
Saves separate failure CSVs for each medical type.
"""

import asyncio
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Add parent of repo root and UpdateCaregivers to path so imports work
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root.parent))
sys.path.insert(0, str(repo_root / "UpdateCaregivers"))

from post_medicals import process_update_only_medical

load_dotenv()

DOWNLOAD_DIR = Path(os.environ.get("GIMBAL_DOWNLOAD_DIR", r"C:\Users\nochum.paltiel\Documents\Gimbal"))

SEMAPHORE = asyncio.Semaphore(3)

result_map = {
    "Completed (In Office)": "86358",
    "Completed (Elsewhere)": "86359",
    "Exempt":                "86360",
    "Declined":              "86361",
    "Negative":              "86379",
    "Completed":             "86350",
}

MEDICAL_NAMES = {
    "75556": "Annual_Health_Assessment",
    "75569": "TB_Screen",
}


async def safe_process(caregiver_code, medical_id, date_performed, result_id):
    async with SEMAPHORE:
        return await process_update_only_medical(
            caregiver_code, medical_id, date_performed, result_id
        )


async def upload_csv(csv_path: Path) -> list[dict]:
    """Upload all rows in a CSV and return list of failure dicts."""
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["Medical ID"])
    df["Medical ID"] = df["Medical ID"].astype(int).astype(str)

    tasks = []
    for _, row in df.iterrows():
        result_id = result_map.get(row["Result"])
        if not result_id:
            print(f"  Skipping {row['Caregiver Code']}: unknown result '{row['Result']}'")
            continue
        tasks.append(safe_process(
            row["Caregiver Code"],
            row["Medical ID"],
            row["Date Performed"],
            result_id,
        ))

    results = await asyncio.gather(*tasks)

    failures = []
    for (caregiver_code, success, error_message) in results:
        if not success:
            failures.append({
                "Caregiver Code": caregiver_code,
                "Error Message":  error_message,
            })

    return failures


FAILURES_DIR = DOWNLOAD_DIR / "Failures"


def save_failures(failures: list[dict], medical_id: str):
    if not failures:
        return
    today = date.today().strftime("%Y-%m-%d")
    name = MEDICAL_NAMES.get(medical_id, medical_id)
    FAILURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FAILURES_DIR / f"Failures_{name}_{today}.csv"
    pd.DataFrame(failures).to_csv(path, index=False)
    print(f"  Saved {len(failures)} failures -> {path}")


async def upload_all(csv_paths: dict[str, Path]):
    for medical_id, csv_path in csv_paths.items():
        name = MEDICAL_NAMES.get(medical_id, medical_id)
        print(f"\nUploading {name}...")
        failures = await upload_csv(csv_path)
        if failures:
            print(f"  {len(failures)} failures.")
            save_failures(failures, medical_id)
        else:
            print(f"  All records uploaded successfully ✅")


def run(csv_paths: dict[str, Path]):
    asyncio.run(upload_all(csv_paths))


if __name__ == "__main__":
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")
    csv_paths = {
        "75556": DOWNLOAD_DIR / f"annual_health_assessment_{today}.csv",
        "75569": DOWNLOAD_DIR / f"tb_screen_{today}.csv",
    }
    for path in csv_paths.values():
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}")
    run(csv_paths)
    print("\nDone.")