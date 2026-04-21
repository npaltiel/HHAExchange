"""
gimbal_upload.py
----------------
Reads the two CSVs produced by gimbal_transform.py and uploads them
to HHAeXchange using the existing post_medicals functions.
Saves:
  - Separate failure CSVs per medical type in a Failures folder
  - A summary CSV with TRUE/FALSE per caregiver per medical type
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
FAILURES_DIR = DOWNLOAD_DIR / "Failures"

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
    "75556": "Annual Health Assessment",
    "75569": "TB Screen",
}


async def safe_process(caregiver_code, medical_id, date_performed, result_id):
    async with SEMAPHORE:
        return await process_update_only_medical(
            caregiver_code, medical_id, date_performed, result_id
        )


async def upload_csv(csv_path: Path) -> list[dict]:
    """Upload all rows in a CSV. Returns list of result dicts with success flag."""
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["Medical ID"])
    df["Medical ID"] = df["Medical ID"].astype(int).astype(str)

    rows = []
    tasks = []
    for _, row in df.iterrows():
        result_id = result_map.get(row["Result"])
        if not result_id:
            print(f"  Skipping {row['Caregiver Code']}: unknown result '{row['Result']}'")
            continue
        rows.append(row)
        tasks.append(safe_process(
            row["Caregiver Code"],
            row["Medical ID"],
            row["Date Performed"],
            result_id,
        ))

    results = await asyncio.gather(*tasks)

    output = []
    for row, (caregiver_code, success, error_message) in zip(rows, results):
        output.append({
            "Caregiver Code": caregiver_code,
            "Success":        success,
            "Error Message":  error_message if not success else None,
        })

    return output


def save_failures(results: list[dict], medical_id: str):
    failures = [r for r in results if not r["Success"]]
    if not failures:
        return
    today = date.today().strftime("%Y-%m-%d")
    name = MEDICAL_NAMES.get(medical_id, medical_id).replace(" ", "_")
    FAILURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FAILURES_DIR / f"Failures_{name}_{today}.csv"
    pd.DataFrame([{"Caregiver Code": r["Caregiver Code"], "Error Message": r["Error Message"]} for r in failures]).to_csv(path, index=False)
    print(f"  Saved {len(failures)} failures -> {path}")


def save_summary(all_results: dict[str, list[dict]]):
    """Save one summary CSV with a row per caregiver and TRUE/FALSE per medical."""
    today = date.today().strftime("%Y-%m-%d")

    # Build a dict: caregiver_code -> {medical_name: bool}
    summary = {}
    for medical_id, results in all_results.items():
        col_name = MEDICAL_NAMES[medical_id]
        for r in results:
            code = r["Caregiver Code"]
            if code not in summary:
                summary[code] = {}
            summary[code][col_name] = r["Success"]

    rows = []
    for code, medicals in summary.items():
        row = {"Caregiver Code": code}
        for col in MEDICAL_NAMES.values():
            row[col] = medicals.get(col, False)
        rows.append(row)

    df = pd.DataFrame(rows)
    path = DOWNLOAD_DIR / f"Summary_{today}.csv"
    df.to_csv(path, index=False)
    print(f"\n  Summary saved -> {path}")


async def upload_all(csv_paths: dict[str, Path]):
    all_results = {}
    for medical_id, csv_path in csv_paths.items():
        name = MEDICAL_NAMES.get(medical_id, medical_id)
        print(f"\nUploading {name}...")
        results = await upload_csv(csv_path)
        all_results[medical_id] = results

        failures = [r for r in results if not r["Success"]]
        successes = [r for r in results if r["Success"]]
        print(f"  {len(successes)} succeeded, {len(failures)} failed.")
        save_failures(results, medical_id)

    save_summary(all_results)


def run(csv_paths: dict[str, Path]):
    asyncio.run(upload_all(csv_paths))


if __name__ == "__main__":
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