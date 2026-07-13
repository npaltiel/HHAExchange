"""
gimbal_transform.py
-------------------
Reads the Gimbal .xlsx and produces two CSVs ready for HHAeXchange upload:
  - annual_health_assessment.csv  (Medical ID 75556, Result: Completed)
  - tb_screen.csv                 (Medical ID 75569, Result: Negative)
"""

import os
from datetime import datetime, date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DOWNLOAD_DIR = Path(os.environ.get("GIMBAL_DOWNLOAD_DIR", r"C:\Users\nochum.paltiel\Documents\Gimbal"))

MEDICALS = [
    {"name": "annual_health_assessment", "medical_id": "75556", "result": "Completed"},
    {"name": "tb_screen",                "medical_id": "75569", "result": "Negative"},
]

PA_MEDICALS = [
    {"name": "tb_screen_pa", "pipeline_key": "75569_pa", "medical_id": "75569", "result": "Negative - PA", "caregiver_prefix": "OHZ"},
]


def transform(xlsx_path: Path, medicals: list[dict] = None) -> dict[str, Path]:
    if medicals is None:
        medicals = MEDICALS

    print(f"\nTransforming: {xlsx_path.name}")
    df = pd.read_excel(xlsx_path)
    print(f"  {len(df)} rows loaded.")

    output_paths = {}

    for medical in medicals:
        prefix = medical.get("caregiver_prefix", "ANT")
        records = []
        for _, row in df.iterrows():
            caregiver_code = f"{prefix}-{str(row['User Code']).strip()}"

            submitted_raw = str(row["Submitted Date"]).strip()
            submitted_raw = submitted_raw.replace(" AM", "").replace(" PM", "")
            dt = datetime.strptime(submitted_raw, "%m/%d/%Y %H:%M")
            date_performed = dt.strftime("%Y-%m-%d")

            records.append({
                "Caregiver Code": caregiver_code,
                "Medical ID":     medical["medical_id"],
                "Date Performed": date_performed,
                "Result":         medical["result"],
            })

        out_df = pd.DataFrame(records)
        today = date.today().strftime("%Y-%m-%d")
        csv_path = DOWNLOAD_DIR / f"{medical['name']}_{today}.csv"
        out_df.to_csv(csv_path, index=False)
        print(f"  Saved {len(records)} records -> {csv_path}")
        key = medical.get("pipeline_key", medical["medical_id"])
        output_paths[key] = csv_path

    return output_paths


def transform_pa(xlsx_path: Path) -> dict[str, Path]:
    return transform(xlsx_path, PA_MEDICALS)


if __name__ == "__main__":
    xlsx_files = sorted(DOWNLOAD_DIR.glob("*.xlsx"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not xlsx_files:
        raise FileNotFoundError(f"No .xlsx files found in {DOWNLOAD_DIR}")
    latest = xlsx_files[0]
    print(f"Using: {latest}")
    paths = transform(latest)
    print("\nDone.")
    for medical_id, path in paths.items():
        print(f"  {medical_id} -> {path}")