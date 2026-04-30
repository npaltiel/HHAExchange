"""
gimbal_notify.py
----------------
1. Takes the list of successfully updated caregivers from the upload step
2. Queries SQL Server to find which ones are on hold or scheduled for hold
3. Sends an email via Mailgun with the results as an HTML table
"""

import os
import sys
from pathlib import Path

import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=anchorHealthdb.cw5ezu5fyhr7.us-east-1.rds.amazonaws.com;"
    "DATABASE=AnchorHealthDB;"
    "UID=AnchorHealthUser;"
    f"PWD={os.environ['SOURCE_PASSWORD']};"
    "TrustServerCertificate=yes;"
)

MAILGUN_API_KEY = os.environ["APIKEY_MAILGUN"]
MAILGUN_DOMAIN  = os.environ["MAILGUN_DOMAIN"]
FROM_EMAIL      = "nochum.paltiel@anchorhc.org"
TO_EMAILS       = [os.environ["NOTIFY_EMAIL"]]

QUERY = """
SELECT
     CASE
        WHEN cg.Status = 'Hold' AND css.CurrentStatus = 'Hold'
            THEN 'Current Hold + Future Hold'
        WHEN cg.Status = 'Hold'
            THEN 'Current Hold Only'
        WHEN css.CurrentStatus = 'Hold'
            THEN 'Future Hold Only'
    END AS Category
    ,cg.FirstName + ' ' + cg.LastName AS 'Caregiver Name'
    ,cg.OfficeAideCode AS 'Caregiver Code'
    ,cg.Status AS 'Current Status'
    ,css.CurrentStatus AS 'Scheduled Status'
    ,css.ScheduleDate AS 'Schedule Status Change Date'
    ,r.Reason

FROM dbo.Caregivers cg
OUTER APPLY (
    SELECT TOP 1
         css.CurrentStatus
        ,css.ScheduleDate
        ,css.ReasonID
        ,css.AgencyID
    FROM dbo.CaregiverStatusSchedule css
    WHERE css.AgencyID = cg.AgencyID
      AND css.CaregiverID = cg.CaregiverID
      AND css.ScheduleDate >= CAST(GETDATE() AS DATE)
      AND css.CurrentStatus = 'Hold'
    ORDER BY css.ScheduleDate ASC
) css
LEFT JOIN dbo.Reasons_REPL r
    ON css.AgencyID = r.AgencyID
   AND css.ReasonID = r.ReasonId
WHERE
    cg.officeId = '2365'
    AND cg.OfficeAideCode IN ({placeholders})
    AND (cg.Status = 'Hold' OR css.CurrentStatus = 'Hold')
ORDER BY css.ScheduleDate DESC;
"""


def get_successful_codes(all_results: dict[str, list[dict]]) -> list[str]:
    """Extract caregiver codes that succeeded for at least one medical."""
    codes = set()
    for results in all_results.values():
        for r in results:
            if r["Success"]:
                codes.add(r["Caregiver Code"])
    return list(codes)


def query_holds(codes: list[str]) -> pd.DataFrame:
    """Query SQL Server for caregivers on hold or scheduled for hold."""
    if not codes:
        return pd.DataFrame()

    placeholders = ",".join(["?" for _ in codes])
    sql = QUERY.format(placeholders=placeholders)

    with pyodbc.connect(DB_CONN) as conn:
        df = pd.read_sql(sql, conn, params=codes)

    return df


def build_html_table(df: pd.DataFrame) -> str:
    """Convert DataFrame to a clean HTML table."""
    df = df.where(pd.notna(df), "")
    for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
    styles = """
    <style>
        body { font-family: Arial, sans-serif; }
        table { border-collapse: collapse; width: 100%; }
        th { background-color: #003366; color: white; padding: 8px 12px; text-align: left; }
        td { padding: 7px 12px; border-bottom: 1px solid #ddd; }
        tr:nth-child(even) { background-color: #f2f2f2; }
    </style>
    """
    table_html = df.to_html(index=False, border=0)
    return f"<html><head>{styles}</head><body>{table_html}</body></html>"


def send_email(df: pd.DataFrame):
    """Send email via Mailgun with hold report as HTML table."""
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")

    if df.empty:
        subject = f"[{today}] Medicals Update — No Hold Caregivers"
        html = "<p>All updated caregivers are active. None are on hold or scheduled for hold.</p>"
    else:
        subject = f"[{today}] Medicals Update — {len(df)} Caregiver(s) On/Scheduled for Hold"
        html = f"""
        <p>The following caregivers had their medicals updated today and are currently on hold
        or scheduled to go on hold:</p>
        {build_html_table(df)}
        """

    response = requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        data={
            "from":    FROM_EMAIL,
            "to":      TO_EMAILS,
            "subject": subject,
            "html":    html,
        }
    )

    if response.status_code == 200:
        print(f"  Email sent to {TO_EMAILS}")
    else:
        print(f"  Email failed: {response.status_code} {response.text}")


def run(all_results: dict[str, list[dict]]):
    print("\nChecking for hold caregivers...")
    codes = get_successful_codes(all_results)
    print(f"  {len(codes)} successfully updated caregivers to check.")

    df = query_holds(codes)
    print(f"  {len(df)} caregivers found on hold or scheduled for hold.")

    send_email(df)


if __name__ == "__main__":
    from datetime import date
    today = date.today().strftime("%Y-%m-%d")

    DOWNLOAD_DIR = Path(os.environ.get("GIMBAL_DOWNLOAD_DIR", r"C:\Users\nochum.paltiel\Documents\Gimbal Medicals Automation"))
    summary_path = DOWNLOAD_DIR / f"Summary_{today}.csv"

    if not summary_path.exists():
        raise FileNotFoundError(f"Summary CSV not found: {summary_path}")

    df_summary = pd.read_csv(summary_path)

    all_results = {"75556": [], "75569": []}
    for _, row in df_summary.iterrows():
        all_results["75556"].append({"Caregiver Code": row["Caregiver Code"], "Success": row["Annual Health Assessment"]})
        all_results["75569"].append({"Caregiver Code": row["Caregiver Code"], "Success": row["TB Screen"]})

    run(all_results)