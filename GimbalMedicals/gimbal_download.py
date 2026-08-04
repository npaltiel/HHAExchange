"""
gimbal_medicals_download.py
------------------
Uses Playwright to:
1. Log in to Gimbal
2. Navigate to the Annual Medicals And TB Screen 2026 project
3. Filter by Status = Approved
4. Click DATA IN EXCEL to trigger report generation
5. Poll report-management page until Status = Generated
6. Download the .xlsx file

Requirements:
    pip install playwright
    playwright install chromium
"""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
GIMBAL_EMAIL       = os.environ["GIMBAL_EMAIL"]
GIMBAL_PASSWORD    = os.environ["GIMBAL_PASSWORD"]
GIMBAL_EMAIL_PA    = os.environ["GIMBAL_EMAIL_PA"]
GIMBAL_PASSWORD_PA = os.environ["GIMBAL_PASSWORD_PA"]
PROJECT_NAME       = "Annual Medicals And TB Screen 2026"
PROJECT_NAME_PA    = "TB screen (PA) 2026"
DOWNLOAD_DIR    = Path(os.environ.get("GIMBAL_DOWNLOAD_DIR", r"C:\Users\nochum.paltiel\Documents\Exchange API Updates"))
POLL_INTERVAL   = 10   # seconds between status checks
POLL_TIMEOUT    = 300  # seconds before giving up (5 minutes)
DATE_WINDOW_DAYS = int(os.environ.get("GIMBAL_DATE_WINDOW_DAYS", "60"))  # restrict export to signed dates within the last N days
REPORT_CREATED_BY = os.environ.get("GIMBAL_REPORT_CREATED_BY", "Nochum Paltiel")  # match our own report row, not other users'


def download_gimbal_report(
    email: str = None,
    password: str = None,
    project_name: str = None,
    days_back: int = None,
    created_by: str = None,
) -> Path:
    email        = email        or GIMBAL_EMAIL
    password     = password     or GIMBAL_PASSWORD
    project_name = project_name or PROJECT_NAME
    days_back    = days_back if days_back is not None else DATE_WINDOW_DAYS
    created_by   = created_by   or REPORT_CREATED_BY

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    date_range = f"{start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # ── Step 1: Login ─────────────────────────────────────────────────────
        print("Logging in to Gimbal...")
        page.goto("https://apps.thegimbal.net/login")
        page.fill("input[type='email'], input[name='email']", email)
        page.fill("input[type='password'], input[name='password']", password)
        page.click("button[type='submit']")
        page.wait_for_url("**/home**", timeout=15000)
        print("  Logged in.")

        # ── Step 2: Navigate to Simple Sign V2 → Projects ─────────────────────
        print("Navigating to Simple Sign V2 projects...")
        page.click("text=Simple Signs")
        page.click("text=Simple Sign V2")
        page.wait_for_selector("text=Search Projects", timeout=10000)
        print("  Projects page loaded.")

        # ── Step 3: Click the eye icon for the target project ─────────────────
        print(f"  Finding project: {project_name}")
        # Find the row containing the project name, then click its eye/view icon
        row = page.locator(f"tr:has-text('{project_name}')").first
        row.locator("button.character-icon.blue-bg").first.click()
        page.wait_for_selector("text=Search Project Users", timeout=10000)
        print("  Project users page loaded.")

        # ── Step 4: Set Status = Approved ─────────────────────────────────────
        print("  Setting Status filter to Approved...")
        page.select_option("select#status", label="Approved")

        print(f"  Setting Signed Date range: {date_range}")
        page.locator("#submitted_date").click()          # open this picker instance
        page.evaluate(
            """([start, end]) => {
                const drp = window.jQuery('#submitted_date').data('daterangepicker');
                drp.setStartDate(start);
                drp.setEndDate(end);
                drp.container.find('button.applyBtn').trigger('click');  // apply only this picker
            }""",
            [start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")],
        )

        page.click("button:has-text('Search')")
        page.wait_for_selector("text=Total Search Result", timeout=10000)
        print("  Filter applied.")

        # ── Step 5: Click Download → DATA IN EXCEL ────────────────────────────
        print("  Clicking Download -> DATA IN EXCEL...")
        page.click("button[title='Download Reports']")
        page.wait_for_selector(".download-options", timeout=5000)
        page.locator("a[onclick*='download-ExcelData']").click(force=True)
        print("  Report generation triggered.")

        # ── Step 6: Catch new tab and poll until Generated ────────────────────
        print("Waiting for report-management tab to open...")
        with context.expect_page(timeout=100000) as new_page_info:
            pass
        report_page = new_page_info.value
        report_page.wait_for_url("**/report-management**", timeout=30000)
        report_page.wait_for_selector("table", timeout=20000)
        print("  Report management page loaded.")

        # Find the first row created by us (Created By = column 8), so we don't
        # grab another user's concurrent report sitting at the top of the table.
        def find_our_row():
            rows = report_page.locator("#reportManagementDataTable tbody tr")
            for i in range(rows.count()):
                r = rows.nth(i)
                if r.locator("td:nth-child(8)").inner_text().strip() == created_by:
                    return r
            return None

        elapsed = 0
        while elapsed < POLL_TIMEOUT:
            # Wait for DataTable to finish processing
            report_page.wait_for_selector("#reportManagementDataTable_processing", state="hidden", timeout=15000)
            report_page.wait_for_selector("#reportManagementDataTable tbody tr", timeout=10000)

            our_row = find_our_row()
            if our_row is not None:
                status_text = our_row.locator("td:nth-child(7)").inner_text().strip()
                print(f"  Our report (by {created_by}) status: {status_text}")
                if status_text == "Generated":
                    print(f"  Report ready after {elapsed}s.")
                    break
            else:
                print(f"  No report by {created_by} in the list yet.")

            print(f"  Not ready yet ({elapsed}s elapsed), retrying in {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)
            report_page.reload()
            elapsed += POLL_INTERVAL
        else:
            raise TimeoutError(f"Report by {created_by} was not generated within {POLL_TIMEOUT} seconds.")

        # ── Step 7: Download the file ─────────────────────────────────────────
        print("  Downloading file...")
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        our_row = find_our_row()
        if our_row is None:
            raise RuntimeError(f"Could not find a report by {created_by} to download.")

        with report_page.expect_download() as download_info:
            our_row.locator("a[title='Download'], button[title='Download'], .fa-download").first.click()

        download = download_info.value
        save_path = DOWNLOAD_DIR / download.suggested_filename
        download.save_as(save_path)
        print(f"  Saved to: {save_path}")

        browser.close()
        return save_path


if __name__ == "__main__":
    path = download_gimbal_report()
    print(f"\nDone. File saved to: {path}")