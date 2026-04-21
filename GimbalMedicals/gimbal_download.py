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
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
GIMBAL_EMAIL    = os.environ["GIMBAL_EMAIL"]
GIMBAL_PASSWORD = os.environ["GIMBAL_PASSWORD"]
PROJECT_NAME    = "Annual Medicals And TB Screen 2026"
DOWNLOAD_DIR    = Path(os.environ.get("GIMBAL_DOWNLOAD_DIR", r"C:\Users\nochum.paltiel\Documents\Exchange API Updates"))
POLL_INTERVAL   = 10   # seconds between status checks
POLL_TIMEOUT    = 300  # seconds before giving up (5 minutes)


def download_gimbal_report() -> Path:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # ── Step 1: Login ─────────────────────────────────────────────────────
        print("Logging in to Gimbal...")
        page.goto("https://apps.thegimbal.net/login")
        page.fill("input[type='email'], input[name='email']", GIMBAL_EMAIL)
        page.fill("input[type='password'], input[name='password']", GIMBAL_PASSWORD)
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
        print(f"  Finding project: {PROJECT_NAME}")
        # Find the row containing the project name, then click its eye/view icon
        row = page.locator(f"tr:has-text('{PROJECT_NAME}')").first
        row.locator("a.svg-icon.preview-icon").first.click()
        page.wait_for_selector("text=Search Project Users", timeout=10000)
        print("  Project users page loaded.")

        # ── Step 4: Set Status = Approved ─────────────────────────────────────
        print("  Setting Status filter to Approved...")
        page.select_option("select#status", label="Approved")
        page.click("button:has-text('Search')")
        page.wait_for_selector("text=Total Search Result", timeout=10000)
        print("  Filter applied.")

        # ── Step 5: Click Download → DATA IN EXCEL ────────────────────────────
        print("  Clicking Download → DATA IN EXCEL...")
        page.click("button[title='Download Reports']")
        page.wait_for_selector(".download-options", timeout=5000)
        page.locator("a[onclick*='download-ExcelData']").click(force=True)
        print("  Report generation triggered.")

        # ── Step 6: Catch new tab and poll until Generated ────────────────────
        print("Waiting for report-management tab to open...")
        with context.expect_page() as new_page_info:
            pass
        report_page = new_page_info.value
        report_page.wait_for_url("**/report-management**", timeout=15000)
        report_page.wait_for_selector("table", timeout=10000)
        print("  Report management page loaded.")

        elapsed = 0
        while elapsed < POLL_TIMEOUT:
            # Wait for DataTable to finish processing
            report_page.wait_for_selector("#reportManagementDataTable_processing", state="hidden", timeout=15000)
            report_page.wait_for_selector("#reportManagementDataTable tbody tr", timeout=10000)

            # Debug: print all cell values in first row
            cells = report_page.locator("#reportManagementDataTable tbody tr:first-child td").all()
            for i, cell in enumerate(cells):
                print(f"  Cell {i+1}: {cell.inner_text().strip()}")

            status_text = report_page.locator("#reportManagementDataTable tbody tr:first-child td:nth-child(7)").inner_text()
            if status_text.strip() == "Generated":
                print(f"  Report ready after {elapsed}s.")
                break

            print(f"  Not ready yet ({elapsed}s elapsed), retrying in {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)
            report_page.reload()
            elapsed += POLL_INTERVAL
        else:
            raise TimeoutError(f"Report was not generated within {POLL_TIMEOUT} seconds.")

        # ── Step 7: Download the file ─────────────────────────────────────────
        print("  Downloading file...")
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        with report_page.expect_download() as download_info:
            report_page.locator("tbody tr:first-child").locator("a[title='Download'], button[title='Download'], .fa-download").first.click()

        download = download_info.value
        save_path = DOWNLOAD_DIR / download.suggested_filename
        download.save_as(save_path)
        print(f"  Saved to: {save_path}")

        browser.close()
        return save_path


if __name__ == "__main__":
    path = download_gimbal_report()
    print(f"\nDone. File saved to: {path}")